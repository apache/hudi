/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.command.procedures

import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.util.ClusteringUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.util.function.Supplier

import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Spark SQL procedure to show all clustering operations for a Hudi table.
 *
 * This procedure provides a comprehensive view of Hudi clustering operations, displaying both
 * completed clusterings with execution metadata and pending clustering plans.
 * For pending operations, execution-specific fields are gracefully set to null.
 *
 * == Parameters ==
 * - `table`: Required. The name of the Hudi table to query
 * - `path`: Optional. The path of the Hudi table (alternative to table name)
 * - `limit`: Optional. Maximum number of clustering operations to return (default: 20)
 * - `showArchived`: Optional. Whether to include archived clustering operations (default: false)
 * - `filter`: Optional. SQL expression to filter results (default: empty string)
 * - `startTime`: Optional. Start time for clustering (format: yyyyMMddHHmmss, default: empty)
 * - `endTime`: Optional. End time for clustering (format: yyyyMMddHHmmss, default: empty)
 *
 * == Output Schema ==
 * - `clustering_time`: Timestamp when the clustering operation was initiated
 * - `state_transition_time`: Time when the clustering transitioned to completed state (null for pending)
 * - `state`: Operation state (COMPLETED, INFLIGHT, REQUESTED)
 * - `action`: The action type (replacecommit/clustering)
 * - `input_group_size`: Number of input groups in the clustering plan
 * - `partition_path`: Partition path for the clustering operation
 * - `input_files_count_per_partition`: Number of input files being clustered in the partition
 * - `output_files_count_per_partition`: Number of output files created in the partition after clustering
 * - `output_size_bytes_per_partition`: Total size of output files in bytes for the partition
 *
 * == Error Handling ==
 * - Throws `IllegalArgumentException` for invalid filter expressions
 * - Returns empty result set if no clustering plans match the criteria
 *
 * == Filter Support ==
 * The `filter` parameter supports SQL expressions for filtering results.
 *
 * === Common Filter Examples ===
 * {{{
 * -- Show only completed clusterings
 * CALL show_clustering(
 *   table => 'my_table',
 *   filter => "state = 'COMPLETED'"
 * )
 *
 * -- Show large clusterings (more than 5 input groups)
 * CALL show_clustering(
 *   table => 'my_table',
 *   filter => "input_group_size > 5"
 * )
 *
 * -- Show clusterings for specific partition
 * CALL show_clustering(
 *   table => 'my_table',
 *   filter => "involved_partitions LIKE '%part1%'"
 * )
 *
 * -- Show pending clusterings
 * CALL show_clustering(
 *   table => 'my_table',
 *   filter => "state IN ('INFLIGHT', 'REQUESTED')"
 * )
 * }}}
 *
 * == Migration from Legacy Procedures ==
 * This procedure replaces separate clustering viewing procedures and provides a unified view.
 * Use filters to replicate legacy functionality:
 * - For plan-only view: `filter => "state != 'COMPLETED'"`
 * - For completed-only view: `filter => "state = 'COMPLETED'"`
 * - For partition view: set appropriate filter on `partition_path` or `involved_partitions`
 */
class ShowClusteringProcedure extends BaseProcedure with ProcedureBuilder with SparkAdapterSupport with Logging {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "path", DataTypes.StringType),
    ProcedureParameter.optional(2, "limit", DataTypes.IntegerType, 20),
    ProcedureParameter.optional(3, "showArchived", DataTypes.BooleanType, false),
    ProcedureParameter.optional(4, "filter", DataTypes.StringType, ""),
    ProcedureParameter.optional(5, "startTime", DataTypes.StringType, ""),
    ProcedureParameter.optional(6, "endTime", DataTypes.StringType, "")
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("clustering_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("state_transition_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("state", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("action", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("timeline_type", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("input_group_size", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("partition_path", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("input_files_count_per_partition", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("output_files_count_per_partition", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("output_size_bytes_per_partition", DataTypes.LongType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getArgValueOrDefault(args, PARAMETERS(1))
    val limit = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[Int]
    val showArchived = getArgValueOrDefault(args, PARAMETERS(3)).get.asInstanceOf[Boolean]
    val filter = getArgValueOrDefault(args, PARAMETERS(4)).get.asInstanceOf[String]
    val startTime = getArgValueOrDefault(args, PARAMETERS(5)).get.asInstanceOf[String]
    val endTime = getArgValueOrDefault(args, PARAMETERS(6)).get.asInstanceOf[String]

    validateFilter(filter, outputType)
    val basePath: String = getBasePath(tableName, tablePath)
    val metaClient = createMetaClient(jsc, basePath)

    val activeResults = getCombinedClusteringsWithPartitionMetadata(metaClient.getActiveTimeline, limit, metaClient, startTime, endTime, "ACTIVE")
    val finalResults = if (showArchived) {
      val archivedResults = getCombinedClusteringsWithPartitionMetadata(metaClient.getArchivedTimeline, limit, metaClient, startTime, endTime, "ARCHIVED")
      val combinedResults = (activeResults ++ archivedResults)
        .sortWith((a, b) => a.getString(0) > b.getString(0))
      if (startTime.trim.nonEmpty && endTime.trim.nonEmpty) {
        combinedResults
      } else {
        combinedResults.take(limit)
      }
    } else {
      if (startTime.trim.nonEmpty && endTime.trim.nonEmpty) {
        activeResults
      } else {
        activeResults.take(limit)
      }
    }

    applyFilter(finalResults, filter, outputType)
  }

  private def getCombinedClusteringsWithPartitionMetadata(timeline: HoodieTimeline,
                                                          limit: Int,
                                                          metaClient: HoodieTableMetaClient,
                                                          startTime: String,
                                                          endTime: String,
                                                          timelineType: String): Seq[Row] = {
    val allRows = scala.collection.mutable.ListBuffer[Row]()

    val filteredClusteringInstants = timeline.getInstants.iterator().asScala
      .filter(p => ClusteringUtils.isClusteringOrReplaceCommitAction(p.getAction))
      .filter { instant =>
        val instantTime = instant.requestedTime()
        val withinStartTime = if (startTime.nonEmpty) instantTime >= startTime else true
        val withinEndTime = if (endTime.nonEmpty) instantTime <= endTime else true
        withinStartTime && withinEndTime
      }
      .toSeq
    val allClusteringInstants = if (startTime.nonEmpty && endTime.nonEmpty) {
      filteredClusteringInstants.sortBy(_.requestedTime).reverse
    } else {
      filteredClusteringInstants.sortBy(_.requestedTime).reverse.take(limit)
    }

    allClusteringInstants.foreach { clusteringInstant =>
      if (clusteringInstant.getState == HoodieInstant.State.COMPLETED) {
        scala.util.Try {
          val commitMetadata = timeline.readCommitMetadata(clusteringInstant)
          val planStats = Try {
            val clusteringPlanOpt = ClusteringUtils.getClusteringPlan(metaClient, clusteringInstant)
            if (clusteringPlanOpt.isPresent) {
              Some(extractClusteringPlanStats(clusteringPlanOpt.get().getRight))
            } else None
          }.getOrElse(None)

          commitMetadata.getPartitionToWriteStats.entrySet.asScala.foreach { partitionEntry =>
            val partitionPath = partitionEntry.getKey
            val writeStats = partitionEntry.getValue.asScala

            val totalInputFiles = planStats.flatMap(s => s.partitionStats.get(partitionPath).map(_.inputFiles))
              .getOrElse(0)

            val totalOutputFiles = writeStats.size
            val totalOutputSizeBytes = writeStats.map(_.getFileSizeInBytes).sum
            val inputGroupSize = planStats.map(_.totalInputGroups).getOrElse(writeStats.size)

            val row = Row(
              clusteringInstant.requestedTime,
              clusteringInstant.getCompletionTime,
              clusteringInstant.getState.name,
              clusteringInstant.getAction,
              timelineType,
              inputGroupSize,
              partitionPath,
              totalInputFiles,
              totalOutputFiles,
              totalOutputSizeBytes
            )
            allRows += row
          }
          if (commitMetadata.getPartitionToWriteStats.isEmpty) {
            val totalInputFiles = planStats.map(_.totalInputFiles).map(Integer.valueOf).orNull
            val totalOutputFiles = commitMetadata.fetchTotalFilesInsert + commitMetadata.fetchTotalFilesUpdated
            val totalOutputSizeBytes = commitMetadata.fetchTotalBytesWritten
            val inputGroupSize = planStats.map(_.totalInputGroups).map(Integer.valueOf).orNull

            val row = Row(
              clusteringInstant.requestedTime,
              clusteringInstant.getCompletionTime,
              clusteringInstant.getState.name,
              clusteringInstant.getAction,
              timelineType,
              inputGroupSize,
              null,
              totalInputFiles,
              totalOutputFiles.toInt,
              totalOutputSizeBytes
            )
            allRows += row
          }
        }.recover {
          case e: Exception =>
            log.warn(s"Failed to read clustering metadata for instant ${clusteringInstant.requestedTime}: ${e.getMessage}")
            val row = createErrorRowForCompletedWithPartition(clusteringInstant, timelineType)
            allRows += row
        }
      } else {
        scala.util.Try {
          val clusteringPlanOpt = ClusteringUtils.getClusteringPlan(metaClient, clusteringInstant)
          if (clusteringPlanOpt.isPresent) {
            val clusteringPlan = clusteringPlanOpt.get()
            val plan = clusteringPlan.getRight
            val stats = extractClusteringPlanStats(plan)
            stats.involvedPartitions.foreach { partitionPath =>
              val partitionStat = stats.partitionStats.get(partitionPath)
              val row = Row(
                clusteringInstant.requestedTime,
                null, // state_transition_time - not available for pending
                clusteringInstant.getState.name,
                clusteringInstant.getAction,
                timelineType,
                stats.totalInputGroups, // input_group_size - total across all partitions
                partitionPath,
                partitionStat.map(_.inputFiles).getOrElse(0), // input_files_count - per partition
                partitionStat.map(_.outputGroups).getOrElse(0), // output_files_count - per partition
                0L // output_size_bytes - not available for pending operations
              )
              allRows += row
            }
          } else {
            // for inflight requests without a clustering plan
            val row = createErrorRowForPendingWithPartition(clusteringInstant, timelineType)
            allRows += row
          }
        }.recover {
          case e: Exception =>
            log.warn(s"Failed to read clustering plan for instant ${clusteringInstant.requestedTime}: ${e.getMessage}")
            val row = createErrorRowForPendingWithPartition(clusteringInstant, timelineType)
            allRows += row
        }
      }
    }
    allRows.toSeq
  }

  private def extractClusteringPlanStats(clusteringPlan: org.apache.hudi.avro.model.HoodieClusteringPlan): ClusteringPlanStatistics = {
    val inputGroups = Option(clusteringPlan.getInputGroups)
      .map(_.asScala.toSeq)
      .getOrElse(Seq.empty)

    val totalInputGroups = inputGroups.size
    val totalInputFiles = inputGroups.map(_.getSlices.size()).sum
    val totalOutputGroups = inputGroups.map(_.getNumOutputFileGroups.toInt).sum

    val involvedPartitions = inputGroups.flatMap(group =>
      Option(group.getSlices).map(_.asScala.map(_.getPartitionPath).toSet).getOrElse(Set.empty)
    ).toSet

    val partitionStats = involvedPartitions.map { partitionPath =>
      val partitionGroups = inputGroups.filter { group =>
        Option(group.getSlices).exists(_.asScala.exists(_.getPartitionPath == partitionPath))
      }

      val partitionInputFiles = partitionGroups.map(_.getSlices.size()).sum
      val partitionOutputGroups = partitionGroups.map(_.getNumOutputFileGroups.toInt).sum

      partitionPath -> ClusteringPartitionStats(
        inputFiles = partitionInputFiles,
        outputGroups = partitionOutputGroups
      )
    }.toMap

    ClusteringPlanStatistics(
      totalInputGroups = totalInputGroups,
      totalInputFiles = totalInputFiles,
      totalOutputGroups = totalOutputGroups,
      involvedPartitions = involvedPartitions,
      partitionStats = partitionStats
    )
  }

  private case class ClusteringPlanStatistics(totalInputGroups: Int,
                                              totalInputFiles: Int,
                                              totalOutputGroups: Int,
                                              involvedPartitions: Set[String],
                                              partitionStats: Map[String, ClusteringPartitionStats]
                                             )

  private case class ClusteringPartitionStats(inputFiles: Int,
                                              outputGroups: Int
                                             )

  private def createErrorRowForCompletedWithPartition(instant: HoodieInstant, timelineType: String): Row = {
    Row(
      instant.requestedTime(), instant.getCompletionTime, instant.getState.name(), instant.getAction,
      timelineType, null, null, null, null, null
    )
  }

  private def createErrorRowForPendingWithPartition(instant: HoodieInstant, timelineType: String): Row = {
    Row(
      instant.requestedTime(), null, instant.getState.name(), instant.getAction,
      timelineType, null, null, null, null, null
    )
  }

  override def build: Procedure = new ShowClusteringProcedure()
}

object ShowClusteringProcedure {
  val NAME = "show_clustering"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowClusteringProcedure
  }
}
