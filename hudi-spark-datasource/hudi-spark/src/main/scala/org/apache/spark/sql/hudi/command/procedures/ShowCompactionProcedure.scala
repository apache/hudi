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
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.util.CompactionUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.util.function.Supplier

import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Spark SQL procedure to show all compaction operations for a MoR Hudi table.
 *
 * This procedure provides a comprehensive view of Hudi compaction operations, displaying both
 * completed compactions with execution metadata and pending compaction plans.
 * For pending operations, execution-specific fields are gracefully set to null.
 *
 * == Parameters ==
 * - `table`: Required. The name of the Hudi table to query
 * - `path`: Optional. The path of the Hudi table (any one of the `table` or `path` must be provided)
 * - `limit`: Optional. Maximum number of compaction operations to return (default: 20)
 * - `showArchived`: Optional. Whether to include archived compaction operations (default: false)
 * - `filter`: Optional. SQL expression to filter results (default: empty string)
 * - `startTime`: Optional. Start time for compactions (format: yyyyMMddHHmmss, default: empty)
 * - `endTime`: Optional. End time for compactions (format: yyyyMMddHHmmss, default: empty)
 *
 * == Output Schema ==
 * - `compaction_time`: Timestamp when the compaction operation was initiated
 * - `state_transition_time`: Time when the compaction transitioned to completed state (null for pending)
 * - `state`: Operation state (COMPLETED, INFLIGHT, REQUESTED)
 * - `action`: The action type (compaction)
 * - `operation_size`: Number of operations in the compaction plan
 * - `partition_path`: Partition path for the compaction operation
 * - `total_log_files_per_partition`: Total number of log records processed in the partition
 * - `total_updated_records_compacted_per_partition`: Total number of updated records compacted in the partition
 * - `total_log_size_compacted_per_partition`: Total size of log files compacted in the partition
 * - `total_write_bytes_per_partition`: Total bytes written during compaction for the partition
 *
 * == Error Handling ==
 * - Throws `IllegalArgumentException` for invalid filter expressions or non-MoR tables
 * - Returns empty result set if no compaction plans match the criteria
 *
 * == Filter Support ==
 * The `filter` parameter supports SQL expressions for filtering results.
 *
 * === Common Filter Examples ===
 * {{{
 * -- Show only completed compactions
 * CALL show_compaction(
 *   table => 'my_table',
 *   filter => "state = 'COMPLETED'"
 * )
 *
 * -- Show large compactions (more than 10 operations)
 * CALL show_compaction(
 *   table => 'my_table',
 *   filter => "operation_size > 10"
 * )
 *
 * -- Show recent compactions within last hour
 * CALL show_compaction(
 *   table => 'my_table',
 *   filter => "compaction_time > '20240101120000000'"
 * )
 *
 * -- Show pending compactions
 * CALL show_compaction(
 *   table => 'my_table',
 *   filter => "state IN ('INFLIGHT', 'REQUESTED')"
 * )
 * }}}
 *
 */
class ShowCompactionProcedure extends BaseProcedure with ProcedureBuilder with SparkAdapterSupport with Logging {
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
    StructField("compaction_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("state_transition_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("state", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("action", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("operation_size", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("partition_path", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("total_log_files_per_partition", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_updated_records_compacted_per_partition", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_log_size_compacted_per_partition", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_write_bytes_per_partition", DataTypes.LongType, nullable = true, Metadata.empty)
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

    if (filter != null && filter.trim.nonEmpty) {
      HoodieProcedureFilterUtils.validateFilterExpression(filter, OUTPUT_TYPE, sparkSession) match {
        case Left(errorMessage) =>
          throw new IllegalArgumentException(s"Invalid filter expression: $errorMessage")
        case Right(_) => // Validation passed, continue
      }
    }
    val basePath: String = getBasePath(tableName, tablePath)
    val metaClient = createMetaClient(jsc, basePath)

    if (metaClient.getTableType != HoodieTableType.MERGE_ON_READ) {
      throw new IllegalArgumentException("Cannot show compaction on a Non Merge On Read table.")
    }

    val activeResults = getCombinedCompactionsWithPartitionMetadata(metaClient.getActiveTimeline, limit, metaClient, startTime, endTime)
    val finalResults = if (showArchived) {
      val archivedResults = getCombinedCompactionsWithPartitionMetadata(metaClient.getArchivedTimeline, limit, metaClient, startTime, endTime)
      (activeResults ++ archivedResults)
        .sortWith((a, b) => a.getString(0) > b.getString(0))
        .take(limit)
    } else {
      activeResults.take(limit)
    }
    if (filter != null && filter.trim.nonEmpty) {
      HoodieProcedureFilterUtils.evaluateFilter(finalResults, filter, OUTPUT_TYPE, sparkSession)
    } else {
      finalResults
    }
  }

  private def getCombinedCompactionsWithPartitionMetadata(timeline: HoodieTimeline,
                                                          limit: Int,
                                                          metaClient: HoodieTableMetaClient,
                                                          startTime: String,
                                                          endTime: String): Seq[Row] = {
    import scala.collection.mutable.ListBuffer

    val allRows = ListBuffer[Row]()

    val filteredCompactionInstants = timeline.getInstants.iterator().asScala
      .filter(p => p.getAction == HoodieTimeline.COMPACTION_ACTION || p.getAction == HoodieTimeline.COMMIT_ACTION)
      .filter { instant =>
        val instantTime = instant.requestedTime()
        val withinStartTime = if (startTime.nonEmpty) instantTime >= startTime else true
        val withinEndTime = if (endTime.nonEmpty) instantTime <= endTime else true
        withinStartTime && withinEndTime
      }
      .toSeq
    val allCompactionInstants = if (startTime.nonEmpty && endTime.nonEmpty) {
      filteredCompactionInstants.sortBy(_.requestedTime).reverse
    } else {
      filteredCompactionInstants.sortBy(_.requestedTime).reverse.take(limit)
    }

    allCompactionInstants.foreach { compactionInstant =>
      if (compactionInstant.getState == HoodieInstant.State.COMPLETED) {
        Try {
          val compactionMetadata = timeline.readCommitMetadata(compactionInstant)
          compactionMetadata.getPartitionToWriteStats.entrySet.asScala.foreach { partitionEntry =>
            val partitionPath = partitionEntry.getKey
            val writeStats = partitionEntry.getValue.asScala
            val operationSize = Try {
              val compactionPlan = CompactionUtils.getCompactionPlan(metaClient, compactionInstant.requestedTime)
              compactionPlan.getOperations.size()
            }.getOrElse(null)

            writeStats.foreach { writeStat =>
              val row = Row(
                compactionInstant.requestedTime(),
                compactionInstant.getCompletionTime,
                compactionInstant.getState.name(),
                compactionInstant.getAction,
                operationSize,
                partitionPath,
                writeStat.getTotalLogFilesCompacted,
                writeStat.getTotalUpdatedRecordsCompacted,
                writeStat.getTotalLogSizeCompacted,
                writeStat.getTotalWriteBytes
              )
              allRows += row
            }
          }
        }.recover {
          case e: Exception =>
            log.warn(s"Failed to read compaction metadata for instant ${compactionInstant.requestedTime}: ${e.getMessage}")
            val row = createErrorRowForCompletedWithPartition(compactionInstant)
            allRows += row
        }
      } else {
        Try {
          val compactionPlan = CompactionUtils.getCompactionPlan(metaClient, compactionInstant.requestedTime)
          val stats = extractCompactionPlanStats(compactionPlan)
          val operations = compactionPlan.getOperations.asScala
          stats.involvedPartitions.foreach { partitionPath =>
            val partitionOps = operations.filter(_.getPartitionPath == partitionPath)
            val partitionDeltaFiles = partitionOps.map(_.getDeltaFilePaths.size()).sum
            val row = Row(
              compactionInstant.requestedTime(),
              null,
              compactionInstant.getState.name(),
              compactionInstant.getAction,
              partitionOps.size,
              partitionPath,
              partitionDeltaFiles.toLong,
              0L,
              0L,
              0L
            )
            allRows += row
          }
        }.recover {
          case e: Exception =>
            log.warn(s"Failed to read compaction plan for instant ${compactionInstant.requestedTime}: ${e.getMessage}")
            val row = createErrorRowForPendingWithPartition(compactionInstant)
            allRows += row
        }
      }
    }
    allRows.toSeq
  }

  private def extractCompactionPlanStats(compactionPlan: org.apache.hudi.avro.model.HoodieCompactionPlan): CompactionPlanStatistics = {
    val operations = Option(compactionPlan.getOperations)
      .map(_.asScala.toSeq)
      .getOrElse(Seq.empty)

    val involvedPartitions = operations.map(_.getPartitionPath).toSet
    val extraMetadata = Option(compactionPlan.getExtraMetadata)
      .filter(!_.isEmpty)
      .map(_.asScala.map { case (k, v) => s"$k=$v" }.mkString(", "))
      .orNull

    CompactionPlanStatistics(
      involvedPartitions = involvedPartitions,
      extraMetadata = extraMetadata
    )
  }

  private case class CompactionPlanStatistics(involvedPartitions: Set[String],
                                              extraMetadata: String
                                             )

  private def createErrorRowForCompletedWithPartition(instant: HoodieInstant): Row = {
    Row(
      instant.requestedTime(), instant.getCompletionTime, instant.getState.name(), instant.getAction,
      null, null, null, null, null, null
    )
  }

  private def createErrorRowForPendingWithPartition(instant: HoodieInstant): Row = {
    Row(
      instant.requestedTime(), null, instant.getState.name(), instant.getAction,
      null, null, null, null, null, null
    )
  }

  override def build: Procedure = new ShowCompactionProcedure()
}

object ShowCompactionProcedure {
  val NAME = "show_compaction"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowCompactionProcedure
  }
}
