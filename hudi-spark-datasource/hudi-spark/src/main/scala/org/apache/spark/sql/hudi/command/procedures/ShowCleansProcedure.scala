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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier

import scala.collection.JavaConverters._

/**
 * Spark SQL procedure to show all clean operations for a Hudi table.
 *
 * This procedure provides a comprehensive view of Hudi clean operations.
 * It displays completed clean operations with full partition metadata for both completed and pending operations.
 *
 * == Parameters ==
 * - `table`: Required. The name of the Hudi table to query
 * - `path`: Optional. The path of the Hudi table (anyone of `table` or `path` must be provided)
 * - `limit`: Optional. Maximum number of clean operations to return (default: 10, ignored if time range specified)
 * - `showArchived`: Optional. Whether to include archived clean operations (default: false)
 * - `filter`: Optional. SQL expression to filter results (default: empty string)
 * - `startTime`: Optional. Start time for clean operations (format: yyyyMMddHHmmss, default: empty)
 * - `endTime`: Optional. End time for clean operations (format: yyyyMMddHHmmss, default: empty)
 *
 * == Output Schema ==
 * - `clean_time`: Timestamp when the clean operation was performed
 * - `state_transition_time`: Time when the clean transitioned to completed state
 * - `state`: Operation state (COMPLETED, INFLIGHT, REQUESTED)
 * - `action`: The action type (always 'clean')
 * - `start_clean_time`: When the clean operation started
 * - `partition_path`: Partition path for the clean operation
 * - `policy`: Clean policy used (KEEP_LATEST_COMMITS, etc.)
 * - `delete_path_patterns`: Number of delete path patterns
 * - `success_delete_files`: Number of successfully deleted files
 * - `failed_delete_files`: Number of files that failed to delete
 * - `is_partition_deleted`: Whether the entire partition was deleted
 * - `time_taken_in_millis`: Duration of the clean operation in milliseconds
 * - `total_files_deleted`: Total number of files deleted during the clean
 * - `earliest_commit_to_retain`: The earliest commit that was retained
 * - `last_completed_commit_timestamp`: The last completed commit at clean time
 * - `version`: Version of the clean operation metadata
 * - `total_partitions_to_clean`: Total partitions to clean (for pending operations)
 * - `total_partitions_to_delete`: Total partitions to delete (for pending operations)
 *
 * == Data Availability by Operation State ==
 * - **COMPLETED operations**: All execution and partition metadata fields are populated
 * - **PENDING operations**: Plan fields are populated, execution fields are null (graceful handling)
 *
 * == Filter Support ==
 * The `filter` parameter supports SQL expressions for filtering results.
 *
 * === Common Filter Examples ===
 * {{{
 * -- Show only completed operations (equivalent to old show_cleans)
 * CALL show_cleans(
 *   table => 'my_table',
 *   filter => "state = 'COMPLETED'"
 * )
 *
 * -- Show only pending operations (equivalent to old show_clean_plans)
 * CALL show_cleans(
 *   table => 'my_table',
 *   filter => "state IN ('REQUESTED', 'INFLIGHT')"
 * )
 *
 * -- Show operations with partition metadata (equivalent to old show_cleans_metadata)
 * CALL show_cleans(
 *   table => 'my_table',
 *   filter => "partition_path IS NOT NULL"
 * )
 *
 * -- Show cleans that deleted many files
 * CALL show_cleans(
 *   table => 'my_table',
 *   filter => "total_files_deleted > 100"
 * )
 *
 * -- Show recent clean operations
 * CALL show_cleans(
 *   table => 'my_table',
 *   filter => "clean_time > '20231201000000'"
 * )
 *
 * -- Show slow clean operations
 * CALL show_cleans(
 *   table => 'my_table',
 *   filter => "time_taken_in_millis > 60000"
 * )
 * }}}
 *
 * == Some Usage Examples ==
 * {{{
 * -- Basic usage: Show last 10 completed cleans
 * CALL show_cleans(table => 'hudi_table_1')
 *
 * -- Include archived clean operations
 * CALL show_cleans(table => 'hudi_table_1', showArchived => true)
 *
 * -- Filter for recent efficient cleans
 * CALL show_cleans(
 *   table => 'hudi_table_1',
 *   filter => "clean_time > '20231201000000' AND total_files_deleted > 0"
 * )
 *
 * -- Show partition-level clean analysis
 * CALL show_cleans(
 *   table => 'hudi_table_1',
 *   filter => "partition_path LIKE 'year=2023%' AND failed_delete_files = 0"
 * )
 *
 * -- Show cleans within a specific time range
 * CALL show_cleans(
 *   table => 'hudi_table_1',
 *   startTime => '20231201000000',
 *   endTime => '20231205235959'
 * )
 *
 * -- Show recent cleans (time range overrides limit)
 * CALL show_cleans(
 *   table => 'hudi_table_1',
 *   startTime => '20231204000000'
 * )
 * }}}
 *
 */
class ShowCleansProcedure extends BaseProcedure with ProcedureBuilder with SparkAdapterSupport with Logging {

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "path", DataTypes.StringType),
    ProcedureParameter.optional(2, "limit", DataTypes.IntegerType, 10),
    ProcedureParameter.optional(3, "showArchived", DataTypes.BooleanType, false),
    ProcedureParameter.optional(4, "filter", DataTypes.StringType, ""),
    ProcedureParameter.optional(5, "startTime", DataTypes.StringType, ""),
    ProcedureParameter.optional(6, "endTime", DataTypes.StringType, "")
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("clean_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("state_transition_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("state", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("action", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("start_clean_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("partition_path", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("policy", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("delete_path_patterns", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("success_delete_files", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("failed_delete_files", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("is_partition_deleted", DataTypes.BooleanType, nullable = true, Metadata.empty),
    StructField("time_taken_in_millis", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_files_deleted", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("earliest_commit_to_retain", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("last_completed_commit_timestamp", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("version", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("total_partitions_to_clean", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("total_partitions_to_delete", DataTypes.IntegerType, nullable = true, Metadata.empty)
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

    val basePath = getBasePath(tableName, tablePath)
    val metaClient = createMetaClient(jsc, basePath)

    val activeResults = getCombinedCleansWithPartitionMetadata(metaClient.getActiveTimeline, limit, metaClient, startTime, endTime)
    val finalResults = if (showArchived) {
      val archivedResults = getCombinedCleansWithPartitionMetadata(metaClient.getArchivedTimeline, limit, metaClient, startTime, endTime)
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

  override def build: Procedure = new ShowCleansProcedure()

  private def getCombinedCleansWithPartitionMetadata(timeline: HoodieTimeline, limit: Int, metaClient: HoodieTableMetaClient, startTime: String, endTime: String): Seq[Row] = {
    import scala.collection.JavaConverters._
    import scala.util.{Failure, Success, Try}

    val filteredCleanInstants = timeline.getCleanerTimeline.getInstants.asScala.toSeq
      .filter { instant =>
        val instantTime = instant.requestedTime()
        val withinStartTime = if (startTime.nonEmpty) instantTime >= startTime else true
        val withinEndTime = if (endTime.nonEmpty) instantTime <= endTime else true
        withinStartTime && withinEndTime
      }
    val allCleanInstants = if (startTime.nonEmpty && endTime.nonEmpty) {
      filteredCleanInstants.sortWith((a, b) => a.requestedTime() > b.requestedTime())
    } else {
      filteredCleanInstants.sortWith((a, b) => a.requestedTime() > b.requestedTime()).take(limit)
    }

    val allRows = scala.collection.mutable.ListBuffer[Row]()

    allCleanInstants.foreach { cleanInstant =>
      if (cleanInstant.getState == HoodieInstant.State.COMPLETED) {
        Try {
          val cleanMetadata = timeline.readCleanMetadata(cleanInstant)

          cleanMetadata.getPartitionMetadata.entrySet.asScala.foreach { partitionMetadataEntry =>
            val partitionPath = partitionMetadataEntry.getKey
            val partitionMetadata = partitionMetadataEntry.getValue

            val row = Row(
              cleanInstant.requestedTime(),
              cleanInstant.getCompletionTime,
              cleanInstant.getState.name(),
              cleanInstant.getAction,
              cleanMetadata.getStartCleanTime,
              partitionPath,
              partitionMetadata.getPolicy,
              partitionMetadata.getDeletePathPatterns.size(),
              partitionMetadata.getSuccessDeleteFiles.size(),
              partitionMetadata.getFailedDeleteFiles.size(),
              partitionMetadata.getIsPartitionDeleted,
              cleanMetadata.getTimeTakenInMillis,
              cleanMetadata.getTotalFilesDeleted,
              cleanMetadata.getEarliestCommitToRetain,
              cleanMetadata.getLastCompletedCommitTimestamp,
              cleanMetadata.getVersion,
              null,
              null
            )
            allRows += row
          }
        } match {
          case Success(_) => // Successfully processed
          case Failure(exception) =>
            logWarning(s"Failed to read clean metadata for instant ${cleanInstant.requestedTime()}", exception)
            allRows += createErrorRowForCompletedWithPartition(cleanInstant)
        }
      } else {
        Try {
          val requestedCleanInstant = metaClient.getInstantGenerator.createNewInstant(
            HoodieInstant.State.REQUESTED,
            cleanInstant.getAction,
            cleanInstant.requestedTime()
          )
          val cleanerPlan = timeline.readCleanerPlan(requestedCleanInstant)

          val planStats = extractCleanPlanStats(cleanerPlan)

          planStats.involvedPartitions.foreach { partitionPath =>
            val partitionFilesToDelete = Option(cleanerPlan.getFilePathsToBeDeletedPerPartition)
              .flatMap(_.asScala.get(partitionPath))
              .map(_.size())
              .getOrElse(0)

            val row = Row(
              cleanInstant.requestedTime(),
              null,
              cleanInstant.getState.name(),
              cleanInstant.getAction,
              null, // start_clean_time - not available in pending
              partitionPath,
              cleanerPlan.getPolicy,
              partitionFilesToDelete, // delete_path_patterns - files to be deleted
              null, // success_delete_files - not available in pending
              null, // failed_delete_files - not available in pending
              null, // is_partition_deleted - not available in pending
              null, // time_taken_in_millis - not available in pending
              null, // total_files_deleted - not available in pending
              planStats.earliestInstantToRetain,
              cleanerPlan.getLastCompletedCommitTimestamp,
              cleanerPlan.getVersion,
              planStats.totalPartitionsToClean,
              planStats.totalPartitionsToDelete
            )
            allRows += row
          }
        } match {
          case Success(_) => // Successfully processed
          case Failure(exception) =>
            logWarning(s"Failed to read clean plan for instant ${cleanInstant.requestedTime()}", exception)
            allRows += createErrorRowForPendingWithPartition(cleanInstant)
        }
      }
    }

    allRows.take(limit).toSeq
  }

  private def createErrorRowForCompletedWithPartition(cleanInstant: HoodieInstant): Row = {
    Row(
      cleanInstant.requestedTime(),
      cleanInstant.getCompletionTime,
      cleanInstant.getState.name(),
      cleanInstant.getAction,
      null, null, null, null, null, null, null, null, null, null, null, null, null, null, null
    )
  }

  private def createErrorRowForPendingWithPartition(cleanInstant: HoodieInstant): Row = {
    Row(
      cleanInstant.requestedTime(),
      null,
      cleanInstant.getState.name(),
      cleanInstant.getAction,
      null, null, null, null, null, null, null, null, null, null, null, null, null, null, null
    )
  }

  private def extractCleanPlanStats(cleanerPlan: org.apache.hudi.avro.model.HoodieCleanerPlan): CleanPlanStatistics = {
    val earliestInstantToRetain = Option(cleanerPlan.getEarliestInstantToRetain)
      .map(_.getTimestamp)
      .orNull

    val totalPartitionsToClean = Option(cleanerPlan.getFilePathsToBeDeletedPerPartition)
      .map(_.size())
      .getOrElse(0)

    val totalPartitionsToDelete = Option(cleanerPlan.getPartitionsToBeDeleted)
      .map(_.size())
      .getOrElse(0)

    val involvedPartitions = Option(cleanerPlan.getFilePathsToBeDeletedPerPartition)
      .map(_.keySet().asScala.toSet)
      .getOrElse(Set.empty[String])

    CleanPlanStatistics(
      earliestInstantToRetain = earliestInstantToRetain,
      totalPartitionsToClean = totalPartitionsToClean,
      totalPartitionsToDelete = totalPartitionsToDelete,
      involvedPartitions = involvedPartitions
    )
  }

  private case class CleanPlanStatistics(earliestInstantToRetain: String,
                                         totalPartitionsToClean: Int,
                                         totalPartitionsToDelete: Int,
                                         involvedPartitions: Set[String]
                                        )
}

object ShowCleansProcedure {
  val NAME = "show_cleans"

  def builder: Supplier[ProcedureBuilder] = () => new ShowCleansProcedure()
}
