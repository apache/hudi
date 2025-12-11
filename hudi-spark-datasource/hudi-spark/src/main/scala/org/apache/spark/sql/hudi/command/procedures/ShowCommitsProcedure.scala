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

import org.apache.hudi.common.model.HoodieWriteStat
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.util.ClusteringUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier

import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Spark SQL procedure to show all commit operations for a Hudi table.
 *
 * This procedure provides a comprehensive view of Hudi commit operations, displaying both
 * completed commits with execution metadata and pending commit plans.
 * For pending operations, execution-specific fields are gracefully set to null.
 *
 * == Parameters ==
 * - `table`: Required. The name of the Hudi table to query
 * - `path`: Optional. The path of the Hudi table (alternative to table name)
 * - `limit`: Optional. Maximum number of commit operations to return (default: 20)
 * - `show_archived`: Optional. Whether to include archived commit operations (default: false)
 * - `filter`: Optional. SQL expression to filter results (default: empty string)
 * - `start_time`: Optional. Start time for commits (format: yyyyMMddHHmmss, default: empty)
 * - `end_time`: Optional. End time for commits (format: yyyyMMddHHmmss, default: empty)
 *
 * == Output Schema ==
 * - `commit_time`: Timestamp when the commit operation was initiated
 * - `state_transition_time`: Time when the commit transitioned to completed state (null for pending)
 * - `state`: Operation state (COMPLETED, INFLIGHT, REQUESTED)
 * - `action`: The action type (commit/deltacommit/replacecommit)
 * - `timeline_type`: Source timeline (ACTIVE, ARCHIVED)
 * - `partition_path`: Partition path for the commit operation
 * - `file_id`: ID of the file
 * - `previous_commit`: Previous commit time for the file
 * - `num_writes`: Number of records written
 * - `num_inserts`: Number of records inserted
 * - `num_deletes`: Number of records deleted
 * - `num_update_writes`: Number of records updated
 * - `total_log_blocks`: Number of log blocks
 * - `total_corrupt_log_blocks`: Number of corrupt log blocks
 * - `total_rollback_blocks`: Number of rollback blocks
 * - `total_log_records`: Number of log records
 * - `total_updated_records_compacted`: Number of updated records compacted
 * - `total_files_added`: Number of files added
 * - `total_files_updated`: Number of files updated
 * - `total_records_written`: Number of records written
 * - `total_records_updated`: Number of records updated
 * - `total_bytes_written`: Total bytes written
 * - `total_errors`: Number of errors encountered
 * - `file_size`: Size of the file in bytes
 * - `avg_record_size`: Average size of each record in bytes
 * - `extra_metadata`: Additional metadata from the commit
 */
class ShowCommitsProcedure extends BaseProcedure with ProcedureBuilder with Logging {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "path", DataTypes.StringType),
    ProcedureParameter.optional(2, "limit", DataTypes.IntegerType, 20),
    ProcedureParameter.optional(3, "show_archived", DataTypes.BooleanType, false),
    ProcedureParameter.optional(4, "show_files", DataTypes.BooleanType, false),
    ProcedureParameter.optional(5, "filter", DataTypes.StringType, ""),
    ProcedureParameter.optional(6, "start_time", DataTypes.StringType, ""),
    ProcedureParameter.optional(7, "end_time", DataTypes.StringType, "")
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("commit_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("state_transition_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("state", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("action", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("timeline_type", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("partition_path", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("file_id", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("previous_commit", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("num_writes", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("num_inserts", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("num_deletes", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("num_update_writes", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_log_blocks", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_corrupt_log_blocks", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_rollback_blocks", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_log_records", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_updated_records_compacted", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_files_added", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_files_updated", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_records_written", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_records_updated", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_bytes_written", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_errors", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("file_size", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("avg_record_size", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("extra_metadata", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getArgValueOrDefault(args, PARAMETERS(1))
    val limit = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[Int]
    val showArchived = getArgValueOrDefault(args, PARAMETERS(3)).get.asInstanceOf[Boolean]
    val showFiles = getArgValueOrDefault(args, PARAMETERS(4)).get.asInstanceOf[Boolean]
    val filter = getArgValueOrDefault(args, PARAMETERS(5)).get.asInstanceOf[String]
    val startTime = getArgValueOrDefault(args, PARAMETERS(6)).get.asInstanceOf[String]
    val endTime = getArgValueOrDefault(args, PARAMETERS(7)).get.asInstanceOf[String]

    validateFilter(filter, outputType)

    val basePath: String = getBasePath(tableName, tablePath)
    val metaClient = createMetaClient(jsc, basePath)

    val activeResults = getCommitsWithPartitionMetadata(
      metaClient.getActiveTimeline, limit, showFiles, startTime, endTime, metaClient, "ACTIVE")

    val finalResults = if (showArchived) {
      val archivedResults = getCommitsWithPartitionMetadata(metaClient.getArchivedTimeline, limit, showFiles, startTime, endTime, metaClient, "ARCHIVED")
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

  private def getCommitsWithPartitionMetadata(timeline: HoodieTimeline,
                                              limit: Int,
                                              showFiles: Boolean,
                                              startTime: String,
                                              endTime: String,
                                              metaClient: HoodieTableMetaClient,
                                              timelineType: String): Seq[Row] = {

    val allRows = scala.collection.mutable.ListBuffer[Row]()

    val commitsTimeline = timeline.getCommitsTimeline

    val filteredCommitInstants = commitsTimeline.getInstants.asScala
      .filter { instant =>
        val instantTime = instant.requestedTime()
        val withinStartTime = if (startTime.nonEmpty) instantTime >= startTime else true
        val withinEndTime = if (endTime.nonEmpty) instantTime <= endTime else true
        withinStartTime && withinEndTime
      }
      .toSeq

    val allCommitInstants = if (startTime.nonEmpty && endTime.nonEmpty) {
      filteredCommitInstants.sortBy(_.requestedTime).reverse
    } else {
      filteredCommitInstants.sortBy(_.requestedTime).reverse.take(limit)
    }

    allCommitInstants.foreach { commitInstant =>
      if (commitInstant.getState == HoodieInstant.State.COMPLETED) {
        Try {
          val commitMetadata = if (ClusteringUtils.isClusteringOrReplaceCommitAction(commitInstant.getAction)) {
            timeline.readReplaceCommitMetadata(commitInstant)
          } else {
            timeline.readCommitMetadata(commitInstant)
          }

          val extraMetadata = Option(commitMetadata.getExtraMetadata)
            .map(_.asScala.map { case (k, v) => s"$k=$v" }.mkString(", "))
            .orNull

          if (showFiles) {
            for (entry <- commitMetadata.getPartitionToWriteStats.entrySet.asScala) {
              val partitionPath = entry.getKey
              val stats = entry.getValue.asScala

              for (stat <- stats) {
                val fileId = stat.getFileId
                val prevCommit = stat.getPrevCommit
                val fileSize = stat.getFileSizeInBytes

                val isInsert = prevCommit == HoodieWriteStat.NULL_COMMIT
                val totalFilesAdded = if (isInsert) 1L else 0L
                val totalFilesUpdated = if (!isInsert) 1L else 0L

                val numWrites = stat.getNumWrites
                val numInserts = stat.getNumInserts
                val numDeletes = stat.getNumDeletes
                val numUpdateWrites = stat.getNumUpdateWrites
                val totalLogBlocks = stat.getTotalLogBlocks
                val totalCorruptLogBlocks = stat.getTotalCorruptLogBlock
                val totalRollbackBlocks = stat.getTotalRollbackBlocks
                val totalLogRecords = stat.getTotalLogRecords
                val totalUpdatedRecordsCompacted = stat.getTotalUpdatedRecordsCompacted
                val totalBytesWritten = stat.getTotalWriteBytes
                val totalErrors = stat.getTotalWriteErrors
                val avgRecordSize = if (numWrites > 0) totalBytesWritten / numWrites else 0L

                allRows += Row(
                  commitInstant.requestedTime,
                  commitInstant.getCompletionTime,
                  commitInstant.getState.name,
                  commitInstant.getAction,
                  timelineType,
                  partitionPath,
                  fileId,
                  prevCommit,
                  numWrites,
                  numInserts,
                  numDeletes,
                  numUpdateWrites,
                  totalLogBlocks,
                  totalCorruptLogBlocks,
                  totalRollbackBlocks,
                  totalLogRecords,
                  totalUpdatedRecordsCompacted,
                  totalFilesAdded,
                  totalFilesUpdated,
                  numWrites,
                  numUpdateWrites,
                  totalBytesWritten,
                  totalErrors,
                  fileSize,
                  avgRecordSize,
                  extraMetadata
                )
              }
            }
          } else {
            for (entry <- commitMetadata.getPartitionToWriteStats.entrySet.asScala) {
              val partitionPath = entry.getKey
              val stats = entry.getValue.asScala

              var totalFilesAdded = 0L
              var totalFilesUpdated = 0L
              var totalRecordsWritten = 0L
              var totalRecordsUpdated = 0L
              var totalBytesWritten = 0L
              var totalErrors = 0L
              var totalNumWrites = 0L
              var totalNumInserts = 0L
              var totalNumDeletes = 0L
              var totalNumUpdateWrites = 0L
              var totalLogBlocks = 0L
              var totalCorruptLogBlocks = 0L
              var totalRollbackBlocks = 0L
              var totalLogRecords = 0L
              var totalUpdatedRecordsCompacted = 0L
              var totalFileSize = 0L

              for (stat <- stats) {
                if (stat.getPrevCommit == HoodieWriteStat.NULL_COMMIT) {
                  totalFilesAdded += 1
                } else {
                  totalFilesUpdated += 1
                }
                totalNumWrites += stat.getNumWrites
                totalNumInserts += stat.getNumInserts
                totalNumDeletes += stat.getNumDeletes
                totalNumUpdateWrites += stat.getNumUpdateWrites
                totalLogBlocks += stat.getTotalLogBlocks
                totalCorruptLogBlocks += stat.getTotalCorruptLogBlock
                totalRollbackBlocks += stat.getTotalRollbackBlocks
                totalLogRecords += stat.getTotalLogRecords
                totalUpdatedRecordsCompacted += stat.getTotalUpdatedRecordsCompacted

                totalRecordsWritten += stat.getNumWrites
                totalRecordsUpdated += stat.getNumUpdateWrites
                totalBytesWritten += stat.getTotalWriteBytes
                totalErrors += stat.getTotalWriteErrors
                totalFileSize += stat.getFileSizeInBytes
              }

              val avgRecordSize = if (totalNumWrites > 0) totalBytesWritten / totalNumWrites else 0L

              allRows += Row(
                commitInstant.requestedTime(),
                commitInstant.getCompletionTime,
                commitInstant.getState.name(),
                commitInstant.getAction,
                timelineType,
                partitionPath,
                "*",
                null,
                totalNumWrites,
                totalNumInserts,
                totalNumDeletes,
                totalNumUpdateWrites,
                totalLogBlocks,
                totalCorruptLogBlocks,
                totalRollbackBlocks,
                totalLogRecords,
                totalUpdatedRecordsCompacted,
                totalFilesAdded,
                totalFilesUpdated,
                totalRecordsWritten,
                totalRecordsUpdated,
                totalBytesWritten,
                totalErrors,
                totalFileSize,
                avgRecordSize,
                extraMetadata
              )
            }

            if (commitMetadata.getPartitionToWriteStats.isEmpty) {
              val totalFilesAdded = commitMetadata.fetchTotalFilesInsert
              val totalFilesUpdated = commitMetadata.fetchTotalFilesUpdated
              val totalRecordsWritten = commitMetadata.fetchTotalRecordsWritten
              val totalRecordsUpdated = commitMetadata.fetchTotalUpdateRecordsWritten
              val totalBytesWritten = commitMetadata.fetchTotalBytesWritten
              val totalErrors = commitMetadata.fetchTotalWriteErrors
              val avgRecordSize = if (totalRecordsWritten > 0) totalBytesWritten / totalRecordsWritten else 0L

              allRows += Row(
                commitInstant.requestedTime,
                commitInstant.getCompletionTime,
                commitInstant.getState.name,
                commitInstant.getAction,
                timelineType,
                null, // partitionPath
                "*", // fileId
                null, // prevCommit
                totalRecordsWritten, // numWrites
                totalFilesAdded, // numInserts
                null, // numDeletes
                totalRecordsUpdated, // numUpdateWrites
                null, // totalLogBlocks
                null, // totalCorruptLogBlocks
                null, // totalRollbackBlocks
                null, // totalLogRecords
                null, // totalUpdatedRecordsCompacted
                totalFilesAdded,
                totalFilesUpdated,
                totalRecordsWritten,
                totalRecordsUpdated,
                totalBytesWritten,
                totalErrors,
                null, // fileSize
                avgRecordSize, // avgRecordSize
                extraMetadata
              )
            }
          }
        }.recover {
          case e: Exception =>
            log.warn(s"Failed to read commit metadata for instant ${commitInstant.requestedTime}: ${e.getMessage}")
            val row = createErrorRow(commitInstant)
            allRows += row
        }
      } else {
        val commitStats = extractCommitPlanStats(commitInstant, metaClient)
        commitStats.involvedPartitions.foreach { partitionPath =>
          allRows += Row(
            commitInstant.requestedTime,
            null, // state_transition_time
            commitInstant.getState.name,
            commitInstant.getAction,
            timelineType,
            partitionPath,
            "*", // fileId
            null, // prevCommit
            0L, // numWrites
            0L, // numInserts
            0L, // numDeletes
            0L, // numUpdateWrites
            0L, // totalLogBlocks
            0L, // totalCorruptLogBlocks
            0L, // totalRollbackBlocks
            0L, // totalLogRecords
            0L, // totalUpdatedRecordsCompacted
            0L, // totalFilesAdded
            0L, // totalFilesUpdated
            0L, // totalRecordsWritten
            0L, // totalRecordsUpdated
            0L, // totalBytesWritten
            0L, // totalErrors
            0L, // fileSize
            0L, // avgRecordSize
            commitStats.extraMetadata
          )
        }
      }
    }

    allRows.toSeq
  }

  private def extractCommitPlanStats(commitInstant: HoodieInstant,
                                     metaClient: HoodieTableMetaClient = null): CommitPlanStatistics = {

    try {
      val extraMetadata = Try {
        if (ClusteringUtils.isClusteringOrReplaceCommitAction(commitInstant.getAction)) {
          val clusteringPlanOpt = ClusteringUtils.getClusteringPlan(metaClient, commitInstant)
          if (clusteringPlanOpt.isPresent) {
            Option(clusteringPlanOpt.get().getRight.getExtraMetadata)
              .map(_.asScala.map { case (k, v) => s"$k=$v" }.mkString(", "))
              .orNull
          } else null
        } else {
          null
        }
      }.getOrElse(null)
      val involvedPartitions = Try {
        if (ClusteringUtils.isClusteringOrReplaceCommitAction(commitInstant.getAction)) {
          val clusteringPlanOpt = ClusteringUtils.getClusteringPlan(metaClient, commitInstant)
          if (clusteringPlanOpt.isPresent) {
            val plan = clusteringPlanOpt.get().getRight
            val inputGroups = Option(plan.getInputGroups)
              .map(_.asScala.toSeq)
              .getOrElse(Seq.empty)

            inputGroups.flatMap(group =>
              Option(group.getSlices).map(_.asScala.map(_.getPartitionPath).toSet).getOrElse(Set.empty)
            ).toSet
          } else Set.empty[String]
        } else {
          Set.empty[String]
        }
      }.getOrElse(Set.empty[String])

      CommitPlanStatistics(
        extraMetadata = extraMetadata,
        involvedPartitions = involvedPartitions
      )
    } catch {
      case e: Exception =>
        log.warn(s"Failed to extract commit plan stats for instant ${commitInstant.requestedTime}: ${e.getMessage}")
        CommitPlanStatistics(null, Set.empty)
    }
  }

  private case class CommitPlanStatistics(extraMetadata: String,
                                          involvedPartitions: Set[String]
                                         )

  private def createErrorRow(instant: HoodieInstant): Row = {
    Row(
      instant.requestedTime,
      instant.getCompletionTime,
      instant.getState.name,
      instant.getAction,
      null, // partitionPath
      null, // fileId
      null, // prevCommit
      null, // numWrites
      null, // numInserts
      null, // numDeletes
      null, // numUpdateWrites
      null, // totalLogBlocks
      null, // totalCorruptLogBlocks
      null, // totalRollbackBlocks
      null, // totalLogRecords
      null, // totalUpdatedRecordsCompacted
      null, // totalFilesAdded
      null, // totalFilesUpdated
      null, // totalRecordsWritten
      null, // totalRecordsUpdated
      null, // totalBytesWritten
      null, // totalErrors
      null, // fileSize
      null, // avgRecordSize
      null // extraMetadata
    )
  }

  override def build: Procedure = new ShowCommitsProcedure()
}

object ShowCommitsProcedure {
  val NAME = "show_commits"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowCommitsProcedure()
  }
}

