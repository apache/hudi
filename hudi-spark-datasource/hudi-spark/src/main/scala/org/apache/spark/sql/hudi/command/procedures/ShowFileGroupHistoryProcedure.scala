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

import org.apache.hudi.common.table.HoodieTableMetaClient

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, StructType}

import java.util
import java.util.function.Supplier

import scala.collection.JavaConverters._

/**
 * Spark SQL procedure to show the complete history of a specific file group in a Hudi table.
 *
 * This procedure displays comprehensive information about all operations performed on a specific file group,
 * including commits, updates, deletions, replacements, and metadata changes in a detailed partition specific view. It tracks the lifecycle of
 * files from creation through various modifications and eventual deletion or replacement.
 *
 * == Parameters ==
 * - `table`: Optional. The name of the Hudi table to query (mutually exclusive with `path`)
 * - `path`: Optional. The base path of the Hudi table (mutually exclusive with `table`)
 * - `fileGroupId`: Required. The unique identifier of the file group to track
 * - `partition`: Optional. Specific partition to filter results (default: all partitions)
 * - `showArchived`: Optional. Whether to include archived timeline data (default: false)
 * - `limit`: Optional. Maximum number of history entries to return (default: 20)
 * - `filter`: Optional. SQL expression to filter results (default: empty string)
 * - `startTime`: Optional. Start timestamp for filtering results (supported formats: yyyyMMddHHMMssmm, yyyyMMddHHmmss, yyyy-MM-dd, yyyy/MM/dd, yyyyMMdd)
 * - `endTime`: Optional. End timestamp for filtering results (supported formats: yyyyMMddHHMMssmm, yyyyMMddHHmmss, yyyy-MM-dd, yyyy/MM/dd, yyyyMMdd)
 * - `verbose` : Optional. Whether to include detailed statistics (default: false)
 *
 * == Output Schema ==
 * - `instant_time`: Timestamp when the operation was performed
 * - `completion_time`: Time when the operation completed (null for pending operations)
 * - `action`: The action type (commit, deltacommit, compaction, clustering, etc.)
 * - `timeline_type`: Whether the data is from ACTIVE or ARCHIVED timeline
 * - `state`: Current state of the operation (REQUESTED, INFLIGHT, COMPLETED)
 * - `partition_path`: Partition path where the file group resides
 * - `file_name`: Name of the file in the file group
 * - `operation_type`: Type of write operation (INSERT, UPDATE, UPSERT, DELETE)
 * - `num_writes`: Total number of records written in this operation
 * - `num_inserts`: Number of new records inserted
 * - `num_updates`: Number of existing records updated
 * - `num_deletes`: Number of records deleted
 * - `file_size_bytes`: Size of the file in bytes
 * - `total_write_bytes`: Total bytes written during the operation
 * - `prev_commit`: Previous commit timestamp that this operation builds upon
 * - `was_deleted`: Whether the file was deleted in a subsequent operation
 * - `delete_action`: Action that caused the deletion (clean, rollback, etc.)
 * - `delete_instant`: Timestamp when the deletion occurred
 * - `is_replaced`: Whether the file was replaced in a subsequent operation
 * - `replace_action`: Action that caused the replacement (compaction, clustering, etc.)
 * - `replace_instant`: Timestamp when the replacement occurred
 * - `total_write_errors`: Number of write errors encountered
 * - `total_scan_time_ms`: Total time spent scanning during the operation
 * - `total_upsert_time_ms`: Total time spent in upsert processing
 * - `total_create_time_ms`: Total time spent in file creation
 * - `prev_base_file`: Previous base file that was replaced (for compaction/clustering)
 * - `column_stats_available`: Whether column statistics are available for this file
 *
 * == Error Handling ==
 * - Throws `IllegalArgumentException` for invalid filter expressions or missing fileGroupId
 * - Throws `HoodieException` for table access issues or invalid file group identifiers
 * - Returns empty result set if no file group history matches the criteria
 * - Gracefully handles archived timeline access failures with warning logs
 *
 * == Filter Support ==
 * The `filter` parameter supports SQL expressions for filtering results on any output column.
 * The filter uses Spark SQL syntax and supports various data types and operations.
 *
 * == Usage Examples ==
 * {{{
 * -- Basic usage: Show file group history
 * CALL show_file_group_history(
 *   table => 'hudi_table_1',
 *   fileGroupId => 'abc123'
 * )
 *
 * -- Show history with custom limit
 * CALL show_file_group_history(
 *   table => 'hudi_table_1',
 *   fileGroupId => 'abc123',
 *   limit => 50
 * )
 *
 * -- Show history for specific partition (partitioned to datetime column here)
 * CALL show_file_group_history(
 *   table => 'hudi_table_1',
 *   fileGroupId => 'abc123',
 *   partition => '2025/08/28'
 * )
 *
 * -- Include archived timeline data
 * CALL show_file_group_history(
 *   table => 'hudi_table_1',
 *   fileGroupId => 'abc123',
 *   showArchived => true
 * )
 *
 * -- Filter for specific operation types
 * CALL show_file_group_history(
 *   table => 'hudi_table_1',
 *   fileGroupId => 'abc123',
 *   filter => "operation_type = 'INSERT'"
 * )
 *
 * -- Filter by date range using yyyy-MM-dd format
 * CALL show_file_group_history(
 *   table => 'hudi_table_1',
 *   fileGroupId => 'abc123',
 *   startTime => '2024-01-01',
 *   endTime => '2024-12-31'
 * )
 *
 * -- Filter by date range using yyyy/MM/dd format
 * CALL show_file_group_history(
 *   table => 'hudi_table_1',
 *   fileGroupId => 'abc123',
 *   startTime => '2024/01/01',
 *   endTime => '2024/12/31'
 * )
 *
 * -- Filter by date range using yyyyMMdd format
 * CALL show_file_group_history(
 *   table => 'hudi_table_1',
 *   fileGroupId => 'abc123',
 *   startTime => '20240101',
 *   endTime => '20241231'
 * )
 * }}}
 *
 * @see [[ShowFileHistoryProcedureUtils]] for underlying utility methods
 * @see [[HoodieProcedureFilterUtils]] for detailed filter expression syntax
 */
class ShowFileGroupHistoryProcedure extends BaseProcedure with ProcedureBuilder with Logging {

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "path", DataTypes.StringType),
    ProcedureParameter.required(2, "fileGroupId", DataTypes.StringType),
    ProcedureParameter.optional(3, "partition", DataTypes.StringType),
    ProcedureParameter.optional(4, "showArchived", DataTypes.BooleanType, false),
    ProcedureParameter.optional(5, "limit", DataTypes.IntegerType, 20),
    ProcedureParameter.optional(6, "filter", DataTypes.StringType, ""),
    ProcedureParameter.optional(7, "startTime", DataTypes.StringType, ""),
    ProcedureParameter.optional(8, "endTime", DataTypes.StringType, "")
  )

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = ShowFileHistoryProcedureUtils.OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getArgValueOrDefault(args, PARAMETERS(1))
    val fileGroupId = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[String]
    val partition = getArgValueOrDefault(args, PARAMETERS(3)).asInstanceOf[Option[String]]
    val showArchived = getArgValueOrDefault(args, PARAMETERS(4)).get.asInstanceOf[Boolean]
    val limit = getArgValueOrDefault(args, PARAMETERS(5)).get.asInstanceOf[Int]
    val filter = getArgValueOrDefault(args, PARAMETERS(6)).get.asInstanceOf[String]
    val startTime = getArgValueOrDefault(args, PARAMETERS(7)).get.asInstanceOf[String]
    val endTime = getArgValueOrDefault(args, PARAMETERS(8)).get.asInstanceOf[String]

    if (filter != null && filter.trim.nonEmpty) {
      HoodieProcedureFilterUtils.validateFilterExpression(filter, outputType, sparkSession) match {
        case Left(errorMessage) =>
          throw new IllegalArgumentException(s"Invalid filter expression: $errorMessage")
        case Right(_) => // Validation passed, continue
      }
    }

    val basePath = getBasePath(tableName, tablePath)
    val metaClient = createMetaClient(jsc, basePath)

    val normalizedStartTime = HoodieProcedureUtils.normalizeTimeFormat(startTime)
    val normalizedEndTime = HoodieProcedureUtils.normalizeTimeFormat(endTime, isEndTime = true)

    val fileGroupHistory = collectFileGroupHistory(metaClient, fileGroupId, partition, showArchived, limit, normalizedStartTime, normalizedEndTime)

    if (filter != null && filter.trim.nonEmpty) {
      HoodieProcedureFilterUtils.evaluateFilter(fileGroupHistory, filter, outputType, sparkSession)
    } else {
      fileGroupHistory
    }
  }

  private def collectFileGroupHistory(metaClient: HoodieTableMetaClient,
                                      fileGroupId: String,
                                      partition: Option[String],
                                      showArchived: Boolean,
                                      limit: Int,
                                      startTime: String,
                                      endTime: String): Seq[Row] = {

    import ShowFileHistoryProcedureUtils._

    val activeEntries = new util.ArrayList[HistoryEntry]()
    val activeTimeline = metaClient.getActiveTimeline
    ShowFileHistoryProcedureUtils.processWriteTimeline(activeTimeline, fileGroupId, partition, "ACTIVE", activeEntries, limit, startTime, endTime)

    val archivedEntries = new util.ArrayList[HistoryEntry]()
    if (showArchived) {
      try {
        val archivedTimeline = metaClient.getArchivedTimeline.reload()
        archivedTimeline.loadCompletedInstantDetailsInMemory()
        ShowFileHistoryProcedureUtils.processWriteTimeline(archivedTimeline, fileGroupId, partition, "ARCHIVED", archivedEntries, limit, startTime, endTime)
      } catch {
        case e: Exception =>
          log.warn(s"Failed to process archived timeline: ${e.getMessage}")
      }
    }

    val allEntries = (activeEntries.asScala ++ archivedEntries.asScala).toList
    val sortedEntries = allEntries
      .sortBy(_.instantTime)(Ordering[String].reverse)

    val finalEntries = if (startTime.trim.nonEmpty && endTime.trim.nonEmpty) {
      sortedEntries
    } else {
      sortedEntries.take(limit)
    }

    val deletionInfo = ShowFileHistoryProcedureUtils.checkForDeletions(metaClient, fileGroupId, partition, showArchived)

    val replacementInfo = ShowFileHistoryProcedureUtils.checkForReplacements(metaClient, fileGroupId, partition, showArchived)

    val rows = finalEntries.map { entry =>
      val deletion = deletionInfo.get(entry.fileName)
      val replacement = replacementInfo.get(entry.fileName).orElse {
        val fileId = entry.fileName.split("_").headOption.getOrElse("")
        replacementInfo.get(fileId)
      }
      Row(
        entry.instantTime,
        entry.completionTime,
        entry.action,
        entry.timelineType,
        entry.state,
        entry.partitionPath,
        entry.fileName,
        entry.operationType,
        entry.numWrites,
        entry.numInserts,
        entry.numUpdates,
        entry.numDeletes,
        entry.fileSizeBytes,
        entry.totalWriteBytes,
        entry.prevCommit,
        deletion.isDefined,
        deletion.map(_.action).orNull,
        deletion.map(_.instant).orNull,
        replacement.isDefined,
        replacement.map(_.action).orNull,
        replacement.map(_.instant).orNull,
        entry.totalWriteErrors,
        entry.totalScanTimeMs,
        entry.totalUpsertTimeMs,
        entry.totalCreateTimeMs,
        entry.prevBaseFile.orNull,
        entry.columnStatsAvailable
      )
    }.toSeq

    if (rows.nonEmpty) {
      val firstRow = rows.head
      val firstRowValues = firstRow.toSeq.toArray
      val markedFirstRow = Row.fromSeq(("*" + firstRowValues(0)) +: firstRowValues.tail)
      markedFirstRow +: rows.tail
    } else {
      rows
    }
  }


  override def build: Procedure = new ShowFileGroupHistoryProcedure()
}

object ShowFileGroupHistoryProcedure {
  val NAME = "show_file_group_history"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowFileGroupHistoryProcedure()
  }
}
