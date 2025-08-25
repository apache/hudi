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

import org.apache.hudi.{HoodieCLIUtils, SparkAdapterSupport}
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline, TimelineLayout}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util
import java.util.Collections
import java.util.function.Supplier

import scala.collection.JavaConverters._

/**
 * Spark SQL procedure to show completed clean operations for a Hudi table.
 *
 * This procedure displays information about clean operations that have been executed.
 * Clean operations remove old file versions to reclaim storage space and maintain table performance.
 *
 * == Parameters ==
 * - `table`: Required. The name of the Hudi table to query
 * - `limit`: Optional. Maximum number of clean operations to return (default: 10)
 * - `showArchived`: Optional. Whether to include archived clean operations (default: false)
 * - `filter`: Optional. SQL expression to filter results (default: empty string)
 *
 * == Output Schema ==
 * - `clean_time`: Timestamp when the clean operation was performed
 * - `state_transition_time`: Time when the clean transitioned to completed state
 * - `action`: The action type (always 'clean')
 * - `start_clean_time`: When the clean operation started
 * - `time_taken_in_millis`: Duration of the clean operation in milliseconds
 * - `total_files_deleted`: Total number of files deleted during the clean
 * - `earliest_commit_to_retain`: The earliest commit that was retained
 * - `last_completed_commit_timestamp`: The last completed commit at clean time
 * - `version`: Version of the clean operation metadata
 * - Additional partition-level metadata columns when using `show_cleans_metadata`
 *
 * == Error Handling ==
 * - Throws `IllegalArgumentException` for invalid filter expressions
 * - Throws `HoodieException` for table access issues
 * - Returns empty result set if no clean plans match the criteria
 *
 * == Filter Support ==
 * The `filter` parameter supports SQL expressions for filtering results.
 *
 * === Common Filter Examples ===
 * {{{
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
 * -- Show clean operations with partition metadata
 * CALL show_cleans_metadata(table => 'hudi_table_1')
 *
 * -- Include archived clean operations
 * CALL show_cleans(table => 'hudi_table_1', showArchived => true)
 *
 * -- Filter for recent efficient cleans
 * CALL show_cleans(
 *   table => 'hudi_table_1',
 *   filter => "clean_time > '20231201000000' AND total_files_deleted > 0"
 * )
 * }}}
 *
 * @param includePartitionMetadata Whether to include partition-level metadata in output
 * @see [[ShowCleansPlanProcedure]] for information about planned clean operations
 * @see [[HoodieProcedureFilterUtils]] for detailed filter expression syntax
 */
class ShowCleansProcedure(includePartitionMetadata: Boolean) extends BaseProcedure with ProcedureBuilder with SparkAdapterSupport with Logging {

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "path", DataTypes.StringType),
    ProcedureParameter.optional(2, "limit", DataTypes.IntegerType, 10),
    ProcedureParameter.optional(3, "showArchived", DataTypes.BooleanType, false),
    ProcedureParameter.optional(4, "filter", DataTypes.StringType, "")
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("clean_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("state_transition_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("action", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("start_clean_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("time_taken_in_millis", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_files_deleted", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("earliest_commit_to_retain", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("last_completed_commit_timestamp", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("version", DataTypes.IntegerType, nullable = true, Metadata.empty)
  ))

  private val PARTITION_METADATA_OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("clean_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("state_transition_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("action", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("start_clean_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("partition_path", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("policy", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("delete_path_patterns", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("success_delete_files", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("failed_delete_files", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("is_partition_deleted", DataTypes.BooleanType, nullable = true, Metadata.empty),
    StructField("time_taken_in_millis", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_files_deleted", DataTypes.IntegerType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = if (includePartitionMetadata) PARTITION_METADATA_OUTPUT_TYPE else OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getArgValueOrDefault(args, PARAMETERS(1))
    val limit = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[Int]
    val showArchived = getArgValueOrDefault(args, PARAMETERS(3)).get.asInstanceOf[Boolean]
    val filter = getArgValueOrDefault(args, PARAMETERS(4)).get.asInstanceOf[String]

    if (filter != null && filter.trim.nonEmpty) {
      HoodieProcedureFilterUtils.validateFilterExpression(filter, outputType, sparkSession) match {
        case Left(errorMessage) =>
          throw new IllegalArgumentException(s"Invalid filter expression: $errorMessage")
        case Right(_) => // Validation passed, continue
      }
    }

    val basePath = getBasePath(tableName, tablePath)
    val metaClient = createMetaClient(jsc, basePath)

    val activeResults = if (includePartitionMetadata) {
      getCleansWithPartitionMetadata(metaClient.getActiveTimeline, limit)
    } else {
      getCleans(metaClient.getActiveTimeline, limit)
    }
    val finalResults = if (showArchived) {
      val archivedResults = if (includePartitionMetadata) {
        getCleansWithPartitionMetadata(metaClient.getArchivedTimeline, limit)
      } else {
        getCleans(metaClient.getArchivedTimeline, limit)
      }
      (activeResults ++ archivedResults)
        .sortWith((a, b) => a.getString(0) > b.getString(0))
        .take(limit)
    } else {
      activeResults
    }
    if (filter != null && filter.trim.nonEmpty) {
      HoodieProcedureFilterUtils.evaluateFilter(finalResults, filter, outputType, sparkSession)
    } else {
      finalResults
    }
  }

  override def build: Procedure = new ShowCleansProcedure(includePartitionMetadata)

  private def getCleansWithPartitionMetadata(timeline: HoodieTimeline,
                                             limit: Int): Seq[Row] = {
    import scala.collection.JavaConverters._

    val (rows: util.ArrayList[Row], cleanInstants: util.ArrayList[HoodieInstant]) = getSortedCleans(timeline)

    var rowCount = 0

    cleanInstants.asScala.takeWhile(_ => rowCount < limit).foreach { cleanInstant =>
      val cleanMetadata = timeline.readCleanMetadata(cleanInstant)

      cleanMetadata.getPartitionMetadata.entrySet.asScala.takeWhile(_ => rowCount < limit).foreach { partitionMetadataEntry =>
        val partitionPath = partitionMetadataEntry.getKey
        val partitionMetadata = partitionMetadataEntry.getValue

        rows.add(Row(
          cleanInstant.requestedTime(),
          cleanInstant.getCompletionTime,
          cleanInstant.getAction,
          cleanMetadata.getStartCleanTime,
          partitionPath,
          partitionMetadata.getPolicy,
          partitionMetadata.getDeletePathPatterns.size(),
          partitionMetadata.getSuccessDeleteFiles.size(),
          partitionMetadata.getFailedDeleteFiles.size(),
          partitionMetadata.getIsPartitionDeleted,
          cleanMetadata.getTimeTakenInMillis,
          cleanMetadata.getTotalFilesDeleted
        ))
        rowCount += 1
      }
    }

    rows.asScala.toList
  }

  private def getSortedCleans(timeline: HoodieTimeline): (util.ArrayList[Row], util.ArrayList[HoodieInstant]) = {
    val rows = new util.ArrayList[Row]
    val cleanInstants: util.List[HoodieInstant] = timeline.getCleanerTimeline.filterCompletedInstants
      .getInstants.toArray().map(instant => instant.asInstanceOf[HoodieInstant]).toList.asJava
    val sortedCleanInstants = new util.ArrayList[HoodieInstant](cleanInstants)
    val layout = TimelineLayout.fromVersion(timeline.getTimelineLayoutVersion)
    Collections.sort(sortedCleanInstants, layout.getInstantComparator.requestedTimeOrderedComparator.reversed)
    (rows, sortedCleanInstants)
  }

  private def getCleans(timeline: HoodieTimeline,
                        limit: Int): Seq[Row] = {
    val (rows: util.ArrayList[Row], cleanInstants: util.ArrayList[HoodieInstant]) = getSortedCleans(timeline)

    for (i <- 0 until Math.min(cleanInstants.size, limit)) {
      val cleanInstant = cleanInstants.get(i)
      val cleanMetadata = timeline.readCleanMetadata(cleanInstant)

      rows.add(Row(
        cleanInstant.requestedTime(),
        cleanInstant.getCompletionTime,
        cleanInstant.getAction,
        cleanMetadata.getStartCleanTime,
        cleanMetadata.getTimeTakenInMillis,
        cleanMetadata.getTotalFilesDeleted,
        cleanMetadata.getEarliestCommitToRetain,
        cleanMetadata.getLastCompletedCommitTimestamp,
        cleanMetadata.getVersion
      ))
    }

    rows.asScala.toList
  }
}

object ShowCleansProcedure {
  val NAME = "show_cleans"

  def builder: Supplier[ProcedureBuilder] = () => new ShowCleansProcedure(false)
}

object ShowCleansPartitionMetadataProcedure {
  val NAME = "show_cleans_metadata"

  def builder: Supplier[ProcedureBuilder] = () => new ShowCleansProcedure(true)
}
