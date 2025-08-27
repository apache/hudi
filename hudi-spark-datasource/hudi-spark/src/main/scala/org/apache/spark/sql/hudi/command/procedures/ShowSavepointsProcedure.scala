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

import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier

import scala.collection.JavaConverters._

/**
 * Spark SQL procedure to show all savepoints for a Hudi table.
 *
 * == Parameters ==
 * - `table`: Required. The name of the Hudi table to query
 * - `path`: Optional. The path of the Hudi table (anyone of `table` or `path` must be provided)
 * - `limit`: Optional. Maximum number of savepoint operations to return (default: 10, ignored if time range specified)
 * - `showArchived`: Optional. Whether to include archived savepoint operations (default: false)
 * - `filter`: Optional. SQL expression to filter results (default: empty string)
 * - `startTime`: Optional. Start time for savepoints (format: yyyyMMddHHmmss, default: empty)
 * - `endTime`: Optional. End time for savepoints (format: yyyyMMddHHmmss, default: empty)
 *
 * == Output Schema ==
 * - `savepoint_time`: Timestamp when the savepoint was created
 * - `state_transition_time`: Time when the savepoint transitioned to completed state
 * - `state`: Operation state (COMPLETED for savepoint)
 * - `action`: The action type (always 'savepoint')
 * - `savepointed_by`: User who created the savepoint
 * - `savepointed_at`: Epoch timestamp when savepoint was created
 * - `comments`: User comments for the savepoint
 * - `partition_path`: Partition path (shows detailed partition info when available)
 * - `savepoint_data_file`: Data file preserved by savepoint
 * - `version`: Version of the savepoint metadata
 *
 * == Usage Examples ==
 * {{{
 * -- Basic usage: Show last 10 savepoints
 * CALL show_savepoints(table => 'my_table')
 *
 * -- Include archived savepoint operations
 * CALL show_savepoints(table => 'my_table', showArchived => true)
 *
 * -- Show savepoints within a time range (ignores limit)
 * CALL show_savepoints(
 *   table => 'my_table',
 *   startTime => '20231201000000',
 *   endTime => '20231205235959'
 * )
 *
 * -- Show recent savepoints with limit
 * CALL show_savepoints(
 *   table => 'my_table',
 *   startTime => '20231204000000',
 *   limit => 5
 * )
 *
 * -- Show savepoints with user filter
 * CALL show_savepoints(
 *   table => 'my_table',
 *   filter => "savepointed_by = 'admin' AND comments LIKE '%backup%'"
 * )
 *
 * -- Show partition-level savepoint details
 * CALL show_savepoints(
 *   table => 'my_table',
 *   filter => "partition_path IS NOT NULL"
 * )
 * }}}
 */
class ShowSavepointsProcedure extends BaseProcedure with ProcedureBuilder with Logging {
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
    StructField("savepoint_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("state_transition_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("state", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("action", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("savepointed_by", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("savepointed_at", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("comments", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("partition_path", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("savepoint_data_file", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("version", DataTypes.IntegerType, nullable = true, Metadata.empty)
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
      HoodieProcedureFilterUtils.validateFilterExpression(filter, outputType, sparkSession) match {
        case Left(errorMessage) =>
          throw new IllegalArgumentException(s"Invalid filter expression: $errorMessage")
        case Right(_) => // Validation passed, continue
      }
    }

    val basePath: String = getBasePath(tableName, tablePath)
    val metaClient = createMetaClient(jsc, basePath)

    val activeResults = getCombinedSavepointsWithPartitionMetadata(metaClient.getActiveTimeline, limit, startTime, endTime)
    val finalResults = if (showArchived) {
      val archivedResults = getCombinedSavepointsWithPartitionMetadata(metaClient.getArchivedTimeline, limit, startTime, endTime)
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

  private def getCombinedSavepointsWithPartitionMetadata(timeline: HoodieTimeline,
                                                         limit: Int,
                                                         startTime: String,
                                                         endTime: String): Seq[Row] = {
    import scala.collection.mutable.ListBuffer
    import scala.util.{Failure, Success, Try}

    val allRows = ListBuffer[Row]()

    val filteredSavepointInstants = timeline.getSavePointTimeline.getInstants.iterator().asScala
      .filter(_.getAction == HoodieTimeline.SAVEPOINT_ACTION)
      .filter { instant =>
        val instantTime = instant.requestedTime()
        val withinStartTime = if (startTime.nonEmpty) instantTime >= startTime else true
        val withinEndTime = if (endTime.nonEmpty) instantTime <= endTime else true
        withinStartTime && withinEndTime
      }
      .toSeq

    val allSavepointInstants = if (startTime.nonEmpty && endTime.nonEmpty) {
      filteredSavepointInstants.sortBy(_.requestedTime).reverse
    } else {
      filteredSavepointInstants.sortBy(_.requestedTime).reverse.take(limit)
    }
    // there is no pending states in the savepoint action, so we are only processing completed ones
    allSavepointInstants.foreach { savepointInstant =>
      if (savepointInstant.getState == HoodieInstant.State.COMPLETED) {
        Try {
          val savepointMetadata = timeline.readSavepointMetadata(savepointInstant)
          if (savepointMetadata.getPartitionMetadata != null && !savepointMetadata.getPartitionMetadata.isEmpty) {
            savepointMetadata.getPartitionMetadata.entrySet.asScala.foreach { partitionEntry =>
              val partitionPath = partitionEntry.getKey
              val partitionMetadata = partitionEntry.getValue
              partitionMetadata.getSavepointDataFile.asScala.foreach { dataFile =>
                val row = Row(
                  savepointInstant.requestedTime(),
                  savepointInstant.getCompletionTime,
                  savepointInstant.getState.name(),
                  savepointInstant.getAction,
                  savepointMetadata.getSavepointedBy,
                  savepointMetadata.getSavepointedAt,
                  savepointMetadata.getComments,
                  partitionPath,
                  dataFile,
                  savepointMetadata.getVersion
                )
                allRows += row
              }
            }
          } else {
            val row = Row(
              savepointInstant.requestedTime(),
              savepointInstant.getCompletionTime,
              savepointInstant.getState.name(),
              savepointInstant.getAction,
              savepointMetadata.getSavepointedBy,
              savepointMetadata.getSavepointedAt,
              savepointMetadata.getComments,
              null,
              null,
              savepointMetadata.getVersion
            )
            allRows += row
          }
        } match {
          case Success(_) => // Success, rows already added
          case Failure(e) =>
            log.warn(s"Failed to read savepoint metadata for instant ${savepointInstant.requestedTime()}", e)
            val row = Row(
              savepointInstant.requestedTime(),
              savepointInstant.getCompletionTime,
              savepointInstant.getState.name(),
              savepointInstant.getAction,
              null,
              null,
              null,
              null,
              null,
              null
            )
            allRows += row
        }
      }
    }
    allRows.toSeq
  }

  override def build: Procedure = new ShowSavepointsProcedure()
}

object ShowSavepointsProcedure {
  val NAME: String = "show_savepoints"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): ShowSavepointsProcedure = new ShowSavepointsProcedure()
  }
}
