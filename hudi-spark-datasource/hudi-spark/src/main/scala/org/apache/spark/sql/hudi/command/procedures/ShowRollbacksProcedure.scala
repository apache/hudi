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
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.table.timeline.HoodieTimeline.ROLLBACK_ACTION
import org.apache.hudi.table.action.rollback.RollbackUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier

import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Spark SQL procedure to show all rollback operations for a Hudi table.
 *
 * This procedure provides a comprehensive view of Hudi rollback operations, displaying both
 * completed rollbacks with execution metadata and pending rollback plans.
 * For pending operations, execution-specific fields are gracefully set to null.
 *
 * == Parameters ==
 * - `table`: Required. The name of the Hudi table to query
 * - `path`: Optional. The path of the Hudi table (anyone of `table` or `path` must be provided)
 * - `limit`: Optional. Maximum number of rollback operations to return (default: 10)
 * - `showArchived`: Optional. Whether to include archived rollback operations (default: false)
 * - `filter`: Optional. SQL expression to filter results (default: empty string)
 * - `startTime`: Optional. Start time for rollbacks (format: yyyyMMddHHmmss, default: empty)
 * - `endTime`: Optional. End time for rollbacks (format: yyyyMMddHHmmss, default: empty)
 *
 * == Output Schema ==
 * - `rollback_time`: Timestamp when the rollback operation was initiated
 * - `state_transition_time`: Time when the rollback transitioned to completed state (null for pending)
 * - `state`: Operation state (COMPLETED, INFLIGHT, REQUESTED)
 * - `action`: The action type (rollback)
 * - `start_rollback_time`: When the rollback operation started (null for pending)
 * - `partition_path`: Partition path for the rollback operation
 * - `rollback_instant`: The commit instant being rolled back
 * - `deleted_file`: Name of the file deleted during rollback (null for pending)
 * - `succeeded`: Whether file deletion succeeded (null for pending)
 * - `total_files_deleted`: Total number of files deleted or planned for deletion (null for pending/partition level)
 * - `time_taken_in_millis`: Total time taken for rollback in milliseconds (null for pending)
 * - `total_partitions`: Total number of partitions affected (null for pending/partition level)
 * - `version`: Version of the rollback metadata
 *
 * == Error Handling ==
 * - Throws `IllegalArgumentException` for invalid filter expressions
 * - Returns empty result set if no rollback operations match the criteria
 *
 * == Filter Support ==
 * The `filter` parameter supports SQL expressions for filtering results.
 *
 * === Common Filter Examples ===
 * {{{
 * -- Show only completed rollbacks
 * CALL show_rollbacks(
 *   table => 'my_table',
 *   filter => "state = 'COMPLETED'"
 * )
 *
 * -- Show rollbacks for specific instant
 * CALL show_rollbacks(
 *   table => 'my_table',
 *   filter => "rollback_instant = '20240101120000000'"
 * )
 *
 * -- Show failed file deletions
 * CALL show_rollbacks(
 *   table => 'my_table',
 *   filter => "succeeded = false"
 * )
 *
 * -- Show rollbacks affecting many files
 * CALL show_rollbacks(
 *   table => 'my_table',
 *   filter => "total_files_deleted_or_planned > 10"
 * )
 * }}}
 *
 */
class ShowRollbacksProcedure extends BaseProcedure with ProcedureBuilder with Logging {
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
    StructField("rollback_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("state_transition_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("state", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("action", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("start_rollback_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("partition_path", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("rollback_instant", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("deleted_file", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("succeeded", DataTypes.BooleanType, nullable = true, Metadata.empty),
    StructField("total_files_deleted", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("time_taken_in_millis", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_partitions", DataTypes.IntegerType, nullable = true, Metadata.empty),
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

    validateFilter(filter, outputType)

    val basePath = getBasePath(tableName, tablePath)
    val metaClient = createMetaClient(jsc, basePath)

    val activeResults = getCombinedRollbacksWithPartitionMetadata(metaClient, metaClient.getActiveTimeline, limit, startTime, endTime)
    val finalResults = if (showArchived) {
      val archivedResults = getCombinedRollbacksWithPartitionMetadata(metaClient, metaClient.getArchivedTimeline, limit, startTime, endTime)
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

  private def getCombinedRollbacksWithPartitionMetadata(metaClient: HoodieTableMetaClient,
                                                        timeline: HoodieTimeline,
                                                        limit: Int,
                                                        startTime: String,
                                                        endTime: String): Seq[Row] = {
    import scala.collection.mutable.ListBuffer

    val allRows = ListBuffer[Row]()

    val filteredRollbackInstants = timeline.getRollbackTimeline.getInstants.iterator().asScala
      .filter(_.getAction == ROLLBACK_ACTION)
      .filter { instant =>
        val instantTime = instant.requestedTime()
        val withinStartTime = if (startTime.nonEmpty) instantTime >= startTime else true
        val withinEndTime = if (endTime.nonEmpty) instantTime <= endTime else true
        withinStartTime && withinEndTime
      }
      .toSeq
    val allRollbackInstants = if (startTime.nonEmpty && endTime.nonEmpty) {
      filteredRollbackInstants.sortBy(_.requestedTime).reverse
    } else {
      filteredRollbackInstants.sortBy(_.requestedTime).reverse.take(limit)
    }
    allRollbackInstants.foreach { rollbackInstant =>
      if (rollbackInstant.getState == HoodieInstant.State.COMPLETED) {
        Try {
          val rollbackMetadata = timeline.readRollbackMetadata(rollbackInstant)
          val metadataStats = extractRollbackMetadataStats(rollbackMetadata)
          rollbackMetadata.getPartitionMetadata.entrySet.asScala.foreach { partitionEntry =>
            val partitionPath = partitionEntry.getKey
            val partitionMetadata = partitionEntry.getValue
            partitionMetadata.getSuccessDeleteFiles.asScala.foreach { deletedFile =>
              val row = Row(
                rollbackInstant.requestedTime(),
                rollbackInstant.getCompletionTime,
                rollbackInstant.getState.name(),
                rollbackInstant.getAction,
                rollbackMetadata.getStartRollbackTime,
                partitionPath,
                rollbackMetadata.getCommitsRollback.toString,
                deletedFile,
                true,
                metadataStats.totalFilesDeleted,
                metadataStats.timeTakenInMillis,
                metadataStats.totalPartitionsAffected,
                rollbackMetadata.getVersion
              )
              allRows += row
            }

            partitionMetadata.getFailedDeleteFiles.asScala.foreach { deletedFile =>
              val row = Row(
                rollbackInstant.requestedTime(),
                rollbackInstant.getCompletionTime,
                rollbackInstant.getState.name(),
                rollbackInstant.getAction,
                rollbackMetadata.getStartRollbackTime,
                partitionPath,
                rollbackMetadata.getCommitsRollback.toString,
                deletedFile,
                false,
                metadataStats.totalFilesDeleted,
                metadataStats.timeTakenInMillis,
                metadataStats.totalPartitionsAffected,
                rollbackMetadata.getVersion
              )
              allRows += row
            }
          }
        }.recover {
          case e: Exception =>
            log.warn(s"Failed to read rollback metadata for instant ${rollbackInstant.requestedTime}: ${e.getMessage}")
            val row = createErrorRowForCompleted(rollbackInstant)
            allRows += row
        }
      } else {
        Try {
          val rollbackPlan = RollbackUtils.getRollbackPlan(metaClient, rollbackInstant)
          val planStats = extractRollbackPlanStats(rollbackPlan)
          planStats.involvedPartitions.foreach { partitionPath =>
            val row = Row(
              rollbackInstant.requestedTime(),
              "", // state_transition_time - not available in pending
              rollbackInstant.getState.name(),
              rollbackInstant.getAction,
              "", // start_rollback_time - not available in pending
              partitionPath,
              planStats.instantToRollback,
              "", // deleted_file - not available in pending
              "", // success_delete_files - not available in pending
              planStats.totalFilesToDelete, // total_files_deleted
              "", // time_taken_in_millis - not available in pending
              planStats.totalPartitionsAffected,
              rollbackPlan.getVersion
            )
            allRows += row
          }
        }.recover {
          case e: Exception =>
            log.warn(s"Failed to read rollback plan for instant ${rollbackInstant.requestedTime}: ${e.getMessage}")
            val row = createErrorRowForPending(rollbackInstant)
            allRows += row
        }
      }
    }
    allRows.toSeq
  }

  private def createErrorRowForCompleted(instant: HoodieInstant): Row = {
    Row(
      instant.requestedTime(), instant.getCompletionTime, instant.getState.name(), instant.getAction,
      null, null, null, null, null, null, null, null, null, null
    )
  }

  private def createErrorRowForPending(instant: HoodieInstant): Row = {
    Row(
      instant.requestedTime(), null, instant.getState.name(), instant.getAction,
      null, null, null, null, null, null, null, null, null, null
    )
  }

  private def extractRollbackMetadataStats(rollbackMetadata: org.apache.hudi.avro.model.HoodieRollbackMetadata): RollbackMetadataStatistics = {
    val totalPartitionsAffected = Option(rollbackMetadata.getPartitionMetadata)
      .map(_.size())
      .getOrElse(0)

    RollbackMetadataStatistics(
      totalFilesDeleted = rollbackMetadata.getTotalFilesDeleted,
      totalPartitionsAffected = totalPartitionsAffected,
      timeTakenInMillis = rollbackMetadata.getTimeTakenInMillis
    )
  }

  private def extractRollbackPlanStats(rollbackPlan: org.apache.hudi.avro.model.HoodieRollbackPlan): RollbackPlanStatistics = {
    val instantToRollback = Option(rollbackPlan.getInstantToRollback)
      .map(instant => s"[${instant.getCommitTime}]")
      .getOrElse("")

    val rollbackRequests = Option(rollbackPlan.getRollbackRequests)
      .map(_.asScala)
      .getOrElse(Seq.empty)

    val involvedPartitions = rollbackRequests.map(_.getPartitionPath).toSet
    val totalPartitionsAffected = involvedPartitions.size

    val totalFilesToDelete = rollbackRequests.map { request =>
      Option(request.getFilesToBeDeleted).map(_.size()).getOrElse(0)
    }.sum

    RollbackPlanStatistics(
      instantToRollback = instantToRollback,
      totalPartitionsAffected = totalPartitionsAffected,
      totalFilesToDelete = totalFilesToDelete,
      involvedPartitions = involvedPartitions
    )
  }

  override def build: Procedure = new ShowRollbacksProcedure()
}

case class RollbackMetadataStatistics(totalFilesDeleted: Int,
                                      totalPartitionsAffected: Int,
                                      timeTakenInMillis: Long
                                     )

case class RollbackPlanStatistics(instantToRollback: String,
                                  totalPartitionsAffected: Int,
                                  totalFilesToDelete: Int,
                                  involvedPartitions: Set[String]
                                 )

object ShowRollbacksProcedure {
  val NAME = "show_rollbacks"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowRollbacksProcedure()
  }
}
