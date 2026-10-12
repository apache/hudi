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

import org.apache.hudi.common.model.{HoodieCommitMetadata, HoodieWriteStat, WriteOperationType}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.util.ClusteringUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util

import scala.collection.JavaConverters._

case class HistoryEntry(instantTime: String,
                        completionTime: String,
                        action: String,
                        timelineType: String,
                        state: String,
                        partitionPath: String,
                        fileName: String,
                        operationType: String,
                        numWrites: Long,
                        numInserts: Long,
                        numUpdates: Long,
                        numDeletes: Long,
                        fileSizeBytes: Long,
                        totalWriteBytes: Long,
                        prevCommit: String,
                        totalWriteErrors: Long,
                        totalScanTimeMs: Long,
                        totalUpsertTimeMs: Long,
                        totalCreateTimeMs: Long,
                        prevBaseFile: Option[String],
                        columnStatsAvailable: Boolean
                       )

case class DeletionInfo(action: String, instant: String, timelineType: String, deleteStatus: String = HoodieProcedureUtils.STATUS_SUCCESS)

case class ReplacementInfo(action: String, instant: String, timelineType: String)

object ShowFileHistoryProcedureUtils extends Logging {

  val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("instant_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("completion_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("action", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("timeline_type", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("state", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("partition_path", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("file_name", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("operation_type", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("num_writes", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("num_inserts", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("num_updates", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("num_deletes", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("file_size_bytes", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_write_bytes", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("prev_commit", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("was_deleted", DataTypes.BooleanType, nullable = true, Metadata.empty),
    StructField("delete_action", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("delete_instant", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("delete_status", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("was_replaced", DataTypes.BooleanType, nullable = true, Metadata.empty),
    StructField("replace_action", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("replace_instant", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("total_write_errors", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_scan_time_ms", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_upsert_time_ms", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_create_time_ms", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("prev_base_file", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("column_stats_available", DataTypes.BooleanType, nullable = true, Metadata.empty)
  ))

  def readCommitMetadata(timeline: HoodieTimeline, instant: HoodieInstant): Option[HoodieCommitMetadata] = {
    try {
      if (ClusteringUtils.isClusteringOrReplaceCommitAction(instant.getAction)) {
        Option(timeline.readReplaceCommitMetadata(instant))
      } else {
        Option(timeline.readCommitMetadata(instant))
      }
    } catch {
      case e: Exception =>
        log.warn(s"Failed to read commit metadata for ${instant.requestedTime}: ${e.getMessage}")
        None
    }
  }

  def determineOperationType(writeStat: HoodieWriteStat): String = {
    val hasInserts = writeStat.getNumInserts > 0
    val hasUpdates = writeStat.getNumUpdateWrites > 0
    val hasDeletes = writeStat.getNumDeletes > 0

    val operations = Seq(
      if (hasInserts) Some("INSERT") else None,
      if (hasUpdates) Some("UPDATE") else None,
      if (hasDeletes) Some("DELETE") else None
    ).flatten

    if (operations.nonEmpty) {
      operations.mkString("_AND_")
    } else {
      "UNKNOWN"
    }
  }

  def processWriteTimeline(timeline: HoodieTimeline,
                           fileGroupId: String,
                           targetPartition: Option[String],
                           timelineType: String,
                           entries: util.ArrayList[HistoryEntry],
                           limit: Int,
                           startTime: String,
                           endTime: String): Unit = {

    val writeTimeline = timeline.getWriteTimeline
    val allInstants = writeTimeline.getInstants.asScala.toSeq

    val filteredInstants = allInstants.filter { instant =>
      val instantTime = instant.requestedTime()
      (startTime.isEmpty || instantTime >= startTime) &&
      (endTime.isEmpty || instantTime <= endTime)
    }

    if (startTime.nonEmpty && endTime.nonEmpty) {
      for (instant <- filteredInstants) {
        try {
          processInstant(timeline, instant, fileGroupId, targetPartition, timelineType, entries)
        } catch {
          case e: Exception =>
            log.warn(s"Failed to process instant ${instant.requestedTime}: ${e.getMessage}")
        }
      }
    } else {
      var foundCount = 0
      for (instant <- filteredInstants if foundCount < limit) {
        try {
          val initialSize = entries.size()
          processInstant(timeline, instant, fileGroupId, targetPartition, timelineType, entries)
          if (entries.size() > initialSize) {
            foundCount += 1
          }
        } catch {
          case e: Exception =>
            log.warn(s"Failed to process instant ${instant.requestedTime}: ${e.getMessage}")
        }
      }
    }
  }

  private def processInstant(timeline: HoodieTimeline,
                             instant: HoodieInstant,
                             fileGroupId: String,
                             targetPartition: Option[String],
                             timelineType: String,
                             entries: util.ArrayList[HistoryEntry]): Unit = {

    if (instant.getState == HoodieInstant.State.INFLIGHT || instant.getState == HoodieInstant.State.REQUESTED) {
      val entry = createInflightRequestedEntry(instant, targetPartition, timelineType)
      entries.add(entry)
    }

    val commitMetadata = readCommitMetadata(timeline, instant)
    if (commitMetadata.isDefined) {
      val metadata = commitMetadata.get
      val partitionsToCheck = targetPartition.map(Set(_)).getOrElse(metadata.getPartitionToWriteStats.keySet().asScala.toSet)

      for (partitionPath <- partitionsToCheck) {
        val writeStatsOpt = Option(metadata.getPartitionToWriteStats.get(partitionPath))
        if (writeStatsOpt.isDefined) {
          val writeStats = writeStatsOpt.get.asScala
          for (writeStat <- writeStats) {
            if (writeStat.getFileId == fileGroupId) {
              val entry = createHistoryEntry(instant, writeStat, partitionPath, timelineType)
              entries.add(entry)
            }
          }
        }
      }
    }
  }

  def createHistoryEntry(instant: HoodieInstant,
                         writeStat: HoodieWriteStat,
                         targetPartition: String,
                         timelineType: String): HistoryEntry = {

    val fileName = Option(writeStat.getPath).map(_.split("/").last).getOrElse("")
    val operationType = determineOperationType(writeStat)

    val runtimeStats = Option(writeStat.getRuntimeStats)
    val scanTime = runtimeStats.map(_.getTotalScanTime).getOrElse(0L)
    val upsertTime = runtimeStats.map(_.getTotalUpsertTime).getOrElse(0L)
    val createTime = runtimeStats.map(_.getTotalCreateTime).getOrElse(0L)

    HistoryEntry(
      instantTime = instant.requestedTime,
      completionTime = instant.getCompletionTime,
      action = instant.getAction,
      timelineType = timelineType,
      state = instant.getState.toString,
      partitionPath = targetPartition,
      fileName = fileName,
      operationType = operationType,
      numWrites = writeStat.getNumWrites,
      numInserts = writeStat.getNumInserts,
      numUpdates = writeStat.getNumUpdateWrites,
      numDeletes = writeStat.getNumDeletes,
      fileSizeBytes = writeStat.getFileSizeInBytes,
      totalWriteBytes = writeStat.getTotalWriteBytes,
      prevCommit = writeStat.getPrevCommit,
      totalWriteErrors = writeStat.getTotalWriteErrors,
      totalScanTimeMs = scanTime,
      totalUpsertTimeMs = upsertTime,
      totalCreateTimeMs = createTime,
      prevBaseFile = Option(writeStat.getPrevBaseFile),
      columnStatsAvailable = writeStat.getColumnStats.isPresent
    )
  }

  def createInflightRequestedEntry(instant: HoodieInstant,
                                   targetPartition: Option[String],
                                   timelineType: String): HistoryEntry = {

    HistoryEntry(
      instantTime = instant.requestedTime,
      completionTime = instant.getCompletionTime,
      action = instant.getAction,
      timelineType = timelineType,
      state = instant.getState.toString,
      partitionPath = targetPartition.getOrElse(""),
      fileName = "",
      operationType = "UNKNOWN",
      numWrites = -1,
      numInserts = -1,
      numUpdates = -1,
      numDeletes = -1,
      fileSizeBytes = -1,
      totalWriteBytes = -1,
      prevCommit = "",
      totalWriteErrors = -1,
      totalScanTimeMs = -1,
      totalUpsertTimeMs = -1,
      totalCreateTimeMs = -1,
      prevBaseFile = None,
      columnStatsAvailable = false
    )
  }

  def checkForDeletions(metaClient: HoodieTableMetaClient,
                        fileGroupId: String,
                        targetPartition: Option[String],
                        showArchived: Boolean): Map[String, DeletionInfo] = {

    val deletions = scala.collection.mutable.Map[String, DeletionInfo]()

    checkDeletionsInTimeline(metaClient.getActiveTimeline, fileGroupId, targetPartition, HoodieProcedureUtils.TIMELINE_TYPE_ACTIVE, deletions)

    if (showArchived) {
      try {
        val archivedTimeline = metaClient.getArchivedTimeline.reload()
        archivedTimeline.loadCompletedInstantDetailsInMemory()
        checkDeletionsInTimeline(archivedTimeline, fileGroupId, targetPartition, HoodieProcedureUtils.TIMELINE_TYPE_ARCHIVED, deletions)
      } catch {
        case e: Exception =>
          log.warn(s"Failed to check deletions in archived timeline: ${e.getMessage}")
      }
    }

    deletions.toMap
  }

  def checkForReplacements(metaClient: HoodieTableMetaClient,
                           fileGroupId: String,
                           targetPartition: Option[String],
                           showArchived: Boolean): Map[String, ReplacementInfo] = {

    val replacements = scala.collection.mutable.Map[String, ReplacementInfo]()

    checkReplacementsInTimeline(metaClient.getActiveTimeline, fileGroupId, targetPartition, HoodieProcedureUtils.TIMELINE_TYPE_ACTIVE, replacements)

    if (showArchived) {
      try {
        val archivedTimeline = metaClient.getArchivedTimeline.reload()
        archivedTimeline.loadCompletedInstantDetailsInMemory()
        checkReplacementsInTimeline(archivedTimeline, fileGroupId, targetPartition, HoodieProcedureUtils.TIMELINE_TYPE_ARCHIVED, replacements)
      } catch {
        case e: Exception =>
          log.warn(s"Failed to check replacements in archived timeline: ${e.getMessage}")
      }
    }

    replacements.toMap
  }

  private def checkDeletionsInTimeline(timeline: HoodieTimeline,
                                       fileGroupId: String,
                                       targetPartition: Option[String],
                                       timelineType: String,
                                       deletions: scala.collection.mutable.Map[String, DeletionInfo]): Unit = {

    val cleanInstants = timeline.getCleanerTimeline.getInstants.iterator().asScala
    for (instant <- cleanInstants) {
      try {
        if (instant.getState == HoodieInstant.State.COMPLETED) {
          val cleanMetadata = timeline.readCleanMetadata(instant)
          val partitionsToCheck = targetPartition.map(Set(_)).getOrElse(cleanMetadata.getPartitionMetadata.keySet().asScala.toSet)

          for {
            partitionPath <- partitionsToCheck
            partitionMetadata <- Option(cleanMetadata.getPartitionMetadata.get(partitionPath))
          } {
            for {
              deletedFile <- partitionMetadata.getSuccessDeleteFiles.asScala
              if matchesDeletedFileGroup(deletedFile, fileGroupId)
            } {
              val deletedFileName = extractActualFileName(deletedFile)
              deletions(deletedFileName) = DeletionInfo(HoodieTimeline.CLEAN_ACTION, instant.requestedTime, timelineType, HoodieProcedureUtils.STATUS_SUCCESS)
            }
            for {
              failedDeleteFile <- partitionMetadata.getFailedDeleteFiles.asScala
              if matchesDeletedFileGroup(failedDeleteFile, fileGroupId)
            } {
              val failedDeleteFileName = extractActualFileName(failedDeleteFile)
              deletions(failedDeleteFileName) = DeletionInfo(HoodieTimeline.CLEAN_ACTION, instant.requestedTime, timelineType, HoodieProcedureUtils.STATUS_FAILED)
            }
          }
        }
      } catch {
        case e: Exception =>
          log.warn(s"Failed to process clean instant ${instant.requestedTime}: ${e.getMessage}")
      }
    }

    val rollbackInstants = timeline.getRollbackTimeline.getInstants.iterator().asScala
    for (instant <- rollbackInstants) {
      try {
        if (instant.getState == HoodieInstant.State.COMPLETED) {
          val rollbackMetadata = timeline.readRollbackMetadata(instant)
          val partitionsToCheck = targetPartition.map(Set(_)).getOrElse(rollbackMetadata.getPartitionMetadata.keySet().asScala.toSet)

          for {
            partitionPath <- partitionsToCheck
            partitionMetadata <- Option(rollbackMetadata.getPartitionMetadata.get(partitionPath))
          } {
            for {
              deletedFile <- partitionMetadata.getSuccessDeleteFiles.asScala
              if matchesDeletedFileGroup(deletedFile, fileGroupId)
            } {
              val deletedFileName = extractActualFileName(deletedFile)
              deletions(deletedFileName) = DeletionInfo(HoodieTimeline.ROLLBACK_ACTION, instant.requestedTime, timelineType, HoodieProcedureUtils.STATUS_SUCCESS)
            }
            for {
              failedDeleteFile <- partitionMetadata.getFailedDeleteFiles.asScala
              if matchesDeletedFileGroup(failedDeleteFile, fileGroupId)
            } {
              val failedDeleteFileName = extractActualFileName(failedDeleteFile)
              deletions(failedDeleteFileName) = DeletionInfo(HoodieTimeline.ROLLBACK_ACTION, instant.requestedTime, timelineType, HoodieProcedureUtils.STATUS_FAILED)
            }
          }
        }
      } catch {
        case e: Exception =>
          log.warn(s"Failed to process rollback instant ${instant.requestedTime}: ${e.getMessage}")
      }
    }
  }

  private def checkReplacementsInTimeline(timeline: HoodieTimeline,
                                          fileGroupId: String,
                                          targetPartition: Option[String],
                                          timelineType: String,
                                          replacements: scala.collection.mutable.Map[String, ReplacementInfo]): Unit = {

    val replacementInstants = timeline.getCommitsAndCompactionTimeline.getInstants.iterator().asScala
    for (instant <- replacementInstants) {
      try {
        if (instant.getState == HoodieInstant.State.COMPLETED) {
          val commitMetadata = readCommitMetadata(timeline, instant)
          if (commitMetadata.isDefined) {
            val metadata = commitMetadata.get

            val isCompaction = (metadata.getCompacted != null && metadata.getCompacted.booleanValue()) ||
              (metadata.getOperationType != null && metadata.getOperationType.toString.toLowerCase.equals(WriteOperationType.COMPACT.value()))
            val isClustering = instant.getAction.equals(HoodieTimeline.REPLACE_COMMIT_ACTION)

            if (isCompaction || isClustering) {
              val operationType = if (isCompaction) HoodieTimeline.COMPACTION_ACTION else HoodieTimeline.CLUSTERING_ACTION

              if (isClustering) {
                metadata match {
                  case replaceMetadata: org.apache.hudi.common.model.HoodieReplaceCommitMetadata =>
                    val partitionToReplaceFileIds = replaceMetadata.getPartitionToReplaceFileIds
                    if (partitionToReplaceFileIds != null) {
                      val partitionsToCheck = targetPartition.map(Set(_)).getOrElse(partitionToReplaceFileIds.keySet().asScala.toSet)
                      for (partitionPath <- partitionsToCheck) {
                        val replacedFileIds = Option(partitionToReplaceFileIds.get(partitionPath))
                        if (replacedFileIds.isDefined) {
                          replacedFileIds.get.asScala.foreach { replacedFileId =>
                            if (fileGroupId.isEmpty || fileGroupId.contains(replacedFileId)) {
                              replacements(replacedFileId) = ReplacementInfo(operationType, instant.requestedTime, timelineType)
                            }
                          }
                        }
                      }
                    }
                }
              } else {
                val partitionsToCheck = targetPartition.map(Set(_)).getOrElse(metadata.getPartitionToWriteStats.keySet().asScala.toSet)
                for {
                  partitionPath <- partitionsToCheck
                  writeStats <- Option(metadata.getPartitionToWriteStats.get(partitionPath))
                  writeStat <- writeStats.asScala
                  if writeStat.getPrevCommit != null && writeStat.getFileId == fileGroupId
                  prevBaseFile <- Option(writeStat.getPrevBaseFile)
                  if prevBaseFile.nonEmpty
                } {
                  val replacedFileName = prevBaseFile.split("/").last
                  replacements(replacedFileName) = ReplacementInfo(operationType, instant.requestedTime, timelineType)
                }
              }
            }
          }
        }
      } catch {
        case e: Exception =>
          log.warn(s"Failed to process ${instant.getAction} instant ${instant.requestedTime}: ${e.getMessage}")
      }
    }
  }


  private def extractActualFileName(filePath: String): String = {
    filePath.split("/").last
  }

  private def matchesDeletedFileGroup(deletedFile: String, fileGroupId: String): Boolean = {
    val extractedFileGroupId = extractFileGroupIdFromDeletedFile(deletedFile)
    extractedFileGroupId == fileGroupId
  }

  private def extractFileGroupIdFromDeletedFile(deletedFile: String): String = {
    val fileName = deletedFile.split("/").last
    val fileNameWithoutExt = fileName.split("\\.").head
    val parts = fileNameWithoutExt.split("_")
    if (parts.length > 1) {
      parts(0)
    } else {
      fileNameWithoutExt
    }
  }
}
