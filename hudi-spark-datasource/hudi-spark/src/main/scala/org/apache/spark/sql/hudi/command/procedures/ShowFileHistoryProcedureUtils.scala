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

import org.apache.hudi.common.model.{HoodieCommitMetadata, HoodieWriteStat}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.util.ClusteringUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util
import scala.collection.JavaConverters._

object FileHistoryType extends Enumeration {
  type FileHistoryType = Value
  val FILE_GROUP, FILE_SLICE = Value
}

case class HistoryEntry(
                         instantTime: String,
                         completionTime: String,
                         action: String,
                         timelineType: String,
                         state: String,
                         partitionPath: String,
                         fileGroupId: String,
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

case class DeletionInfo(action: String, instant: String, timelineType: String)

object ShowFileHistoryProcedureUtils extends Logging {

  import FileHistoryType._

  val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("instant_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("completion_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("action", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("timeline_type", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("state", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("partition_path", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("file_group_id", DataTypes.StringType, nullable = true, Metadata.empty),
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
    if (writeStat.getNumInserts > 0 && writeStat.getNumUpdateWrites == 0) "INSERT"
    else if (writeStat.getNumInserts == 0 && writeStat.getNumUpdateWrites > 0) "UPDATE"
    else if (writeStat.getNumInserts > 0 && writeStat.getNumUpdateWrites > 0) "UPSERT"
    else if (writeStat.getNumDeletes > 0) "DELETE"
    else "UNKNOWN"
  }

  def matchesTarget(writeStat: HoodieWriteStat,
                    target: String,
                    targetPartition: Option[String],
                    historyType: FileHistoryType): Boolean = {
    historyType match {
      case FILE_GROUP => matchesFileGroup(writeStat, target, targetPartition)
      case FILE_SLICE => matchesFileName(writeStat, target, targetPartition)
    }
  }

  private def matchesFileGroup(writeStat: HoodieWriteStat, targetFileGroupId: String, targetPartition: Option[String]): Boolean = {
    val fileId = writeStat.getFileId
    val partitionPath = writeStat.getPartitionPath
    val fileIdMatches = fileId == targetFileGroupId
    val partitionMatches = targetPartition.isEmpty || targetPartition.get == partitionPath
    fileIdMatches && partitionMatches
  }

  private def matchesFileName(writeStat: HoodieWriteStat, targetFileName: String, targetPartition: Option[String]): Boolean = {
    val partitionPath = writeStat.getPartitionPath
    val partitionMatches = targetPartition.isEmpty || targetPartition.get == partitionPath

    if (!partitionMatches) return false

    val filePath = writeStat.getPath
    if (filePath != null && filePath.contains(targetFileName)) {
      return true
    }
    val fileId = writeStat.getFileId
    if (fileId != null && fileId.contains(targetFileName)) {
      return true
    }

    false
  }

  def processTimeline(
                       timeline: HoodieTimeline,
                       targetIdOrName: String,
                       targetPartition: Option[String],
                       timelineType: String,
                       entries: util.ArrayList[HistoryEntry],
                       limit: Int,
                       historyType: FileHistoryType): Unit = {

    val commits = timeline.getCommitsTimeline
    val instants = commits.getInstants.iterator().asScala.toList
      .sortBy(_.requestedTime)(Ordering[String].reverse)

    var foundCount = 0
    for (instant <- instants if foundCount < limit) {
      try {
        val initialSize = entries.size()
        processInstant(timeline, instant, targetIdOrName, targetPartition, timelineType, entries, historyType)
        if (entries.size() > initialSize) {
          foundCount += 1
        }
      } catch {
        case e: Exception =>
          log.warn(s"Failed to process instant ${instant.requestedTime}: ${e.getMessage}")
      }
    }
  }

  private def processInstant(
                              timeline: HoodieTimeline,
                              instant: HoodieInstant,
                              targetFileIdOrName: String,
                              targetPartition: Option[String],
                              timelineType: String,
                              entries: util.ArrayList[HistoryEntry],
                              historyType: FileHistoryType): Unit = {

    if (instant.getState == HoodieInstant.State.INFLIGHT || instant.getState == HoodieInstant.State.REQUESTED) {
      val entry = createInflightRequestedEntry(instant, targetFileIdOrName, targetPartition, timelineType, historyType)
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
            if (matchesTarget(writeStat, targetFileIdOrName, targetPartition, historyType)) {
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
                         partitionPath: String,
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
      partitionPath = partitionPath,
      fileGroupId = writeStat.getFileId,
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
                                   target: String,
                                   targetPartition: Option[String],
                                   timelineType: String,
                                   historyType: FileHistoryType): HistoryEntry = {

    val (displayFileGroupId, displayFileName) = historyType match {
      case FILE_GROUP => (target, "")
      case FILE_SLICE => ("", target)
    }

    HistoryEntry(
      instantTime = instant.requestedTime,
      completionTime = instant.getCompletionTime,
      action = instant.getAction,
      timelineType = timelineType,
      state = instant.getState.toString,
      partitionPath = targetPartition.getOrElse(""),
      fileGroupId = displayFileGroupId,
      fileName = displayFileName,
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
                        target: String,
                        partition: Option[String],
                        showArchived: Boolean,
                        historyType: FileHistoryType): Map[String, DeletionInfo] = {

    val deletions = scala.collection.mutable.Map[String, DeletionInfo]()

    checkDeletionsInTimeline(metaClient.getActiveTimeline, target, partition, "ACTIVE", deletions, historyType)

    if (showArchived) {
      try {
        val archivedTimeline = metaClient.getArchivedTimeline.reload()
        archivedTimeline.loadCompletedInstantDetailsInMemory()
        checkDeletionsInTimeline(archivedTimeline, target, partition, "ARCHIVED", deletions, historyType)
      } catch {
        case e: Exception =>
          log.warn(s"Failed to check deletions in archived timeline: ${e.getMessage}")
      }
    }

    deletions.toMap
  }

  def checkDeletionsInTimeline(timeline: HoodieTimeline,
                               target: String,
                               partition: Option[String],
                               timelineType: String,
                               deletions: scala.collection.mutable.Map[String, DeletionInfo],
                               historyType: FileHistoryType): Unit = {

    val cleanInstants = timeline.getCleanerTimeline.getInstants.iterator().asScala
    for (instant <- cleanInstants) {
      try {
        if (instant.getState == HoodieInstant.State.COMPLETED) {
          val cleanMetadata = timeline.readCleanMetadata(instant)
          val partitionsToCheck = partition.map(Set(_)).getOrElse(cleanMetadata.getPartitionMetadata.keySet().asScala.toSet)

          for {
            partitionPath <- partitionsToCheck
            partitionMetadata <- Option(cleanMetadata.getPartitionMetadata.get(partitionPath))
            deletedFile <- partitionMetadata.getSuccessDeleteFiles.asScala
            if matchesDeletedFile(deletedFile, target, historyType)
          } {
            deletions(target) = DeletionInfo("clean", instant.requestedTime, timelineType)
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
          val partitionsToCheck = partition.map(Set(_)).getOrElse(rollbackMetadata.getPartitionMetadata.keySet().asScala.toSet)

          for {
            partitionPath <- partitionsToCheck
            partitionMetadata <- Option(rollbackMetadata.getPartitionMetadata.get(partitionPath))
            deletedFile <- partitionMetadata.getSuccessDeleteFiles.asScala
            if matchesDeletedFile(deletedFile, target, historyType)
          } {
            deletions(target) = DeletionInfo("rollback", instant.requestedTime, timelineType)
          }
        }
      } catch {
        case e: Exception =>
          log.warn(s"Failed to process rollback instant ${instant.requestedTime}: ${e.getMessage}")
      }
    }
  }

  def processCleanAndRollbackOperations(timeline: HoodieTimeline,
                                        target: String,
                                        targetPartition: Option[String],
                                        timelineType: String,
                                        entries: util.ArrayList[HistoryEntry],
                                        limit: Int,
                                        historyType: FileHistoryType): Unit = {

    var foundCount = 0
    val cleanInstants = timeline.getCleanerTimeline.getInstants.iterator().asScala.toList
      .sortBy(_.requestedTime)(Ordering[String].reverse)

    for (instant <- cleanInstants if foundCount < limit) {
      val entryOpt = if (instant.getState == HoodieInstant.State.COMPLETED) {
        createCompletedCleanOrRollbackHistoryEntry(timeline, instant, target, targetPartition, timelineType, "clean", historyType)
      } else {
        Some(createInflightRequestedEntry(instant, target, targetPartition, timelineType, historyType))
      }

      entryOpt.foreach { entry =>
        entries.add(entry)
        foundCount += 1
      }
    }

    foundCount = 0

    val rollbackInstants = timeline.getRollbackTimeline.getInstants.iterator().asScala.toList
      .sortBy(_.requestedTime)(Ordering[String].reverse)

    for (instant <- rollbackInstants if foundCount < limit) {
      val entryOpt = if (instant.getState == HoodieInstant.State.COMPLETED) {
        createCompletedCleanOrRollbackHistoryEntry(timeline, instant, target, targetPartition, timelineType, "rollback", historyType)
      } else {
        Some(createInflightRequestedEntry(instant, target, targetPartition, timelineType, historyType))
      }
      entryOpt.foreach { entry =>
        entries.add(entry)
        foundCount += 1
      }
    }
  }

  def createCompletedCleanOrRollbackHistoryEntry(timeline: HoodieTimeline,
                                                 instant: HoodieInstant,
                                                 target: String,
                                                 targetPartition: Option[String],
                                                 timelineType: String,
                                                 action: String,
                                                 historyType: FileHistoryType): Option[HistoryEntry] = {

    try {
      val (displayFileGroupId, displayFileName) = historyType match {
        case FILE_GROUP => (target, "")
        case FILE_SLICE => ("", target)
      }
      val (wasSpecificallyAffected, totalDeleted) = if (action == "clean") {
        val cleanMetadata = timeline.readCleanMetadata(instant)
        val partitionsToCheck = targetPartition.map(Set(_)).getOrElse(cleanMetadata.getPartitionMetadata.keySet().asScala.toSet)

        val affected = partitionsToCheck.exists { partitionPath =>
          Option(cleanMetadata.getPartitionMetadata.get(partitionPath)).exists { partitionMetadata =>
            partitionMetadata.getSuccessDeleteFiles.asScala.exists(deletedFile =>
              matchesDeletedFile(deletedFile, target, historyType))
          }
        }
        (affected, cleanMetadata.getTotalFilesDeleted)
      } else {
        val rollbackMetadata = timeline.readRollbackMetadata(instant)
        val partitionsToCheck = targetPartition.map(Set(_)).getOrElse(rollbackMetadata.getPartitionMetadata.keySet().asScala.toSet)

        val affected = partitionsToCheck.exists { partitionPath =>
          Option(rollbackMetadata.getPartitionMetadata.get(partitionPath)).exists { partitionMetadata =>
            partitionMetadata.getSuccessDeleteFiles.asScala.exists(deletedFile =>
              matchesDeletedFile(deletedFile, target, historyType))
          }
        }
        (affected, rollbackMetadata.getTotalFilesDeleted)
      }

      if (wasSpecificallyAffected) {
        Some(HistoryEntry(
          instantTime = instant.requestedTime,
          completionTime = instant.getCompletionTime,
          action = action,
          timelineType = timelineType,
          state = instant.getState.toString,
          partitionPath = targetPartition.getOrElse("ALL_PARTITIONS"),
          fileGroupId = displayFileGroupId,
          fileName = displayFileName,
          operationType = action,
          numWrites = 0,
          numInserts = 0,
          numUpdates = 0,
          numDeletes = totalDeleted,
          fileSizeBytes = 0,
          totalWriteBytes = 0,
          prevCommit = instant.requestedTime,
          totalWriteErrors = 0,
          totalScanTimeMs = 0,
          totalUpsertTimeMs = 0,
          totalCreateTimeMs = 0,
          prevBaseFile = None,
          columnStatsAvailable = false
        ))
      } else {
        None
      }
    } catch {
      case e: Exception =>
        log.warn(s"Failed to create clean/rollback entry for ${instant.requestedTime}: ${e.getMessage}")
        Some(createInflightRequestedEntry(instant, target, targetPartition, timelineType, historyType))
    }
  }

  private def matchesDeletedFile(deletedFile: String, target: String, historyType: FileHistoryType): Boolean = {
    historyType match {
      case FILE_GROUP =>
        val fileGroupId = extractFileGroupIdFromDeletedFile(deletedFile)
        fileGroupId == target
      case FILE_SLICE =>
        val fileName = extractFileNameFromDeletedFile(deletedFile)
        fileName.contains(target) || fileName == target
    }
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

  private def extractFileNameFromDeletedFile(deletedFile: String): String = {
    deletedFile.split("/").last
  }
}