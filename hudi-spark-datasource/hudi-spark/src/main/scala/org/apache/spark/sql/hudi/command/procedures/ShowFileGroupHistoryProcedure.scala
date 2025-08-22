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
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util
import java.util.function.Supplier

import scala.collection.JavaConverters._

class ShowFileGroupHistoryProcedure extends BaseProcedure with ProcedureBuilder with Logging {

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.required(1, "file_group_id", DataTypes.StringType),
    ProcedureParameter.optional(2, "partition", DataTypes.StringType),
    ProcedureParameter.optional(3, "show_archived", DataTypes.BooleanType, false),
    ProcedureParameter.optional(4, "limit", DataTypes.IntegerType, 20)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("instant_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("completion_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("action", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("timeline_type", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("state", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("partition_path", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("file_group_id", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("file_id", DataTypes.StringType, nullable = true, Metadata.empty),
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
    StructField("delete_instant", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0)).get.asInstanceOf[String]
    val fileGroupId = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[String]
    val partition = getArgValueOrDefault(args, PARAMETERS(2)).asInstanceOf[Option[String]]
    val showArchived = getArgValueOrDefault(args, PARAMETERS(3)).get.asInstanceOf[Boolean]
    val limit = getArgValueOrDefault(args, PARAMETERS(4)).get.asInstanceOf[Int]

    val basePath = getBasePath(Option(tableName), Option.empty)
    val metaClient = createMetaClient(jsc, basePath)

    val fileGroupHistory = collectFileGroupHistory(metaClient, fileGroupId, partition, showArchived, limit)

    fileGroupHistory
  }

  private def collectFileGroupHistory(
                                       metaClient: HoodieTableMetaClient,
                                       fileGroupId: String,
                                       partition: Option[String],
                                       showArchived: Boolean,
                                       limit: Int): Seq[Row] = {

    val activeEntries = new util.ArrayList[FileGroupHistoryEntry]()
    val activeTimeline = metaClient.getActiveTimeline
    processTimeline(activeTimeline, fileGroupId, partition, "ACTIVE", activeEntries, limit)
    processCleanAndRollbackOperations(activeTimeline, fileGroupId, partition, "ACTIVE", activeEntries, limit)

    val archivedEntries = new util.ArrayList[FileGroupHistoryEntry]()
    if (showArchived) {
      try {
        val archivedTimeline = metaClient.getArchivedTimeline.reload()
        archivedTimeline.loadCompletedInstantDetailsInMemory()
        processTimeline(archivedTimeline, fileGroupId, partition, "ARCHIVED", archivedEntries, limit)
        processCleanAndRollbackOperations(archivedTimeline, fileGroupId, partition, "ARCHIVED", archivedEntries, limit)
      } catch {
        case e: Exception =>
          log.warn(s"Failed to process archived timeline: ${e.getMessage}")
      }
    }

    val allEntries = (activeEntries.asScala ++ archivedEntries.asScala).toList
    val sortedEntries = allEntries
      .sortBy(_.instantTime)(Ordering[String].reverse)
      .take(limit)

    val deletionInfo = checkForDeletions(metaClient, fileGroupId, partition, showArchived)

    sortedEntries.map { entry =>
      val deletion = deletionInfo.get(entry.fileId)
      Row(
        entry.instantTime,
        entry.completionTime,
        entry.action,
        entry.timelineType,
        entry.state,
        entry.partitionPath,
        entry.fileGroupId,
        entry.fileId,
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
        deletion.map(_.instant).orNull
      )
    }.toSeq
  }

  private def processTimeline(
                               timeline: HoodieTimeline,
                               targetFileGroupId: String,
                               targetPartition: Option[String],
                               timelineType: String,
                               entries: util.ArrayList[FileGroupHistoryEntry],
                               limit: Int): Unit = {

    val commits = timeline.getCommitsTimeline
    val instants = commits.getInstants.iterator().asScala.toList
      .sortBy(_.requestedTime)(Ordering[String].reverse)

    var foundCount = 0
    for (instant <- instants if foundCount < limit) {
      try {
        val initialSize = entries.size()
        processInstant(timeline, instant, targetFileGroupId, targetPartition, timelineType, entries)
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
                              targetFileGroupId: String,
                              targetPartition: Option[String],
                              timelineType: String,
                              entries: util.ArrayList[FileGroupHistoryEntry]): Unit = {

    if (instant.getState == HoodieInstant.State.INFLIGHT || instant.getState == HoodieInstant.State.REQUESTED) {
      val entry = createInflightRequestedEntry(instant, targetFileGroupId, targetPartition, timelineType)
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
            if (matchesFileGroup(writeStat, targetFileGroupId, targetPartition)) {
              val entry = createHistoryEntry(instant, writeStat, partitionPath, timelineType)
              entries.add(entry)
            }
          }
        }
      }
    }
  }

  private def readCommitMetadata(timeline: HoodieTimeline, instant: HoodieInstant): Option[HoodieCommitMetadata] = {
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

  private def matchesFileGroup(writeStat: HoodieWriteStat, targetFileGroupId: String, targetPartition: Option[String]): Boolean = {
    val fileId = writeStat.getFileId
    val partitionPath = writeStat.getPartitionPath
    val fileIdMatches = fileId == targetFileGroupId
    val partitionMatches = targetPartition.forall(_ == partitionPath)
    fileIdMatches && partitionMatches
  }

  private def createHistoryEntry(
                                  instant: HoodieInstant,
                                  writeStat: HoodieWriteStat,
                                  partitionPath: String,
                                  timelineType: String): FileGroupHistoryEntry = {

    val fileName = Option(writeStat.getPath).map(_.split("/").last).getOrElse("")
    val operationType = determineOperationType(writeStat)

    FileGroupHistoryEntry(
      instantTime = instant.requestedTime,
      completionTime = instant.getCompletionTime,
      action = instant.getAction,
      timelineType = timelineType,
      state = instant.getState.toString,
      partitionPath = partitionPath,
      fileGroupId = writeStat.getFileId,
      fileId = writeStat.getFileId,
      fileName = fileName,
      operationType = operationType,
      numWrites = writeStat.getNumWrites,
      numInserts = writeStat.getNumInserts,
      numUpdates = writeStat.getNumUpdateWrites,
      numDeletes = writeStat.getNumDeletes,
      fileSizeBytes = writeStat.getFileSizeInBytes,
      totalWriteBytes = writeStat.getTotalWriteBytes,
      prevCommit = writeStat.getPrevCommit
    )
  }

  private def determineOperationType(writeStat: HoodieWriteStat): String = {
    if (writeStat.getNumInserts > 0 && writeStat.getNumUpdateWrites == 0) "INSERT"
    else if (writeStat.getNumInserts == 0 && writeStat.getNumUpdateWrites > 0) "UPDATE"
    else if (writeStat.getNumInserts > 0 && writeStat.getNumUpdateWrites > 0) "UPSERT"
    else if (writeStat.getNumDeletes > 0) "DELETE"
    else "UNKNOWN"
  }

  private def checkForDeletions(
                                 metaClient: HoodieTableMetaClient,
                                 fileGroupId: String,
                                 partition: Option[String],
                                 showArchived: Boolean): Map[String, DeletionInfo] = {

    val deletions = scala.collection.mutable.Map[String, DeletionInfo]()

    checkDeletionsInTimeline(metaClient.getActiveTimeline, fileGroupId, partition, "ACTIVE", deletions)

    if (showArchived) {
      try {
        val archivedTimeline = metaClient.getArchivedTimeline.reload()
        archivedTimeline.loadCompletedInstantDetailsInMemory()
        checkDeletionsInTimeline(archivedTimeline, fileGroupId, partition, "ARCHIVED", deletions)
      } catch {
        case e: Exception =>
          log.warn(s"Failed to check deletions in archived timeline: ${e.getMessage}")
      }
    }

    deletions.toMap
  }

  private def processCleanAndRollbackOperations(
                                                 timeline: HoodieTimeline,
                                                 targetFileGroupId: String,
                                                 targetPartition: Option[String],
                                                 timelineType: String,
                                                 entries: util.ArrayList[FileGroupHistoryEntry],
                                                 limit: Int): Unit = {

    var foundCount = 0

    val cleanInstants = timeline.getCleanerTimeline.getInstants.iterator().asScala.toList
      .sortBy(_.requestedTime)(Ordering[String].reverse)

    for (instant <- cleanInstants if foundCount < limit) {
      val entryOpt = if (instant.getState == HoodieInstant.State.COMPLETED) {
        createCompletedCleanOrRollbackHistoryEntry(timeline, instant, targetFileGroupId, targetPartition, timelineType, "CLEAN")
      } else {
        Some(createInflightRequestedEntry(instant, targetFileGroupId, targetPartition, timelineType))
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
        createCompletedCleanOrRollbackHistoryEntry(timeline, instant, targetFileGroupId, targetPartition, timelineType, "ROLLBACK")
      } else {
        Some(createInflightRequestedEntry(instant, targetFileGroupId, targetPartition, timelineType))
      }
      entryOpt.foreach { entry =>
        entries.add(entry)
        foundCount += 1
      }
    }
  }

  private def createCompletedCleanOrRollbackHistoryEntry(
                                                          timeline: HoodieTimeline,
                                                          instant: HoodieInstant,
                                                          targetFileGroupId: String,
                                                          targetPartition: Option[String],
                                                          timelineType: String,
                                                          operationType: String): Option[FileGroupHistoryEntry] = {
    try {
      val cleanMetadata = timeline.readCleanMetadata(instant)
      val totalFilesDeleted = cleanMetadata.getTotalFilesDeleted

      val wasThisFileGroupCleaned = targetPartition match {
        case Some(partition) =>
          Option(cleanMetadata.getPartitionMetadata.get(partition))
            .exists(_.getSuccessDeleteFiles.asScala.exists(_.contains(targetFileGroupId)))
        case None =>
          cleanMetadata.getPartitionMetadata.asScala.values
            .exists(_.getSuccessDeleteFiles.asScala.exists(_.contains(targetFileGroupId)))
      }

      if (wasThisFileGroupCleaned) {
        Some(FileGroupHistoryEntry(
          instantTime = instant.requestedTime,
          completionTime = instant.getCompletionTime,
          action = instant.getAction,
          timelineType = timelineType,
          state = instant.getState.toString,
          partitionPath = targetPartition.getOrElse("ALL_PARTITIONS"),
          fileGroupId = targetFileGroupId,
          fileId = targetFileGroupId,
          fileName = s"${operationType}_$targetFileGroupId",
          operationType = operationType,
          numWrites = 0L, // Clean operations don't write records
          numInserts = 0L,
          numUpdates = 0L,
          numDeletes = totalFilesDeleted,
          fileSizeBytes = 0L,
          totalWriteBytes = 0L,
          prevCommit = instant.requestedTime
        ))
      } else {
        None
      }
    } catch {
      case e: Exception =>
        log.warn(s"Failed to read clean metadata for ${instant.requestedTime}: ${e.getMessage}")
        Some(createInflightRequestedEntry(instant, targetFileGroupId, targetPartition, timelineType))
    }
  }

  private def createInflightRequestedEntry(
                                            instant: HoodieInstant,
                                            targetFileGroupId: String,
                                            targetPartition: Option[String],
                                            timelineType: String): FileGroupHistoryEntry = {

    FileGroupHistoryEntry(
      instantTime = instant.requestedTime,
      completionTime = instant.getCompletionTime,
      action = instant.getAction,
      timelineType = timelineType,
      state = instant.getState.toString,
      partitionPath = targetPartition.getOrElse("NULL"),
      fileGroupId = targetFileGroupId,
      fileId = targetFileGroupId,
      fileName = "NULL",
      operationType = "UNVERIFIED",
      numWrites = -1L, // Use -1 to differentiate it from actual 0
      numInserts = -1L,
      numUpdates = -1L,
      numDeletes = -1L,
      fileSizeBytes = -1L,
      totalWriteBytes = -1L,
      prevCommit = "UNVERIFIED"
    )
  }

  private def checkDeletionsInTimeline(
                                        timeline: HoodieTimeline,
                                        fileGroupId: String,
                                        partition: Option[String],
                                        timelineType: String,
                                        deletions: scala.collection.mutable.Map[String, DeletionInfo]): Unit = {

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
            if deletedFile.contains(fileGroupId)
          } {
            deletions(fileGroupId) = DeletionInfo("clean", instant.requestedTime, timelineType)
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
            if deletedFile.contains(fileGroupId)
          } {
            deletions(fileGroupId) = DeletionInfo("rollback", instant.requestedTime, timelineType)
          }
        }
      } catch {
        case e: Exception =>
          log.warn(s"Failed to process rollback instant ${instant.requestedTime}: ${e.getMessage}")
      }
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

case class FileGroupHistoryEntry(
                                  instantTime: String,
                                  completionTime: String,
                                  action: String,
                                  timelineType: String,
                                  state: String,
                                  partitionPath: String,
                                  fileGroupId: String,
                                  fileId: String,
                                  fileName: String,
                                  operationType: String,
                                  numWrites: Long,
                                  numInserts: Long,
                                  numUpdates: Long,
                                  numDeletes: Long,
                                  fileSizeBytes: Long,
                                  totalWriteBytes: Long,
                                  prevCommit: String
                                )

case class DeletionInfo(action: String, instant: String, timelineType: String)
