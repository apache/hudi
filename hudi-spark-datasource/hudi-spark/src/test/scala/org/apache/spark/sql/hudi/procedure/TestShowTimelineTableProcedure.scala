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

package org.apache.spark.sql.hudi.procedure

import org.apache.hudi.DummyActiveAction
import org.apache.hudi.HoodieSparkUtils
import org.apache.hudi.avro.model.HoodieArchivedMetaEntry
import org.apache.hudi.client.timeline.versioning.v2.LSMTimelineWriter
import org.apache.hudi.common.engine.{HoodieEngineContext, HoodieLocalEngineContext}
import org.apache.hudi.common.engine.LocalTaskContextSupplier
import org.apache.hudi.common.model.{ActionType, HoodieArchivedLogFile, HoodieAvroIndexedRecord, HoodieCommitMetadata, HoodieLogFile, HoodieRecord, WriteOperationType}
import org.apache.hudi.common.table.{HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.common.table.log.HoodieLogFormat
import org.apache.hudi.common.table.log.block.{HoodieAvroDataBlock, HoodieLogBlock}
import org.apache.hudi.common.table.timeline.{ActiveAction, HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion
import org.apache.hudi.common.table.timeline.versioning.v1.{ArchivedTimelineV1, InstantGeneratorV1}
import org.apache.hudi.common.testutils.{HoodieTestTable, HoodieTestUtils}
import org.apache.hudi.common.testutils.HoodieTestUtils.{convertMetadataToByteArray, INSTANT_GENERATOR}
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.index.HoodieIndex

import org.apache.avro.generic.IndexedRecord
import org.apache.spark.sql.Row
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

import java.util

import scala.collection.JavaConverters._

/**
 * Lightweight tests for show_timeline procedure with precise assertions.
 * Uses direct timeline setup similar to TestArchivedTimelineV1 and TestArchivedTimelineV2
 * instead of end-to-end table operations for creating timeline.
 */
class TestShowTimelineTableProcedure extends HoodieSparkSqlTestBase {

  /**
   * Helper: Create ActiveAction with embedded plan to avoid reading from active timeline.
   * This prevents inconsistency where requested instants appear in active timeline.
   */
  private class ActiveActionWithEmbeddedPlan(
      requested: HoodieInstant,
      inflight: HoodieInstant,
      completed: util.List[HoodieInstant],
      cleanPlanBytes: Option[Array[Byte]],
      compactionPlanBytes: Option[Array[Byte]]) extends ActiveAction(requested, inflight, completed) {
    override def getCleanPlan(metaClient: HoodieTableMetaClient): org.apache.hudi.common.util.Option[Array[Byte]] = {
      if (cleanPlanBytes.isDefined) {
        org.apache.hudi.common.util.Option.of(cleanPlanBytes.get)
      } else {
        super.getCleanPlan(metaClient)
      }
    }
    override def getCompactionPlan(metaClient: HoodieTableMetaClient): org.apache.hudi.common.util.Option[Array[Byte]] = {
      if (compactionPlanBytes.isDefined) {
        org.apache.hudi.common.util.Option.of(compactionPlanBytes.get)
      } else {
        super.getCompactionPlan(metaClient)
      }
    }
  }

  private def createCleanPlan(timestamp: String): org.apache.hudi.avro.model.HoodieCleanerPlan = {
    import org.apache.hudi.avro.model.{HoodieCleanerPlan, HoodieActionInstant}
    HoodieCleanerPlan.newBuilder()
      .setEarliestInstantToRetainBuilder(HoodieActionInstant.newBuilder()
        .setAction("commit")
        .setTimestamp(timestamp)
        .setState("COMPLETED"))
      .setPolicy("KEEP_LATEST_COMMITS")
      .setFilesToBeDeletedPerPartition(new util.HashMap[String, util.List[String]]())
      .setVersion(1)
      .build()
  }

  private def createCleanMetadata(timestamp: String): org.apache.hudi.avro.model.HoodieCleanMetadata = {
    import org.apache.hudi.avro.model.HoodieCleanMetadata
    HoodieCleanMetadata.newBuilder()
      .setVersion(1)
      .setTimeTakenInMillis(100)
      .setTotalFilesDeleted(1)
      .setStartCleanTime(timestamp)
      .setEarliestCommitToRetain(timestamp)
      .setLastCompletedCommitTimestamp("")
      .setPartitionMetadata(new util.HashMap[String, org.apache.hudi.avro.model.HoodieCleanPartitionMetadata]())
      .build()
  }

  private def createRollbackMetadata(timestamp: String, commitsRollback: util.List[String]): org.apache.hudi.avro.model.HoodieRollbackMetadata = {
    import org.apache.hudi.avro.model.{HoodieRollbackMetadata, HoodieRollbackPartitionMetadata, HoodieInstantInfo}
    HoodieRollbackMetadata.newBuilder()
      .setVersion(1)
      .setStartRollbackTime(timestamp)
      .setTotalFilesDeleted(1)
      .setTimeTakenInMillis(1000)
      .setCommitsRollback(commitsRollback)
      .setPartitionMetadata(new util.HashMap[String, HoodieRollbackPartitionMetadata]())
      .setInstantsRollback(util.Collections.singletonList(
        new HoodieInstantInfo(timestamp, HoodieTimeline.ROLLBACK_ACTION)))
      .build()
  }

  private def createCompactionPlan(): org.apache.hudi.avro.model.HoodieCompactionPlan = {
    import org.apache.hudi.avro.model.HoodieCompactionPlan
    HoodieCompactionPlan.newBuilder()
      .setOperations(new util.ArrayList())
      .setVersion(1)
      .build()
  }

  private def createClusteringRequestedMetadata(timestamp: String): org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata = {
    import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata
    HoodieRequestedReplaceMetadata.newBuilder()
      .setOperationType(WriteOperationType.CLUSTER.name())
      .setVersion(1)
      .build()
  }

  private def createActiveInstantV2(
      activeTimeline: org.apache.hudi.common.table.timeline.HoodieActiveTimeline,
      timestamp: String,
      action: String,
      state: HoodieInstant.State,
      testTable: HoodieTestTable,
      completionTime: Option[String] = None): Unit = {
    state match {
      case HoodieInstant.State.COMPLETED =>
        // For compaction, we need to create requested and inflight first, then complete as COMMIT
        if (action == HoodieTimeline.COMPACTION_ACTION) {
          // Create compaction requested
          val compactionRequested = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, timestamp)
          val compactionPlan = createCompactionPlan()
          activeTimeline.saveToCompactionRequested(compactionRequested, compactionPlan)
          // Create compaction inflight
          val compactionInflight = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, timestamp)
          activeTimeline.createNewInstant(compactionInflight)
          // Complete as COMMIT (compaction becomes commit when completed)
          val commitInflight = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, timestamp)
          activeTimeline.createNewInstant(commitInflight)
          val compTime = completionTime.getOrElse(String.valueOf(timestamp.toLong + 1))
          val metadata = testTable.createCommitMetadata(timestamp, WriteOperationType.COMPACT, util.Arrays.asList("par1"), 10, false)
          activeTimeline.saveAsComplete(false, commitInflight, org.apache.hudi.common.util.Option.of(metadata),
            org.apache.hudi.common.util.Option.of(compTime))
        } else {
          val inflightInstant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, action, timestamp)
          activeTimeline.createNewInstant(inflightInstant)
          val compTime = completionTime.getOrElse(String.valueOf(timestamp.toLong + 1))
          val metadata: Any = action match {
            case HoodieTimeline.DELTA_COMMIT_ACTION | HoodieTimeline.COMMIT_ACTION =>
              testTable.createCommitMetadata(timestamp, WriteOperationType.INSERT, util.Arrays.asList("par1"), 10, false)
            case HoodieTimeline.CLEAN_ACTION =>
              createCleanMetadata(timestamp)
            case HoodieTimeline.ROLLBACK_ACTION =>
              createRollbackMetadata(timestamp, util.Collections.singletonList(timestamp))
            case HoodieTimeline.REPLACE_COMMIT_ACTION =>
              // Determine if clustering or insert overwrite based on timestamp
              // Timestamps ending in 500 are insert overwrite, others are clustering
              val operationType = if (timestamp.endsWith("500") || timestamp.endsWith("200")) {
                WriteOperationType.INSERT_OVERWRITE
              } else {
                WriteOperationType.CLUSTER
              }
              testTable.createCommitMetadata(timestamp, operationType, util.Arrays.asList("par1"), 10, false)
            case _ =>
              testTable.createCommitMetadata(timestamp, WriteOperationType.INSERT, util.Arrays.asList("par1"), 10, false)
          }
          activeTimeline.saveAsComplete(false, inflightInstant, org.apache.hudi.common.util.Option.of(metadata),
            org.apache.hudi.common.util.Option.of(compTime))
        }
      case HoodieInstant.State.INFLIGHT =>
        val inflightInstant = INSTANT_GENERATOR.createNewInstant(state, action, timestamp)
        activeTimeline.createNewInstant(inflightInstant)
      case HoodieInstant.State.REQUESTED =>
        val requestedInstant = INSTANT_GENERATOR.createNewInstant(state, action, timestamp)
        action match {
          case HoodieTimeline.CLEAN_ACTION =>
            val cleanPlan = createCleanPlan(timestamp)
            activeTimeline.saveToCleanRequested(requestedInstant, org.apache.hudi.common.util.Option.of(cleanPlan))
          case HoodieTimeline.COMPACTION_ACTION =>
            val compactionPlan = createCompactionPlan()
            activeTimeline.saveToCompactionRequested(requestedInstant, compactionPlan)
          case HoodieTimeline.CLUSTERING_ACTION =>
            val clusteringMetadata = createClusteringRequestedMetadata(timestamp)
            activeTimeline.createRequestedCommitWithReplaceMetadata(timestamp, action)
          case HoodieTimeline.ROLLBACK_ACTION =>
            // Rollback requested is created differently
            activeTimeline.createNewInstant(requestedInstant)
          case HoodieTimeline.REPLACE_COMMIT_ACTION =>
            // Could be clustering or insert overwrite - create requested with appropriate metadata
            // For simplicity, we'll create it and the metadata will be in the completed state
            activeTimeline.createRequestedCommitWithReplaceMetadata(timestamp, action)
          case _ =>
            activeTimeline.createNewInstant(requestedInstant)
        }
    }
  }

  private def writeArchivedInstantsV2(
      metaClient: HoodieTableMetaClient,
      instants: Seq[(String, String, HoodieInstant.State)],
      testTable: HoodieTestTable): Unit = {
    val writeConfig = HoodieWriteConfig.newBuilder()
      .withPath(metaClient.getBasePath)
      .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
      .withMarkersType("DIRECT")
      .build()
    val engineContext = new HoodieLocalEngineContext(HadoopFSUtils.getStorageConf(spark.sparkContext.hadoopConfiguration))
    val writer = LSMTimelineWriter.getInstance(writeConfig, new LocalTaskContextSupplier(), metaClient)

    val instantBuffer = new util.ArrayList[ActiveAction]()
    instants.foreach { case (timestamp, action, state) =>
      if (state == HoodieInstant.State.COMPLETED) {
        val completionTime = String.valueOf(timestamp.toLong + 1)
        val instant = INSTANT_GENERATOR.createNewInstant(state, action, timestamp, completionTime)
        val metadata: Any = action match {
          case HoodieTimeline.DELTA_COMMIT_ACTION | HoodieTimeline.COMMIT_ACTION =>
            testTable.createCommitMetadata(timestamp, WriteOperationType.INSERT, util.Arrays.asList("par1"), 10, false)
          case HoodieTimeline.CLEAN_ACTION =>
            createCleanMetadata(timestamp)
          case HoodieTimeline.ROLLBACK_ACTION =>
            createRollbackMetadata(timestamp, util.Collections.singletonList(timestamp))
          case HoodieTimeline.REPLACE_COMMIT_ACTION =>
            testTable.createCommitMetadata(timestamp, WriteOperationType.CLUSTER, util.Arrays.asList("par1"), 10, false)
          case _ =>
            testTable.createCommitMetadata(timestamp, WriteOperationType.INSERT, util.Arrays.asList("par1"), 10, false)
        }
        val serializedMetadata = convertMetadataToByteArray(metadata)
        // For clean actions, use ActiveActionWithEmbeddedPlan to avoid creating requested files in active timeline
        if (action == HoodieTimeline.CLEAN_ACTION) {
          val cleanPlan = createCleanPlan(timestamp)
          val cleanPlanBytes = convertMetadataToByteArray(cleanPlan)
          val requestedInstant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, action, timestamp)
          val inflightInstant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, action, timestamp)
          instantBuffer.add(new ActiveActionWithEmbeddedPlan(
            requestedInstant,
            inflightInstant,
            util.Collections.singletonList(instant),
            Some(cleanPlanBytes),
            None))
        } else {
          instantBuffer.add(new DummyActiveAction(instant, serializedMetadata))
        }
      }
    }

    if (!instantBuffer.isEmpty) {
      writer.write(instantBuffer, org.apache.hudi.common.util.Option.empty(), org.apache.hudi.common.util.Option.empty())
      writer.compactAndClean(engineContext)
    }
  }

  private val INSTANT_GENERATOR_V1 = new InstantGeneratorV1()

  private def createActiveInstantV1(
      activeTimeline: org.apache.hudi.common.table.timeline.HoodieActiveTimeline,
      timestamp: String,
      action: String,
      state: HoodieInstant.State,
      testTable: HoodieTestTable,
      completionTime: Option[String] = None): Unit = {
    state match {
      case HoodieInstant.State.COMPLETED =>
        // For compaction, we need to create requested and inflight first, then complete as COMMIT
        if (action == HoodieTimeline.COMPACTION_ACTION) {
          val compactionRequested = INSTANT_GENERATOR_V1.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, timestamp)
          val compactionPlan = createCompactionPlan()
          activeTimeline.saveToCompactionRequested(compactionRequested, compactionPlan)
          val compactionInflight = INSTANT_GENERATOR_V1.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, timestamp)
          activeTimeline.createNewInstant(compactionInflight)
          // Complete as COMMIT (compaction becomes commit when completed)
          val commitInflight = INSTANT_GENERATOR_V1.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, timestamp)
          activeTimeline.createNewInstant(commitInflight)
          val compTime = completionTime.getOrElse(String.valueOf(timestamp.toLong + 1))
          val metadata = testTable.createCommitMetadata(timestamp, WriteOperationType.COMPACT, util.Arrays.asList("par1"), 10, false)
          activeTimeline.saveAsComplete(false, commitInflight, org.apache.hudi.common.util.Option.of(metadata),
            org.apache.hudi.common.util.Option.of(compTime))
        } else {
          val inflightInstant = INSTANT_GENERATOR_V1.createNewInstant(HoodieInstant.State.INFLIGHT, action, timestamp)
          activeTimeline.createNewInstant(inflightInstant)
          val compTime = completionTime.getOrElse(String.valueOf(timestamp.toLong + 1))
          val metadata: Any = action match {
            case HoodieTimeline.DELTA_COMMIT_ACTION | HoodieTimeline.COMMIT_ACTION =>
              testTable.createCommitMetadata(timestamp, WriteOperationType.INSERT, util.Arrays.asList("par1"), 10, false)
            case HoodieTimeline.CLEAN_ACTION =>
              createCleanMetadata(timestamp)
            case HoodieTimeline.ROLLBACK_ACTION =>
              createRollbackMetadata(timestamp, util.Collections.singletonList(timestamp))
            case HoodieTimeline.REPLACE_COMMIT_ACTION =>
              // Use timestamp pattern to determine operation type (ending in 500/200 = insert overwrite, else clustering)
              val operationType = if (timestamp.endsWith("500") || timestamp.endsWith("200")) {
                WriteOperationType.INSERT_OVERWRITE
              } else {
                WriteOperationType.CLUSTER
              }
              testTable.createCommitMetadata(timestamp, operationType, util.Arrays.asList("par1"), 10, false)
            case _ =>
              testTable.createCommitMetadata(timestamp, WriteOperationType.INSERT, util.Arrays.asList("par1"), 10, false)
          }
          activeTimeline.saveAsComplete(false, inflightInstant, org.apache.hudi.common.util.Option.of(metadata),
            org.apache.hudi.common.util.Option.of(compTime))
        }
      case HoodieInstant.State.INFLIGHT =>
        val inflightInstant = INSTANT_GENERATOR_V1.createNewInstant(state, action, timestamp)
        activeTimeline.createNewInstant(inflightInstant)
      case HoodieInstant.State.REQUESTED =>
        val requestedInstant = INSTANT_GENERATOR_V1.createNewInstant(state, action, timestamp)
        action match {
          case HoodieTimeline.CLEAN_ACTION =>
            val cleanPlan = createCleanPlan(timestamp)
            activeTimeline.saveToCleanRequested(requestedInstant, org.apache.hudi.common.util.Option.of(cleanPlan))
          case HoodieTimeline.COMPACTION_ACTION =>
            val compactionPlan = createCompactionPlan()
            activeTimeline.saveToCompactionRequested(requestedInstant, compactionPlan)
          case HoodieTimeline.REPLACE_COMMIT_ACTION =>
            activeTimeline.createRequestedCommitWithReplaceMetadata(timestamp, action)
          case HoodieTimeline.ROLLBACK_ACTION =>
            activeTimeline.createNewInstant(requestedInstant)
          case _ =>
            activeTimeline.createNewInstant(requestedInstant)
        }
    }
  }

  private def createArchivedMetaWrapperV1(
      metaClient: HoodieTableMetaClient,
      hoodieInstant: HoodieInstant,
      testTable: HoodieTestTable): HoodieArchivedMetaEntry = {
    import org.apache.hudi.avro.model.{HoodieActionInstant, HoodieReplaceCommitMetadata}
    val archivedMetaWrapper = new HoodieArchivedMetaEntry()
    archivedMetaWrapper.setCommitTime(hoodieInstant.requestedTime())
    archivedMetaWrapper.setActionState(hoodieInstant.getState.name())
    hoodieInstant.getAction match {
      case HoodieTimeline.COMMIT_ACTION =>
        archivedMetaWrapper.setActionType(ActionType.commit.name())
        archivedMetaWrapper.setHoodieCommitMetadata(org.apache.hudi.avro.model.HoodieCommitMetadata.newBuilder().build())
      case HoodieTimeline.COMPACTION_ACTION =>
        archivedMetaWrapper.setActionType(ActionType.compaction.name())
        archivedMetaWrapper.setHoodieCompactionPlan(createCompactionPlan())
      case HoodieTimeline.REPLACE_COMMIT_ACTION =>
        archivedMetaWrapper.setActionType(ActionType.replacecommit.name())
        if (hoodieInstant.isCompleted) {
          import org.apache.hudi.avro.model.HoodieReplaceCommitMetadata
          archivedMetaWrapper.setHoodieReplaceCommitMetadata(HoodieReplaceCommitMetadata.newBuilder().build())
        } else if (hoodieInstant.isInflight) {
          // For inflight replace commit, use HoodieCommitMetadata (avro model)
          // Inflight replacecommit files have the same metadata body as HoodieCommitMetadata
          archivedMetaWrapper.setHoodieInflightReplaceMetadata(org.apache.hudi.avro.model.HoodieCommitMetadata.newBuilder().build())
        } else {
          archivedMetaWrapper.setHoodieRequestedReplaceMetadata(createClusteringRequestedMetadata(hoodieInstant.requestedTime()))
        }
      case HoodieTimeline.DELTA_COMMIT_ACTION =>
        archivedMetaWrapper.setActionType(ActionType.deltacommit.name())
        archivedMetaWrapper.setHoodieCommitMetadata(org.apache.hudi.avro.model.HoodieCommitMetadata.newBuilder().build())
      case HoodieTimeline.CLEAN_ACTION =>
        archivedMetaWrapper.setActionType(ActionType.clean.name())
        if (hoodieInstant.isCompleted) {
          archivedMetaWrapper.setHoodieCleanMetadata(createCleanMetadata(hoodieInstant.requestedTime()))
        } else {
          archivedMetaWrapper.setHoodieCleanerPlan(createCleanPlan(hoodieInstant.requestedTime()))
        }
      case HoodieTimeline.ROLLBACK_ACTION =>
        archivedMetaWrapper.setActionType(ActionType.rollback.name())
        if (hoodieInstant.isCompleted) {
          archivedMetaWrapper.setHoodieRollbackMetadata(createRollbackMetadata(hoodieInstant.requestedTime(), util.Collections.singletonList(hoodieInstant.requestedTime())))
        }
      case _ =>
    }
    archivedMetaWrapper
  }

  private def writeArchivedInstantsV1(
      metaClient: HoodieTableMetaClient,
      instants: Seq[(String, String, HoodieInstant.State)],
      testTable: HoodieTestTable): Unit = {
    val archivePath = metaClient.getArchivePath()
    val archiveFilePath = ArchivedTimelineV1.getArchiveLogPath(archivePath)
    val storage = metaClient.getStorage()

    // Create archive directory if it doesn't exist
    if (!storage.exists(archivePath)) {
      storage.createDirectory(archivePath)
    }

    val writer = HoodieLogFormat.newWriterBuilder()
      .onParentPath(archiveFilePath.getParent())
      .withFileId(archiveFilePath.getName())
      .withFileExtension(HoodieArchivedLogFile.ARCHIVE_EXTENSION)
      .withStorage(storage)
      .withInstantTime("")
      .build()

    val records = new util.ArrayList[IndexedRecord]()
    instants.foreach { case (timestamp, action, state) =>
      val instant = INSTANT_GENERATOR_V1.createNewInstant(state, action, timestamp)
      records.add(createArchivedMetaWrapperV1(metaClient, instant, testTable))
    }

    if (!records.isEmpty) {
      val header = new util.HashMap[HoodieLogBlock.HeaderMetadataType, String]()
      header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, HoodieArchivedMetaEntry.getClassSchema.toString)
      val keyField = metaClient.getTableConfig.getRecordKeyFieldProp
      val indexRecords = new util.ArrayList[HoodieRecord[_]]()
      records.asScala.foreach { record =>
        indexRecords.add(new HoodieAvroIndexedRecord(record))
      }
      val block = new HoodieAvroDataBlock(indexRecords, header, keyField)
      writer.appendBlock(block)
    }
    writer.close()
  }

  /**
   * Helper function to filter instants by time range.
   */
  private def filterInstantsByTimeRange(instants: Seq[(String, String, HoodieInstant.State)],
                                        startTime: Option[String] = None,
                                        endTime: Option[String] = None): Seq[String] = {
    instants.filter { case (timestamp, _, _) =>
      val afterStart = startTime.isEmpty || timestamp >= startTime.get
      val beforeEnd = endTime.isEmpty || timestamp <= endTime.get
      afterStart && beforeEnd
    }.map(_._1)
  }

  /**
   * Helper function to verify results.
   */
  private def verifyResults(results: Array[Row],
                            expectedActive: Seq[String],
                            expectedArchived: Seq[String],
                            expectedTotal: Option[Int] = None,
                            description: String): Unit = {
    val resultTimes = results.map(_.getString(0)).toSet
    val activeResults = results.filter(_.getString(6) == "ACTIVE").map(_.getString(0)).toSet
    val archivedResults = results.filter(_.getString(6) == "ARCHIVED").map(_.getString(0)).toSet

    if (expectedTotal.isDefined) {
      assert(results.length == expectedTotal.get,
        s"$description: Expected total count ${expectedTotal.get}, got ${results.length}")
    }

    expectedActive.foreach { timestamp =>
      assert(resultTimes.contains(timestamp),
        s"$description: Expected active instant $timestamp not found in results")
      assert(activeResults.contains(timestamp),
        s"$description: Active instant $timestamp should have timeline type ACTIVE")
    }

    expectedArchived.foreach { timestamp =>
      if (resultTimes.contains(timestamp)) {
        assert(archivedResults.contains(timestamp),
          s"$description: Archived instant $timestamp should have timeline type ARCHIVED")
      }
    }
  }

  /**
   * Run all test cases for show_timeline procedure.
   */
  private def runShowTimelineTestCases(
      tableName: String,
      activeInstants: Seq[(String, String, HoodieInstant.State)],
      archivedInstants: Seq[(String, String, HoodieInstant.State)],
      expectedTotal: Int,
      initTs: String): Unit = {
    // Calculate test timestamps from actual instant data
    val allActiveTimestamps = activeInstants.map(_._1).sorted
    val allArchivedTimestamps = archivedInstants.map(_._1).sorted
    val allTimestamps = (allActiveTimestamps ++ allArchivedTimestamps).sorted

    // Find timestamps in the middle of active range for test cases
    val midActiveIdx = allActiveTimestamps.length / 2
    val startTimeInActive = if (midActiveIdx > 0 && midActiveIdx < allActiveTimestamps.length) {
      allActiveTimestamps(midActiveIdx)
    } else if (allActiveTimestamps.nonEmpty) {
      allActiveTimestamps.head
    } else {
      s"${initTs}350000"
    }

    val endTimeInActive = if (allActiveTimestamps.nonEmpty) {
      allActiveTimestamps.last
    } else {
      s"${initTs}400000"
    }

    // Find timestamps in archived range
    val startTimeInArchived = if (allArchivedTimestamps.nonEmpty) {
      allArchivedTimestamps.head
    } else {
      s"${initTs}260000"
    }

    val endTimeInArchived = if (allArchivedTimestamps.nonEmpty && allArchivedTimestamps.length > 1) {
      allArchivedTimestamps(allArchivedTimestamps.length / 2)
    } else if (allArchivedTimestamps.nonEmpty) {
      allArchivedTimestamps.head
    } else {
      s"${initTs}310000"
    }
    // Test Case 1: Timeline total
    // Note: Completed compaction creates additional instants (REQUESTED, INFLIGHT compaction, COMPLETED commit)
    {
      val results = spark.sql(s"call show_timeline(table => '$tableName', showArchived => true, limit => 100)").collect()
      assert(results.length == expectedTotal, s"Test 1: Expected total count $expectedTotal, got ${results.length}")
      assert(results(0).getString(0) == activeInstants.head._1, s"Test 1: Expected first instant not found in results")
      assert(results(expectedTotal - 1).getString(0) == archivedInstants.last._1, s"Test 1: Expected last instant not found in results")
      verifyResults(results, activeInstants.map(_._1), archivedInstants.map(_._1), Some(expectedTotal), "Test 1: Total count")
    }

    // Test Case 2: Limit 10
    {
      val results = spark.sql(s"call show_timeline(table => '$tableName', showArchived => true, limit => 10)").collect()
      assert(results.length == 10, s"Test 2: Expected count 10 with limit, got ${results.length}")
      assert(results(0).getString(0) == activeInstants.head._1, s"Test 2: Expected first instant not found in results")
      assert(results(9).getString(0) == activeInstants(9)._1, s"Test 2: Expected last instant not found in results")
    }

    // Test Case 3: Limit 20, verify active has 20 and archived has 0
    {
      val results = spark.sql(s"call show_timeline(table => '$tableName', showArchived => true)").collect()
      assert(results.length == 20, s"Test 3: Expected count 20 with limit, got ${results.length}")
      assert(results(0).getString(0) == activeInstants.head._1, s"Test 3: Expected first instant not found in results")
      assert(results(19).getString(0) == activeInstants(19)._1, s"Test 3: Expected last instant not found in results")
    }

    // Test Case 4: Start timestamp within active timeline
    {
      val startTime = startTimeInActive
      val results = spark.sql(s"call show_timeline(table => '$tableName', showArchived => true, startTime => '$startTime', limit => 100)").collect()
      val expectedActive = filterInstantsByTimeRange(activeInstants, Some(startTime), None)
      val expectedArchived = filterInstantsByTimeRange(archivedInstants, Some(startTime), None)
      verifyResults(results, expectedActive, expectedArchived, None, "Test 4: Start in active")
      val expectedCount = expectedActive.length + expectedArchived.length
      if (expectedCount > 0) {
        assert(results.length == expectedCount, s"Test 4: Expected count $expectedCount, got ${results.length}")
        if (expectedActive.nonEmpty) {
          assert(results(0).getString(0) == expectedActive.head, s"Test 4: Expected first instant not found in results")
        }
      }
    }

    // Test Case 5: End timestamp within active timeline
    {
      val endTime = endTimeInActive
      val results = spark.sql(s"call show_timeline(table => '$tableName', showArchived => true, endTime => '$endTime', limit => 100)").collect()
      val expectedActive = filterInstantsByTimeRange(activeInstants, None, Some(endTime))
      val expectedArchived = filterInstantsByTimeRange(archivedInstants, None, Some(endTime))
      verifyResults(results, expectedActive, expectedArchived, None, "Test 5: End in active")
      val expectedCount = expectedActive.length + expectedArchived.length
      assert(results.length == expectedCount, s"Test 5: Expected count $expectedCount, got ${results.length}")
      if (expectedActive.nonEmpty) {
        assert(results(0).getString(0) == expectedActive.head, s"Test 5: Expected first instant not found in results")
      }
      if (expectedArchived.nonEmpty) {
        assert(results(results.length - 1).getString(0) == expectedArchived.last, s"Test 5: Expected last instant not found in results")
      }
    }

    // Test Case 6: Start and end within active timeline
    {
      val startTime = startTimeInActive
      val endTime = endTimeInActive
      val results = spark.sql(s"call show_timeline(table => '$tableName', showArchived => true, startTime => '$startTime', endTime => '$endTime', limit => 100)").collect()
      val expectedActive = filterInstantsByTimeRange(activeInstants, Some(startTime), Some(endTime))
      val expectedArchived = filterInstantsByTimeRange(archivedInstants, Some(startTime), Some(endTime))
      verifyResults(results, expectedActive, expectedArchived, None, "Test 6: Start and end in active")
    }

    // Test Case 7: Start in archived, archived not enabled
    {
      val startTime = startTimeInArchived
      val results = spark.sql(s"call show_timeline(table => '$tableName', showArchived => false, startTime => '$startTime', limit => 100)").collect()
      val expectedActive = filterInstantsByTimeRange(activeInstants, Some(startTime), None)
      // Archived should not be included
      val archivedResults = results.filter(_.getString(6) == "ARCHIVED")
      assert(archivedResults.isEmpty,
        s"Test 7: Expected no archived results when showArchived=false, got ${archivedResults.length}")
      verifyResults(results, expectedActive, Seq.empty, None, "Test 7: Start in archived, archived disabled")
    }

    // Test Case 8: Empty start and end, archived enabled
    {
      val results = spark.sql(s"call show_timeline(table => '$tableName', showArchived => true, startTime => '', endTime => '', limit => 100)").collect()
      verifyResults(results, activeInstants.map(_._1), archivedInstants.map(_._1), Some(expectedTotal), "Test 8: Empty start/end, archived enabled")
    }

    // Test Case 9: Start in archived, end in active, archived not enabled
    {
      val startTime = startTimeInArchived
      val endTime = endTimeInActive
      val results = spark.sql(s"call show_timeline(table => '$tableName', showArchived => false, startTime => '$startTime', endTime => '$endTime', limit => 100)").collect()
      val expectedActive = filterInstantsByTimeRange(activeInstants, Some(startTime), Some(endTime))
      val archivedResults = results.filter(_.getString(6) == "ARCHIVED")
      assert(archivedResults.isEmpty,
        s"Test 9: Expected no archived results when showArchived=false, got ${archivedResults.length}")
      verifyResults(results, expectedActive, Seq.empty, None, "Test 9: Start in archived, end in active, archived disabled")
    }

    // Test Case 10: Start in archived, end in active, archived enabled
    {
      val startTime = startTimeInArchived
      val endTime = endTimeInActive
      val results = spark.sql(s"call show_timeline(table => '$tableName', showArchived => true, startTime => '$startTime', endTime => '$endTime', limit => 100)").collect()
      val expectedActive = filterInstantsByTimeRange(activeInstants, Some(startTime), Some(endTime))
      val expectedArchived = filterInstantsByTimeRange(archivedInstants, Some(startTime), Some(endTime))
      verifyResults(results, expectedActive, expectedArchived, None, "Test 10: Start in archived, end in active, archived enabled")
    }

    // Test Case 11: Start and end in archived, archived not enabled
    {
      val startTime = startTimeInArchived
      val endTime = endTimeInArchived
      val results = spark.sql(s"call show_timeline(table => '$tableName', showArchived => false, startTime => '$startTime', endTime => '$endTime', limit => 100)").collect()
      val expectedActive = filterInstantsByTimeRange(activeInstants, Some(startTime), Some(endTime))
      val archivedResults = results.filter(_.getString(6) == "ARCHIVED")
      assert(archivedResults.isEmpty,
        s"Test 11: Expected no archived results when showArchived=false, got ${archivedResults.length}")
      verifyResults(results, expectedActive, Seq.empty, None, "Test 11: Start and end in archived, archived disabled")
    }

    // Test Case 12: Start and end in archived, archived enabled
    {
      val startTime = startTimeInArchived
      val endTime = endTimeInArchived
      val results = spark.sql(s"call show_timeline(table => '$tableName', showArchived => true, startTime => '$startTime', endTime => '$endTime', limit => 100)").collect()
      val expectedActive = filterInstantsByTimeRange(activeInstants, Some(startTime), Some(endTime))
      val expectedArchived = filterInstantsByTimeRange(archivedInstants, Some(startTime), Some(endTime))
      verifyResults(results, expectedActive, expectedArchived, None, "Test 12: Start and end in archived, archived enabled")
    }
  }

  test("Test show_timeline with various parameters - V2 MOR") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tableLocation = tmp.getCanonicalPath
      if (HoodieSparkUtils.isSpark3_4) {
        spark.sql("set spark.sql.defaultColumn.enabled = false")
      }

      // Create V2 MOR table
      spark.sql(
        s"""
           |create table $tableName (
           | id int,
           | name string,
           | price double,
           | ts long
           |) using hudi
           | location '$tableLocation'
           | tblproperties (
           |   primaryKey = 'id',
           |   type = 'mor',
           |   preCombineField = 'ts'
           | )
           |""".stripMargin)

      val metaClient = HoodieTableMetaClient.builder()
        .setBasePath(tableLocation)
        .setConf(HadoopFSUtils.getStorageConf(spark.sparkContext.hadoopConfiguration))
        .build()

      val activeTimeline = metaClient.getActiveTimeline
      val testTable = HoodieTestTable.of(metaClient)
      val initTs = "20251126023";

      // Assign continuous timestamps, not explicit strings, similar to V2 COW test.
      val activeInstants = Seq(
        // 34 continuous active instants
        (s"${initTs}10034", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10033", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.INFLIGHT),
        (s"${initTs}10032", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10031", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10030", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10029", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10028", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10027", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.INFLIGHT),
        (s"${initTs}10026", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10025", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10024", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10023", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10022", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10021", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10020", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10019", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10018", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10017", HoodieTimeline.ROLLBACK_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10016", HoodieTimeline.ROLLBACK_ACTION, HoodieInstant.State.REQUESTED),
        (s"${initTs}10015", HoodieTimeline.REPLACE_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10014", HoodieTimeline.REPLACE_COMMIT_ACTION, HoodieInstant.State.INFLIGHT),
        (s"${initTs}10013", HoodieTimeline.CLUSTERING_ACTION, HoodieInstant.State.REQUESTED),
        (s"${initTs}10012", HoodieTimeline.REPLACE_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10011", HoodieTimeline.REPLACE_COMMIT_ACTION, HoodieInstant.State.INFLIGHT),
        (s"${initTs}10010", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10009", HoodieTimeline.COMPACTION_ACTION, HoodieInstant.State.INFLIGHT),
        (s"${initTs}10008", HoodieTimeline.CLEAN_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10007", HoodieTimeline.CLEAN_ACTION, HoodieInstant.State.INFLIGHT),
        (s"${initTs}10006", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10005", HoodieTimeline.CLEAN_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10004", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10003", HoodieTimeline.CLEAN_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10002", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10001", HoodieTimeline.CLEAN_ACTION, HoodieInstant.State.COMPLETED)
      )

      // Create all active instants
      activeInstants.foreach { case (timestamp, action, state) =>
        createActiveInstantV2(activeTimeline, timestamp, action, state, testTable)
      }

      val archivedInstants = Seq(
        (s"${initTs}10000", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}09999", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}09998", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}09997", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}09996", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED)
      )

      // Write archived instants
      writeArchivedInstantsV2(metaClient, archivedInstants, testTable)

      // Run all test cases
      // Expected total: 39 (34 active + 5 archived, but compaction creates additional instants)
      runShowTimelineTestCases(tableName, activeInstants, archivedInstants, 39, initTs)
    }
  }

  test("Test show_timeline with various parameters - V2 COW") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tableLocation = tmp.getCanonicalPath
      if (HoodieSparkUtils.isSpark3_4) {
        spark.sql("set spark.sql.defaultColumn.enabled = false")
      }

      // Create V2 COW table
      spark.sql(
        s"""
           |create table $tableName (
           | id int,
           | name string,
           | price double,
           | ts long
           |) using hudi
           | location '$tableLocation'
           | tblproperties (
           |   primaryKey = 'id',
           |   type = 'cow',
           |   preCombineField = 'ts'
           | )
           |""".stripMargin)

      val metaClient = HoodieTableMetaClient.builder()
        .setBasePath(tableLocation)
        .setConf(HadoopFSUtils.getStorageConf(spark.sparkContext.hadoopConfiguration))
        .build()

      val activeTimeline = metaClient.getActiveTimeline
      val testTable = HoodieTestTable.of(metaClient)
      val initTs = "202511300819";
      val activeInstants = Seq(
        // Completed commits
        (s"${initTs}10041", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10040", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10039", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10038", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10037", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10036", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10035", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10034", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10033", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10032", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10031", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10030", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10029", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10028", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10027", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10026", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),

        // Inflight commits
        (s"${initTs}10025", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.INFLIGHT),
        (s"${initTs}10024", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.INFLIGHT),

        // Completed clean commits
        (s"${initTs}10023", HoodieTimeline.CLEAN_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10022", HoodieTimeline.CLEAN_ACTION, HoodieInstant.State.COMPLETED),

        // Inflight clean commits
        (s"${initTs}10021", HoodieTimeline.CLEAN_ACTION, HoodieInstant.State.INFLIGHT),

        // Completed compaction
        (s"${initTs}10020", HoodieTimeline.COMPACTION_ACTION, HoodieInstant.State.COMPLETED),

        // Inflight compaction
        (s"${initTs}10019", HoodieTimeline.COMPACTION_ACTION, HoodieInstant.State.INFLIGHT),

        // Completed replace commit - clustering
        (s"${initTs}10018", HoodieTimeline.REPLACE_COMMIT_ACTION, HoodieInstant.State.COMPLETED),

        // Inflight replace commit - clustering
        (s"${initTs}10017", HoodieTimeline.REPLACE_COMMIT_ACTION, HoodieInstant.State.INFLIGHT),

        // Completed replace commit - insert overwrite
        (s"${initTs}10016", HoodieTimeline.REPLACE_COMMIT_ACTION, HoodieInstant.State.COMPLETED),

        // Inflight replace commit - insert overwrite
        (s"${initTs}10015", HoodieTimeline.REPLACE_COMMIT_ACTION, HoodieInstant.State.INFLIGHT),

        // Completed rollback
        (s"${initTs}10014", HoodieTimeline.ROLLBACK_ACTION, HoodieInstant.State.COMPLETED),

        // Pending rollback
        (s"${initTs}10013", HoodieTimeline.ROLLBACK_ACTION, HoodieInstant.State.REQUESTED)
      )

      val archivedInstants = Seq(
        (s"${initTs}10012", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10011", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10010", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10008", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10001", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED)
      )

      // Create all active instants
      activeInstants.foreach { case (timestamp, action, state) =>
        createActiveInstantV2(activeTimeline, timestamp, action, state, testTable)
      }

      // Write archived instants
      writeArchivedInstantsV2(metaClient, archivedInstants, testTable)

      // Run all test cases
      // Expected total: 34 (29 active + 5 archived, but compaction creates additional instants?)
      runShowTimelineTestCases(tableName, activeInstants, archivedInstants, 34, initTs)
    }
  }

  test("Test show_timeline with various parameters - V1 MOR") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tableLocation = tmp.getCanonicalPath
      if (HoodieSparkUtils.isSpark3_4) {
        spark.sql("set spark.sql.defaultColumn.enabled = false")
      }

      // Create V1 MOR table with table version 6 (which uses timeline layout version 1)
      spark.sql(
        s"""
           |create table $tableName (
           | id int,
           | name string,
           | price double,
           | ts long
           |) using hudi
           | location '$tableLocation'
           | tblproperties (
           |   primaryKey = 'id',
           |   type = 'mor',
           |   preCombineField = 'ts',
           |   'hoodie.write.table.version' = '6'
           | )
           |""".stripMargin)

      val metaClient = HoodieTableMetaClient.builder()
        .setBasePath(tableLocation)
        .setConf(HadoopFSUtils.getStorageConf(spark.sparkContext.hadoopConfiguration))
        .build()

      // Verify timeline layout version is V1
      val timelineLayoutVersion = metaClient.getTimelineLayoutVersion.getVersion
      assert(timelineLayoutVersion == TimelineLayoutVersion.VERSION_1,
        s"V1 MOR test: Expected timeline layout version 1, got $timelineLayoutVersion")

      val activeTimeline = metaClient.getActiveTimeline
      val testTable = HoodieTestTable.of(metaClient)
      val initTs = "20251126023"

      // Assign continuous timestamps, not explicit strings, similar to V2 tests.
      val activeInstants = Seq(
        // 34 continuous active instants
        (s"${initTs}10034", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10033", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.INFLIGHT),
        (s"${initTs}10032", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10031", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10030", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10029", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10028", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10027", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.INFLIGHT),
        (s"${initTs}10026", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10025", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10024", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10023", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10022", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10021", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10020", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10019", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10018", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10017", HoodieTimeline.ROLLBACK_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10016", HoodieTimeline.ROLLBACK_ACTION, HoodieInstant.State.REQUESTED),
        (s"${initTs}10015", HoodieTimeline.REPLACE_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10014", HoodieTimeline.REPLACE_COMMIT_ACTION, HoodieInstant.State.INFLIGHT),
        // no clustering action in v1, so we use REPLACE_COMMIT_ACTION instead
        (s"${initTs}10013", HoodieTimeline.REPLACE_COMMIT_ACTION, HoodieInstant.State.REQUESTED),
        (s"${initTs}10012", HoodieTimeline.REPLACE_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10011", HoodieTimeline.REPLACE_COMMIT_ACTION, HoodieInstant.State.INFLIGHT),
        (s"${initTs}10010", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10009", HoodieTimeline.COMPACTION_ACTION, HoodieInstant.State.INFLIGHT),
        (s"${initTs}10008", HoodieTimeline.CLEAN_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10007", HoodieTimeline.CLEAN_ACTION, HoodieInstant.State.INFLIGHT),
        (s"${initTs}10006", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10005", HoodieTimeline.CLEAN_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10004", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10003", HoodieTimeline.CLEAN_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10002", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10001", HoodieTimeline.CLEAN_ACTION, HoodieInstant.State.COMPLETED)
      )

      // Create all active instants
      activeInstants.foreach { case (timestamp, action, state) =>
        createActiveInstantV1(activeTimeline, timestamp, action, state, testTable)
      }

      val archivedInstants = Seq(
        (s"${initTs}10000", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}09999", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}09998", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}09997", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}09996", HoodieTimeline.DELTA_COMMIT_ACTION, HoodieInstant.State.COMPLETED)
      )

      // Write archived instants
      writeArchivedInstantsV1(metaClient, archivedInstants, testTable)

      // Run all test cases
      // Expected total: 39 (34 active + 5 archived, but compaction creates additional instants)
      runShowTimelineTestCases(tableName, activeInstants, archivedInstants, 39, initTs)
    }
  }

  test("Test show_timeline with various parameters - V1 COW") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tableLocation = tmp.getCanonicalPath
      if (HoodieSparkUtils.isSpark3_4) {
        spark.sql("set spark.sql.defaultColumn.enabled = false")
      }

      // Create V1 COW table with table version 6 (which uses timeline layout version 1)
      spark.sql(
        s"""
           |create table $tableName (
           | id int,
           | name string,
           | price double,
           | ts long
           |) using hudi
           | location '$tableLocation'
           | tblproperties (
           |   primaryKey = 'id',
           |   type = 'cow',
           |   preCombineField = 'ts',
           |   'hoodie.write.table.version' = '6'
           | )
           |""".stripMargin)

      val metaClient = HoodieTableMetaClient.builder()
        .setBasePath(tableLocation)
        .setConf(HadoopFSUtils.getStorageConf(spark.sparkContext.hadoopConfiguration))
        .build()

      // Verify timeline layout version is V1
      val timelineLayoutVersion = metaClient.getTimelineLayoutVersion.getVersion
      assert(timelineLayoutVersion == TimelineLayoutVersion.VERSION_1,
        s"V1 COW test: Expected timeline layout version 1, got $timelineLayoutVersion")

      val activeTimeline = metaClient.getActiveTimeline
      val testTable = HoodieTestTable.of(metaClient)
      val initTs = "202511300819"
      val activeInstants = Seq(
        // Completed commits
        (s"${initTs}10041", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10040", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10039", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10038", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10037", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10036", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10035", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10034", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10033", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10032", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10031", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10030", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10029", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10028", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10027", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10026", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),

        // Inflight commits
        (s"${initTs}10025", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.INFLIGHT),
        (s"${initTs}10024", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.INFLIGHT),

        // Completed clean commits
        (s"${initTs}10023", HoodieTimeline.CLEAN_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10022", HoodieTimeline.CLEAN_ACTION, HoodieInstant.State.COMPLETED),

        // Inflight clean commits
        (s"${initTs}10021", HoodieTimeline.CLEAN_ACTION, HoodieInstant.State.INFLIGHT),

        // Completed compaction
        (s"${initTs}10020", HoodieTimeline.COMPACTION_ACTION, HoodieInstant.State.COMPLETED),

        // Inflight compaction
        (s"${initTs}10019", HoodieTimeline.COMPACTION_ACTION, HoodieInstant.State.INFLIGHT),

        // Completed replace commit - clustering
        (s"${initTs}10018", HoodieTimeline.REPLACE_COMMIT_ACTION, HoodieInstant.State.COMPLETED),

        // Inflight replace commit - clustering
        (s"${initTs}10017", HoodieTimeline.REPLACE_COMMIT_ACTION, HoodieInstant.State.INFLIGHT),

        // Completed replace commit - insert overwrite
        (s"${initTs}10016", HoodieTimeline.REPLACE_COMMIT_ACTION, HoodieInstant.State.COMPLETED),

        // Inflight replace commit - insert overwrite
        (s"${initTs}10015", HoodieTimeline.REPLACE_COMMIT_ACTION, HoodieInstant.State.INFLIGHT),

        // Completed rollback
        (s"${initTs}10014", HoodieTimeline.ROLLBACK_ACTION, HoodieInstant.State.COMPLETED),

        // Pending rollback
        (s"${initTs}10013", HoodieTimeline.ROLLBACK_ACTION, HoodieInstant.State.REQUESTED)
      )

      val archivedInstants = Seq(
        (s"${initTs}10012", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10011", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10010", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10008", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED),
        (s"${initTs}10001", HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED)
      )

      // Create all active instants
      activeInstants.foreach { case (timestamp, action, state) =>
        createActiveInstantV1(activeTimeline, timestamp, action, state, testTable)
      }

      // Write archived instants
      writeArchivedInstantsV1(metaClient, archivedInstants, testTable)

      // Run all test cases
      // Expected total: 34 (29 active + 5 archived, but compaction creates additional instants)
      // 28 listed active instants + 1 from completed compaction = 29 active
      runShowTimelineTestCases(tableName, activeInstants, archivedInstants, 34, initTs)
    }
  }
}
