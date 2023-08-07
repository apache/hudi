/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.functional

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.client.transaction.PreferWriterConflictResolutionStrategy
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model._
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.config._
import org.apache.hudi.exception.HoodieWriteConflictException
import org.apache.hudi.metadata.{HoodieBackedTableMetadata, MetadataPartitionType}
import org.apache.hudi.util.JavaConversions
import org.apache.spark.sql._
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, EnumSource}

import java.util.Collections
import java.util.concurrent.Executors
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.Using

@Tag("functional")
class TestRecordLevelIndex extends RecordLevelIndexTestBase {
  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testRLIInitialization(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name())
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testRLIUpsert(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name())
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
  }

  @ParameterizedTest
  @CsvSource(Array("COPY_ON_WRITE,true", "COPY_ON_WRITE,false", "MERGE_ON_READ,true", "MERGE_ON_READ,false"))
  def testRLIBulkInsertThenInsertOverwrite(tableType: HoodieTableType, enableRowWriter: Boolean): Unit = {
    val hudiOpts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      DataSourceWriteOptions.ENABLE_ROW_WRITER.key -> enableRowWriter.toString
    )
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OVERWRITE_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testRLIUpsertAndRollback(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name())
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    rollbackLastInstant(hudiOpts)
    validateDataAndRecordIndices(hudiOpts)
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testRLIPartiallyFailedUpsertAndRollback(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name())
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)

    deleteLastCompletedCommitFromTimeline(hudiOpts)

    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testRLIPartiallyFailedMetadataTableCommitAndRollback(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name())
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)

    deleteLastCompletedCommitFromDataAndMetadataTimeline(hudiOpts)
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testRLIWithDelete(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name())
    val insertDf = doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    val deleteDf = insertDf.limit(1)
    deleteDf.write.format("org.apache.hudi")
      .options(hudiOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DELETE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    val prevDf = mergedDfList.last
    mergedDfList = mergedDfList :+ prevDf.except(deleteDf)
    validateDataAndRecordIndices(hudiOpts)
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testRLIWithDeletePartition(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name())
    val latestSnapshot = doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)

    Using(getHoodieWriteClient(getWriteConfig(hudiOpts))) { client =>
      val commitTime = client.startCommit
      client.startCommitWithTime(commitTime, HoodieTimeline.REPLACE_COMMIT_ACTION)
      val deletingPartition = dataGen.getPartitionPaths.last
      val partitionList = Collections.singletonList(deletingPartition)
      client.deletePartitions(partitionList, commitTime)

      val deletedDf = latestSnapshot.filter(s"partition = $deletingPartition")
      validateDataAndRecordIndices(hudiOpts, deletedDf)
    }
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testRLIUpsertAndDropIndex(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name())
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)

    val writeConfig = getWriteConfig(hudiOpts)
    metadataWriter(writeConfig).dropMetadataPartitions(Collections.singletonList(MetadataPartitionType.RECORD_INDEX))
    assertEquals(0, getFileGroupCountForRecordIndex(writeConfig))
    metaClient.getTableConfig.getMetadataPartitionsInflight

    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testRLIWithDTCleaning(tableType: HoodieTableType): Unit = {
    var hudiOpts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key() -> "1")
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      hudiOpts = hudiOpts ++ Map(
        HoodieCompactionConfig.INLINE_COMPACT.key() -> "true",
        HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key() -> "2",
        HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key() -> "15"
      )
    }

    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)

    val lastCleanInstant = metaClient.getActiveTimeline.getCleanerTimeline.lastInstant()
    assertTrue(lastCleanInstant.isPresent)

    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    assertTrue(metaClient.getActiveTimeline.getCleanerTimeline.lastInstant().get().getTimestamp
      .compareTo(lastCleanInstant.get().getTimestamp) > 0)

    var rollbackedInstant: Option[HoodieInstant] = Option.empty
    while (rollbackedInstant.isEmpty || rollbackedInstant.get.getAction != ActionType.clean.name()) {
      // rollback clean instant
      rollbackedInstant = Option.apply(rollbackLastInstant(hudiOpts))
    }
    validateDataAndRecordIndices(hudiOpts)
  }

  @Test
  def testRLIWithDTCompaction(): Unit = {
    val hudiOpts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> HoodieTableType.MERGE_ON_READ.name(),
      HoodieCompactionConfig.INLINE_COMPACT.key() -> "true",
      HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key() -> "2",
      HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key() -> "0"
    )

    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)

    var lastCompactionInstant = getLatestCompactionInstant()
    assertTrue(lastCompactionInstant.isPresent)

    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    assertTrue(getLatestCompactionInstant().get().getTimestamp.compareTo(lastCompactionInstant.get().getTimestamp) > 0)
    lastCompactionInstant = getLatestCompactionInstant()

    var rollbackedInstant: Option[HoodieInstant] = Option.empty
    while (rollbackedInstant.isEmpty || rollbackedInstant.get.getTimestamp != lastCompactionInstant.get().getTimestamp) {
      // rollback compaction instant
      rollbackedInstant = Option.apply(rollbackLastInstant(hudiOpts))
    }
    validateDataAndRecordIndices(hudiOpts)
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testRLIWithDTClustering(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      HoodieClusteringConfig.INLINE_CLUSTERING.key() -> "true",
      HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key() -> "2"
    )

    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)

    val lastClusteringInstant = getLatestClusteringInstant()
    assertTrue(lastClusteringInstant.isPresent)

    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)

    assertTrue(getLatestClusteringInstant().get().getTimestamp.compareTo(lastClusteringInstant.get().getTimestamp) > 0)
    assertEquals(getLatestClusteringInstant(), metaClient.getActiveTimeline.lastInstant())
    // We are validating rollback of a DT clustering instant here
    rollbackLastInstant(hudiOpts)
    validateDataAndRecordIndices(hudiOpts)
  }

  @ParameterizedTest
  @CsvSource(value = Array(
    "COPY_ON_WRITE,COLUMN_STATS",
    "COPY_ON_WRITE,BLOOM_FILTERS",
    "COPY_ON_WRITE,COLUMN_STATS:BLOOM_FILTERS",
    "MERGE_ON_READ,COLUMN_STATS",
    "MERGE_ON_READ,BLOOM_FILTERS",
    "MERGE_ON_READ,COLUMN_STATS:BLOOM_FILTERS")
  )
  def testRLIWithOtherMetadataPartitions(tableType: String, metadataPartitionTypes: String): Unit = {
    var hudiOpts = commonOpts
    val metadataPartitions = metadataPartitionTypes.split(":").toStream.map(p => MetadataPartitionType.valueOf(p)).toList
    for (metadataPartition <- metadataPartitions) {
      if (metadataPartition == MetadataPartitionType.COLUMN_STATS) {
        hudiOpts += (HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true")
      } else if (metadataPartition == MetadataPartitionType.BLOOM_FILTERS) {
        hudiOpts += (HoodieMetadataConfig.ENABLE_METADATA_INDEX_BLOOM_FILTER.key() -> "true")
      }
    }

    hudiOpts = hudiOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType)
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    assertTrue(metadataWriter(getWriteConfig(hudiOpts)).getEnabledPartitionTypes.containsAll(metadataPartitions.asJava))
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testEnableDisableRLI(tableType: HoodieTableType): Unit = {
    var hudiOpts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name()
    )

    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)

    hudiOpts += (HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key -> "false")
    metaClient.getTableConfig.setMetadataPartitionState(metaClient, MetadataPartitionType.RECORD_INDEX, false)

    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      validate = false)

    try {
      validateDataAndRecordIndices(hudiOpts)
    } catch {
      case e: Exception =>
        assertTrue(e.isInstanceOf[IllegalStateException])
        assertTrue(e.getMessage.contains("Record index is not initialized in MDT"))
    }

    hudiOpts += (HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key -> "true")
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    validateDataAndRecordIndices(hudiOpts)
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testRLIWithMDTCompaction(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key() -> "1"
    )

    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    val metadataTableFSView = getHoodieTable(metaClient, getWriteConfig(hudiOpts)).getMetadata
      .asInstanceOf[HoodieBackedTableMetadata].getMetadataFileSystemView
    val compactionTimeline = metadataTableFSView.getVisibleCommitsAndCompactionTimeline.filterCompletedAndCompactionInstants()
    val lastCompactionInstant = compactionTimeline
      .filter(JavaConversions.getPredicate((instant: HoodieInstant) =>
        HoodieCommitMetadata.fromBytes(compactionTimeline.getInstantDetails(instant).get, classOf[HoodieCommitMetadata])
          .getOperationType == WriteOperationType.COMPACT))
      .lastInstant()
    val compactionBaseFile = metadataTableFSView.getAllBaseFiles(MetadataPartitionType.RECORD_INDEX.getPartitionPath)
      .filter(JavaConversions.getPredicate((f: HoodieBaseFile) => f.getCommitTime.equals(lastCompactionInstant.get().getTimestamp)))
      .findAny()
    assertTrue(compactionBaseFile.isPresent)
  }

  @Disabled("Would take a long time to run on regular basis")
  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testRLIWithMDTCleaning(tableType: HoodieTableType): Unit = {
    var hudiOpts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key() -> "1")
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)

    hudiOpts = hudiOpts + (HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key() -> "40")
    val function = () => doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    executeFunctionNTimes(function, 20)

    assertTrue(getMetadataMetaClient(hudiOpts).getActiveTimeline.getCleanerTimeline.lastInstant().isPresent)
    rollbackLastInstant(hudiOpts)
    // Rolling back clean instant from MDT
    rollbackLastInstant(hudiOpts)
    validateDataAndRecordIndices(hudiOpts)
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testRLIWithMultiWriter(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key() -> "optimistic_concurrency_control",
      HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key() -> "LAZY",
      HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key() -> "org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider",
      HoodieLockConfig.WRITE_CONFLICT_RESOLUTION_STRATEGY_CLASS_NAME.key() -> classOf[PreferWriterConflictResolutionStrategy].getName
    )

    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      validate = false)

    val executor = Executors.newFixedThreadPool(2)
    implicit val executorContext: ExecutionContext = ExecutionContext.fromExecutor(executor)
    val function = new Function0[Boolean] {
      override def apply(): Boolean = {
        try {
          doWriteAndValidateDataAndRecordIndex(hudiOpts,
            operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
            saveMode = SaveMode.Append,
            validate = false)
          true
        } catch {
          case _: HoodieWriteConflictException => false
          case e => throw new Exception("Multi write failed", e)
        }
      }
    }
    val f1 = Future[Boolean] {
      function.apply()
    }
    val f2 = Future[Boolean] {
      function.apply()
    }

    Await.result(f1, Duration("5 minutes"))
    Await.result(f2, Duration("5 minutes"))

    assertTrue(f1.value.get.get || f2.value.get.get)
    executor.shutdownNow()
    validateDataAndRecordIndices(hudiOpts)
  }
}
