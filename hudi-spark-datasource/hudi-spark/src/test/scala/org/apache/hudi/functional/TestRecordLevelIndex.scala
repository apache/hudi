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

import org.apache.hadoop.fs.Path
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.client.utils.MetadataConversionUtils
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model._
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.config.{HoodieCleanConfig, HoodieClusteringConfig, HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.metadata.{HoodieBackedTableMetadata, HoodieTableMetadataUtil, MetadataPartitionType}
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.hudi.util.JavaConversions
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, not}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, EnumSource}

import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Collectors
import java.util.{Collections, Properties}
import scala.collection.JavaConverters._
import scala.collection.{JavaConverters, mutable}
import scala.util.Using

@Tag("functional")
class TestRecordLevelIndex extends HoodieSparkClientTestBase {
  var spark: SparkSession = _
  var instantTime: AtomicInteger = _
  val metadataOpts = Map(
    HoodieMetadataConfig.ENABLE.key -> "true",
    HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key -> "true"
  )
  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
    RECORDKEY_FIELD.key -> "_row_key",
    PARTITIONPATH_FIELD.key -> "partition",
    PRECOMBINE_FIELD.key -> "timestamp",
    HoodieTableConfig.POPULATE_META_FIELDS.key -> "true"
  ) ++ metadataOpts
  var mergedDfList: List[DataFrame] = List.empty

  @BeforeEach
  override def setUp() {
    initPath()
    initSparkContexts()
    initFileSystem()
    initTestDataGenerator()

    setTableName("hoodie_test")
    initMetaClient()

    instantTime = new AtomicInteger(1)

    spark = sqlContext.sparkSession
  }

  @AfterEach
  override def tearDown() = {
    cleanupFileSystem()
    cleanupSparkContexts()
  }

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
  def testRLIWithCleaning(tableType: HoodieTableType): Unit = {
    var hudiOpts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key() -> "1")
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      hudiOpts = hudiOpts ++ Map(
        HoodieCompactionConfig.INLINE_COMPACT.key() -> "true",
        HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key() -> "1"
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

    rollbackLastInstant(hudiOpts)
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

    val lastCompactionInstant = getLatestCompactionInstant()
    assertTrue(lastCompactionInstant.isPresent)

    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    assertTrue(getLatestCompactionInstant().get().getTimestamp.compareTo(lastCompactionInstant.get().getTimestamp) > 0)

    val writeConfig = getWriteConfig(hudiOpts)
    Using(new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)) { client =>
      val lastInstant = getHoodieTable(metaClient, writeConfig).getCompletedCommitsTimeline.lastInstant()
      client.rollback(lastInstant.get().getTimestamp)
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

  @Disabled("needs investigation")
  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testRLIWithMDTCleaning(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name())

    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    val metadataTableFSView = getHoodieTable(metaClient, getWriteConfig(hudiOpts)).getMetadata
      .asInstanceOf[HoodieBackedTableMetadata].getMetadataFileSystemView
    assertTrue(
      metadataTableFSView.getTimeline
        .filter(JavaConversions.getPredicate(instant => instant.getAction == ActionType.clean.name()))
        .lastInstant()
        .isPresent)
  }

  private def rollbackLastInstant(hudiOpts: Map[String, String]): Unit = {
    if (getLatestCompactionInstant() != metaClient.getCommitsAndCompactionTimeline.lastInstant()) {
      mergedDfList = mergedDfList.take(mergedDfList.size - 1)
    }
    val writeConfig = getWriteConfig(hudiOpts)
    Using(new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)) { client =>
      val lastInstant = getHoodieTable(metaClient, writeConfig).getCompletedCommitsTimeline.lastInstant()
      client.rollback(lastInstant.get().getTimestamp)
    }
  }

  private def deleteLastCompletedCommitFromDataAndMetadataTimeline(hudiOpts: Map[String, String]): Unit = {
    val writeConfig = getWriteConfig(hudiOpts)
    val lastInstant = getHoodieTable(metaClient, writeConfig).getCompletedCommitsTimeline.lastInstant().get()
    val metadataTableMetaClient = getHoodieTable(metaClient, writeConfig).getMetadataTable.asInstanceOf[HoodieBackedTableMetadata].getMetadataMetaClient
    val metadataTableLastInstant = metadataTableMetaClient.getCommitsTimeline.lastInstant().get()
    assertTrue(fs.delete(new Path(metaClient.getMetaPath, lastInstant.getFileName), false))
    assertTrue(fs.delete(new Path(metadataTableMetaClient.getMetaPath, metadataTableLastInstant.getFileName), false))
    mergedDfList = mergedDfList.take(mergedDfList.size - 1)
  }

  private def deleteLastCompletedCommitFromTimeline(hudiOpts: Map[String, String]): Unit = {
    val writeConfig = getWriteConfig(hudiOpts)
    val lastInstant = getHoodieTable(metaClient, writeConfig).getCompletedCommitsTimeline.lastInstant().get()
    assertTrue(fs.delete(new Path(metaClient.getMetaPath, lastInstant.getFileName), false))
    mergedDfList = mergedDfList.take(mergedDfList.size - 1)
  }

  private def getLatestCompactionInstant(): org.apache.hudi.common.util.Option[HoodieInstant] = {
    metaClient.reloadActiveTimeline()
      .filter(JavaConversions.getPredicate(s => Option(
        try {
          val commitMetadata = MetadataConversionUtils.getHoodieCommitMetadata(metaClient, s)
            .orElse(new HoodieCommitMetadata())
          commitMetadata
        } catch {
          case _: Exception => new HoodieCommitMetadata()
        })
        .map(c => c.getOperationType == WriteOperationType.COMPACT)
        .get))
      .lastInstant()
  }

  private def getLatestClusteringInstant(): org.apache.hudi.common.util.Option[HoodieInstant] = {
    metaClient.getActiveTimeline.getCompletedReplaceTimeline.lastInstant()
  }

  private def doWriteAndValidateDataAndRecordIndex(hudiOpts: Map[String, String],
                                                   operation: String,
                                                   saveMode: SaveMode,
                                                   validate: Boolean = true): DataFrame = {
    var latestBatch: mutable.Buffer[String] = null
    if (operation == UPSERT_OPERATION_OPT_VAL) {
      val instantTime = getInstantTime()
      val records = recordsToStrings(dataGen.generateUniqueUpdates(instantTime, 1))
      records.addAll(recordsToStrings(dataGen.generateInserts(instantTime, 1)))
      latestBatch = records.asScala
    } else if (operation == INSERT_OVERWRITE_OPERATION_OPT_VAL) {
      latestBatch = recordsToStrings(dataGen.generateInsertsForPartition(
        getInstantTime(), 5, dataGen.getPartitionPaths.last)).asScala
    } else {
      latestBatch = recordsToStrings(dataGen.generateInserts(getInstantTime(), 5)).asScala
    }
    val latestBatchDf = spark.read.json(spark.sparkContext.parallelize(latestBatch, 2))
    latestBatchDf.cache()
    latestBatchDf.write.format("org.apache.hudi")
      .options(hudiOpts)
      .option(DataSourceWriteOptions.OPERATION.key, operation)
      .mode(saveMode)
      .save(basePath)
    val deletedDf = calculateMergedDf(latestBatchDf, operation)
    deletedDf.cache()
    if (validate) {
      validateDataAndRecordIndices(hudiOpts, deletedDf)
    }
    deletedDf.unpersist()
    latestBatchDf
  }

  /**
   * @return [[DataFrame]] that should not exist as of the latest instant; used for non-existence validation.
   */
  def calculateMergedDf(latestBatchDf: DataFrame, operation: String): DataFrame = {
    val prevDfOpt = mergedDfList.lastOption
    if (prevDfOpt.isEmpty) {
      mergedDfList = mergedDfList :+ latestBatchDf
      sparkSession.emptyDataFrame
    } else {
      if (operation == INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL) {
        mergedDfList = mergedDfList :+ latestBatchDf
        // after insert_overwrite_table, all previous snapshot's records should be deleted from RLI
        prevDfOpt.get
      } else if (operation == INSERT_OVERWRITE_OPERATION_OPT_VAL) {
        val overwrittenPartitions = latestBatchDf.select("partition")
          .collectAsList().stream().map[String](JavaConversions.getFunction[Row, String](r => r.getString(0))).collect(Collectors.toList[String])
        val prevDf = prevDfOpt.get
        val latestSnapshot = prevDf
          .filter(not(col("partition").isInCollection(overwrittenPartitions)))
          .union(latestBatchDf)
        mergedDfList = mergedDfList :+ latestSnapshot

        // after insert_overwrite (partition), all records in the overwritten partitions should be deleted from RLI
        prevDf.filter(col("partition").isInCollection(overwrittenPartitions))
      } else {
        val prevDf = prevDfOpt.get
        val prevDfOld = prevDf.join(latestBatchDf, prevDf("_row_key") === latestBatchDf("_row_key")
          && prevDf("partition") === latestBatchDf("partition"), "leftanti")
        val latestSnapshot = prevDfOld.union(latestBatchDf)
        mergedDfList = mergedDfList :+ latestSnapshot
        sparkSession.emptyDataFrame
      }
    }
  }

  private def getInstantTime(): String = {
    String.format("%03d", new Integer(instantTime.getAndIncrement()))
  }

  private def getWriteConfig(hudiOpts: Map[String, String]): HoodieWriteConfig = {
    val props = new Properties()
    props.putAll(JavaConverters.mapAsJavaMapConverter(hudiOpts).asJava)
    HoodieWriteConfig.newBuilder()
      .withProps(props)
      .withPath(basePath)
      .build()
  }

  def getFileGroupCountForRecordIndex(writeConfig: HoodieWriteConfig): Long = {
    val tableMetadata = getHoodieTable(metaClient, writeConfig).getMetadataTable.asInstanceOf[HoodieBackedTableMetadata]
    tableMetadata.getMetadataFileSystemView.getAllFileGroups(MetadataPartitionType.RECORD_INDEX.getPartitionPath).count
  }

  private def validateDataAndRecordIndices(hudiOpts: Map[String, String],
                                           deletedDf: DataFrame = sparkSession.emptyDataFrame): Unit = {
    metaClient = HoodieTableMetaClient.reload(metaClient)
    val writeConfig = getWriteConfig(hudiOpts)
    val metadata = metadataWriter(writeConfig).getTableMetadata
    val readDf = spark.read.format("hudi").load(basePath)
    val rowArr = readDf.collect()
    val recordIndexMap = metadata.readRecordIndex(
      JavaConverters.seqAsJavaListConverter(rowArr.map(row => row.getAs("_hoodie_record_key").toString).toList).asJava)

    assertTrue(rowArr.length > 0)
    for (row <- rowArr) {
      val recordKey: String = row.getAs("_hoodie_record_key")
      val partitionPath: String = row.getAs("_hoodie_partition_path")
      val fileName: String = row.getAs("_hoodie_file_name")
      val recordLocation = recordIndexMap.get(recordKey)
      assertEquals(partitionPath, recordLocation.getPartitionPath)
      assertTrue(fileName.startsWith(recordLocation.getFileId), fileName + " should start with " + recordLocation.getFileId)
    }

    val deletedRows = deletedDf.collect()
    val recordIndexMapForDeletedRows = metadata.readRecordIndex(
      JavaConverters.seqAsJavaListConverter(deletedRows.map(row => row.getAs("_row_key").toString).toList).asJava)
    assertEquals(0, recordIndexMapForDeletedRows.size(), "deleted records should not present in RLI")

    assertEquals(rowArr.length, recordIndexMap.keySet.size)
    val estimatedFileGroupCount = HoodieTableMetadataUtil.estimateFileGroupCount(MetadataPartitionType.RECORD_INDEX, rowArr.length, 48,
      writeConfig.getRecordIndexMinFileGroupCount, writeConfig.getRecordIndexMaxFileGroupCount,
      writeConfig.getRecordIndexGrowthFactor, writeConfig.getRecordIndexMaxFileGroupSizeBytes)
    assertEquals(estimatedFileGroupCount, getFileGroupCountForRecordIndex(writeConfig))
    val prevDf = mergedDfList.last.drop("tip_history")
    val nonMatchingRecords = readDf.drop("_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key",
      "_hoodie_partition_path", "_hoodie_file_name", "tip_history")
      .join(prevDf, prevDf.columns, "leftanti")
    assertEquals(0, nonMatchingRecords.count())
    assertEquals(readDf.count(), prevDf.count())
  }
}
