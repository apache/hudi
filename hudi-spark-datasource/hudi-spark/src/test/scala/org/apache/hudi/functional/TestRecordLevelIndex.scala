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
import org.apache.hudi.common.model.{HoodieCommitMetadata, HoodieTableType, WriteOperationType}
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.config.{HoodieCleanConfig, HoodieClusteringConfig, HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.metadata.{HoodieBackedTableMetadata, HoodieTableMetadataUtil, MetadataPartitionType}
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.spark.sql._
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, EnumSource}

import java.util.concurrent.atomic.AtomicInteger
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
    HoodieMetadataConfig.RECORD_INDEX_CREATE_PROP.key -> "true"
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
  var dfList: List[DataFrame] = List.empty

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
  def testMetadataRecordLevelIndexInitialization(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name())
    doWriteAndValidateRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testMetadataRecordLevelIndexUpsert(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name())
    doWriteAndValidateRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    doWriteAndValidateRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testMetadataRecordLevelIndexUpsertAndRollback(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name())
    doWriteAndValidateRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    doWriteAndValidateRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    doWriteAndValidateRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    rollbackLastInstant(hudiOpts)
    validateRecordIndices(hudiOpts)
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testMetadataRecordLevelIndexPartiallyFailedUpsertAndRollback(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name())
    doWriteAndValidateRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    doWriteAndValidateRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)

    deleteLastCompletedCommitFromTimeline(hudiOpts)

    doWriteAndValidateRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testMetadataRecordLevelIndexPartiallyFailedMetadataTableCommitAndRollback(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name())
    doWriteAndValidateRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    doWriteAndValidateRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)

    deleteLastCompletedCommitFromDataAndMetadataTimeline(hudiOpts)
    doWriteAndValidateRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testMetadataRecordLevelIndexWithDelete(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name())
    val insertDf = doWriteAndValidateRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    val deleteDf = insertDf.limit(20)
    deleteDf.write.format("org.apache.hudi")
      .options(hudiOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DELETE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    val prevDf = dfList.last
    dfList = dfList :+ prevDf.except(deleteDf)
    validateRecordIndices(hudiOpts)
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testMetadataRecordLevelIndexUpsertAndDropIndex(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name())
    doWriteAndValidateRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)

    val writeConfig = getWriteConfig(hudiOpts)
    val dropIndexInstantTime = getInstantTime()
    getHoodieTable(metaClient, writeConfig).getMetadataWriter(dropIndexInstantTime).get()
      .deletePartitions(dropIndexInstantTime, Collections.singletonList(MetadataPartitionType.RECORD_INDEX))
    assertEquals(0, getFileGroupCountForRecordIndex(writeConfig))
    metaClient.getTableConfig.getMetadataPartitionsInflight

    doWriteAndValidateRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testMetadataRecordLevelIndexWithCleaning(tableType: HoodieTableType): Unit = {
    var hudiOpts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key() -> "1")
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      hudiOpts = hudiOpts ++ Map(
        HoodieCompactionConfig.INLINE_COMPACT.key() -> "true",
        HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key() -> "1"
      )
    }

    doWriteAndValidateRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    doWriteAndValidateRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    doWriteAndValidateRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)

    val lastCleanInstant = metaClient.getActiveTimeline.getCleanerTimeline.lastInstant()
    assertTrue(lastCleanInstant.isPresent)

    doWriteAndValidateRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    assertTrue(metaClient.getActiveTimeline.getCleanerTimeline.lastInstant().get().getTimestamp
      .compareTo(lastCleanInstant.get().getTimestamp) > 0)

    rollbackLastInstant(hudiOpts)
    validateRecordIndices(hudiOpts)
  }

  @Test
  def testMetadataRecordLevelIndexWithCompaction(): Unit = {
    val hudiOpts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> HoodieTableType.MERGE_ON_READ.name(),
      HoodieCompactionConfig.INLINE_COMPACT.key() -> "true",
      HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key() -> "2"
    )

    doWriteAndValidateRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    doWriteAndValidateRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    doWriteAndValidateRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)

    val lastCompactionInstant = getLatestCompactionInstant()
    assertTrue(lastCompactionInstant.isPresent)

    doWriteAndValidateRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    assertTrue(getLatestCompactionInstant().get().getTimestamp.compareTo(lastCompactionInstant.get().getTimestamp) > 0)

    val writeConfig = getWriteConfig(hudiOpts)
    Using(new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)) { client =>
      val lastInstant = getHoodieTable(metaClient, writeConfig).getCompletedCommitsTimeline.lastInstant()
      client.rollback(lastInstant.get().getTimestamp)
    }
    validateRecordIndices(hudiOpts)
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testMetadataRecordLevelIndexWithClustering(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      HoodieClusteringConfig.INLINE_CLUSTERING.key() -> "true",
      HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key() -> "2"
    )

    doWriteAndValidateRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    doWriteAndValidateRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    doWriteAndValidateRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)

    val lastCompactionInstant = getLatestCompactionInstant()
    assertTrue(lastCompactionInstant.isPresent)

    doWriteAndValidateRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    assertTrue(getLatestCompactionInstant().get().getTimestamp.compareTo(lastCompactionInstant.get().getTimestamp) > 0)

    val writeConfig = getWriteConfig(hudiOpts)
    Using(new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)) { client =>
      val lastInstant = getHoodieTable(metaClient, writeConfig).getCompletedCommitsTimeline.lastInstant()
      client.rollback(lastInstant.get().getTimestamp)
    }
    validateRecordIndices(hudiOpts)
  }

  private def rollbackLastInstant(hudiOpts: Map[String, String]): Unit = {
    if (getLatestCompactionInstant() != metaClient.getCommitsAndCompactionTimeline.lastInstant()) {
      dfList = dfList.take(dfList.size - 1)
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
    dfList = dfList.take(dfList.size - 1)
  }

  private def deleteLastCompletedCommitFromTimeline(hudiOpts: Map[String, String]): Unit = {
    val writeConfig = getWriteConfig(hudiOpts)
    val lastInstant = getHoodieTable(metaClient, writeConfig).getCompletedCommitsTimeline.lastInstant().get()
    assertTrue(fs.delete(new Path(metaClient.getMetaPath, lastInstant.getFileName), false))
    dfList = dfList.take(dfList.size - 1)
  }

  private def getLatestCompactionInstant(): org.apache.hudi.common.util.Option[HoodieInstant] = {
    metaClient.getActiveTimeline.filter(
      s => Option(
        try {
          val commitMetadata = MetadataConversionUtils.getHoodieCommitMetadata(metaClient, s)
            .orElse(new HoodieCommitMetadata())
          commitMetadata
        } catch {
          case _ : Exception => new HoodieCommitMetadata()
        })
        .map(c => c.getOperationType == WriteOperationType.COMPACT)
        .get)
      .lastInstant()
  }

  private def doWriteAndValidateRecordIndex(hudiOpts: Map[String, String],
                                            operation: String,
                                            saveMode: SaveMode): DataFrame = {
    var records1: mutable.Buffer[String] = null
    if (operation == UPSERT_OPERATION_OPT_VAL) {
      records1 = recordsToStrings(dataGen.generateUniqueUpdates(getInstantTime(), 20)).asScala
    } else {
      records1 = recordsToStrings(dataGen.generateInserts(getInstantTime(), 100)).asScala
    }
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(hudiOpts)
      .option(DataSourceWriteOptions.OPERATION.key, operation)
      .mode(saveMode)
      .save(basePath)
    calculateMergedDf(inputDF1)
    validateRecordIndices(hudiOpts)
    inputDF1
  }

  def calculateMergedDf(inputDF1: DataFrame): Unit = {
    val prevDfOpt = dfList.lastOption
    if (prevDfOpt.isEmpty) {
      dfList = dfList :+ inputDF1
      return
    }
    val prevDf = prevDfOpt.get
    val prevDfOld = prevDf.join(inputDF1, prevDf("_row_key") === inputDF1("_row_key")
      && prevDf("partition") === inputDF1("partition"), "leftanti")
    System.out.println("LOKI calculateMergedDf prevDfOld")
    prevDfOld.show(500, false)
    val unionDf = prevDfOld.union(inputDF1)
    System.out.println("LOKI calculateMergedDf unionDf")
    unionDf.show(500, false)
    dfList = dfList :+ unionDf
  }

  private def getInstantTime(): String = {
    String.format("%03d", new Integer(instantTime.getAndIncrement()))
  }

  private def getWriteConfig(hudiOpts: Map[String, String]): HoodieWriteConfig = {
    val props = new Properties()
    props.putAll(JavaConverters.mapAsJavaMap(hudiOpts))
    HoodieWriteConfig.newBuilder()
      .withProps(props)
      .withPath(basePath)
      .build()
  }

  def getFileGroupCountForRecordIndex(writeConfig: HoodieWriteConfig): Long = {
    val tableMetadata = getHoodieTable(metaClient, writeConfig).getMetadataTable
    tableMetadata.asInstanceOf[HoodieBackedTableMetadata].getMetadataPartitionFileGroupCount(MetadataPartitionType.RECORD_INDEX)
    //    val metadataTable = getHoodieTable(tableMetadata.)
    //    val recordIndexPartitionPath = metadataTable.getAllPartitionPaths.stream().filter(p => p.contains(MetadataPartitionType.RECORD_INDEX.getPartitionPath)).findFirst()
    //    metadataTable.getAllFilesInPartition(new Path(recordIndexPartitionPath.get())).length
  }

  private def validateRecordIndices(hudiOpts: Map[String, String]): Unit = {
//    val partitionPaths = FSUtils.getAllPartitionPaths(new HoodieSparkEngineContext(jsc), HoodieMetadataConfig.newBuilder().build(), basePath)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    val writeConfig = getWriteConfig(hudiOpts)
    val metadata = metadataWriter(writeConfig).getTableMetadata
    val readDf = spark.read.format("hudi").load(basePath)
    val rowArr = readDf.collect()
    val recordIndexMap = metadata.readRecordIndex(
      JavaConverters.seqAsJavaList(rowArr.map(row => row.getAs("_hoodie_record_key").toString).toList))

    assertTrue(rowArr.length > 0)
    for (row <- rowArr) {
      val recordKey: String = row.getAs("_hoodie_record_key")
      val partitionPath: String = row.getAs("_hoodie_partition_path")
      val fileName: String = row.getAs("_hoodie_file_name")
      val recordLocation = recordIndexMap.get(recordKey)
      assertEquals(partitionPath, recordLocation.getPartitionPath)
      assertTrue(fileName.contains(recordLocation.getFileId))
    }

    assertEquals(rowArr.length, recordIndexMap.keySet.size)
    val estimatedFileGroupCount = HoodieTableMetadataUtil.estimateFileGroupCount(MetadataPartitionType.RECORD_INDEX, rowArr.length, 48,
      writeConfig.getRecordIndexMinFileGroupCount, writeConfig.getRecordIndexMaxFileGroupCount,
      writeConfig.getRecordIndexGrowthFactor, writeConfig.getRecordIndexMaxFileGroupSizeBytes)
    assertEquals(estimatedFileGroupCount, getFileGroupCountForRecordIndex(writeConfig))
    val prevDf = dfList.last.drop("tip_history")
    val nonMatchingRecords = readDf.drop("_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key",
      "_hoodie_partition_path", "_hoodie_file_name", "tip_history")
      .join(prevDf, prevDf.columns, "leftanti")
    nonMatchingRecords.show(500, false)
    assertEquals(0, nonMatchingRecords.count())
    System.out.println("LOKI validateRecordIndices readDf")
    readDf.show(500, false)
    System.out.println("LOKI validateRecordIndices prevDf")
    prevDf.show(500, false)
    assertEquals(readDf.count(), prevDf.count())
  }
}

object TestRecordLevelIndex {

  case class ColumnStatsTestCase(tableType: HoodieTableType, shouldReadInMemory: Boolean)

  def testMetadataColumnStatsIndexParams: java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(HoodieTableType.values().toStream.flatMap(tableType =>
      Seq(Arguments.arguments(ColumnStatsTestCase(tableType, shouldReadInMemory = true)),
        Arguments.arguments(ColumnStatsTestCase(tableType, shouldReadInMemory = false)))
    ): _*)
  }
}
