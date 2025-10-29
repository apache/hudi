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
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.client.transaction.PreferWriterConflictResolutionStrategy
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model._
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.testutils.{HoodieTestDataGenerator, InProcessTimeGenerator}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings
import org.apache.hudi.config._
import org.apache.hudi.exception.HoodieWriteConflictException
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.index.record.HoodieRecordIndex
import org.apache.hudi.metadata.{HoodieBackedTableMetadata, HoodieTableMetadataUtil, MetadataPartitionType}
import org.apache.hudi.util.JavaConversions

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.jupiter.api._
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, CsvSource, EnumSource, MethodSource}
import org.junit.jupiter.params.provider.Arguments.arguments

import java.util
import java.util.{Collections, Properties}
import java.util.concurrent.Executors

import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.language.postfixOps
import scala.util.Using

@Tag("functional-b")
class TestGlobalRecordLevelIndex extends RecordLevelIndexTestBase {

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testRLIInitialization(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name())
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertFalse(HoodieRecordIndex.isPartitioned(metaClient.getIndexMetadata.get().getIndexDefinitions.get(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX)))
  }

  @Test
  def testRLIInitializationForMorGlobalIndex(): Unit = {
    val tableType = HoodieTableType.MERGE_ON_READ
    val hudiOpts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name()) +
      (HoodieMetadataConfig.RECORD_INDEX_MIN_FILE_GROUP_COUNT_PROP.key -> "1") +
      (HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_COUNT_PROP.key -> "1") +
      (HoodieIndexConfig.INDEX_TYPE.key -> HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX.name()) +
      (HoodieIndexConfig.RECORD_INDEX_UPDATE_PARTITION_PATH_ENABLE.key -> "true") -
      HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key

    val dataGen1 = HoodieTestDataGenerator.createTestGeneratorFirstPartition()
    val dataGen2 = HoodieTestDataGenerator.createTestGeneratorSecondPartition()

    // batch1 inserts (5 records)
    val instantTime1 = getNewInstantTime()
    val latestBatch = recordsToStrings(dataGen1.generateInserts(instantTime1, 5)).asScala.toSeq
    var operation = INSERT_OPERATION_OPT_VAL
    val latestBatchDf = spark.read.json(spark.sparkContext.parallelize(latestBatch, 1))
    latestBatchDf.cache()
    latestBatchDf.write.format("hudi")
      .options(hudiOpts)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val deletedDf1 = calculateMergedDf(latestBatchDf, operation, true)
    deletedDf1.cache()

    // batch2. upsert. update few records to 2nd partition from partition1 and insert a few to partition2.
    val instantTime2 = getNewInstantTime()

    val latestBatch2_1 = recordsToStrings(dataGen1.generateUniqueUpdates(instantTime2, 3)).asScala.toSeq
    val latestBatchDf2_1 = spark.read.json(spark.sparkContext.parallelize(latestBatch2_1, 1))
    val latestBatchDf2_2 = latestBatchDf2_1.withColumn("partition", lit(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH))
      .withColumn("partition_path", lit(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH))
    val latestBatch2_3 = recordsToStrings(dataGen2.generateInserts(instantTime2, 2)).asScala.toSeq
    val latestBatchDf2_3 = spark.read.json(spark.sparkContext.parallelize(latestBatch2_3, 1))
    val latestBatchDf2Final = latestBatchDf2_3.union(latestBatchDf2_2)
    latestBatchDf2Final.cache()
    latestBatchDf2Final.write.format("hudi")
      .options(hudiOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    operation = UPSERT_OPERATION_OPT_VAL
    val deletedDf2 = calculateMergedDf(latestBatchDf2Final, operation, true)
    deletedDf2.cache()

    val hudiOpts2 = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name()) +
      (HoodieMetadataConfig.RECORD_INDEX_MIN_FILE_GROUP_COUNT_PROP.key -> "1") +
      (HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_COUNT_PROP.key -> "1") +
      (HoodieIndexConfig.INDEX_TYPE.key -> HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX.name()) +
      (HoodieIndexConfig.RECORD_INDEX_UPDATE_PARTITION_PATH_ENABLE.key -> "true") +
      (HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key -> "true")

    val instantTime3 = getNewInstantTime()
    // batch3. update 2 records from newly inserted records from commit 2 to partition2
    val latestBatch3 = recordsToStrings(dataGen2.generateUniqueUpdates(instantTime3, 2)).asScala.toSeq
    val latestBatchDf3 = spark.read.json(spark.sparkContext.parallelize(latestBatch3, 1))
    latestBatchDf3.cache()
    latestBatchDf3.write.format("hudi")
      .options(hudiOpts2)
      .mode(SaveMode.Append)
      .save(basePath)
    val deletedDf3 = calculateMergedDf(latestBatchDf3, operation, true)
    deletedDf3.cache()
    metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build()
    validateDataAndRecordIndices(hudiOpts, deletedDf3)
    deletedDf2.unpersist()
    deletedDf3.unpersist()
  }

  private def getNewInstantTime(): String = {
    InProcessTimeGenerator.createNewInstantTime();
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testRLIUpsertTableTypes(tableType: HoodieTableType): Unit = {
    testRLIUpsert(tableType, "GLOBAL_RECORD_LEVEL_INDEX")
  }

  @ParameterizedTest
  @CsvSource(value = Array(
    "RECORD_INDEX",
    "GLOBAL_RECORD_LEVEL_INDEX")
  )
  def testRLIUpsertIndexTypes(indexType: String): Unit = {
    testRLIUpsert(HoodieTableType.COPY_ON_WRITE, indexType)
  }

  @ParameterizedTest
  @CsvSource(value = Array(
    "hoodie.metadata.record.index.enable",
    "hoodie.metadata.global.record.level.index.enable")
  )
  def testRLIUpsertRecordKIndexWriteConfigVariants(recordIndexWriteConfigToEnable: String): Unit = {
    testRLIUpsert(HoodieTableType.COPY_ON_WRITE, HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX.name(), recordIndexWriteConfigToEnable)
  }

  private def testRLIUpsert(tableType: HoodieTableType, indexType: String,
                            recordIndexWriteConfigToEnable : String = HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key()): Unit = {
    val hudiOpts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name()) +
      (HoodieIndexConfig.INDEX_TYPE.key -> indexType) + (recordIndexWriteConfigToEnable -> "true")
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testRLIUpsertNonPartitioned(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts - PARTITIONPATH_FIELD.key + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name())
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
    val props = new Properties()
    for ((k, v) <- hudiOpts) {
      props.put(k, v)
    }
    initMetaClient(tableType, props)

    val insertDf = doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    val deleteDf = insertDf.limit(1)
    deleteDf.write.format("hudi")
      .options(hudiOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DELETE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    val prevDf = mergedDfList.last
    mergedDfList = mergedDfList :+ prevDf.except(deleteDf)
    validateDataAndRecordIndices(hudiOpts, deleteDf)
  }

  @Test
  def testRLIWithEmptyPayload(): Unit = {
    val hudiOpts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> HoodieTableType.MERGE_ON_READ.name(),
      HoodieWriteConfig.MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key -> "0")
    val insertDf = doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)

    val deleteDf = insertDf.limit(2)
    deleteDf.cache()
    deleteDf.write.format("hudi")
      .options(hudiOpts)
      .option(DataSourceWriteOptions.OPERATION.key(), DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    val prevDf = mergedDfList.last
    mergedDfList = mergedDfList :+ prevDf.except(deleteDf)
    validateDataAndRecordIndices(hudiOpts, deleteDf)
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testRLIForDeletesWithHoodieIsDeletedColumn(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name()) +
      (HoodieIndexConfig.INDEX_TYPE.key -> HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX.name()) +
      (HoodieIndexConfig.RECORD_INDEX_UPDATE_PARTITION_PATH_ENABLE.key -> "true")
    val insertDf = doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    insertDf.cache()

    val instantTime = getNewInstantTime
    // Issue four deletes, one with the original partition, one with an updated partition,
    // and two with an older ordering value that should be ignored
    val deletedRecords = dataGen.generateUniqueDeleteRecords(instantTime, 1)
    deletedRecords.addAll(dataGen.generateUniqueDeleteRecordsWithUpdatedPartition(instantTime, 1))
    val inputRecords = new util.ArrayList[HoodieRecord[_]](deletedRecords)
    val lowerOrderingValue = 1L
    inputRecords.addAll(dataGen.generateUniqueDeleteRecords(instantTime, 1, lowerOrderingValue))
    inputRecords.addAll(dataGen.generateUniqueDeleteRecordsWithUpdatedPartition(instantTime, 1, lowerOrderingValue))
    val deleteBatch = recordsToStrings(inputRecords).asScala
    val deleteDf = spark.read.json(spark.sparkContext.parallelize(deleteBatch.toSeq, 1))
    deleteDf.cache()
    val recordKeyToDelete1 = deleteDf.collectAsList().get(0).getAs("_row_key").asInstanceOf[String]
    val recordKeyToDelete2 = deleteDf.collectAsList().get(1).getAs("_row_key").asInstanceOf[String]
    deleteDf.write.format("hudi")
      .options(hudiOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val prevDf = mergedDfList.last
    mergedDfList = mergedDfList :+ prevDf.filter(row => row.getAs("_row_key").asInstanceOf[String] != recordKeyToDelete1 &&
      row.getAs("_row_key").asInstanceOf[String] != recordKeyToDelete2)
    validateDataAndRecordIndices(hudiOpts, spark.read.json(spark.sparkContext.parallelize(recordsToStrings(deletedRecords).asScala.toSeq, 1)))
    deleteDf.unpersist()
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testRLIForDeletesWithCommitTimeOrdering(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name()) +
      (HoodieIndexConfig.INDEX_TYPE.key -> HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX.name()) +
      (HoodieIndexConfig.RECORD_INDEX_UPDATE_PARTITION_PATH_ENABLE.key -> "true") +
      (HoodieTableConfig.ORDERING_FIELDS.key -> "")
    val insertDf = doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    insertDf.cache()

    val instantTime = getNewInstantTime
    val lowerOrderingValue = 1L
    // Issue two deletes, one with the original partition, one with an updated partition,
    // Both have an older ordering value but that is ignored since the table uses commit time ordering
    val deletedRecords = dataGen.generateUniqueDeleteRecords(instantTime, 1, lowerOrderingValue)
    deletedRecords.addAll(dataGen.generateUniqueDeleteRecordsWithUpdatedPartition(instantTime, 1, lowerOrderingValue))
    val deleteBatch = recordsToStrings(deletedRecords).asScala
    val deleteDf = spark.read.json(spark.sparkContext.parallelize(deleteBatch.toSeq, 1))
    deleteDf.cache()
    val recordKeyToDelete1 = deleteDf.collectAsList().get(0).getAs("_row_key").asInstanceOf[String]
    val recordKeyToDelete2 = deleteDf.collectAsList().get(1).getAs("_row_key").asInstanceOf[String]
    deleteDf.write.format("hudi")
      .options(hudiOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val prevDf = mergedDfList.last
    mergedDfList = mergedDfList :+ prevDf.filter(row => row.getAs("_row_key").asInstanceOf[String] != recordKeyToDelete1 &&
      row.getAs("_row_key").asInstanceOf[String] != recordKeyToDelete2)
    validateDataAndRecordIndices(hudiOpts, deleteDf)
    deleteDf.unpersist()
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testRLIForDeletesWithSQLDelete(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name())
    val insertDf = doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    insertDf.cache()

    spark.sql(s"SET hoodie.hfile.block.cache.size = 200")
    spark.sql(s"CREATE TABLE IF NOT EXISTS hudi_indexed_table USING hudi OPTIONS (hoodie.metadata.enable = 'true', hoodie.metadata.record.index.enable = 'true', hoodie.write.merge.handle.class = 'org.apache.hudi.io.FileGroupReaderBasedMergeHandle') LOCATION '$basePath'")
    val existingKeys = dataGen.getExistingKeys
    spark.sql(s"DELETE FROM hudi_indexed_table WHERE _row_key IN ('${existingKeys.get(0)}', '${existingKeys.get(1)}')")

    val prevDf = mergedDfList.last
    mergedDfList = mergedDfList :+ prevDf.filter(row => row.getAs("_row_key").asInstanceOf[String] != existingKeys.get(0) &&
      row.getAs("_row_key").asInstanceOf[String] != existingKeys.get(1))
    val structType = new StructType(Array(StructField("_row_key", StringType)))
    val convertToRow: Function[String, Row] = key => new GenericRowWithSchema(Array(key), structType)
    val rows: java.util.List[Row] = util.Arrays.asList(convertToRow.apply(existingKeys.get(0)), convertToRow.apply(existingKeys.get(1)))
    val deleteDf = spark.createDataFrame(rows, structType)
    validateDataAndRecordIndices(hudiOpts, deleteDf)
    deleteDf.unpersist()
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testRLIWithDeletePartition(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name())
    val latestSnapshot = doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)

    Using(getHoodieWriteClient(getWriteConfig(hudiOpts))) { client =>
      val commitTime = client.startCommit(HoodieTimeline.REPLACE_COMMIT_ACTION)
      val deletingPartition = dataGen.getPartitionPaths.last
      val partitionList = Collections.singletonList(deletingPartition)
      val result = client.deletePartitions(partitionList, commitTime)
      client.commit(commitTime, result.getWriteStatuses, org.apache.hudi.common.util.Option.empty(), HoodieTimeline.REPLACE_COMMIT_ACTION,
        result.getPartitionToReplaceFileIds, org.apache.hudi.common.util.Option.empty());

      val deletedDf = latestSnapshot.filter(s"partition = $deletingPartition")
      validateDataAndRecordIndices(hudiOpts, deletedDf)
    }
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testRLIUpsertAndDropIndex(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts ++ Map(DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true")
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)

    val writeConfig = getWriteConfig(hudiOpts)
    writeConfig.setSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
    getHoodieWriteClient(writeConfig).dropIndex(Collections.singletonList(MetadataPartitionType.RECORD_INDEX.getPartitionPath))
    assertEquals(0, getFileGroupCountForRecordIndex(writeConfig))
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertEquals(0, metaClient.getTableConfig.getMetadataPartitionsInflight.size())
    // only files, col stats, partition stats partition should be present.
    assertEquals(3, metaClient.getTableConfig.getMetadataPartitions.size())

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

    val lastCleanInstant = getLatestMetaClient(false).getActiveTimeline.getCleanerTimeline.lastInstant()
    assertTrue(lastCleanInstant.isPresent)
    val writeConfig = getWriteConfig(hudiOpts)
    val client = new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)
    client.savepoint("user", "note")

    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    assertTrue(getLatestMetaClient(false).getActiveTimeline.getCleanerTimeline.lastInstant().get().requestedTime
      .compareTo(lastCleanInstant.get().requestedTime) > 0)

    client.restoreToSavepoint()
    client.close()
    // last commit is no longer present so remove it from the mergedDfList
    mergedDfList = mergedDfList.take(mergedDfList.size - 1)
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
    assertTrue(getLatestCompactionInstant().get().requestedTime.compareTo(lastCompactionInstant.get().requestedTime) > 0)
    lastCompactionInstant = getLatestCompactionInstant()

    var rollbackedInstant: Option[HoodieInstant] = Option.empty
    while (rollbackedInstant.isEmpty || rollbackedInstant.get.requestedTime != lastCompactionInstant.get().requestedTime) {
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
    val props = new Properties()
    for ((k, v) <- hudiOpts) {
      props.put(k, v)
    }
    initMetaClient(tableType, props)

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

    assertTrue(getLatestClusteringInstant().get().requestedTime.compareTo(lastClusteringInstant.get().requestedTime) > 0)
    assertEquals(getLatestClusteringInstant(), metaClient.getActiveTimeline.lastInstant())
    validateDataAndRecordIndices(hudiOpts)
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
  @MethodSource(Array("testEnableDisableRLIParams"))
  def testEnableDisableRLI(tableType: HoodieTableType, isPartitioned: Boolean): Unit = {
    var hudiOpts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name()
    )

    if (!isPartitioned) {
      hudiOpts = hudiOpts - PARTITIONPATH_FIELD.key
    }

    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)

    hudiOpts += (HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key -> "false")
    metaClient.getTableConfig.setMetadataPartitionState(metaClient, MetadataPartitionType.RECORD_INDEX.getPartitionPath, false)

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

    hudiOpts += (HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key -> "true")
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
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    val metadataTableFSView = getHoodieTable(metaClient, getWriteConfig(hudiOpts)).getMetadataTable
      .asInstanceOf[HoodieBackedTableMetadata].getMetadataFileSystemView
    val compactionTimeline = metadataTableFSView.getVisibleCommitsAndCompactionTimeline.filterCompletedAndCompactionInstants()
    val lastCompactionInstant = compactionTimeline
      .filter(JavaConversions.getPredicate((instant: HoodieInstant) =>
        compactionTimeline.readCommitMetadata(instant)
          .getOperationType == WriteOperationType.COMPACT))
      .lastInstant()
    val compactionBaseFile = metadataTableFSView.getAllBaseFiles(MetadataPartitionType.RECORD_INDEX.getPartitionPath)
      .filter(JavaConversions.getPredicate((f: HoodieBaseFile) => f.getCommitTime.equals(lastCompactionInstant.get().requestedTime)))
      .findAny()
    assertTrue(compactionBaseFile.isPresent)
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testRLIWithMDTCleaning(tableType: HoodieTableType): Unit = {
    var hudiOpts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key() -> "1")
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)

    hudiOpts = hudiOpts + (
      HoodieCleanConfig.CLEANER_FILE_VERSIONS_RETAINED.key() -> "1",
      HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key() -> "4"
    )
    val function = () => doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    executeFunctionNTimes(function, 3)

    // create a savepoint on the data table before the metadata table clean operation
    assertFalse(getMetadataMetaClient(hudiOpts).getActiveTimeline.getCleanerTimeline.lastInstant().isPresent)
    val writeConfig = getWriteConfig(hudiOpts)
    val client = new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)
    client.savepoint("user", "note")

    // validate that the clean is present in the metadata table timeline
    var iterations = 0
    while (getMetadataMetaClient(hudiOpts).getActiveTimeline.getCleanerTimeline.lastInstant().isEmpty) {
      doWriteAndValidateDataAndRecordIndex(hudiOpts,
        operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
        saveMode = SaveMode.Append)
      iterations += 1
    }
    assertTrue(getMetadataMetaClient(hudiOpts).getActiveTimeline.getCleanerTimeline.lastInstant().isPresent)
    // restore to the savepoint to force the metadata table state to roll back to before the clean
    client.restoreToSavepoint()
    client.close()
    // remove the commits that were created after the savepoint
    mergedDfList = mergedDfList.take(mergedDfList.size - iterations)
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
    val timestamp = System.currentTimeMillis()
    val function = new Function0[Boolean] {
      override def apply(): Boolean = {
        try {
          doWriteAndValidateDataAndRecordIndex(hudiOpts,
            operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
            saveMode = SaveMode.Append,
            validate = false,
            timestamp = timestamp)
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

object TestGlobalRecordLevelIndex {

  def testEnableDisableRLIParams(): java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(
      arguments(HoodieTableType.COPY_ON_WRITE, new java.lang.Boolean(false)),
      arguments(HoodieTableType.COPY_ON_WRITE, new java.lang.Boolean(true)),
      arguments(HoodieTableType.MERGE_ON_READ, new java.lang.Boolean(false)),
      arguments(HoodieTableType.MERGE_ON_READ, new java.lang.Boolean(true))
    )
  }
}
