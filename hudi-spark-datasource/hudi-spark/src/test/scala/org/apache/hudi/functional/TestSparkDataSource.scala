/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.functional

import org.apache.hudi.{AvroConversionUtils, DataSourceReadOptions, DataSourceWriteOptions, HoodieDataSourceHelpers}
import org.apache.hudi.common.config.{HoodieMetadataConfig, RecordMergeMode}
import org.apache.hudi.common.model.{HoodieRecord, HoodieTableType}
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.functional.CommonOptionUtils.getWriterReaderOpts
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.index.HoodieIndex.IndexType
import org.apache.hudi.keygen.NonpartitionedKeyGenerator
import org.apache.hudi.testutils.{DataSourceTestUtils, SparkClientFunctionalTestHarness}
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness.getSparkSqlConf

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

import scala.collection.JavaConverters._

class TestSparkDataSource extends SparkClientFunctionalTestHarness {

  override def conf: SparkConf = conf(getSparkSqlConf)

  val parallelism: Integer = 4

  val commonOpts: Map[String, String] = Map(
    "hoodie.insert.shuffle.parallelism" -> s"$parallelism",
    "hoodie.upsert.shuffle.parallelism" -> s"$parallelism",
    "hoodie.bulkinsert.shuffle.parallelism" -> s"$parallelism",
    "hoodie.delete.shuffle.parallelism" -> s"$parallelism",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
  )

  @ParameterizedTest
  @CsvSource(value = Array(
    "COPY_ON_WRITE|org.apache.hudi.keygen.SimpleKeyGenerator|BLOOM",
    "COPY_ON_WRITE|org.apache.hudi.keygen.SimpleKeyGenerator|SIMPLE",
    "COPY_ON_WRITE|org.apache.hudi.keygen.NonpartitionedKeyGenerator|GLOBAL_BLOOM",
    "MERGE_ON_READ|org.apache.hudi.keygen.SimpleKeyGenerator|BLOOM",
    "MERGE_ON_READ|org.apache.hudi.keygen.SimpleKeyGenerator|SIMPLE",
    "MERGE_ON_READ|org.apache.hudi.keygen.NonpartitionedKeyGenerator|GLOBAL_BLOOM"
  ), delimiter = '|')
  def testCoreFlow(tableType: String, keyGenClass: String, indexType: String): Unit = {
    val isMetadataEnabledOnWrite = true
    val isMetadataEnabledOnRead = true
    val partitionField = if (classOf[NonpartitionedKeyGenerator].getName.equals(keyGenClass)) "" else "partition"
    val options: Map[String, String] = commonOpts +
      (HoodieMetadataConfig.ENABLE.key -> String.valueOf(isMetadataEnabledOnWrite)) +
      (DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key() -> keyGenClass) +
      (DataSourceWriteOptions.PARTITIONPATH_FIELD.key() -> partitionField) +
      (DataSourceWriteOptions.TABLE_TYPE.key() -> tableType) +
      (HoodieIndexConfig.INDEX_TYPE.key() -> indexType)
    // order of cols in inputDf and hudiDf differs slightly. so had to choose columns specifically to compare df directly.
    val colsToSelect = "_row_key, begin_lat,  begin_lon, city_to_state.LA, current_date, current_ts, distance_in_meters, driver, end_lat, end_lon, fare.amount, fare.currency, partition, partition_path, rider, timestamp, weight, _hoodie_is_deleted"
    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val fs = HadoopFSUtils.getFs(basePath, spark.sparkContext.hadoopConfiguration)
    // Insert Operation
    val records0 = recordsToStrings(dataGen.generateInserts("000", 10)).asScala.toList
    val inputDf0 = spark.read.json(spark.sparkContext.parallelize(records0, parallelism)).cache
    inputDf0.write.format("org.apache.hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val commitCompletionTime1 = DataSourceTestUtils.latestCommitCompletionTime(fs, basePath)
    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))

    // Snapshot query
    val snapshotDf1 = spark.read.format("org.apache.hudi")
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabledOnRead)
      .load(basePath).cache
    assertEquals(10, snapshotDf1.count())
    compareEntireInputDfWithHudiDf(inputDf0, snapshotDf1, colsToSelect)
    val snapshotRows1 = snapshotDf1.collect.toList
    snapshotDf1.unpersist(true)

    val records1 = recordsToStrings(dataGen.generateUniqueUpdates("001", 5)).asScala.toList
    val updateDf = spark.read.json(spark.sparkContext.parallelize(records1, parallelism)).cache
    updateDf.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)
    val commitInstantTime2 = HoodieDataSourceHelpers.latestCommit(fs, basePath)

    val snapshotDf2 = spark.read.format("org.apache.hudi")
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabledOnRead)
      .load(basePath).cache
    assertEquals(10, snapshotDf2.count())
    compareUpdateDfWithHudiDf(updateDf, snapshotDf2, snapshotRows1, colsToSelect)
    val snapshotRows2 = snapshotDf2.collect.toList
    snapshotDf2.unpersist(true)

    val records2 = recordsToStrings(dataGen.generateUniqueUpdates("002", 6)).asScala.toList
    val inputDf2 = spark.read.json(spark.sparkContext.parallelize(records2, parallelism)).cache
    val uniqueKeyCnt2 = inputDf2.select("_row_key").distinct().count()
    inputDf2.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)

    val commitInstantTime3 = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    val commitCompletionTime3 = DataSourceTestUtils.latestCommitCompletionTime(fs, basePath)
    assertEquals(3, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").size())

    // Snapshot Query
    val snapshotDf3 = spark.read.format("org.apache.hudi")
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabledOnRead)
      .load(basePath).cache
    assertEquals(10, snapshotDf3.count(), "should still be 10, since we only updated")
    compareUpdateDfWithHudiDf(inputDf2, snapshotDf3, snapshotRows2, colsToSelect)
    snapshotDf3.unpersist(true)

    // Read Incremental Query
    // we have 2 commits, try pulling the first commit (which is not the latest)
    val firstCommit = HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").get(0)
    val hoodieIncViewDf1 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commitCompletionTime1)
      .option(DataSourceReadOptions.END_COMMIT.key, commitCompletionTime1)
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabledOnRead)
      .load(basePath)
    assertEquals(10, hoodieIncViewDf1.count(), "should have pulled 10 initial inserts")
    var countsPerCommit = hoodieIncViewDf1.groupBy("_hoodie_commit_time").count().collect()
    assertEquals(1, countsPerCommit.length)
    assertEquals(firstCommit, countsPerCommit(0).get(0))

    val records3 = recordsToStrings(dataGen.generateUniqueUpdates("003", 8)).asScala.toList
    val inputDf3 = spark.read.json(spark.sparkContext.parallelize(records3, parallelism)).cache
    inputDf3.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)

    // another incremental query with commit2 and commit3
    val hoodieIncViewDf2 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commitCompletionTime3)
      .option(DataSourceReadOptions.END_COMMIT.key(), commitCompletionTime3)
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabledOnRead)
      .load(basePath)

    assertEquals(uniqueKeyCnt2, hoodieIncViewDf2.count(), "should have pulled 6 records")
    countsPerCommit = hoodieIncViewDf2.groupBy("_hoodie_commit_time").count().collect()
    assertEquals(1, countsPerCommit.length)
    assertEquals(commitInstantTime3, countsPerCommit(0).get(0))

    // time travel query.
    val timeTravelDf = spark.read.format("org.apache.hudi")
      .option("as.of.instant", commitInstantTime2)
      .load(basePath).cache
    assertEquals(10, timeTravelDf.count())
    compareEntireInputRowsWithHudiDf(snapshotRows2, timeTravelDf, colsToSelect)
    timeTravelDf.unpersist(true)

    if (tableType.equals("MERGE_ON_READ")) {
      doMORReadOptimizedQuery(inputDf0, colsToSelect, isMetadataEnabledOnRead)

      val snapshotRows4 = spark.read.format("org.apache.hudi")
        .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabledOnRead)
        .load(basePath).collect.toList
      assertEquals(10, snapshotRows4.length)

      // trigger compaction and try out Read optimized query.
      val records4 = recordsToStrings(dataGen.generateUniqueUpdates("004", 4)).asScala.toList
      val inputDf4 = spark.read.json(spark.sparkContext.parallelize(records4, parallelism)).cache
      inputDf4.write.format("org.apache.hudi")
        .options(options)
        .option(HoodieCompactionConfig.INLINE_COMPACT.key(), "true")
        .option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "3")
        .mode(SaveMode.Append)
        .save(basePath)

      val snapshotDf5 = spark.read.format("org.apache.hudi")
        .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabledOnRead)
        .load(basePath).cache

      compareUpdateDfWithHudiDf(inputDf4, snapshotDf5, snapshotRows4, colsToSelect)
      inputDf4.unpersist(true)
      snapshotDf5.unpersist(true)
      // compaction is expected to have completed. both RO and RT are expected to return same results.
      compareROAndRT(basePath, colsToSelect, isMetadataEnabledOnRead)
    }

    inputDf0.unpersist(true)
    updateDf.unpersist(true)
    inputDf2.unpersist(true)
    inputDf3.unpersist(true)
  }

  @ParameterizedTest
  @CsvSource(value = Array(
    "COPY_ON_WRITE|insert|org.apache.hudi.keygen.SimpleKeyGenerator|BLOOM",
    "COPY_ON_WRITE|insert|org.apache.hudi.keygen.SimpleKeyGenerator|SIMPLE",
    "COPY_ON_WRITE|insert|org.apache.hudi.keygen.NonpartitionedKeyGenerator|GLOBAL_BLOOM",
    "MERGE_ON_READ|insert|org.apache.hudi.keygen.SimpleKeyGenerator|BLOOM",
    "MERGE_ON_READ|insert|org.apache.hudi.keygen.SimpleKeyGenerator|SIMPLE",
    "MERGE_ON_READ|insert|org.apache.hudi.keygen.NonpartitionedKeyGenerator|GLOBAL_BLOOM",
    "COPY_ON_WRITE|bulk_insert|org.apache.hudi.keygen.SimpleKeyGenerator|BLOOM",
    "COPY_ON_WRITE|bulk_insert|org.apache.hudi.keygen.SimpleKeyGenerator|SIMPLE",
    "COPY_ON_WRITE|bulk_insert|org.apache.hudi.keygen.NonpartitionedKeyGenerator|GLOBAL_BLOOM",
    "MERGE_ON_READ|bulk_insert|org.apache.hudi.keygen.SimpleKeyGenerator|BLOOM",
    "MERGE_ON_READ|bulk_insert|org.apache.hudi.keygen.SimpleKeyGenerator|SIMPLE",
    "MERGE_ON_READ|bulk_insert|org.apache.hudi.keygen.NonpartitionedKeyGenerator|GLOBAL_BLOOM"
  ), delimiter = '|')
  def testImmutableUserFlow(tableType: String, operation: String, keyGenClass: String, indexType: String): Unit = {
    val isMetadataEnabledOnWrite = true
    val isMetadataEnabledOnRead = true
    val partitionField = if (classOf[NonpartitionedKeyGenerator].getName.equals(keyGenClass)) "" else "partition"
    val options: Map[String, String] = commonOpts +
      (HoodieMetadataConfig.ENABLE.key -> String.valueOf(isMetadataEnabledOnWrite)) +
      (DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key() -> keyGenClass) +
      (DataSourceWriteOptions.PARTITIONPATH_FIELD.key() -> partitionField) +
      (DataSourceWriteOptions.TABLE_TYPE.key() -> tableType) +
      (HoodieIndexConfig.INDEX_TYPE.key() -> indexType)
    // order of cols in inputDf and hudiDf differs slightly. so had to choose columns specifically to compare df directly.
    val colsToSelect = "_row_key, begin_lat,  begin_lon, city_to_state.LA, current_date, current_ts, distance_in_meters, driver, end_lat, end_lon, fare.amount, fare.currency, partition, partition_path, rider, timestamp, weight, _hoodie_is_deleted"
    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val fs = HadoopFSUtils.getFs(basePath, spark.sparkContext.hadoopConfiguration)
    // Insert Operation
    val records0 = recordsToStrings(dataGen.generateInserts("000", 10)).asScala.toList
    val inputDf0 = spark.read.json(spark.sparkContext.parallelize(records0, parallelism)).cache
    inputDf0.write.format("org.apache.hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key, operation)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))

    // Snapshot query
    val snapshotDf1 = spark.read.format("org.apache.hudi")
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabledOnRead)
      .load(basePath)
    assertEquals(10, snapshotDf1.count())

    val records1 = recordsToStrings(dataGen.generateInserts("001", 5)).asScala.toList
    val inputDf1 = spark.read.json(spark.sparkContext.parallelize(records1, parallelism)).cache
    inputDf1.write.format("org.apache.hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key, operation)
      .mode(SaveMode.Append)
      .save(basePath)

    val snapshotDf2 = spark.read.format("hudi")
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabledOnRead)
      .load(basePath).cache
    assertEquals(15, snapshotDf2.count())
    compareEntireInputDfWithHudiDf(inputDf1.union(inputDf0), snapshotDf2, colsToSelect)
    snapshotDf2.unpersist(true)

    val records2 = recordsToStrings(dataGen.generateInserts("002", 6)).asScala.toList
    val inputDf2 = spark.read.json(spark.sparkContext.parallelize(records2, parallelism)).cache
    inputDf2.write.format("org.apache.hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key, operation)
      .mode(SaveMode.Append)
      .save(basePath)

    assertEquals(3, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").size())

    // Snapshot Query
    val snapshotDf3 = spark.read.format("org.apache.hudi")
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabledOnRead)
      .load(basePath).cache
    assertEquals(21, snapshotDf3.count())
    compareEntireInputDfWithHudiDf(inputDf1.union(inputDf0).union(inputDf2), snapshotDf3, colsToSelect)
    snapshotDf3.unpersist(true)

    inputDf0.unpersist(true)
    inputDf1.unpersist(true)
    inputDf2.unpersist(true)
  }

  @ParameterizedTest
  @CsvSource(value = Array("COPY_ON_WRITE,8,EVENT_TIME_ORDERING,RECORD_INDEX",
    "COPY_ON_WRITE,8,COMMIT_TIME_ORDERING,RECORD_INDEX",
    "COPY_ON_WRITE,8,EVENT_TIME_ORDERING,GLOBAL_SIMPLE",
    "COPY_ON_WRITE,8,COMMIT_TIME_ORDERING,GLOBAL_SIMPLE",
    "MERGE_ON_READ,8,EVENT_TIME_ORDERING,RECORD_INDEX",
    "MERGE_ON_READ,8,COMMIT_TIME_ORDERING,RECORD_INDEX",
    "MERGE_ON_READ,8,EVENT_TIME_ORDERING,GLOBAL_SIMPLE",
    "MERGE_ON_READ,8,COMMIT_TIME_ORDERING,GLOBAL_SIMPLE"))
  def testDeletesWithHoodieIsDeleted(tableType: HoodieTableType, tableVersion: Int, mergeMode: RecordMergeMode, indexType: IndexType): Unit = {
    var (writeOpts, readOpts) = getWriterReaderOpts(HoodieRecordType.AVRO)
    writeOpts = writeOpts ++ Map("hoodie.write.table.version" -> tableVersion.toString,
      "hoodie.datasource.write.table.type" -> tableType.name(),
      HoodieTableConfig.ORDERING_FIELDS.key() -> "ts",
      "hoodie.write.record.merge.mode" -> mergeMode.name(),
      "hoodie.index.type" -> indexType.name(),
      "hoodie.metadata.record.index.enable" -> "true",
      "hoodie.record.index.update.partition.path" -> "true",
      "hoodie.parquet.small.file.limit" -> "0")

    writeOpts = writeOpts + (if (indexType == IndexType.RECORD_INDEX) {
      "hoodie.record.index.update.partition.path" -> "true"
    } else {
      "hoodie.simple.index.update.partition.path" -> "true"
    })

    // generate the inserts
    val schema = DataSourceTestUtils.getStructTypeExampleSchema
    val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
    val inserts = DataSourceTestUtils.generateRandomRows(400)
    val df = spark.createDataFrame(spark.sparkContext.parallelize(convertRowListToSeq(inserts)), structType)

    df.write.format("hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType.name())
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val hudiSnapshotDF1 = spark.read.format("hudi")
      .options(readOpts)
      .load(basePath)
    assertEquals(400, hudiSnapshotDF1.count())

    // ingest batch2 with mix of updates and deletes. some of them are updating same partition, some of them are moving to new partition.
    // some are having higher ts and some are having lower ts.
    ingestNewBatch(tableType, 200, structType, inserts.subList(0, 200), writeOpts)

    val expectedRecordCount2 = if (mergeMode == RecordMergeMode.EVENT_TIME_ORDERING) 350 else 300;
    val hudiSnapshotDF2 = spark.read.format("hudi")
      .options(readOpts)
      .load(basePath)
    assertEquals(expectedRecordCount2, hudiSnapshotDF2.count())

    // querying subset of column. even if not including _hoodie_is_deleted, snapshot read should return right data.
    assertEquals(expectedRecordCount2, spark.read.format("hudi")
      .options(readOpts).load(basePath).select("_hoodie_record_key", "_hoodie_partition_path").count())

    // ingest batch3 with mix of updates and deletes. some of them are updating same partition, some of them are moving to new partition.
    // some are having higher ts and some are having lower ts.
    ingestNewBatch(tableType, 200, structType, inserts.subList(200, 400), writeOpts)

    val expectedRecordCount3 = if (mergeMode == RecordMergeMode.EVENT_TIME_ORDERING) 300 else 200;
    val hudiSnapshotDF3 = spark.read.format("hudi")
      .options(readOpts)
      .load(basePath)
    assertEquals(expectedRecordCount3, hudiSnapshotDF3.count())

    // querying subset of column. even if not including _hoodie_is_deleted, snapshot read should return right data.
    assertEquals(expectedRecordCount3, spark.read.format("hudi")
      .options(readOpts).load(basePath).select("_hoodie_record_key", "_hoodie_partition_path").count())
  }

  def ingestNewBatch(tableType: HoodieTableType, recordsToUpdate: Integer, structType: StructType, inserts: java.util.List[Row],
                     writeOpts: Map[String, String]): Unit = {
    val toUpdate = sqlContext.createDataFrame(DataSourceTestUtils.getUniqueRows(inserts, recordsToUpdate), structType).collectAsList()

    val updateToSamePartitionHigherTs = sqlContext.createDataFrame(toUpdate.subList(0, recordsToUpdate / 4), structType)
    val rowsToUpdate1 = DataSourceTestUtils.updateRowsWithUpdatedTs(updateToSamePartitionHigherTs)
    val updates1 = rowsToUpdate1.subList(0, recordsToUpdate / 8)
    val updateDf1 = spark.createDataFrame(spark.sparkContext.parallelize(convertRowListToSeq(updates1)), structType)
    val deletes1 = rowsToUpdate1.subList(recordsToUpdate / 8, recordsToUpdate / 4)
    val deleteDf1 = spark.createDataFrame(spark.sparkContext.parallelize(convertRowListToSeq(deletes1)), structType)
    val batch1 = deleteDf1.withColumn("_hoodie_is_deleted", lit(true)).union(updateDf1)
    batch1.cache()

    val updateToDiffPartitionHigherTs = sqlContext.createDataFrame(toUpdate.subList(recordsToUpdate / 4, recordsToUpdate / 2), structType)
    val rowsToUpdate2 = DataSourceTestUtils.updateRowsWithUpdatedTs(updateToDiffPartitionHigherTs, false, true)
    val updates2 = rowsToUpdate2.subList(0, recordsToUpdate / 8)
    val updateDf2 = spark.createDataFrame(spark.sparkContext.parallelize(convertRowListToSeq(updates2)), structType)
    val deletes2 = rowsToUpdate2.subList(recordsToUpdate / 8, recordsToUpdate / 4)
    val deleteDf2 = spark.createDataFrame(spark.sparkContext.parallelize(convertRowListToSeq(deletes2)), structType)
    val batch2 = deleteDf2.withColumn("_hoodie_is_deleted", lit(true)).union(updateDf2)
    batch2.cache()

    val updateToSamePartitionLowerTs = sqlContext.createDataFrame(toUpdate.subList(recordsToUpdate / 2, recordsToUpdate * 3 / 4), structType)
    val rowsToUpdate3 = DataSourceTestUtils.updateRowsWithUpdatedTs(updateToSamePartitionLowerTs, true, false)
    val updates3 = rowsToUpdate3.subList(0, recordsToUpdate / 8)
    val updateDf3 = spark.createDataFrame(spark.sparkContext.parallelize(convertRowListToSeq(updates3)), structType)
    val deletes3 = rowsToUpdate3.subList(recordsToUpdate / 8, recordsToUpdate / 4)
    val deleteDf3 = spark.createDataFrame(spark.sparkContext.parallelize(convertRowListToSeq(deletes3)), structType)
    val batch3 = deleteDf3.withColumn("_hoodie_is_deleted", lit(true)).union(updateDf3)
    batch3.cache()

    val updateToDiffPartitionLowerTs = sqlContext.createDataFrame(toUpdate.subList(recordsToUpdate * 3 / 4, recordsToUpdate), structType)
    val rowsToUpdate4 = DataSourceTestUtils.updateRowsWithUpdatedTs(updateToDiffPartitionLowerTs, true, true)
    val updates4 = rowsToUpdate4.subList(0, recordsToUpdate / 8)
    val updateDf4 = spark.createDataFrame(spark.sparkContext.parallelize(convertRowListToSeq(updates4)), structType)
    val deletes4 = rowsToUpdate4.subList(recordsToUpdate / 8, recordsToUpdate / 4)
    val deleteDf4 = spark.createDataFrame(spark.sparkContext.parallelize(convertRowListToSeq(deletes4)), structType)
    val batch4 = deleteDf4.withColumn("_hoodie_is_deleted", lit(true)).union(updateDf4)
    batch4.cache()

    batch1.union(batch2).union(batch3).union(batch4).write.format("hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType.name())
      .mode(SaveMode.Append)
      .save(basePath)
  }

  def compareUpdateDfWithHudiDf(inputDf: Dataset[Row], hudiDf: Dataset[Row], beforeRows: List[Row], colsToCompare: String): Unit = {
    val hudiWithoutMetaDf = hudiDf.drop(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD, HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.FILENAME_METADATA_FIELD)
    hudiWithoutMetaDf.registerTempTable("hudiTbl")
    inputDf.registerTempTable("inputTbl")
    val beforeDf = spark.createDataFrame(beforeRows.asJava, hudiDf.schema)
    beforeDf.registerTempTable("beforeTbl")
    val hudiDfToCompare = spark.sqlContext.sql("select " + colsToCompare + " from hudiTbl")
    val inputDfToCompare = spark.sqlContext.sql("select " + colsToCompare + " from inputTbl")
    val beforeDfToCompare = spark.sqlContext.sql("select " + colsToCompare + " from beforeTbl")

    assertEquals(inputDfToCompare.count, hudiDfToCompare.intersect(inputDfToCompare).count)
    assertEquals(0, hudiDfToCompare.except(inputDfToCompare).except(beforeDfToCompare).count, 0)
  }

  def compareEntireInputRowsWithHudiDf(inputRows: List[Row], hudiDf: Dataset[Row], colsToCompare: String): Unit = {
    val inputDf = spark.createDataFrame(inputRows.asJava, hudiDf.schema)
    compareEntireInputDfWithHudiDf(inputDf, hudiDf, colsToCompare)
  }

  def compareEntireInputDfWithHudiDf(inputDf: Dataset[Row], hudiDf: Dataset[Row], colsToCompare: String): Unit = {
    val hudiWithoutMetaDf = hudiDf.drop(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD, HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.FILENAME_METADATA_FIELD)
    hudiWithoutMetaDf.registerTempTable("hudiTbl")
    inputDf.registerTempTable("inputTbl")
    val hudiDfToCompare = spark.sqlContext.sql("select " + colsToCompare + " from hudiTbl")
    val inputDfToCompare = spark.sqlContext.sql("select " + colsToCompare + " from inputTbl")

    assertEquals(inputDfToCompare.count, hudiDfToCompare.intersect(inputDfToCompare).count)
    assertEquals(0, hudiDfToCompare.except(inputDfToCompare).count)
  }

  def doMORReadOptimizedQuery(inputDf: Dataset[Row], colsToSelect: String, isMetadataEnabledOnRead: Boolean): Unit = {
    // read optimized query.
    val readOptDf = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
      .option(HoodieMetadataConfig.ENABLE.key(), isMetadataEnabledOnRead)
      .load(basePath)
    compareEntireInputDfWithHudiDf(inputDf, readOptDf, colsToSelect)
  }

  def compareROAndRT(basePath: String, colsToCompare: String, isMetadataEnabledOnRead: Boolean): Unit = {
    val roDf = spark.read.format("org.apache.hudi")
      .option(HoodieMetadataConfig.ENABLE.key(), isMetadataEnabledOnRead)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL).load(basePath)
    val rtDf = spark.read.format("org.apache.hudi")
      .option(HoodieMetadataConfig.ENABLE.key(), isMetadataEnabledOnRead)
      .load(basePath)

    val hudiWithoutMeta1Df = roDf
      .drop(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD, HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.FILENAME_METADATA_FIELD)
    val hudiWithoutMeta2Df = rtDf
      .drop(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD, HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.FILENAME_METADATA_FIELD)
    hudiWithoutMeta1Df.registerTempTable("hudiTbl1")
    hudiWithoutMeta2Df.registerTempTable("hudiTbl2")

    val hudiDf1ToCompare = spark.sqlContext.sql("select " + colsToCompare + " from hudiTbl1")
    val hudiDf2ToCompare = spark.sqlContext.sql("select " + colsToCompare + " from hudiTbl2")

    assertEquals(hudiDf1ToCompare.count, hudiDf1ToCompare.intersect(hudiDf2ToCompare).count)
    assertEquals(0, hudiDf1ToCompare.except(hudiDf2ToCompare).count)
  }

  def convertRowListToSeq(inputList: java.util.List[Row]): Seq[Row] =
    asScalaIteratorConverter(inputList.iterator).asScala.toSeq
}
