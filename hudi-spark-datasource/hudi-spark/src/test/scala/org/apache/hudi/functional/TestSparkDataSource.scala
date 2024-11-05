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

import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.keygen.NonpartitionedKeyGenerator
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieDataSourceHelpers}

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

import scala.collection.JavaConverters._

class TestSparkDataSource extends SparkClientFunctionalTestHarness {

  val parallelism: Integer = 4

  val commonOpts: Map[String, String] = Map(
    "hoodie.insert.shuffle.parallelism" -> s"$parallelism",
    "hoodie.upsert.shuffle.parallelism" -> s"$parallelism",
    "hoodie.bulkinsert.shuffle.parallelism" -> s"$parallelism",
    "hoodie.delete.shuffle.parallelism" -> s"$parallelism",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "timestamp",
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
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, "000")
      .option(DataSourceReadOptions.END_INSTANTTIME.key, firstCommit)
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
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, commitInstantTime2)
      .option(DataSourceReadOptions.END_INSTANTTIME.key(), commitInstantTime3)
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
}
