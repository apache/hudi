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
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.keygen.TimestampBasedKeyGenerator
import org.apache.hudi.keygen.constant.KeyGeneratorOptions.Config
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieDataSourceHelpers}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, lit}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Tag
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, ValueSource}

import scala.collection.JavaConversions._

class TestSparkDataSource extends SparkClientFunctionalTestHarness {

  var commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    "hoodie.bulkinsert.shuffle.parallelism" -> "4",
    "hoodie.delete.shuffle.parallelism" -> "2",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
  )

  @ParameterizedTest
  @CsvSource(value = Array(
    "COPY_ON_WRITE|false|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|BLOOM",
    "COPY_ON_WRITE|true|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|BLOOM",
    "COPY_ON_WRITE|true|true|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|BLOOM",
    "COPY_ON_WRITE|false|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|SIMPLE",
    "COPY_ON_WRITE|true|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|SIMPLE",
    "COPY_ON_WRITE|true|true|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|SIMPLE",
    "COPY_ON_WRITE|false|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|_row_key|GLOBAL_BLOOM",
    "COPY_ON_WRITE|true|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|_row_key|GLOBAL_BLOOM",
    "COPY_ON_WRITE|true|true|org.apache.hudi.keygen.NonpartitionedKeyGenerator|_row_key|GLOBAL_BLOOM",
    "MERGE_ON_READ|false|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|BLOOM",
    "MERGE_ON_READ|true|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|BLOOM",
    "MERGE_ON_READ|true|true|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|BLOOM",
    "MERGE_ON_READ|false|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|SIMPLE",
    "MERGE_ON_READ|true|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|SIMPLE",
    "MERGE_ON_READ|true|true|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|SIMPLE",
    "MERGE_ON_READ|false|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|_row_key|GLOBAL_BLOOM",
    "MERGE_ON_READ|true|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|_row_key|GLOBAL_BLOOM",
    "MERGE_ON_READ|true|true|org.apache.hudi.keygen.NonpartitionedKeyGenerator|_row_key|GLOBAL_BLOOM"
  ), delimiter = '|')
  def testCoreFlow(tableType: String, isMetadataEnabledOnWrite: Boolean, isMetadataEnabledOnRead: Boolean, keyGenClass: String, recordKeys: String, indexType: String): Unit = {
    var options: Map[String, String] = commonOpts +
      (HoodieMetadataConfig.ENABLE.key -> String.valueOf(isMetadataEnabledOnWrite)) +
      (DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key() -> keyGenClass) +
      (DataSourceWriteOptions.RECORDKEY_FIELD.key() -> recordKeys) +
      (DataSourceWriteOptions.TABLE_TYPE.key() -> tableType) +
      (HoodieIndexConfig.INDEX_TYPE.key() -> indexType)
    // order of cols in inputDf and hudiDf differs slightly. so had to choose columns specifically to compare df directly.
    val colsToSelect = "_row_key, begin_lat,  begin_lon, city_to_state.LA, current_date, current_ts, distance_in_meters, driver, end_lat, end_lon, fare.amount, fare.currency, partition, partition_path, rider, timestamp, weight, _hoodie_is_deleted"
    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val fs = FSUtils.getFs(basePath, spark.sparkContext.hadoopConfiguration)
    // Insert Operation
    val records0 = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF0 = spark.read.json(spark.sparkContext.parallelize(records0, 2))
    inputDF0.write.format("org.apache.hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))

    // Snapshot query
    val snapshotDF1 = spark.read.format("org.apache.hudi")
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabledOnRead)
      .load(basePath)
    snapshotDF1.cache()
    assertEquals(100, snapshotDF1.count())

    val records1 = recordsToStrings(dataGen.generateUniqueUpdates("001", 50)).toList
    val updateDf = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    updateDf.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)
    val commitInstantTime2 = HoodieDataSourceHelpers.latestCommit(fs, basePath)

    val snapshotDF2 = spark.read.format("hudi")
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabledOnRead)
      .load(basePath)
    snapshotDF1.unpersist()
    snapshotDF2.cache()
    assertEquals(100, snapshotDF2.count())

    compareUpdateDfWithHudiDf(updateDf, snapshotDF2, colsToSelect)

    val records2 = recordsToStrings(dataGen.generateUniqueUpdates("002", 60)).toList
    var inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    val uniqueKeyCnt2 = inputDF2.select("_row_key").distinct().count()

    inputDF2.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)

    val commitInstantTime3 = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(3, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").size())

    // Snapshot Query
    val snapshotDF3 = spark.read.format("org.apache.hudi")
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabledOnRead)
      .load(basePath)
    snapshotDF3.cache()
    assertEquals(100, snapshotDF3.count()) // still 100, since we only updated

    compareUpdateDfWithHudiDf(inputDF2, snapshotDF3, colsToSelect)

    // Read Incremental Query
    // we have 2 commits, try pulling the first commit (which is not the latest)
    val firstCommit = HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").get(0)
    val hoodieIncViewDF1 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, "000")
      .option(DataSourceReadOptions.END_INSTANTTIME.key, firstCommit)
      .load(basePath)
    assertEquals(100, hoodieIncViewDF1.count()) // 100 initial inserts must be pulled
    var countsPerCommit = hoodieIncViewDF1.groupBy("_hoodie_commit_time").count().collect()
    assertEquals(1, countsPerCommit.length)
    assertEquals(firstCommit, countsPerCommit(0).get(0))

    val records3 = recordsToStrings(dataGen.generateUniqueUpdates("003", 80)).toList
    var inputDF3 = spark.read.json(spark.sparkContext.parallelize(records3, 2))
    val uniqueKeyCnt3 = inputDF3.select("_row_key").distinct().count()
    inputDF3.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)

    // another incremental query with commit2 and commit3
    val hoodieIncViewDF2 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, commitInstantTime2)
      .option(DataSourceReadOptions.END_INSTANTTIME.key(), commitInstantTime3)
      .load(basePath)

    assertEquals(uniqueKeyCnt2, hoodieIncViewDF2.count()) // 60 records must be pulled
    countsPerCommit = hoodieIncViewDF2.groupBy("_hoodie_commit_time").count().collect()
    assertEquals(1, countsPerCommit.length)
    assertEquals(commitInstantTime3, countsPerCommit(0).get(0))

    // time travel query.
    val timeTravelDF = spark.read.format("org.apache.hudi")
      .option("as.of.instant", commitInstantTime2)
      .load(basePath)
    assertEquals(100, timeTravelDF.count())
    compareEntireInputDfWithHudiDf(snapshotDF2, timeTravelDF, colsToSelect)

    if (tableType.equals("MERGE_ON_READ")) {
      doMORReadOptimizedQquery(inputDF0, colsToSelect, isMetadataEnabledOnRead)

      // trigger compaction and try out Read optimized query.
      val records4 = recordsToStrings(dataGen.generateUniqueUpdates("004", 40)).toList
      var inputDF4 = spark.read.json(spark.sparkContext.parallelize(records3, 2))
      val uniqueKeyCnt4 = inputDF4.select("_row_key").distinct().count()

      inputDF4.write.format("org.apache.hudi")
        .options(options)
        .option(HoodieCompactionConfig.INLINE_COMPACT.key(), "true")
        .option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "3")
        .mode(SaveMode.Append)
        .save(basePath)

      val snapshotDF4 = spark.read.format("org.apache.hudi")
        .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabledOnRead)
        .load(basePath)
      snapshotDF4.cache()

      compareUpdateDfWithHudiDf(inputDF4, snapshotDF4, colsToSelect)
      // compaction is expected to have completed. both RO and RT are expected to return same results.
      compareROAndRT(basePath, colsToSelect, isMetadataEnabledOnRead)
    }
  }

  @ParameterizedTest
  @CsvSource(value = Array(
    "COPY_ON_WRITE|INSERT|false|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|SIMPLE"
  ), delimiter = '|')
  def testBulkInsertFlow(tableType: String, operation: String, isMetadataEnabledOnWrite: Boolean, isMetadataEnabledOnRead: Boolean, keyGenClass: String, recordKeys: String, indexType: String): Unit = {
    var options: Map[String, String] = commonOpts +
      (HoodieMetadataConfig.ENABLE.key -> String.valueOf(isMetadataEnabledOnWrite)) +
      (DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key() -> keyGenClass) +
      (DataSourceWriteOptions.RECORDKEY_FIELD.key() -> recordKeys) +
      (DataSourceWriteOptions.TABLE_TYPE.key() -> tableType) +
      (HoodieIndexConfig.INDEX_TYPE.key() -> indexType)
    // order of cols in inputDf and hudiDf differs slightly. so had to choose columns specifically to compare df directly.
    val colsToSelect = "_row_key, begin_lat,  begin_lon, city_to_state.LA, current_date, current_ts, distance_in_meters, driver, end_lat, end_lon, fare.amount, fare.currency, partition, partition_path, rider, timestamp, weight, _hoodie_is_deleted"
    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val fs = FSUtils.getFs(basePath, spark.sparkContext.hadoopConfiguration)
    // Insert Operation
    val records0 = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDf0 = spark.read.json(spark.sparkContext.parallelize(records0, 2))
    inputDf0.cache()
    inputDf0.write.format("org.apache.hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key, operation)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))

    // Snapshot query
    val snapshotDF1 = spark.read.format("org.apache.hudi")
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabledOnRead)
      .load(basePath)
    snapshotDF1.cache()
    assertEquals(100, snapshotDF1.count())

    val records1 = recordsToStrings(dataGen.generateInserts("001", 50)).toList
    val inputDf1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDf1.cache()
    inputDf1.write.format("org.apache.hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key, operation)
      .mode(SaveMode.Append)
      .save(basePath)

    val snapshotDF2 = spark.read.format("hudi")
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabledOnRead)
      .load(basePath)
    snapshotDF1.unpersist()
    snapshotDF2.cache()
    assertEquals(150, snapshotDF2.count())

    compareEntireInputDfWithHudiDf(inputDf1.union(inputDf0), snapshotDF2, colsToSelect)

    val records2 = recordsToStrings(dataGen.generateInserts("002", 60)).toList
    var inputDf2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDf2.cache()

    inputDf2.write.format("org.apache.hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key, operation)
      .mode(SaveMode.Append)
      .save(basePath)

    assertEquals(3, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").size())

    // Snapshot Query
    val snapshotDF3 = spark.read.format("org.apache.hudi")
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabledOnRead)
      .load(basePath)
    snapshotDF3.cache()
    assertEquals(210, snapshotDF3.count())
    compareEntireInputDfWithHudiDf(inputDf1.union(inputDf0).union(inputDf2), snapshotDF3, colsToSelect)
  }

    def compareUpdateDfWithHudiDf(inputDf: Dataset[Row], hudiDf: Dataset[Row], colsToCompare: String ) : Unit = {
    val hudiWithoutMeta = hudiDf.drop(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD, HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.FILENAME_METADATA_FIELD)
    hudiWithoutMeta.registerTempTable("hudiTbl")
    inputDf.registerTempTable("inputTbl")
    val hudiDfToCompare = spark.sqlContext.sql("select " + colsToCompare + " from hudiTbl")
    val inputDfToCompare = spark.sqlContext.sql("select " + colsToCompare + " from inputTbl")

    assertEquals(hudiDfToCompare.intersect(inputDfToCompare).count, inputDfToCompare.count)
    assertEquals(hudiDfToCompare.except(inputDfToCompare).except(hudiDfToCompare).count, 0)
  }

  def compareEntireInputDfWithHudiDf(inputDf: Dataset[Row], hudiDf: Dataset[Row], colsToCompare: String ) : Unit = {
    val hudiWithoutMeta = hudiDf.drop(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD, HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.FILENAME_METADATA_FIELD)
    hudiWithoutMeta.registerTempTable("hudiTbl")
    inputDf.registerTempTable("inputTbl")
    val hudiDfToCompare = spark.sqlContext.sql("select " + colsToCompare + " from hudiTbl")
    val inputDfToCompare = spark.sqlContext.sql("select " + colsToCompare + " from inputTbl")

    assertEquals(hudiDfToCompare.intersect(inputDfToCompare).count, inputDfToCompare.count)
    assertEquals(hudiDfToCompare.except(inputDfToCompare).count, 0)
  }

  def doMORReadOptimizedQquery(inputDf: Dataset[Row], colsToSelect: String, isMetadataEnabledOnRead: Boolean): Unit = {
    // read optimized query.
    val readOptDf = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
      .option(HoodieMetadataConfig.ENABLE.key(), isMetadataEnabledOnRead)
      .load(basePath)
    compareUpdateDfWithHudiDf(inputDf, readOptDf, colsToSelect)
  }

  def compareROAndRT(basePath: String, colsToCompare: String, isMetadataEnabledOnRead: Boolean ) : Unit = {
    val roDf  = spark.read.format("org.apache.hudi")
      .option(HoodieMetadataConfig.ENABLE.key(), isMetadataEnabledOnRead)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL).load(basePath)
    val rtDf = spark.read.format("org.apache.hudi")
      .option(HoodieMetadataConfig.ENABLE.key(), isMetadataEnabledOnRead)
      .load(basePath)

    val hudiWithoutMeta1 = roDf
      .drop(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD, HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.FILENAME_METADATA_FIELD)
    val hudiWithoutMeta2 = rtDf
      .drop(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD, HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.FILENAME_METADATA_FIELD)
    hudiWithoutMeta1.registerTempTable("hudiTbl1")
    hudiWithoutMeta2.registerTempTable("hudiTbl2")

    val hudiDf1ToCompare = spark.sqlContext.sql("select " + colsToCompare + " from hudiTbl1")
    val hudiDf2ToCompare = spark.sqlContext.sql("select " + colsToCompare + " from hudiTbl2")

    assertEquals(hudiDf1ToCompare.intersect(hudiDf2ToCompare).count, hudiDf1ToCompare.count)
    assertEquals(hudiDf1ToCompare.except(hudiDf2ToCompare).count, 0)
  }
}
