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
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen.constant.KeyGeneratorOptions.Config
import org.apache.hudi.keygen.{ComplexKeyGenerator, TimestampBasedKeyGenerator}
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieDataSourceHelpers}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, lit}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.{Disabled, Tag}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, ValueSource}

import scala.collection.JavaConversions._


@Tag("functional")
class TestCOWDataSourceStorage extends SparkClientFunctionalTestHarness {

  var commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    "hoodie.bulkinsert.shuffle.parallelism" -> "2",
    "hoodie.delete.shuffle.parallelism" -> "1",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
  )

  val verificationCol: String = "driver"
  val updatedVerificationVal: String = "driver_update"

  @ParameterizedTest
  @CsvSource(Array(
    "true,org.apache.hudi.keygen.SimpleKeyGenerator",
    "true,org.apache.hudi.keygen.ComplexKeyGenerator",
    "true,org.apache.hudi.keygen.TimestampBasedKeyGenerator",
    "false,org.apache.hudi.keygen.SimpleKeyGenerator",
    "false,org.apache.hudi.keygen.ComplexKeyGenerator",
    "false,org.apache.hudi.keygen.TimestampBasedKeyGenerator"
  ))
  def testCopyOnWriteStorage(isMetadataEnabled: Boolean, keyGenClass: String): Unit = {
    commonOpts += DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key() -> keyGenClass
    if (classOf[ComplexKeyGenerator].getName.equals(keyGenClass)) {
      commonOpts += DataSourceWriteOptions.RECORDKEY_FIELD.key() -> "_row_key, pii_col"
    }
    if (classOf[TimestampBasedKeyGenerator].getName.equals(keyGenClass)) {
      commonOpts += DataSourceWriteOptions.RECORDKEY_FIELD.key() -> "_row_key"
      commonOpts += DataSourceWriteOptions.PARTITIONPATH_FIELD.key() -> "current_ts"
      commonOpts += Config.TIMESTAMP_TYPE_FIELD_PROP -> "EPOCHMILLISECONDS"
      commonOpts += Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP -> "yyyyMMdd"
    }
    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val fs = FSUtils.getFs(basePath, spark.sparkContext.hadoopConfiguration)
    // Insert Operation
    val records0 = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF0 = spark.read.json(spark.sparkContext.parallelize(records0, 2))
    inputDF0.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))
    val commitInstantTime1 = HoodieDataSourceHelpers.latestCommit(fs, basePath)

    // Snapshot query
    val snapshotDF1 = spark.read.format("org.apache.hudi")
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .load(basePath)
    assertEquals(100, snapshotDF1.count())

    val records1 = recordsToStrings(dataGen.generateUpdates("001", 100)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    val verificationRowKey = inputDF1.limit(1).select("_row_key").first.getString(0)
    var updateDf: DataFrame = null
    if (classOf[TimestampBasedKeyGenerator].getName.equals(keyGenClass)) {
      // update current_ts to be same as original record so that partition path does not change with timestamp based key gen
      val originalRow = snapshotDF1.filter(col("_row_key") === verificationRowKey).collectAsList().get(0)
      updateDf = inputDF1.filter(col("_row_key") === verificationRowKey)
        .withColumn(verificationCol, lit(updatedVerificationVal))
        .withColumn("current_ts", lit(originalRow.getAs[Long]("current_ts")))
        .limit(1)
      val updatedRow = updateDf.collectAsList().get(0)
      assertEquals(originalRow.getAs[Long]("current_ts"), updatedRow.getAs[Long]("current_ts"));
    } else {
      updateDf = snapshotDF1.filter(col("_row_key") === verificationRowKey).withColumn(verificationCol, lit(updatedVerificationVal))
    }

    updateDf.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .mode(SaveMode.Append)
      .save(basePath)
    val commitInstantTime2 = HoodieDataSourceHelpers.latestCommit(fs, basePath)

    val snapshotDF2 = spark.read.format("hudi")
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .load(basePath)
    assertEquals(100, snapshotDF2.count())
    assertEquals(updatedVerificationVal, snapshotDF2.filter(col("_row_key") === verificationRowKey).select(verificationCol).first.getString(0))

    // Upsert Operation without Hudi metadata columns
    val records2 = recordsToStrings(dataGen.generateUpdates("002", 100)).toList
    var inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))

    if (classOf[TimestampBasedKeyGenerator].getName.equals(keyGenClass)) {
      // incase of Timestamp based key gen, current_ts should not be updated. but dataGen.generateUpdates() would have updated
      // the value of current_ts. So, we need to revert it back to original value.
      // here is what we are going to do. Copy values to temp columns, join with original df and update the current_ts
      // and drop the temp columns.

      val inputDF2WithTempCols = inputDF2.withColumn("current_ts_temp", col("current_ts"))
        .withColumn("_row_key_temp", col("_row_key"))
      val originalRowCurrentTsDf = inputDF0.select("_row_key", "current_ts")
      // join with original df
      val joinedDf = inputDF2WithTempCols.drop("_row_key", "current_ts").join(originalRowCurrentTsDf, (inputDF2WithTempCols("_row_key_temp") === originalRowCurrentTsDf("_row_key")))
      // copy values from temp back to original cols and drop temp cols
      inputDF2 = joinedDf.withColumn("current_ts_temp", col("current_ts"))
        .drop("current_ts", "_row_key_temp").withColumn("current_ts", col("current_ts_temp"))
        .drop("current_ts_temp")
    }

    val uniqueKeyCnt = inputDF2.select("_row_key").distinct().count()

    inputDF2.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .mode(SaveMode.Append)
      .save(basePath)

    val commitInstantTime3 = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(3, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").size())

    // Snapshot Query
    val snapshotDF3 = spark.read.format("org.apache.hudi")
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .load(basePath)
    assertEquals(100, snapshotDF3.count()) // still 100, since we only updated

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

    // Test incremental query has no instant in range
    val emptyIncDF = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, "000")
      .option(DataSourceReadOptions.END_INSTANTTIME.key, "002")
      .load(basePath)
    assertEquals(0, emptyIncDF.count())

    // Upsert an empty dataFrame
    val emptyRecords = recordsToStrings(dataGen.generateUpdates("003", 0)).toList
    val emptyDF = spark.read.json(spark.sparkContext.parallelize(emptyRecords, 1))
    emptyDF.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .mode(SaveMode.Append)
      .save(basePath)

    // pull the latest commit
    val hoodieIncViewDF2 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, commitInstantTime2)
      .load(basePath)

    assertEquals(uniqueKeyCnt, hoodieIncViewDF2.count()) // 100 records must be pulled
    countsPerCommit = hoodieIncViewDF2.groupBy("_hoodie_commit_time").count().collect()
    assertEquals(1, countsPerCommit.length)
    assertEquals(commitInstantTime3, countsPerCommit(0).get(0))

    // pull the latest commit within certain partitions
    val hoodieIncViewDF3 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, commitInstantTime2)
      .option(DataSourceReadOptions.INCR_PATH_GLOB.key, "/2016/*/*/*")
      .load(basePath)
    assertEquals(hoodieIncViewDF2.filter(col("_hoodie_partition_path").contains("2016")).count(), hoodieIncViewDF3.count())

    val timeTravelDF = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, "000")
      .option(DataSourceReadOptions.END_INSTANTTIME.key, firstCommit)
      .load(basePath)
    assertEquals(100, timeTravelDF.count()) // 100 initial inserts must be pulled
  }

  @ParameterizedTest
  @ValueSource(strings = Array("insert_overwrite", "delete_partition"))
  def testArchivalWithReplaceCommitActions(writeOperation: String): Unit = {

    val dataGen = new HoodieTestDataGenerator()
    // use this to generate records only for certain partitions.
    val dataGenPartition1 = new HoodieTestDataGenerator(Array[String](HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH))
    val dataGenPartition2 = new HoodieTestDataGenerator(Array[String](HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH))

    // do one bulk insert to all partitions
    val records = recordsToStrings(dataGen.generateInserts("%05d".format(1), 100)).toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    val partition1RecordCount = inputDF.filter(row => row.getAs("partition_path")
      .equals(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)).count()
    inputDF.write.format("hudi")
      .options(commonOpts)
      .option("hoodie.keep.min.commits", "2")
      .option("hoodie.keep.max.commits", "3")
      .option("hoodie.cleaner.commits.retained", "1")
      .option("hoodie.metadata.enable", "false")
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertRecordCount(basePath, 100)

    // issue delete partition to partition1
    writeRecords(2, dataGenPartition1, writeOperation, basePath)

    val expectedRecCount = if (writeOperation.equals(DataSourceWriteOptions.INSERT_OVERWRITE_OPERATION_OPT_VAL)) {
      200 - partition1RecordCount
    } else {
      100 - partition1RecordCount
    }
    assertRecordCount(basePath, expectedRecCount)

    // add more data to partition2.
    for (i <- 3 to 7) {
      writeRecords(i, dataGenPartition2, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL, basePath)
    }

    assertRecordCount(basePath, expectedRecCount + 500)
    val metaClient = HoodieTableMetaClient.builder().setConf(spark.sparkContext.hadoopConfiguration).setBasePath(basePath)
      .setLoadActiveTimelineOnLoad(true).build()
    val commits = metaClient.getActiveTimeline.filterCompletedInstants().getInstants.toArray
      .map(instant => instant.asInstanceOf[HoodieInstant].getAction)
    // assert replace commit is archived and not part of active timeline.
    assertFalse(commits.contains(HoodieTimeline.REPLACE_COMMIT_ACTION))
    // assert that archival timeline has replace commit actions.
    val archivedTimeline = metaClient.getArchivedTimeline();
    assertTrue(archivedTimeline.getInstants.toArray.map(instant => instant.asInstanceOf[HoodieInstant].getAction)
      .filter(action => action.equals(HoodieTimeline.REPLACE_COMMIT_ACTION)).size > 0)
  }

  def writeRecords(commitTime: Int, dataGen: HoodieTestDataGenerator, writeOperation: String, basePath: String): Unit = {
    val records = recordsToStrings(dataGen.generateInserts("%05d".format(commitTime), 100)).toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    inputDF.write.format("hudi")
      .options(commonOpts)
      .option("hoodie.keep.min.commits", "2")
      .option("hoodie.keep.max.commits", "3")
      .option("hoodie.cleaner.commits.retained", "1")
      .option("hoodie.metadata.enable", "false")
      .option(DataSourceWriteOptions.OPERATION.key, writeOperation)
      .mode(SaveMode.Append)
      .save(basePath)
  }

  def assertRecordCount(basePath: String, expectedRecordCount: Long): Unit = {
    val snapshotDF = spark.read.format("org.apache.hudi")
      .load(basePath + "/*/*/*/*")
    assertEquals(expectedRecordCount, snapshotDF.count())
  }
}
