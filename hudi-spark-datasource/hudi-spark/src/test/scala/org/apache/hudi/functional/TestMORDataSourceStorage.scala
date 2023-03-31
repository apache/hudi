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
import org.apache.hudi.common.model.{HoodieRecord, HoodieTableType}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.{DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH, DEFAULT_THIRD_PARTITION_PATH, getCommitTimeAtUTC}
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness.getSparkSqlConf
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieDataSourceHelpers}
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, lit}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Tag
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

import scala.collection.JavaConversions._


@Tag("functional")
class TestMORDataSourceStorage extends SparkClientFunctionalTestHarness {

  override def conf: SparkConf = conf(getSparkSqlConf)

  @ParameterizedTest
  @CsvSource(Array(
    "true,",
    "true,fare.currency",
    "false,",
    "false,fare.currency"
  ))
  def testMergeOnReadStorage(isMetadataEnabled: Boolean, preCombineField: String): Unit = {
    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      "hoodie.bulkinsert.shuffle.parallelism" -> "2",
      "hoodie.delete.shuffle.parallelism" -> "1",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition_path",
      DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "timestamp",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
    )
    val verificationCol: String = "driver"
    val updatedVerificationVal: String = "driver_update"

    var options: Map[String, String] = commonOpts +
      (HoodieMetadataConfig.ENABLE.key -> String.valueOf(isMetadataEnabled))
    if (!StringUtils.isNullOrEmpty(preCombineField)) {
      options += (DataSourceWriteOptions.PRECOMBINE_FIELD.key() -> preCombineField)
    }
    val dataGen = new HoodieTestDataGenerator(0xDEEF)
    val fs = FSUtils.getFs(basePath, spark.sparkContext.hadoopConfiguration)
    // Bulk Insert Operation
    val records1 = recordsToStrings(dataGen.generateInserts("001", 100)).toList
    val inputDF1: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(options)
      .option("hoodie.compact.inline", "false") // else fails due to compaction & deltacommit instant times being same
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))

    // Read RO View
    val hudiRODF1 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .load(basePath)

    assertEquals(100, hudiRODF1.count()) // still 100, since we only updated
    val insertCommitTime = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    val insertCommitTimes = hudiRODF1.select("_hoodie_commit_time").distinct().collectAsList().map(r => r.getString(0)).toList
    assertEquals(List(insertCommitTime), insertCommitTimes)

    // Upsert operation without Hudi metadata columns
    val records2 = recordsToStrings(dataGen.generateUniqueUpdates("002", 100)).toList
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)

    // Read Snapshot query
    val updateCommitTime = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    val hudiSnapshotDF2 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .load(basePath)

    val updateCommitTimes = hudiSnapshotDF2.select("_hoodie_commit_time").distinct().collectAsList().map(r => r.getString(0)).toList
    assertEquals(List(updateCommitTime), updateCommitTimes)

    // Upsert based on the written table with Hudi metadata columns
    val verificationRowKey = hudiSnapshotDF2.limit(1).select("_row_key").first.getString(0)
    val inputDF3 = hudiSnapshotDF2.filter(col("_row_key") === verificationRowKey).withColumn(verificationCol, lit(updatedVerificationVal))

    inputDF3.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)

    val hudiSnapshotDF3 = spark.read.format("hudi")
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .load(basePath)
    assertEquals(100, hudiSnapshotDF3.count())
    assertEquals(updatedVerificationVal, hudiSnapshotDF3.filter(col("_row_key") === verificationRowKey).select(verificationCol).first.getString(0))
  }

  @ParameterizedTest
  @CsvSource(Array(
    "true,GLOBAL_SIMPLE",
    "true,GLOBAL_BLOOM",
    "false,GLOBAL_SIMPLE",
    "false,GLOBAL_BLOOM"
  ))
  def testMergeOnReadStorageWithGlobalIndexUpdatePartition(isMetadataEnabled: Boolean, indexType: String): Unit = {
    val totalRecords = 10
    val parallelism = 2
    val commonOpts = Map(
      HoodieTableConfig.NAME.key -> "hudi_trips_mor_global_update_partition",
      DataSourceWriteOptions.TABLE_TYPE.key -> HoodieTableType.MERGE_ON_READ.name,
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition_path",
      DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "timestamp",
      HoodieMetadataConfig.ENABLE.key -> isMetadataEnabled.toString,
      HoodieIndexConfig.INDEX_TYPE.key -> indexType,
      HoodieIndexConfig.SIMPLE_INDEX_UPDATE_PARTITION_PATH_ENABLE.key -> "true",
      HoodieIndexConfig.GLOBAL_INDEX_DEDUP_PARALLELISM.key -> parallelism.toString,
      HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key -> parallelism.toString,
      HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key -> parallelism.toString,
      HoodieWriteConfig.BULKINSERT_PARALLELISM_VALUE.key -> parallelism.toString,
      HoodieWriteConfig.DELETE_PARALLELISM_VALUE.key -> parallelism.toString
    )
    val dataGen = new HoodieTestDataGenerator(0xDEEF)

    def upsertAndValidate(records: java.util.List[HoodieRecord[_]], partition: String): Unit = {
      // upsert records
      spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records), parallelism))
        .write.format("hudi")
        .options(commonOpts)
        .mode(SaveMode.Append)
        .save(basePath)

      // validate all records are in the partition
      val snapshotDF = spark.read.format("org.apache.hudi")
        .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
        .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
        .load(basePath)
      assertEquals(totalRecords, snapshotDF.count)
      assertEquals(totalRecords, snapshotDF.filter(s"partition_path = '$partition'").count)
    }

    // insert all records to partition 1
    val inserts1 = dataGen.generateInsertsForPartition(
      getCommitTimeAtUTC(1), totalRecords, DEFAULT_FIRST_PARTITION_PATH)
    upsertAndValidate(inserts1, DEFAULT_FIRST_PARTITION_PATH)

    // update all records to partition 2
    val updates2 = dataGen.generateUpdatesForDifferentPartition(
      getCommitTimeAtUTC(2), inserts1, DEFAULT_SECOND_PARTITION_PATH)
    upsertAndValidate(updates2, DEFAULT_SECOND_PARTITION_PATH)

    // update all records to partition 3
    val updates3 = dataGen.generateUpdatesForDifferentPartition(
      getCommitTimeAtUTC(3), updates2, DEFAULT_THIRD_PARTITION_PATH)
    upsertAndValidate(updates3, DEFAULT_THIRD_PARTITION_PATH)

    // update all records back to partition 1
    val updates4 = dataGen.generateUpdatesForDifferentPartition(
      getCommitTimeAtUTC(4), updates3, DEFAULT_FIRST_PARTITION_PATH)
    upsertAndValidate(updates4, DEFAULT_FIRST_PARTITION_PATH)
  }
}
