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
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator.Config
import org.apache.hudi.keygen.constant.KeyGeneratorType
import org.apache.hudi.keygen.{ComplexKeyGenerator, SimpleKeyGenerator, TimestampBasedKeyGenerator}
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieDataSourceHelpers}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions.{col, lit, when}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Tag
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

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

  val keyGenTypeToClass = Map(
    KeyGeneratorType.SIMPLE.name() -> classOf[SimpleKeyGenerator].getName,
    KeyGeneratorType.COMPLEX.name() -> classOf[ComplexKeyGenerator].getName,
    KeyGeneratorType.TIMESTAMP.name() -> classOf[TimestampBasedKeyGenerator].getName
  )

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testCopyOnWriteStorage(isMetadataEnabled: Boolean): Unit = {
    for ((k, v) <- keyGenTypeToClass) {
      commonOpts += HoodieWriteConfig.KEYGENERATOR_TYPE.key() -> k
      commonOpts += DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key() -> v
      println(">>> KeyGen Class: " + v)
      if (classOf[ComplexKeyGenerator].getName.equals(v)) {
        commonOpts += DataSourceWriteOptions.RECORDKEY_FIELD.key() -> "_row_key, pii_col"
      }
      if (classOf[TimestampBasedKeyGenerator].getName.equals(v)) {
        commonOpts += DataSourceWriteOptions.RECORDKEY_FIELD.key() -> "_row_key"
        commonOpts += DataSourceWriteOptions.PARTITIONPATH_FIELD.key() -> "current_ts"
        commonOpts += Config.TIMESTAMP_TYPE_FIELD_PROP -> "EPOCHMILLISECONDS"
        commonOpts += Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP -> "yyyyMMdd"
      }
      println(">>> Record Key: " + commonOpts.get(DataSourceWriteOptions.RECORDKEY_FIELD.key()))
      val dataGen = new HoodieTestDataGenerator()
      val fs = FSUtils.getFs(basePath, spark.sparkContext.hadoopConfiguration)
      // Insert Operation
      val records0 = recordsToStrings(dataGen.generateInserts("000", 10)).toList
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
      assertEquals(10, snapshotDF1.count())

      val records1 = recordsToStrings(dataGen.generateUpdates("001", 10)).toList
      val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
      val verificationRowKey = inputDF1.limit(1).select("_row_key").first.getString(0)
      // update current_ts to be same as original record so that partition path does not change with timestamp based key gen
      val orignalRow = inputDF1.filter(col("_row_key") === verificationRowKey).collectAsList().get(0)
      val updateDf = snapshotDF1.filter(col("_row_key") === verificationRowKey).withColumn(verificationCol, lit(updatedVerificationVal))
        .withColumn("current_ts", lit(orignalRow.getAs("current_ts")))

      /*println("original record ")
      inputDF1.collectAsList().foreach(row => println("Row " + row.getAs("current_ts") + ", key " + row.getAs("_row_key") + ", partition path "
        + row.getAs("partition_path") + ", beginlat " + row.getAs("begin_lat")))

      println("updated record ")
      val updateRows = updateDf.collectAsList()
      updateRows.foreach(row => println("Row " + row.getAs("current_ts") + ", key " + row.getAs("_row_key") + ", partition path "
        + row.getAs("partition_path") + ", beginlat " + row.getAs("begin_lat")))*/

      updateDf.write.format("org.apache.hudi")
        .options(commonOpts)
        .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
        .mode(SaveMode.Append)
        .save(basePath)
      val commitInstantTime2 = HoodieDataSourceHelpers.latestCommit(fs, basePath)

      val snapshotDF2 = spark.read.format("hudi")
        .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
        .load(basePath)
      assertEquals(10, snapshotDF2.count())
      assertEquals(updatedVerificationVal, snapshotDF2.filter(col("_row_key") === verificationRowKey).select(verificationCol).first.getString(0))

      // Upsert Operation without Hudi metadata columns
      val records2 = recordsToStrings(dataGen.generateUpdates("002", 10)).toList
      val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))


      /*println("original record ")
      inputDF0.collectAsList().foreach(row => println("Row " + row.getAs("current_ts") + ", key " + row.getAs("_row_key") + ", partition path "
        + row.getAs("partition_path") + ", beginlat " + row.getAs("begin_lat")))

      println("original updated record ")
      inputDF2.collectAsList().foreach(row => println("Row " + row.getAs("current_ts") + ", key " + row.getAs("_row_key") + ", partition path "
        + row.getAs("partition_path") + ", beginlat " + row.getAs("begin_lat")))*/

      val inputDF3 = inputDF2.withColumn("current_ts_updated", col("current_ts"))
        .withColumn("_row_key_updated", col("_row_key"))

      val originalRowCurrentTsDf = inputDF0.select("_row_key","current_ts")
      val temp1 = inputDF3.drop("_row_key","current_ts").join(originalRowCurrentTsDf, (inputDF3("_row_key_updated") === originalRowCurrentTsDf("_row_key")))

      /*println("tep1 " + temp1.schema.toString())
      temp1.collectAsList().foreach(row => println("Row " + row.getAs("current_ts") + ", current ts original "
        + row.getAs("current_ts_updated")  + ", key " + row.getAs("_row_key") + ", partition path "
        + row.getAs("partition_path") + ", beginlat " + row.getAs("begin_lat")))*/

      val temp2 = temp1.withColumn("current_ts_updated", col("current_ts"))
        .drop("current_ts", "_row_key_updated").withColumn("current_ts", col("current_ts_updated"))
        .drop("current_ts_updated")

      /*println("temp2222 " + temp2.schema.toString())
      temp2.collectAsList().foreach(row => println("Row " + row.getAs("current_ts")
        + ", key " + row.getAs("_row_key") + ", partition path "
        + row.getAs("partition_path") + ", beginlat " + row.getAs("begin_lat")))*/

      val uniqueKeyCnt = inputDF2.select("_row_key").distinct().count()

      temp2.write.format("org.apache.hudi")
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
      assertEquals(10, snapshotDF3.count()) // still 100, since we only updated

      // Read Incremental Query
      // we have 2 commits, try pulling the first commit (which is not the latest)
      val firstCommit = HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").get(0)
      val hoodieIncViewDF1 = spark.read.format("org.apache.hudi")
        .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
        .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, "000")
        .option(DataSourceReadOptions.END_INSTANTTIME.key, firstCommit)
        .load(basePath)
      assertEquals(10, hoodieIncViewDF1.count()) // 100 initial inserts must be pulled
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
      assertEquals(10, timeTravelDF.count()) // 100 initial inserts must be pulled
    }
  }
}
