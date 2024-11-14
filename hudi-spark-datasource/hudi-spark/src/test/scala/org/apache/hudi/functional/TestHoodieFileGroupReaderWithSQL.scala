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

import org.apache.hudi.DataSourceWriteOptions.{OPERATION, PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.common.config.{HoodieMetadataConfig, HoodieReaderConfig, HoodieStorageConfig}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.functional.TestHoodieFileGroupReaderWithSQL.{FG_READER_OPTS, METADATA_OPTS, SPARK_OPTS, TABLE_OPTS}
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.hudi.{DataSourceWriteOptions, DefaultSparkRecordMerger}
import org.apache.spark.sql.{Row, SaveMode}
import org.junit.jupiter.api.Test
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class TestHoodieFileGroupReaderWithSQL extends SparkClientFunctionalTestHarness with Matchers {
  @Test
  def testMapKey(): Unit = {
    val columns = Seq("ts", "key", "rider", "driver", "fare", "number")
    val opts = METADATA_OPTS ++ SPARK_OPTS ++ FG_READER_OPTS ++ TABLE_OPTS

    // insert.
    val insertData0 = Seq(
      (1, Map("1" -> "1"), "rider-A0", "driver-A0", 19.10, 7),
      (2, Map("2" -> "2"), "rider-B0", "driver-B0", 27.70, 1),
      (3, Map("3" -> "3"), "rider-C0", "driver-C0", 33.90, 10),
      (4, Map("4" -> "4"), "rider-D0", "driver-D0", 34.15, 6),
      (5, Map("5" -> "5"), "rider-E0", "driver-E0", 17.85, 10))
    val insert0 = spark.createDataFrame(insertData0).toDF(columns: _*)
    insert0.write.format("hudi")
      .options(opts)
      .mode(SaveMode.Overwrite)
      .save(basePath())

    // update.
    val updateData1 = Seq(
      (6, Map("1" -> "1"), "rider-A1", "driver-A1", 20.10, 8),
      (7, Map("2" -> "2"), "rider-B1", "driver-B1", 28.70, 2))
    val update1 = spark.createDataFrame(updateData1).toDF(columns: _*)
    update1.write.format("hudi")
      .option(OPERATION.key(), "upsert")
      .options(opts).mode(SaveMode.Append)
      .save(basePath())

    // delete.
    val deleteData1 = Seq(
      (8, Map("4" -> "4"), "rider-D1", "driver-D1", 20.10, 8),
      (9, Map("5" -> "5"), "rider-E1", "driver-E1", 28.70, 2))
    val delete1 = spark.createDataFrame(deleteData1).toDF(columns: _*)
    delete1.write.format("hudi")
      .option(OPERATION.key(), "delete")
      .options(opts)
      .mode(SaveMode.Append)
      .save(basePath())

    // read.
    val df = spark.read.format("hudi").options(opts).load(basePath())
    val keys: List[Map[String, String]] = df.select("key").collect().map {
      case Row(map: Map[_, _]) => map.asInstanceOf[Map[String, String]]
    }.toList
    val sortedKeys = keys.sortBy(_.toString())
    val expectedKeys = List(Map("1" -> "1"), Map("2" -> "2"), Map("3" -> "3")).sortBy(_.toString())
    sortedKeys shouldBe expectedKeys
  }
}

// TODO: add tests for list, json, or even custom data types.

object TestHoodieFileGroupReaderWithSQL {
  val TABLE_NAME = "any_table"
  val METADATA_OPTS: Map[String, String] = Map(
    HoodieMetadataConfig.ENABLE.key -> "true")

  val SPARK_OPTS: Map[String, String] = Map(
    HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key -> "parquet",
    HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key ->
      classOf[DefaultSparkRecordMerger].getName)

  // TODO: create a parameterize test to flip on or off the fg reader.
  val FG_READER_OPTS: Map[String, String] = Map(
    HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key -> "true",
    HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS.key -> "true")

  val TABLE_OPTS: Map[String, String] = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    HoodieWriteConfig.TBL_NAME.key -> TABLE_NAME,
    DataSourceWriteOptions.TABLE_TYPE.key -> "MERGE_ON_READ",
    RECORDKEY_FIELD.key -> "key",
    PRECOMBINE_FIELD.key -> "ts",
    HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
    HoodieCompactionConfig.INLINE_COMPACT.key() -> "true",
    HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key() -> "1")
}
