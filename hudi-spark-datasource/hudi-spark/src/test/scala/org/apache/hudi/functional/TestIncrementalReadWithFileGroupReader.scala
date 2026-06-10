/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.functional

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableVersion}
import org.apache.hudi.config.HoodieCompactionConfig
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

/**
 * Asserts incremental queries with the file group reader return correct results when the query
 * does not project `_hoodie_commit_time` (count(), isEmpty(), narrow projections), since the
 * incremental span filters are enforced via parquet push-down on that column. Runs on a session
 * without HoodieSparkSessionExtension, so the filters are not injected into the logical plan
 * and the file format alone must keep the filter columns readable.
 */
class TestIncrementalReadWithFileGroupReader extends SparkClientFunctionalTestHarness {

  val inserts: Seq[(Int, String, String, Double, String)] = Seq(
    (10, "1", "rider-A", 19.10, "pt1"),
    (10, "2", "rider-B", 27.70, "pt1"),
    (10, "3", "rider-C", 33.90, "pt1"),
    (10, "4", "rider-D", 34.15, "pt1"),
    (10, "5", "rider-E", 17.85, "pt1"),
    (10, "6", "rider-F", 41.06, "pt1"))
  val updates: Seq[(Int, String, String, Double, String)] = Seq(
    (11, "1", "rider-A", 1.10, "pt1"),
    (11, "2", "rider-B", 2.20, "pt1"),
    (11, "3", "rider-C", 3.30, "pt1"))
  val columns: Seq[String] = Seq("ts", "key", "rider", "fare", "pt")

  @ParameterizedTest
  @CsvSource(value = Array(
    "COPY_ON_WRITE,true",
    "COPY_ON_WRITE,false",
    "MERGE_ON_READ,true",
    "MERGE_ON_READ,false"
  ))
  def testCountAndPrunedProjections(tableType: String, forceV1Read: Boolean): Unit = {
    write(inserts, tableType, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL, SaveMode.Overwrite)
    write(updates, tableType, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL, SaveMode.Append)
    if (tableType == "MERGE_ON_READ") {
      // updates must land in log files so the incremental read merges logs
      Assertions.assertTrue(fs.listStatus(new Path(basePath, "pt1")).exists(_.getPath.getName.contains(".log.")),
        "Expected log files in the MOR table")
    }

    Assertions.assertEquals(6, readIncremental(forceV1Read).collect().length)
    // these query shapes prune `_hoodie_commit_time` out of the scan schema
    Assertions.assertEquals(6L, readIncremental(forceV1Read).count())
    Assertions.assertFalse(readIncremental(forceV1Read).isEmpty)
    Assertions.assertEquals(6L, readIncremental(forceV1Read).select("key").count())
  }

  private def write(data: Seq[(Int, String, String, Double, String)],
                    tableType: String, operation: String, mode: SaveMode): Unit = {
    spark.createDataFrame(data).toDF(columns: _*).write.format("hudi")
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "key")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "pt")
      .option(HoodieTableConfig.ORDERING_FIELDS.key, "ts")
      .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType)
      .option(DataSourceWriteOptions.TABLE_NAME.key, "test_incr_read_fgr")
      .option(HoodieCompactionConfig.INLINE_COMPACT.key, "false")
      .option(DataSourceWriteOptions.OPERATION.key, operation)
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .mode(mode)
      .save(basePath)
  }

  private def readIncremental(forceV1Read: Boolean): DataFrame = {
    val reader = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key(), "000")
    val readerWithVersion = if (forceV1Read) {
      // same access pattern as the S3/GCS event incremental sources
      reader.option(DataSourceReadOptions.INCREMENTAL_READ_TABLE_VERSION.key(),
        HoodieTableVersion.SIX.versionCode().toString)
    } else {
      reader
    }
    readerWithVersion.load(basePath)
  }
}
