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

import org.apache.hadoop.fs.FileSystem
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.{DataSourceTestUtils, HoodieTestDataGenerator}
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieDataSourceHelpers}
import org.apache.spark.sql._
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.{BeforeEach, Test}

import scala.collection.JavaConversions._

/**
 * Test for Spark DataSourceV1 Realtime Relation.
 */
class TestRealtimeDataSource {

  var spark: SparkSession = null
  var dataGen: HoodieTestDataGenerator = null
  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY -> "partition",
    DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> "timestamp",
    HoodieWriteConfig.TABLE_NAME -> "hoodie_test"
  )
  var basePath: String = null
  var fs: FileSystem = null

  @BeforeEach def initialize(@TempDir tempDir: java.nio.file.Path) {
    spark = SparkSession.builder
      .appName("Hoodie Datasource test")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate
    dataGen = new HoodieTestDataGenerator()
    basePath = tempDir.toAbsolutePath.toString
    fs = FSUtils.getFs(basePath, spark.sparkContext.hadoopConfiguration)
  }

  @Test
  def testSparkDatasourceForMergeOnRead() {
    // First Operation:
    // Producing parquet files to three default partitions.
    // SNAPSHOT view on MOR table with parquet files only.
    val records1 = DataSourceTestUtils.convertToStringList(dataGen.generateInserts("001", 100)).toList
    val inputDF1: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
      .option("hoodie.compact.inline", "false") // else fails due to compaction & deltacommit instant times being same
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))
    val hoodieROViewDF1 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath)
    assertEquals(100, hoodieROViewDF1.count()) // still 100, since we only updated

    // Second Operation:
    // Upsert the update to the default partitions with duplicate records. Produced a log file for each parquet.
    // SNAPSHOT view should read the log files only with the latest commit time.
    val records2 = DataSourceTestUtils.convertToStringList(dataGen.generateUniqueUpdates("002", 100)).toList
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val hoodieROViewDF2 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath);
    assertEquals(100, hoodieROViewDF2.count()) // still 100, since we only updated
    assertEquals(hoodieROViewDF2.select("_hoodie_commit_time").distinct().count(), 1)
    assertTrue(hoodieROViewDF2.select("_hoodie_commit_time").head().get(0).toString > hoodieROViewDF1.select("_hoodie_commit_time").head().get(0).toString)

    // Third Operation:
    // Upsert another update to the default partitions with duplicate records. Produced the second log file for each parquet.
    // SNAPSHOT view should read the latest log files.
    val records3 = DataSourceTestUtils.convertToStringList(dataGen.generateUniqueUpdates("003", 100)).toList
    val inputDF3: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records3, 2))
    inputDF3.write.format("org.apache.hudi")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val hoodieROViewDF3 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath)
    assertEquals(100, hoodieROViewDF3.count()) // still 100, since we only updated
    assertEquals(hoodieROViewDF3.select("_hoodie_commit_time").distinct().count(), 1)
    assertTrue(hoodieROViewDF3.select("_hoodie_commit_time").head().get(0).toString > hoodieROViewDF2.select("_hoodie_commit_time").head().get(0).toString)

    // Fourth Operation:
    // Insert records to a new partition. Produced a new parquet file.
    // SNAPSHOT view should read the latest log files from the default partition and parquet from the new partition.
    val partitionPaths = new Array[String](1)
    partitionPaths.update(0, "2020/01/10")
    val newDataGen = new HoodieTestDataGenerator(partitionPaths)
    val records4 = DataSourceTestUtils.convertToStringList(newDataGen.generateInserts("004", 100)).toList
    val inputDF4: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records4, 2))
    inputDF4.write.format("org.apache.hudi")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val hoodieROViewDF4 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath)
    assertEquals(200, hoodieROViewDF4.count()) // 200 since we insert 100 records to a new partition

    // Fifth Operation:
    // Upsert records to the new partition. Produced a newer version of parquet file.
    // SNAPSHOT view should read the latest log files from the default partition and the latest parquet from the new partition.
    val records5 = DataSourceTestUtils.convertToStringList(newDataGen.generateUpdates("005", 100)).toList
    val inputDF5: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records5, 2))
    inputDF5.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    val hoodieROViewDF5 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath)
    assertEquals(200, hoodieROViewDF5.count())
  }
}
