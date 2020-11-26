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

import java.time.Instant
import java.util.Collections

import collection.JavaConverters._
import org.apache.hadoop.fs.FileSystem
import org.apache.hudi.bootstrap.SparkParquetBootstrapDataProvider
import org.apache.hudi.client.TestBootstrap
import org.apache.hudi.client.bootstrap.selector.FullRecordBootstrapModeSelector
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieDataSourceHelpers}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.config.{HoodieBootstrapConfig, HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.keygen.SimpleKeyGenerator
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.io.TempDir

class TestDataSourceForBootstrap {

  var spark: SparkSession = _
  val commonOpts = Map(
    HoodieWriteConfig.INSERT_PARALLELISM -> "4",
    HoodieWriteConfig.UPSERT_PARALLELISM -> "4",
    HoodieWriteConfig.DELETE_PARALLELISM -> "4",
    HoodieWriteConfig.BULKINSERT_PARALLELISM -> "4",
    HoodieWriteConfig.FINALIZE_WRITE_PARALLELISM -> "4",
    HoodieBootstrapConfig.BOOTSTRAP_PARALLELISM -> "4",
    DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY -> "partition",
    DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> "timestamp",
    HoodieWriteConfig.TABLE_NAME -> "hoodie_test"
  )
  var basePath: String = _
  var srcPath: String = _
  var fs: FileSystem = _

  @BeforeEach def initialize(@TempDir tempDir: java.nio.file.Path) {
    spark = SparkSession.builder
      .appName("Hoodie Datasource test")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate
    basePath = tempDir.toAbsolutePath.toString + "/base"
    srcPath = tempDir.toAbsolutePath.toString + "/src"
    fs = FSUtils.getFs(basePath, spark.sparkContext.hadoopConfiguration)
  }

  @AfterEach def tearDown(): Unit ={
    // Close spark session
    if (spark != null) {
      spark.stop()
      spark = null
    }

    // Close file system
    if (fs != null) {
      fs.close()
      fs = null
    }
  }

  @Test def testMetadataBootstrapCOWNonPartitioned(): Unit = {
    val timestamp = Instant.now.toEpochMilli
    val numRecords = 100
    val jsc = JavaSparkContext.fromSparkContext(spark.sparkContext)

    val sourceDF = TestBootstrap.generateTestRawTripDataset(timestamp, 0, numRecords, Collections.emptyList(), jsc,
      spark.sqlContext)

    // Write source data non-partitioned
    sourceDF.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(srcPath)

    // Perform bootstrap
    val bootstrapDF = spark.emptyDataFrame
    bootstrapDF.write
      .format("hudi")
      .options(commonOpts)
      .option(HoodieWriteConfig.TABLE_NAME, "hoodie_test")
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.BOOTSTRAP_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "_row_key")
      .option(HoodieBootstrapConfig.BOOTSTRAP_BASE_PATH_PROP, srcPath)
      .option(HoodieBootstrapConfig.BOOTSTRAP_KEYGEN_CLASS, "org.apache.hudi.keygen.NonpartitionedKeyGenerator")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val commitInstantTime1: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS, commitInstantTime1)

    // Read bootstrapped table and verify count
    var hoodieROViewDF1 = spark.read.format("hudi").load(basePath + "/*")
    assertEquals(numRecords, hoodieROViewDF1.count())

    // Perform upsert
    val updateTimestamp = Instant.now.toEpochMilli
    val numRecordsUpdate = 10
    val updateDF = TestBootstrap.generateTestRawTripDataset(updateTimestamp, 0, numRecordsUpdate,
      Collections.emptyList(), jsc, spark.sqlContext)

    updateDF.write
      .format("hudi")
      .options(commonOpts)
      .option(HoodieWriteConfig.TABLE_NAME, "hoodie_test")
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "_row_key")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "timestamp")
      .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY, "org.apache.hudi.keygen.NonpartitionedKeyGenerator")
      .mode(SaveMode.Append)
      .save(basePath)

    val commitInstantTime2: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(1, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, commitInstantTime1).size())

    // Read table after upsert and verify count
    hoodieROViewDF1 = spark.read.format("hudi").load(basePath + "/*")
    assertEquals(numRecords, hoodieROViewDF1.count())
    assertEquals(numRecordsUpdate, hoodieROViewDF1.filter(s"timestamp == $updateTimestamp").count())

    // incrementally pull only changes in the bootstrap commit, which would pull all the initial records written
    // during bootstrap
    val hoodieIncViewDF1 = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "000")
      .option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY, commitInstantTime1)
      .load(basePath)

    assertEquals(numRecords, hoodieIncViewDF1.count())
    var countsPerCommit = hoodieIncViewDF1.groupBy("_hoodie_commit_time").count().collect();
    assertEquals(1, countsPerCommit.length)
    assertEquals(commitInstantTime1, countsPerCommit(0).get(0))

    // incrementally pull only changes in the latest commit, which would pull only the updated records in the
    // latest commit
    val hoodieIncViewDF2 = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, commitInstantTime1)
      .load(basePath);

    assertEquals(numRecordsUpdate, hoodieIncViewDF2.count())
    countsPerCommit = hoodieIncViewDF2.groupBy("_hoodie_commit_time").count().collect();
    assertEquals(1, countsPerCommit.length)
    assertEquals(commitInstantTime2, countsPerCommit(0).get(0))
  }

  @Test def testMetadataBootstrapCOWHiveStylePartitioned(): Unit = {
    val timestamp = Instant.now.toEpochMilli
    val numRecords = 100
    val partitionPaths = List("2020-04-01", "2020-04-02", "2020-04-03")
    val jsc = JavaSparkContext.fromSparkContext(spark.sparkContext)

    val sourceDF = TestBootstrap.generateTestRawTripDataset(timestamp, 0, numRecords, partitionPaths.asJava, jsc,
      spark.sqlContext)

    // Write source data hive style partitioned
    sourceDF.write
      .partitionBy("datestr")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(srcPath)

    // Perform bootstrap
    val bootstrapDF = spark.emptyDataFrame
    bootstrapDF.write
      .format("hudi")
      .options(commonOpts)
      .option(HoodieWriteConfig.TABLE_NAME, "hoodie_test")
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.BOOTSTRAP_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "_row_key")
      .option(HoodieBootstrapConfig.BOOTSTRAP_BASE_PATH_PROP, srcPath)
      .option(HoodieBootstrapConfig.BOOTSTRAP_KEYGEN_CLASS, "org.apache.hudi.keygen.SimpleKeyGenerator")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val commitInstantTime1: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS, commitInstantTime1)

    // Read bootstrapped table and verify count
    val hoodieROViewDF1 = spark.read.format("hudi").load(basePath + "/*")
    assertEquals(numRecords, hoodieROViewDF1.count())

    // Perform upsert
    val updateTimestamp = Instant.now.toEpochMilli
    val numRecordsUpdate = 10
    val updateDF = TestBootstrap.generateTestRawTripDataset(updateTimestamp, 0, numRecordsUpdate, partitionPaths.asJava,
      jsc, spark.sqlContext)

    updateDF.write
      .format("hudi")
      .options(commonOpts)
      .option(HoodieWriteConfig.TABLE_NAME, "hoodie_test")
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "_row_key")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "timestamp")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "datestr")
      // Required because source data is hive style partitioned
      .option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY, "true")
      .mode(SaveMode.Append)
      .save(basePath)

    val commitInstantTime2: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(1, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, commitInstantTime1).size())

    // Read table after upsert and verify count
    val hoodieROViewDF2 = spark.read.format("hudi").load(basePath + "/*")
    assertEquals(numRecords, hoodieROViewDF2.count())
    assertEquals(numRecordsUpdate, hoodieROViewDF2.filter(s"timestamp == $updateTimestamp").count())

    // incrementally pull only changes in the bootstrap commit, which would pull all the initial records written
    // during bootstrap
    val hoodieIncViewDF1 = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "000")
      .option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY, commitInstantTime1)
      .load(basePath)

    assertEquals(numRecords, hoodieIncViewDF1.count())
    var countsPerCommit = hoodieIncViewDF1.groupBy("_hoodie_commit_time").count().collect();
    assertEquals(1, countsPerCommit.length)
    assertEquals(commitInstantTime1, countsPerCommit(0).get(0))

    // incrementally pull only changes in the latest commit, which would pull only the updated records in the
    // latest commit
    val hoodieIncViewDF2 = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, commitInstantTime1)
      .load(basePath);

    assertEquals(numRecordsUpdate, hoodieIncViewDF2.count())
    countsPerCommit = hoodieIncViewDF2.groupBy("_hoodie_commit_time").count().collect();
    assertEquals(1, countsPerCommit.length)
    assertEquals(commitInstantTime2, countsPerCommit(0).get(0))

    // pull the latest commit within certain partitions
    val hoodieIncViewDF3 = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, commitInstantTime1)
      .option(DataSourceReadOptions.INCR_PATH_GLOB_OPT_KEY, "/datestr=2020-04-02/*")
      .load(basePath)

    assertEquals(hoodieIncViewDF2.filter(col("_hoodie_partition_path").contains("2020-04-02")).count(),
      hoodieIncViewDF3.count())
  }

  @Test def testMetadataBootstrapCOWPartitioned(): Unit = {
    val timestamp = Instant.now.toEpochMilli
    val numRecords = 100
    val partitionPaths = List("2020-04-01", "2020-04-02", "2020-04-03")
    val jsc = JavaSparkContext.fromSparkContext(spark.sparkContext)

    var sourceDF = TestBootstrap.generateTestRawTripDataset(timestamp, 0, numRecords, partitionPaths.asJava, jsc,
      spark.sqlContext)

    // Writing data for each partition instead of using partitionBy to avoid hive-style partitioning and hence
    // have partitioned columns stored in the data file
    partitionPaths.foreach(partitionPath => {
      sourceDF
        .filter(sourceDF("datestr").equalTo(lit(partitionPath)))
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .save(srcPath + "/" + partitionPath)
    })

    // Perform bootstrap
    val bootstrapDF = spark.emptyDataFrame
    bootstrapDF.write
      .format("hudi")
      .options(commonOpts)
      .option(HoodieWriteConfig.TABLE_NAME, "hoodie_test")
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.BOOTSTRAP_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "_row_key")
      .option(HoodieBootstrapConfig.BOOTSTRAP_BASE_PATH_PROP, srcPath)
      .option(HoodieBootstrapConfig.BOOTSTRAP_KEYGEN_CLASS, "org.apache.hudi.keygen.SimpleKeyGenerator")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val commitInstantTime1: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS, commitInstantTime1)

    // Read bootstrapped table and verify count
    val hoodieROViewDF1 = spark.read.format("hudi").load(basePath + "/*")
    assertEquals(numRecords, hoodieROViewDF1.count())

    // Perform upsert
    val updateTimestamp = Instant.now.toEpochMilli
    val numRecordsUpdate = 10
    var updateDF = TestBootstrap.generateTestRawTripDataset(updateTimestamp, 0, numRecordsUpdate, partitionPaths.asJava,
      jsc, spark.sqlContext)

    updateDF.write
      .format("hudi")
      .options(commonOpts)
      .option(HoodieWriteConfig.TABLE_NAME, "hoodie_test")
      .option("hoodie.upsert.shuffle.parallelism", "4")
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "_row_key")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "timestamp")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "datestr")
      .mode(SaveMode.Append)
      .save(basePath)

    val commitInstantTime2: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(1, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, commitInstantTime1).size())

    // Read table after upsert and verify count
    val hoodieROViewDF2 = spark.read.format("hudi").load(basePath + "/*")
    assertEquals(numRecords, hoodieROViewDF2.count())
    assertEquals(numRecordsUpdate, hoodieROViewDF2.filter(s"timestamp == $updateTimestamp").count())

    // incrementally pull only changes in the bootstrap commit, which would pull all the initial records written
    // during bootstrap
    val hoodieIncViewDF1 = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "000")
      .option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY, commitInstantTime1)
      .load(basePath)

    assertEquals(numRecords, hoodieIncViewDF1.count())
    var countsPerCommit = hoodieIncViewDF1.groupBy("_hoodie_commit_time").count().collect();
    assertEquals(1, countsPerCommit.length)
    assertEquals(commitInstantTime1, countsPerCommit(0).get(0))

    // incrementally pull only changes in the latest commit, which would pull only the updated records in the
    // latest commit
    val hoodieIncViewDF2 = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, commitInstantTime1)
      .load(basePath);

    assertEquals(numRecordsUpdate, hoodieIncViewDF2.count())
    countsPerCommit = hoodieIncViewDF2.groupBy("_hoodie_commit_time").count().collect();
    assertEquals(1, countsPerCommit.length)
    assertEquals(commitInstantTime2, countsPerCommit(0).get(0))

    // pull the latest commit within certain partitions
    val hoodieIncViewDF3 = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, commitInstantTime1)
      .option(DataSourceReadOptions.INCR_PATH_GLOB_OPT_KEY, "/2020-04-02/*")
      .load(basePath)

    assertEquals(hoodieIncViewDF2.filter(col("_hoodie_partition_path").contains("2020-04-02")).count(),
      hoodieIncViewDF3.count())
  }

  @Test def testMetadataBootstrapMORPartitionedInlineCompactionOn(): Unit = {
    val timestamp = Instant.now.toEpochMilli
    val numRecords = 100
    val partitionPaths = List("2020-04-01", "2020-04-02", "2020-04-03")
    val jsc = JavaSparkContext.fromSparkContext(spark.sparkContext)

    val sourceDF = TestBootstrap.generateTestRawTripDataset(timestamp, 0, numRecords, partitionPaths.asJava, jsc,
      spark.sqlContext)

    // Writing data for each partition instead of using partitionBy to avoid hive-style partitioning and hence
    // have partitioned columns stored in the data file
    partitionPaths.foreach(partitionPath => {
      sourceDF
        .filter(sourceDF("datestr").equalTo(lit(partitionPath)))
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .save(srcPath + "/" + partitionPath)
    })

    // Perform bootstrap
    val bootstrapDF = spark.emptyDataFrame
    bootstrapDF.write
      .format("hudi")
      .options(commonOpts)
      .option(HoodieWriteConfig.TABLE_NAME, "hoodie_test")
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.BOOTSTRAP_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "_row_key")
      .option(HoodieBootstrapConfig.BOOTSTRAP_BASE_PATH_PROP, srcPath)
      .option(HoodieBootstrapConfig.BOOTSTRAP_KEYGEN_CLASS, "org.apache.hudi.keygen.SimpleKeyGenerator")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val commitInstantTime1: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS, commitInstantTime1)

    // Read bootstrapped table and verify count
    val hoodieROViewDF1 = spark.read.format("hudi")
                            .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY,
                              DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
                            .load(basePath + "/*")
    assertEquals(numRecords, hoodieROViewDF1.count())

    // Perform upsert
    val updateTimestamp = Instant.now.toEpochMilli
    val numRecordsUpdate = 10
    val updateDF = TestBootstrap.generateTestRawTripDataset(updateTimestamp, 0, numRecordsUpdate, partitionPaths.asJava,
      jsc, spark.sqlContext)

    updateDF.write
      .format("hudi")
      .options(commonOpts)
      .option(HoodieWriteConfig.TABLE_NAME, "hoodie_test")
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "_row_key")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "timestamp")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "datestr")
      .option(HoodieCompactionConfig.INLINE_COMPACT_PROP, "true")
      .option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS_PROP, "1")
      .mode(SaveMode.Append)
      .save(basePath)

    // Expect 2 new commits since meta bootstrap - delta commit and compaction commit (due to inline compaction)
    assertEquals(2, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, commitInstantTime1).size())

    // Read table after upsert and verify count. Since we have inline compaction enabled the RO view will have
    // the updated rows.
    val hoodieROViewDF2 = spark.read.format("hudi")
                            .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY,
                              DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
                            .load(basePath + "/*")
    assertEquals(numRecords, hoodieROViewDF2.count())
    assertEquals(numRecordsUpdate, hoodieROViewDF2.filter(s"timestamp == $updateTimestamp").count())
  }

  @Test def testMetadataBootstrapMORPartitioned(): Unit = {
    val timestamp = Instant.now.toEpochMilli
    val numRecords = 100
    val partitionPaths = List("2020-04-01", "2020-04-02", "2020-04-03")
    val jsc = JavaSparkContext.fromSparkContext(spark.sparkContext)

    val sourceDF = TestBootstrap.generateTestRawTripDataset(timestamp, 0, numRecords, partitionPaths.asJava, jsc,
      spark.sqlContext)

    // Writing data for each partition instead of using partitionBy to avoid hive-style partitioning and hence
    // have partitioned columns stored in the data file
    partitionPaths.foreach(partitionPath => {
      sourceDF
        .filter(sourceDF("datestr").equalTo(lit(partitionPath)))
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .save(srcPath + "/" + partitionPath)
    })

    // Perform bootstrap
    val bootstrapDF = spark.emptyDataFrame
    bootstrapDF.write
      .format("hudi")
      .options(commonOpts)
      .option(HoodieWriteConfig.TABLE_NAME, "hoodie_test")
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.BOOTSTRAP_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "_row_key")
      .option(HoodieBootstrapConfig.BOOTSTRAP_BASE_PATH_PROP, srcPath)
      .option(HoodieBootstrapConfig.BOOTSTRAP_KEYGEN_CLASS, "org.apache.hudi.keygen.SimpleKeyGenerator")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val commitInstantTime1: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS, commitInstantTime1)

    // Read bootstrapped table and verify count
    val hoodieROViewDF1 = spark.read.format("hudi")
                            .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY,
                              DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
                            .load(basePath + "/*")
    assertEquals(numRecords, hoodieROViewDF1.count())

    // Perform upsert
    val updateTimestamp = Instant.now.toEpochMilli
    val numRecordsUpdate = 10
    val updateDF = TestBootstrap.generateTestRawTripDataset(updateTimestamp, 0, numRecordsUpdate,
      partitionPaths.asJava, jsc, spark.sqlContext)

    updateDF.write
      .format("hudi")
      .options(commonOpts)
      .option(HoodieWriteConfig.TABLE_NAME, "hoodie_test")
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "_row_key")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "timestamp")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "datestr")
      .mode(SaveMode.Append)
      .save(basePath)

    // Expect 1 new commit since meta bootstrap - delta commit (because inline compaction is off)
    assertEquals(1, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, commitInstantTime1).size())

    // Read table after upsert and verify count. Since we have inline compaction off the RO view will have
    // no updated rows.
    val hoodieROViewDF2 = spark.read.format("hudi")
                            .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY,
                              DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
                            .load(basePath + "/*")
    assertEquals(numRecords, hoodieROViewDF2.count())
    assertEquals(0, hoodieROViewDF2.filter(s"timestamp == $updateTimestamp").count())
  }

  @Test def testFullBootstrapCOWPartitioned(): Unit = {
    val timestamp = Instant.now.toEpochMilli
    val numRecords = 100
    val partitionPaths = List("2020-04-01", "2020-04-02", "2020-04-03")
    val jsc = JavaSparkContext.fromSparkContext(spark.sparkContext)

    val sourceDF = TestBootstrap.generateTestRawTripDataset(timestamp, 0, numRecords, partitionPaths.asJava, jsc,
      spark.sqlContext)

    // Writing data for each partition instead of using partitionBy to avoid hive-style partitioning and hence
    // have partitioned columns stored in the data file
    partitionPaths.foreach(partitionPath => {
      sourceDF
        .filter(sourceDF("datestr").equalTo(lit(partitionPath)))
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .save(srcPath + "/" + partitionPath)
    })

    // Perform bootstrap
    val bootstrapDF = spark.emptyDataFrame
    bootstrapDF.write
      .format("hudi")
      .options(commonOpts)
      .option(HoodieWriteConfig.TABLE_NAME, "hoodie_test")
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.BOOTSTRAP_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "_row_key")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "datestr")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "timestamp")
      .option(HoodieBootstrapConfig.BOOTSTRAP_BASE_PATH_PROP, srcPath)
      .option(HoodieBootstrapConfig.BOOTSTRAP_KEYGEN_CLASS, classOf[SimpleKeyGenerator].getName)
      .option(HoodieBootstrapConfig.BOOTSTRAP_MODE_SELECTOR, classOf[FullRecordBootstrapModeSelector].getName)
      .option(HoodieBootstrapConfig.FULL_BOOTSTRAP_INPUT_PROVIDER, classOf[SparkParquetBootstrapDataProvider].getName)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val commitInstantTime1: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS, commitInstantTime1)

    // Read bootstrapped table and verify count
    val hoodieROViewDF1 = spark.read.format("hudi").load(basePath + "/*")
    assertEquals(numRecords, hoodieROViewDF1.count())

    // Perform upsert
    val updateTimestamp = Instant.now.toEpochMilli
    val numRecordsUpdate = 10
    val updateDF = TestBootstrap.generateTestRawTripDataset(updateTimestamp, 0, numRecordsUpdate, partitionPaths.asJava,
      jsc, spark.sqlContext)

    updateDF.write
      .format("hudi")
      .options(commonOpts)
      .option(HoodieWriteConfig.TABLE_NAME, "hoodie_test")
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "_row_key")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "timestamp")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "datestr")
      .mode(SaveMode.Append)
      .save(basePath)

    val commitInstantTime2: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(1, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, commitInstantTime1).size())

    // Read table after upsert and verify count
    val hoodieROViewDF2 = spark.read.format("hudi").load(basePath + "/*")
    assertEquals(numRecords, hoodieROViewDF2.count())
    assertEquals(numRecordsUpdate, hoodieROViewDF2.filter(s"timestamp == $updateTimestamp").count())

    // incrementally pull only changes in the bootstrap commit, which would pull all the initial records written
    // during bootstrap
    val hoodieIncViewDF1 = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "000")
      .option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY, commitInstantTime1)
      .load(basePath)

    assertEquals(numRecords, hoodieIncViewDF1.count())
    var countsPerCommit = hoodieIncViewDF1.groupBy("_hoodie_commit_time").count().collect();
    assertEquals(1, countsPerCommit.length)
    assertEquals(commitInstantTime1, countsPerCommit(0).get(0))

    // incrementally pull only changes in the latest commit, which would pull only the updated records in the
    // latest commit
    val hoodieIncViewDF2 = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, commitInstantTime1)
      .load(basePath);

    assertEquals(numRecordsUpdate, hoodieIncViewDF2.count())
    countsPerCommit = hoodieIncViewDF2.groupBy("_hoodie_commit_time").count().collect();
    assertEquals(1, countsPerCommit.length)
    assertEquals(commitInstantTime2, countsPerCommit(0).get(0))

    // pull the latest commit within certain partitions
    val hoodieIncViewDF3 = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, commitInstantTime1)
      .option(DataSourceReadOptions.INCR_PATH_GLOB_OPT_KEY, "/2020-04-02/*")
      .load(basePath)

    assertEquals(hoodieIncViewDF2.filter(col("_hoodie_partition_path").contains("2020-04-02")).count(),
      hoodieIncViewDF3.count())
  }
}
