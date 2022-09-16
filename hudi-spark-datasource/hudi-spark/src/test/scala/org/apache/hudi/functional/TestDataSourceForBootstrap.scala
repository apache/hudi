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

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hudi.bootstrap.SparkParquetBootstrapDataProvider
import org.apache.hudi.client.bootstrap.selector.FullRecordBootstrapModeSelector
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.config.{HoodieBootstrapConfig, HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.keygen.{NonpartitionedKeyGenerator, SimpleKeyGenerator}
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieDataSourceHelpers}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.time.Instant
import java.util.Collections
import scala.collection.JavaConverters._

class TestDataSourceForBootstrap {

  var spark: SparkSession = _
  val commonOpts: Map[String, String] = Map(
    HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key -> "4",
    HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key -> "4",
    HoodieWriteConfig.DELETE_PARALLELISM_VALUE.key -> "4",
    HoodieWriteConfig.BULKINSERT_PARALLELISM_VALUE.key -> "4",
    HoodieWriteConfig.FINALIZE_WRITE_PARALLELISM_VALUE.key -> "4",
    HoodieBootstrapConfig.PARALLELISM_VALUE.key -> "4",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
  )
  var basePath: String = _
  var srcPath: String = _
  var fs: FileSystem = _

  val partitionPaths: List[String] = List("2020-04-01", "2020-04-02", "2020-04-03")
  val numRecords: Int = 100
  val numRecordsUpdate: Int = 10
  val verificationRowKey: String = "trip_0"
  val verificationCol: String = "driver"
  val originalVerificationVal: String = "driver_0"
  val updatedVerificationVal: String = "driver_update"

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
    val jsc = JavaSparkContext.fromSparkContext(spark.sparkContext)

    val sourceDF = TestBootstrap.generateTestRawTripDataset(timestamp, 0, numRecords, Collections.emptyList(), jsc,
      spark.sqlContext)

    // Write source data non-partitioned
    sourceDF.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(srcPath)

    // Perform bootstrap
    val bootstrapKeygenClass = classOf[NonpartitionedKeyGenerator].getName
    val options = commonOpts.-(DataSourceWriteOptions.PARTITIONPATH_FIELD.key)
    val commitInstantTime1 = runMetadataBootstrapAndVerifyCommit(
      DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL,
      extraOpts = options ++ Map(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> bootstrapKeygenClass),
      bootstrapKeygenClass = bootstrapKeygenClass
    )
    // check marked directory clean up
    assert(!fs.exists(new Path(basePath, ".hoodie/.temp/00000000000001")))

    // Read bootstrapped table and verify count using glob path
    val hoodieROViewDF1 = spark.read.format("hudi").load(basePath + "/*")
    assertEquals(numRecords, hoodieROViewDF1.count())
    // Read bootstrapped table and verify count using Hudi file index
    val hoodieROViewDF2 = spark.read.format("hudi").load(basePath)
    assertEquals(numRecords, hoodieROViewDF2.count())

    // Perform upsert
    val updateTimestamp = Instant.now.toEpochMilli
    val updateDF = TestBootstrap.generateTestRawTripDataset(updateTimestamp, 0, numRecordsUpdate,
      Collections.emptyList(), jsc, spark.sqlContext)

    updateDF.write
      .format("hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key, bootstrapKeygenClass)
      .mode(SaveMode.Append)
      .save(basePath)

    val commitInstantTime2: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(1, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, commitInstantTime1).size())

    // Read table after upsert and verify count using glob path
    val hoodieROViewDF3 = spark.read.format("hudi").load(basePath + "/*")
    assertEquals(numRecords, hoodieROViewDF3.count())
    assertEquals(numRecordsUpdate, hoodieROViewDF3.filter(s"timestamp == $updateTimestamp").count())
    // Read with base path using Hudi file index
    val hoodieROViewDF1WithBasePath = spark.read.format("hudi").load(basePath)
    assertEquals(numRecords, hoodieROViewDF1WithBasePath.count())
    assertEquals(numRecordsUpdate, hoodieROViewDF1WithBasePath.filter(s"timestamp == $updateTimestamp").count())

    verifyIncrementalViewResult(commitInstantTime1, commitInstantTime2, isPartitioned = false, isHiveStylePartitioned = false)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("METADATA_ONLY", "FULL_RECORD"))
  def testMetadataBootstrapCOWHiveStylePartitioned(bootstrapMode: String): Unit = {
    val timestamp = Instant.now.toEpochMilli
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
    val commitInstantTime1 = runMetadataBootstrapAndVerifyCommit(
      DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL,
      commonOpts.updated(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "datestr") ++
        Map(
          DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key -> "true",
          HoodieBootstrapConfig.PARTITION_SELECTOR_REGEX_MODE.key -> bootstrapMode),
      classOf[SimpleKeyGenerator].getName)

    // check marked directory clean up
    assert(!fs.exists(new Path(basePath, ".hoodie/.temp/00000000000001")))

    // Read bootstrapped table and verify count
    val hoodieROViewDF1 = spark.read.format("hudi").load(basePath + "/*")
    assertEquals(numRecords, hoodieROViewDF1.count())
    // Read bootstrapped table and verify count using Hudi file index
    val hoodieROViewDF2 = spark.read.format("hudi").load(basePath)
    assertEquals(numRecords, hoodieROViewDF2.count())

    // Perform upsert
    val updateTimestamp = Instant.now.toEpochMilli
    val updateDF = TestBootstrap.generateTestRawTripDataset(updateTimestamp, 0, numRecordsUpdate, partitionPaths.asJava,
      jsc, spark.sqlContext)

    updateDF.write
      .format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "datestr")
      // Required because source data is hive style partitioned
      .option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key, "true")
      .mode(SaveMode.Append)
      .save(basePath)

    val commitInstantTime2: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(1, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, commitInstantTime1).size())

    // Read table after upsert and verify count using glob path
    val hoodieROViewDF3 = spark.read.format("hudi").load(basePath + "/*")
    assertEquals(numRecords, hoodieROViewDF3.count())
    assertEquals(numRecordsUpdate, hoodieROViewDF3.filter(s"timestamp == $updateTimestamp").count())
    // Read table after upsert and verify count using Hudi file index
    val hoodieROViewDF4 = spark.read.format("hudi").load(basePath)
    assertEquals(numRecords, hoodieROViewDF4.count())
    assertEquals(numRecordsUpdate, hoodieROViewDF3.filter(s"timestamp == $updateTimestamp").count())

    verifyIncrementalViewResult(commitInstantTime1, commitInstantTime2, isPartitioned = true, isHiveStylePartitioned = true)
  }

  @Test def testMetadataBootstrapCOWPartitioned(): Unit = {
    val timestamp = Instant.now.toEpochMilli
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
    val commitInstantTime1 = runMetadataBootstrapAndVerifyCommit(
      DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL,
      commonOpts.updated(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "datestr"),
      classOf[SimpleKeyGenerator].getName)

    // Read bootstrapped table and verify count using glob path
    val hoodieROViewDF1 = spark.read.format("hudi").load(basePath + "/*")
    assertEquals(numRecords, hoodieROViewDF1.count())
    // Read with base path using Hudi file index
    val hoodieROViewWithBasePathDF1 = spark.read.format("hudi").load(basePath)
    assertEquals(numRecords, hoodieROViewWithBasePathDF1.count())

    // Perform upsert based on the written bootstrap table
    val updateDf1 = hoodieROViewDF1.filter(col("_row_key") === verificationRowKey).withColumn(verificationCol, lit(updatedVerificationVal))
    updateDf1.write
      .format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "datestr")
      .mode(SaveMode.Append)
      .save(basePath)

    // Read table after upsert and verify the updated value
    assertEquals(1, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, commitInstantTime1).size())
    val hoodieROViewDF2 = spark.read.format("hudi").load(basePath + "/*")
    hoodieROViewDF2.collect()
    assertEquals(updatedVerificationVal, hoodieROViewDF2.filter(col("_row_key") === verificationRowKey).select(verificationCol).first.getString(0))

    // Perform upsert based on the source data
    val updateTimestamp = Instant.now.toEpochMilli
    val updateDF2 = TestBootstrap.generateTestRawTripDataset(updateTimestamp, 0, numRecordsUpdate, partitionPaths.asJava,
      jsc, spark.sqlContext)

    updateDF2.write
      .format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "datestr")
      .mode(SaveMode.Append)
      .save(basePath)

    val commitInstantTime3: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(2, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, commitInstantTime1).size())

    // Read table after upsert and verify count using glob paths
    val hoodieROViewDF3 = spark.read.format("hudi").load(basePath + "/*")
    assertEquals(numRecords, hoodieROViewDF3.count())
    assertEquals(numRecordsUpdate, hoodieROViewDF3.filter(s"timestamp == $updateTimestamp").count())
    // Read table after upsert and verify count using Hudi file index
    val hoodieROViewDF4 = spark.read.format("hudi").load(basePath)
    assertEquals(numRecords, hoodieROViewDF4.count())
    assertEquals(numRecordsUpdate, hoodieROViewDF4.filter(s"timestamp == $updateTimestamp").count())

    verifyIncrementalViewResult(commitInstantTime1, commitInstantTime3, isPartitioned = true, isHiveStylePartitioned = false)
  }

  @Test def testMetadataBootstrapMORPartitionedInlineCompactionOn(): Unit = {
    val timestamp = Instant.now.toEpochMilli
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
    val commitInstantTime1 = runMetadataBootstrapAndVerifyCommit(
      DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL,
      commonOpts.updated(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "datestr"),
      classOf[SimpleKeyGenerator].getName)

    // Read bootstrapped table and verify count
    val hoodieROViewDF1 = spark.read.format("hudi")
                            .option(DataSourceReadOptions.QUERY_TYPE.key,
                              DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
                            .load(basePath + "/*")
    assertEquals(numRecords, hoodieROViewDF1.count())

    // Perform upsert
    val updateTimestamp = Instant.now.toEpochMilli
    val updateDF = TestBootstrap.generateTestRawTripDataset(updateTimestamp, 0, numRecordsUpdate, partitionPaths.asJava,
      jsc, spark.sqlContext)

    updateDF.write
      .format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "datestr")
      .option(HoodieCompactionConfig.INLINE_COMPACT.key, "true")
      .option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key, "1")
      .mode(SaveMode.Append)
      .save(basePath)

    // Expect 2 new commits since meta bootstrap - delta commit and compaction commit (due to inline compaction)
    assertEquals(2, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, commitInstantTime1).size())

    // Read table after upsert and verify count. Since we have inline compaction enabled the RO view will have
    // the updated rows.
    val hoodieROViewDF2 = spark.read.format("hudi")
                            .option(DataSourceReadOptions.QUERY_TYPE.key,
                              DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
                            .load(basePath + "/*")
    assertEquals(numRecords, hoodieROViewDF2.count())
    assertEquals(numRecordsUpdate, hoodieROViewDF2.filter(s"timestamp == $updateTimestamp").count())
    // Test query without "*" for MOR READ_OPTIMIZED
    val hoodieROViewDFWithBasePath = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key,
        DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
      .load(basePath)
    assertEquals(numRecords, hoodieROViewDFWithBasePath.count())
    assertEquals(numRecordsUpdate, hoodieROViewDFWithBasePath.filter(s"timestamp == $updateTimestamp").count())
  }

  @Test def testMetadataBootstrapMORPartitioned(): Unit = {
    val timestamp = Instant.now.toEpochMilli
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
    val commitInstantTime1 = runMetadataBootstrapAndVerifyCommit(
      DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL,
      commonOpts.updated(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "datestr"),
      classOf[SimpleKeyGenerator].getName)

    // Read bootstrapped table and verify count
    val hoodieROViewDF1 = spark.read.format("hudi")
                            .option(DataSourceReadOptions.QUERY_TYPE.key,
                              DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
                            .load(basePath + "/*")
    assertEquals(numRecords, hoodieROViewDF1.count())
    // Read bootstrapped table without "*"
    val hoodieROViewDFWithBasePath = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key,
        DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
      .load(basePath)
    assertEquals(numRecords, hoodieROViewDFWithBasePath.count())

    // Perform upsert based on the written bootstrap table
    val updateDf1 = hoodieROViewDF1.filter(col("_row_key") === verificationRowKey).withColumn(verificationCol, lit(updatedVerificationVal))
    updateDf1.write
      .format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "datestr")
      .mode(SaveMode.Append)
      .save(basePath)

    // Read table after upsert and verify the value
    assertEquals(1, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, commitInstantTime1).size())
    val hoodieROViewDF2 = spark.read.format("hudi")
                            .option(DataSourceReadOptions.QUERY_TYPE.key,
                              DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
                            .load(basePath + "/*")
    hoodieROViewDF2.collect()
    assertEquals(originalVerificationVal, hoodieROViewDF2.filter(col("_row_key") === verificationRowKey).select(verificationCol).first.getString(0))

    // Perform upsert based on the source data
    val updateTimestamp = Instant.now.toEpochMilli
    val updateDF2 = TestBootstrap.generateTestRawTripDataset(updateTimestamp, 0, numRecordsUpdate,
      partitionPaths.asJava, jsc, spark.sqlContext)

    updateDF2.write
      .format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "datestr")
      .mode(SaveMode.Append)
      .save(basePath)

    // Expect 2 new commit since meta bootstrap - 2 delta commits (because inline compaction is off)
    assertEquals(2, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, commitInstantTime1).size())

    // Read table after upsert and verify count. Since we have inline compaction off the RO view will have
    // no updated rows.
    val hoodieROViewDF3 = spark.read.format("hudi")
                            .option(DataSourceReadOptions.QUERY_TYPE.key,
                              DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
                            .load(basePath + "/*")
    assertEquals(numRecords, hoodieROViewDF3.count())
    assertEquals(0, hoodieROViewDF3.filter(s"timestamp == $updateTimestamp").count())
  }

  @Test def testFullBootstrapCOWPartitioned(): Unit = {
    val timestamp = Instant.now.toEpochMilli
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
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BOOTSTRAP_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "datestr")
      .option(HoodieBootstrapConfig.BASE_PATH.key, srcPath)
      .option(HoodieBootstrapConfig.KEYGEN_CLASS_NAME.key, classOf[SimpleKeyGenerator].getName)
      .option(HoodieBootstrapConfig.MODE_SELECTOR_CLASS_NAME.key, classOf[FullRecordBootstrapModeSelector].getName)
      .option(HoodieBootstrapConfig.FULL_BOOTSTRAP_INPUT_PROVIDER_CLASS_NAME.key, classOf[SparkParquetBootstrapDataProvider].getName)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val commitInstantTime1: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS, commitInstantTime1)

    // Read bootstrapped table and verify count
    val hoodieROViewDF1 = spark.read.format("hudi").load(basePath + "/*")
    assertEquals(numRecords, hoodieROViewDF1.count())

    val hoodieROViewDFWithBasePath = spark.read.format("hudi").load(basePath)
    assertEquals(numRecords, hoodieROViewDFWithBasePath.count())

    // Perform upsert
    val updateTimestamp = Instant.now.toEpochMilli
    val updateDF = TestBootstrap.generateTestRawTripDataset(updateTimestamp, 0, numRecordsUpdate, partitionPaths.asJava,
      jsc, spark.sqlContext)

    updateDF.write
      .format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "datestr")
      .mode(SaveMode.Append)
      .save(basePath)

    val commitInstantTime2: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(1, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, commitInstantTime1).size())

    // Read table after upsert and verify count
    val hoodieROViewDF2 = spark.read.format("hudi").load(basePath + "/*")
    assertEquals(numRecords, hoodieROViewDF2.count())
    assertEquals(numRecordsUpdate, hoodieROViewDF2.filter(s"timestamp == $updateTimestamp").count())

    verifyIncrementalViewResult(commitInstantTime1, commitInstantTime2, isPartitioned = true, isHiveStylePartitioned = false)
  }

  def runMetadataBootstrapAndVerifyCommit(tableType: String,
                                          extraOpts: Map[String, String] = Map.empty,
                                          bootstrapKeygenClass: String): String = {
    val bootstrapDF = spark.emptyDataFrame
    bootstrapDF.write
      .format("hudi")
      .options(extraOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BOOTSTRAP_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType)
      .option(HoodieBootstrapConfig.BASE_PATH.key, srcPath)
      .option(HoodieBootstrapConfig.KEYGEN_CLASS_NAME.key, bootstrapKeygenClass)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val commitInstantTime1: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    val expectedBootstrapInstant =
      if ("FULL_RECORD".equals(extraOpts.getOrElse(HoodieBootstrapConfig.PARTITION_SELECTOR_REGEX_MODE.key, HoodieBootstrapConfig.PARTITION_SELECTOR_REGEX_MODE.defaultValue)))
        HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS
      else HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS
    assertEquals(expectedBootstrapInstant, commitInstantTime1)
    commitInstantTime1
  }

  def verifyIncrementalViewResult(bootstrapCommitInstantTime: String, latestCommitInstantTime: String,
                                  isPartitioned: Boolean, isHiveStylePartitioned: Boolean): Unit = {
    // incrementally pull only changes in the bootstrap commit, which would pull all the initial records written
    // during bootstrap
    val hoodieIncViewDF1 = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, "000")
      .option(DataSourceReadOptions.END_INSTANTTIME.key, bootstrapCommitInstantTime)
      .load(basePath)

    assertEquals(numRecords, hoodieIncViewDF1.count())
    var countsPerCommit = hoodieIncViewDF1.groupBy("_hoodie_commit_time").count().collect()
    assertEquals(1, countsPerCommit.length)
    assertEquals(bootstrapCommitInstantTime, countsPerCommit(0).get(0))

    // incrementally pull only changes after bootstrap commit, which would pull only the updated records in the
    // later commits
    val hoodieIncViewDF2 = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, bootstrapCommitInstantTime)
      .load(basePath)

    assertEquals(numRecordsUpdate, hoodieIncViewDF2.count())
    countsPerCommit = hoodieIncViewDF2.groupBy("_hoodie_commit_time").count().collect()
    assertEquals(1, countsPerCommit.length)
    assertEquals(latestCommitInstantTime, countsPerCommit(0).get(0))

    if (isPartitioned) {
      val relativePartitionPath = if (isHiveStylePartitioned) "/datestr=2020-04-02/*" else "/2020-04-02/*"
      // pull the update commits within certain partitions
      val hoodieIncViewDF3 = spark.read.format("hudi")
        .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
        .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, bootstrapCommitInstantTime)
        .option(DataSourceReadOptions.INCR_PATH_GLOB.key, relativePartitionPath)
        .load(basePath)

      assertEquals(hoodieIncViewDF2.filter(col("_hoodie_partition_path").contains("2020-04-02")).count(),
        hoodieIncViewDF3.count())
    }
  }
}
