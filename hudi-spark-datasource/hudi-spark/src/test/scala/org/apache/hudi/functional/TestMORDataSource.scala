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

import org.apache.hudi.{ColumnStatsIndexSupport, DataSourceReadOptions, DataSourceUtils, DataSourceWriteOptions, DefaultSparkRecordMerger, HoodieDataSourceHelpers, ScalaAssertionSupport, SparkDatasetMixin}
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.HoodieConversionUtils.toJavaOption
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.common.config.{HoodieMemoryConfig, HoodieMetadataConfig, HoodieStorageConfig, RecordMergeMode}
import org.apache.hudi.common.config.TimestampKeyGeneratorConfig.{TIMESTAMP_INPUT_DATE_FORMAT, TIMESTAMP_OUTPUT_DATE_FORMAT, TIMESTAMP_TIMEZONE_FORMAT, TIMESTAMP_TYPE_FIELD}
import org.apache.hudi.common.model._
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.common.testutils.{HoodieTestDataGenerator, HoodieTestUtils}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings
import org.apache.hudi.common.util.Option
import org.apache.hudi.common.util.StringUtils.isNullOrEmpty
import org.apache.hudi.config.{HoodieCleanConfig, HoodieCompactionConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.exception.HoodieUpgradeDowngradeException
import org.apache.hudi.index.HoodieIndex.IndexType
import org.apache.hudi.metadata.HoodieTableMetadataUtil.{metadataPartitionExists, PARTITION_NAME_SECONDARY_INDEX_PREFIX}
import org.apache.hudi.storage.{StoragePath, StoragePathInfo}
import org.apache.hudi.table.action.compact.CompactionTriggerStrategy
import org.apache.hudi.table.upgrade.TestUpgradeDowngrade.getFixtureName
import org.apache.hudi.testutils.{DataSourceTestUtils, HoodieSparkClientTestBase}
import org.apache.hudi.util.JFunction

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Disabled, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, EnumSource, ValueSource}

import java.io.File
import java.nio.file.Files
import java.util.function.Consumer
import java.util.stream.Collectors

import scala.collection.JavaConverters._

/**
 * Tests on Spark DataSource for MOR table.
 */
class TestMORDataSource extends HoodieSparkClientTestBase with SparkDatasetMixin with ScalaAssertionSupport {

  var spark: SparkSession = null
  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
  )
  val sparkOpts = Map(
    HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key -> classOf[DefaultSparkRecordMerger].getName,
    HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key -> "parquet"
  )

  val verificationCol: String = "driver"
  val updatedVerificationVal: String = "driver_update"

  @BeforeEach override def setUp() {
    setTableName("hoodie_test")
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initHoodieStorage()
  }

  @AfterEach override def tearDown() = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
  }

  override def getSparkSessionExtensionsInjector: Option[Consumer[SparkSessionExtensions]] =
    toJavaOption(
      Some(
        JFunction.toJavaConsumer((receiver: SparkSessionExtensions) => new HoodieSparkSessionExtension().apply(receiver)))
    )

  @ParameterizedTest
  @CsvSource(Array(
    // Inferred as COMMIT_TIME_ORDERING
    "AVRO, AVRO, avro, false,,,CURRENT", "AVRO, SPARK, parquet, false,,,CURRENT",
    "SPARK, AVRO, parquet, false,,,CURRENT", "SPARK, SPARK, parquet, false,,,CURRENT",
    "AVRO, AVRO, avro, false,,,EIGHT", "AVRO, SPARK, parquet, false,,,EIGHT",
    "SPARK, AVRO, parquet, false,,,EIGHT", "SPARK, SPARK, parquet, false,,,EIGHT",
    // EVENT_TIME_ORDERING without precombine field
    "AVRO, AVRO, avro, false,,EVENT_TIME_ORDERING,CURRENT", "AVRO, SPARK, parquet, false,,EVENT_TIME_ORDERING,CURRENT",
    "SPARK, AVRO, parquet, false,,EVENT_TIME_ORDERING,CURRENT", "SPARK, SPARK, parquet, false,,EVENT_TIME_ORDERING,CURRENT",
    "AVRO, AVRO, avro, false,,EVENT_TIME_ORDERING,EIGHT", "AVRO, SPARK, parquet, false,,EVENT_TIME_ORDERING,EIGHT",
    "SPARK, AVRO, parquet, false,,EVENT_TIME_ORDERING,EIGHT", "SPARK, SPARK, parquet, false,,EVENT_TIME_ORDERING,EIGHT",
    // EVENT_TIME_ORDERING with empty precombine field
    "AVRO, AVRO, avro, true,,EVENT_TIME_ORDERING,CURRENT", "AVRO, SPARK, parquet, true,,EVENT_TIME_ORDERING,CURRENT",
    "SPARK, AVRO, parquet, true,,EVENT_TIME_ORDERING,CURRENT", "SPARK, SPARK, parquet, true,,EVENT_TIME_ORDERING,CURRENT",
    "AVRO, AVRO, avro, true,,EVENT_TIME_ORDERING,EIGHT", "AVRO, SPARK, parquet, true,,EVENT_TIME_ORDERING,EIGHT",
    "SPARK, AVRO, parquet, true,,EVENT_TIME_ORDERING,EIGHT", "SPARK, SPARK, parquet, true,,EVENT_TIME_ORDERING,EIGHT",
    // Inferred as EVENT_TIME_ORDERING
    "AVRO, AVRO, avro, true, timestamp,,CURRENT", "AVRO, SPARK, parquet, true, timestamp,,CURRENT",
    "SPARK, AVRO, parquet, true, timestamp,,CURRENT", "SPARK, SPARK, parquet, true, timestamp,,CURRENT",
    "AVRO, AVRO, avro, true, timestamp,,EIGHT", "AVRO, SPARK, parquet, true, timestamp,,EIGHT",
    "SPARK, AVRO, parquet, true, timestamp,,EIGHT", "SPARK, SPARK, parquet, true, timestamp,,EIGHT",
    // test table version 6
    "AVRO, AVRO, avro, true,timestamp,EVENT_TIME_ORDERING,SIX",
    "AVRO, AVRO, avro, true,timestamp,COMMIT_TIME_ORDERING,SIX"))
  def testCount(readType: HoodieRecordType,
                writeType: HoodieRecordType,
                logType: String,
                hasPreCombineField: Boolean,
                precombineField: String,
                recordMergeMode: String,
                tableVersionName: String): Unit = {
    val tableVersion = if (tableVersionName.equals("CURRENT")) {
      HoodieTableVersion.current()
    } else {
      HoodieTableVersion.valueOf(tableVersionName)
    }
    var (_, readOpts) = getWriterReaderOpts(readType)
    var (writeOpts, _) = getWriterReaderOpts(writeType)
    readOpts = readOpts ++ Map(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key -> logType)
    writeOpts = writeOpts ++ Map(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key -> logType)
    readOpts = readOpts - HoodieTableConfig.ORDERING_FIELDS.key
    if (!hasPreCombineField) {
      writeOpts = writeOpts - HoodieTableConfig.ORDERING_FIELDS.key
    } else {
      writeOpts = writeOpts ++ Map(HoodieTableConfig.ORDERING_FIELDS.key ->
        (if (isNullOrEmpty(precombineField)) "" else precombineField))
    }
    if (!isNullOrEmpty(recordMergeMode)) {
      writeOpts = writeOpts ++ Map(HoodieWriteConfig.RECORD_MERGE_MODE.key -> recordMergeMode)
    }
    if (isNullOrEmpty(recordMergeMode)) {
      assertFalse(writeOpts.contains(HoodieWriteConfig.RECORD_MERGE_MODE.key))
    }
    val firstWriteOpts = if (tableVersion == HoodieTableVersion.SIX) {
      writeOpts = writeOpts ++ Map(HoodieWriteConfig.WRITE_TABLE_VERSION.key() -> HoodieTableVersion.SIX.versionCode().toString)
      writeOpts = writeOpts - HoodieWriteConfig.RECORD_MERGE_MODE.key
      if (recordMergeMode.equals(RecordMergeMode.COMMIT_TIME_ORDERING.name)) {
        writeOpts = writeOpts ++ Map(DataSourceWriteOptions.PAYLOAD_CLASS_NAME.key() -> classOf[OverwriteWithLatestAvroPayload].getName)
      } else if (recordMergeMode.equals(RecordMergeMode.EVENT_TIME_ORDERING.name)) {
        writeOpts = writeOpts ++ Map(DataSourceWriteOptions.PAYLOAD_CLASS_NAME.key() -> classOf[DefaultHoodieRecordPayload].getName)
      }
      writeOpts
    } else if (tableVersion == HoodieTableVersion.EIGHT) {
      writeOpts = writeOpts ++ Map(
        HoodieWriteConfig.WRITE_TABLE_VERSION.key() -> HoodieTableVersion.EIGHT.versionCode().toString)
      writeOpts
    } else { // For Current version.
      writeOpts
    }

    // First Operation:
    // Producing parquet files to three default partitions.
    // SNAPSHOT view on MOR table with parquet files only.
    val records1 = recordsToStrings(dataGen.generateInserts("001", 100)).asScala.toSeq
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(firstWriteOpts)
      .option("hoodie.compact.inline", "false") // else fails due to compaction & deltacommit instant times being same
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    assertTrue(HoodieDataSourceHelpers.hasNewCommits(storage, basePath, "000"))
    val expectedMergeMode = if (isNullOrEmpty(recordMergeMode)) {
      if (hasPreCombineField) {
        RecordMergeMode.EVENT_TIME_ORDERING.name
      }
      else {
        RecordMergeMode.COMMIT_TIME_ORDERING.name
      }
    } else {
      recordMergeMode
    }
    val expectedConfigs = (
      if (tableVersion.versionCode() == HoodieTableVersion.current().versionCode()) {
        Map(HoodieTableConfig.RECORD_MERGE_MODE.key -> expectedMergeMode,
          HoodieTableConfig.VERSION.key -> HoodieTableVersion.current().versionCode().toString)
      } else if (tableVersion == HoodieTableVersion.EIGHT) {
        Map(HoodieTableConfig.RECORD_MERGE_MODE.key -> expectedMergeMode,
          HoodieTableConfig.VERSION.key -> HoodieTableVersion.EIGHT.versionCode().toString)
      } else {
        Map(HoodieTableConfig.VERSION.key -> HoodieTableVersion.SIX.versionCode().toString)
      } ++
      (if (hasPreCombineField && !isNullOrEmpty(precombineField)) {
        Map(HoodieTableConfig.ORDERING_FIELDS.key -> precombineField)
      } else {
        Map()
      })).asJava
    val nonExistentConfigs: java.util.List[String] = (if (hasPreCombineField) {
      Seq[String]()
    } else {
      Seq(HoodieTableConfig.ORDERING_FIELDS.key)
    }).asJava
    HoodieTestUtils.validateTableConfig(storage, basePath, expectedConfigs, nonExistentConfigs)
    val commit1CompletionTime = if (tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)) {
      DataSourceTestUtils.latestCommitCompletionTime(storage, basePath)
    } else {
      DataSourceTestUtils.latestCommitRequestTime(storage, basePath)
    }
    val hudiSnapshotDF1 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath)

    assertEquals(100, hudiSnapshotDF1.count()) // still 100, since we only updated

    // Second Operation:
    // Upsert the update to the default partitions with duplicate records. Produced a log file for each parquet.
    // SNAPSHOT view should read the log files only with the latest commit time.
    val records2 = recordsToStrings(dataGen.generateUniqueUpdates("002", 100)).asScala.toSeq
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    HoodieTestUtils.validateTableConfig(storage, basePath, expectedConfigs, nonExistentConfigs)
    val commit2CompletionTime = if (tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)) {
      DataSourceTestUtils.latestCommitCompletionTime(storage, basePath)
    } else {
      DataSourceTestUtils.latestCommitRequestTime(storage, basePath)
    }
    val hudiSnapshotDF2 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath)
    assertEquals(100, hudiSnapshotDF2.count()) // still 100, since we only updated
    val commit1Time = hudiSnapshotDF1.select("_hoodie_commit_time").head().get(0).toString
    val commit2Time = hudiSnapshotDF2.select("_hoodie_commit_time").head().get(0).toString
    assertEquals(hudiSnapshotDF2.select("_hoodie_commit_time").distinct().count(), 1)
    assertTrue(commit2Time > commit1Time)
    assertEquals(100, hudiSnapshotDF2.join(hudiSnapshotDF1, Seq("_hoodie_record_key"), "left").count())

    // incremental view
    // validate incremental queries only for table version 8
    // 1.0 reader (table version 8) supports incremental query reads using completion time
    if (tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)) {
      // base file only
      val hudiIncDF1 = spark.read.format("org.apache.hudi")
        .options(readOpts)
        .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
        .option(DataSourceReadOptions.START_COMMIT.key, commit1CompletionTime)
        .option(DataSourceReadOptions.END_COMMIT.key, commit1CompletionTime)
        .load(basePath)
      assertEquals(100, hudiIncDF1.count())
      assertEquals(1, hudiIncDF1.select("_hoodie_commit_time").distinct().count())
      assertEquals(commit1Time, hudiIncDF1.select("_hoodie_commit_time").head().get(0).toString)
      hudiIncDF1.show(1)

      // log file only
      val hudiIncDF2 = spark.read.format("org.apache.hudi")
        .options(readOpts)
        .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
        .option(DataSourceReadOptions.START_COMMIT.key, commit2CompletionTime)
        .option(DataSourceReadOptions.END_COMMIT.key, commit2CompletionTime)
        .load(basePath)
      assertEquals(100, hudiIncDF2.count())
      assertEquals(1, hudiIncDF2.select("_hoodie_commit_time").distinct().count())
      assertEquals(commit2Time, hudiIncDF2.select("_hoodie_commit_time").head().get(0).toString)
      hudiIncDF2.show(1)

      // base file + log file
      val hudiIncDF3 = spark.read.format("org.apache.hudi")
        .options(readOpts)
        .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
        .option(DataSourceReadOptions.START_COMMIT.key, commit1CompletionTime)
        .option(DataSourceReadOptions.END_COMMIT.key, commit2CompletionTime)
        .load(basePath)
      assertEquals(100, hudiIncDF3.count())
      // log file being load
      assertEquals(1, hudiIncDF3.select("_hoodie_commit_time").distinct().count())
      assertEquals(commit2Time, hudiIncDF3.select("_hoodie_commit_time").head().get(0).toString)

      // Test incremental query has no instant in range
      val emptyIncDF = spark.read.format("org.apache.hudi")
        .options(readOpts)
        .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
        .option(DataSourceReadOptions.START_COMMIT.key, "000")
        .option(DataSourceReadOptions.END_COMMIT.key, "001")
        .load(basePath)
      assertEquals(0, emptyIncDF.count())
    }

    // Unmerge
    val hudiSnapshotSkipMergeDF2 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .option(DataSourceReadOptions.REALTIME_MERGE.key, DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL)
      .load(basePath)
    assertEquals(200, hudiSnapshotSkipMergeDF2.count())
    assertEquals(100, hudiSnapshotSkipMergeDF2.select("_hoodie_record_key").distinct().count())
    assertEquals(200, hudiSnapshotSkipMergeDF2.join(hudiSnapshotDF2, Seq("_hoodie_record_key"), "left").count())

    // Test Read Optimized Query on MOR table
    val hudiRODF2 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
      .load(basePath)
    assertEquals(100, hudiRODF2.count())

    // Third Operation:
    // Upsert another update to the default partitions with 50 duplicate records. Produced the second log file for each parquet.
    // SNAPSHOT view should read the latest log files.
    val records3 = recordsToStrings(dataGen.generateUniqueUpdates("003", 50)).asScala.toSeq
    val inputDF3: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records3, 2))
    inputDF3.write.format("org.apache.hudi")
      .options(writeOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    HoodieTestUtils.validateTableConfig(storage, basePath, expectedConfigs, nonExistentConfigs)
    val commit3CompletionTime = if (tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)) {
      DataSourceTestUtils.latestCommitCompletionTime(storage, basePath)
    } else {
      DataSourceTestUtils.latestCommitRequestTime(storage, basePath)
    }
    val hudiSnapshotDF3 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath)
    // still 100, because we only updated the existing records
    assertEquals(100, hudiSnapshotDF3.count())

    // 50 from commit2, 50 from commit3
    assertEquals(hudiSnapshotDF3.select("_hoodie_commit_time").distinct().count(), 2)
    assertEquals(50, hudiSnapshotDF3.filter(col("_hoodie_commit_time") > commit2Time).count())
    assertEquals(50,
      hudiSnapshotDF3.join(hudiSnapshotDF2, Seq("_hoodie_record_key", "_hoodie_commit_time"), "inner").count())

    // incremental query from commit2Time
    // validate incremental queries only for table version 8
    // 1.0 reader (table version 8) supports incremental query reads using completion time
    if (tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)) {
      val hudiIncDF4 = spark.read.format("org.apache.hudi")
        .options(readOpts)
        .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
        .option(DataSourceReadOptions.START_COMMIT.key, commit3CompletionTime)
        .load(basePath)
      assertEquals(50, hudiIncDF4.count())

      // skip merge incremental view
      // including commit 2 and commit 3
      val hudiIncDF4SkipMerge = spark.read.format("org.apache.hudi")
        .options(readOpts)
        .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
        .option(DataSourceReadOptions.START_COMMIT.key, commit1CompletionTime)
        .option(DataSourceReadOptions.REALTIME_MERGE.key, DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL)
        .load(basePath)
      assertEquals(250, hudiIncDF4SkipMerge.count())
    }

    // Fourth Operation:
    // Insert records to a new partition. Produced a new parquet file.
    // SNAPSHOT view should read the latest log files from the default partition and parquet from the new partition.
    val partitionPaths = new Array[String](1)
    partitionPaths.update(0, "2020/01/10")
    val newDataGen = new HoodieTestDataGenerator(partitionPaths)
    val records4 = recordsToStrings(newDataGen.generateInserts("004", 100)).asScala.toSeq
    val inputDF4: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records4, 2))
    inputDF4.write.format("org.apache.hudi")
      .options(writeOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    HoodieTestUtils.validateTableConfig(storage, basePath, expectedConfigs, nonExistentConfigs)
    val hudiSnapshotDF4 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath)
    // 200, because we insert 100 records to a new partition
    assertEquals(200, hudiSnapshotDF4.count())
    assertEquals(100,
      hudiSnapshotDF1.join(hudiSnapshotDF4, Seq("_hoodie_record_key"), "inner").count())

    // Incremental query, 50 from log file, 100 from base file of the new partition.
    // validate incremental queries only for table version 8
    // 1.0 reader (table version 8) supports incremental query reads using completion time
    if (tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)) {
      val hudiIncDF5 = spark.read.format("org.apache.hudi")
        .options(readOpts)
        .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
        .option(DataSourceReadOptions.START_COMMIT.key, commit3CompletionTime)
        .load(basePath)
      assertEquals(150, hudiIncDF5.count())
    }

    // Fifth Operation:
    // Upsert records to the new partition. Produced a newer version of parquet file.
    // SNAPSHOT view should read the latest log files from the default partition
    // and the latest parquet from the new partition.
    val records5 = recordsToStrings(newDataGen.generateUniqueUpdates("005", 50)).asScala.toSeq
    val inputDF5: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records5, 2))
    inputDF5.write.format("org.apache.hudi")
      .options(writeOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    HoodieTestUtils.validateTableConfig(storage, basePath, expectedConfigs, nonExistentConfigs)
    val hudiSnapshotDF5 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath)
    assertEquals(200, hudiSnapshotDF5.count())

    // Sixth Operation:
    // Insert 2 records and trigger compaction.
    val records6 = recordsToStrings(newDataGen.generateInserts("006", 2)).asScala.toSeq
    val inputDF6: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records6, 2))
    inputDF6.write.format("org.apache.hudi")
      .options(writeOpts)
      .option("hoodie.compact.inline", "true")
      .mode(SaveMode.Append)
      .save(basePath)
    HoodieTestUtils.validateTableConfig(storage, basePath, expectedConfigs, nonExistentConfigs)
    val commit6CompletionTime = if (tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)) {
      DataSourceTestUtils.latestCommitCompletionTime(storage, basePath)
    } else {
      DataSourceTestUtils.latestCommitRequestTime(storage, basePath)
    }
    val hudiSnapshotDF6 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/2020/01/10/*")
    assertEquals(102, hudiSnapshotDF6.count())
    // validate incremental queries only for table version 8
    // 1.0 reader (table version 8) supports incremental query reads using completion time
    if (tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)) {
      val hudiIncDF6 = spark.read.format("org.apache.hudi")
        .options(readOpts)
        .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
        .option(DataSourceReadOptions.START_COMMIT.key, commit6CompletionTime)
        .option(DataSourceReadOptions.END_COMMIT.key, commit6CompletionTime)
        .load(basePath)
      // even though compaction updated 150 rows, since preserve commit metadata is true, they won't be part of incremental query.
      // inserted 2 new row
      assertEquals(2, hudiIncDF6.count())
    }
  }

  @Test
  def testSpill() {
    val (writeOpts, readOpts) = getWriterReaderOpts(HoodieRecordType.SPARK)

    val records1 = recordsToStrings(dataGen.generateInserts("001", 100)).asScala.toSeq
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option("hoodie.compact.inline", "false") // else fails due to compaction & deltacommit instant times being same
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val records2 = recordsToStrings(dataGen.generateUniqueUpdates("002", 100)).asScala.toSeq
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .mode(SaveMode.Append)
      .save(basePath)

    // Make force spill
    spark.sparkContext.hadoopConfiguration.set(
      HoodieMemoryConfig.MAX_MEMORY_FRACTION_FOR_COMPACTION.key, "0.00001")
    val hudiSnapshotDF1 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath)
    assertEquals(100, hudiSnapshotDF1.count()) // still 100, since we only updated
    spark.sparkContext.hadoopConfiguration.set(
      HoodieMemoryConfig.MAX_MEMORY_FRACTION_FOR_COMPACTION.key,
      HoodieMemoryConfig.DEFAULT_MR_COMPACTION_MEMORY_FRACTION)
  }

  @ParameterizedTest
  @CsvSource(value = Array("AVRO,6", "AVRO,8", "SPARK,6", "SPARK,8"))
  def testPayloadDelete(recordType: HoodieRecordType, tableVersion: Int) {
    var (writeOpts, readOpts) = getWriterReaderOpts(recordType)
    writeOpts = writeOpts + (HoodieTableConfig.VERSION.key() -> tableVersion.toString,
      HoodieWriteConfig.WRITE_TABLE_VERSION.key() -> tableVersion.toString)

    // First Operation:
    // Producing parquet files to three default partitions.
    // SNAPSHOT view on MOR table with parquet files only.
    val records1 = recordsToStrings(dataGen.generateInserts("001", 100)).asScala.toSeq
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option("hoodie.compact.inline", "false") // else fails due to compaction & deltacommit instant times being same
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    assertTrue(HoodieDataSourceHelpers.hasNewCommits(storage, basePath, "000"))
    val hudiSnapshotDF1 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath)
    assertEquals(100, hudiSnapshotDF1.count()) // still 100, since we only updated

    // Second Operation:
    // Upsert 50 delete records
    // Snopshot view should only read 50 records
    val records2 = recordsToStrings(dataGen.generateUniqueDeleteRecords("002", 50)).asScala.toSeq
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val commit2CompletionTime = DataSourceTestUtils.latestCommitCompletionTime(storage, basePath)
    val hudiSnapshotDF2 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath)
    assertEquals(50, hudiSnapshotDF2.count()) // 50 records were deleted
    assertEquals(hudiSnapshotDF2.select("_hoodie_commit_time").distinct().count(), 1)
    val commit1Time = hudiSnapshotDF1.select("_hoodie_commit_time").head().get(0).toString
    val commit2Time = hudiSnapshotDF2.select("_hoodie_commit_time").head().get(0).toString
    assertTrue(commit1Time.equals(commit2Time))
    assertEquals(50, hudiSnapshotDF2.join(hudiSnapshotDF1, Seq("_hoodie_record_key"), "left").count())

    // unmerge query, skip the delete records
    val hudiSnapshotDF2Unmerge = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .option(DataSourceReadOptions.REALTIME_MERGE.key, DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL)
      .load(basePath)
    assertEquals(100, hudiSnapshotDF2Unmerge.count())

    // incremental query, read 50 delete records from log file and get 0 count.
    val hudiIncDF1 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commit2CompletionTime)
      .load(basePath)
    assertEquals(0, hudiIncDF1.count())

    // Third Operation:
    // Upsert 50 delete records to delete the reset
    // Snopshot view should read 0 record
    val records3 = recordsToStrings(dataGen.generateUniqueDeleteRecords("003", 50)).asScala.toSeq
    val inputDF3: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records3, 2))
    inputDF3.write.format("org.apache.hudi")
      .options(writeOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val hudiSnapshotDF3 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath)
    assertEquals(0, hudiSnapshotDF3.count()) // 100 records were deleted, 0 record to load
  }

  @ParameterizedTest
  @CsvSource(value = Array("AVRO,6", "AVRO,8", "SPARK,6", "SPARK,8"))
  def testPrunedFiltered(recordType: HoodieRecordType, tableVersion: Int) {
    var (writeOpts, readOpts) = getWriterReaderOpts(recordType)
    writeOpts = writeOpts + (HoodieTableConfig.VERSION.key() -> tableVersion.toString,
      HoodieWriteConfig.WRITE_TABLE_VERSION.key() -> tableVersion.toString)

    // First Operation:
    // Producing parquet files to three default partitions.
    // SNAPSHOT view on MOR table with parquet files only.

    // Overriding the partition-path field
    val opts = writeOpts + (DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition_path")

    val hoodieRecords1 = dataGen.generateInserts("001", 100)

    val inputDF1 = toDataset(spark, hoodieRecords1)
    inputDF1.write.format("org.apache.hudi")
      .options(opts)
      .option("hoodie.compact.inline", "false") // else fails due to compaction & deltacommit instant times being same
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.PAYLOAD_CLASS_NAME.key, classOf[DefaultHoodieRecordPayload].getName)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val hudiSnapshotDF1 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath)
    val commit1Time = hudiSnapshotDF1.select("_hoodie_commit_time").head().get(0).toString

    assertEquals(100, hudiSnapshotDF1.count())
    // select nested columns with order different from the actual schema
    assertEquals("amount,currency,tip_history,_hoodie_commit_seqno",
      hudiSnapshotDF1
        .select("fare.amount", "fare.currency", "tip_history", "_hoodie_commit_seqno")
        .orderBy(desc("_hoodie_commit_seqno"))
        .columns.mkString(","))

    // Second Operation:
    // Upsert 50 update records
    // Snopshot view should read 100 records
    val records2 = dataGen.generateUniqueUpdates("002", 50)
    val inputDF2 = toDataset(spark, records2)
    inputDF2.write.format("org.apache.hudi")
      .options(opts)
      .mode(SaveMode.Append)
      .save(basePath)
    val commit2CompletionTime = DataSourceTestUtils.latestCommitCompletionTime(storage, basePath)
    val hudiSnapshotDF2 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath)
    val hudiIncDF1 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, "000")
      .load(basePath)
    val hudiIncDF1Skipmerge = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.REALTIME_MERGE.key, DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, "000")
      .load(basePath)
    val hudiIncDF2 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, if (tableVersion == 6) commit1Time else commit2CompletionTime)
      .load(basePath)

    // filter first commit and only read log records
    assertEquals(50,  hudiSnapshotDF2.select("_hoodie_commit_seqno", "fare.amount", "fare.currency", "tip_history")
      .filter(col("_hoodie_commit_time") > commit1Time).count())
    assertEquals(50,  hudiIncDF1.select("_hoodie_commit_seqno", "fare.amount", "fare.currency", "tip_history")
      .filter(col("_hoodie_commit_time") > commit1Time).count())
    assertEquals(50,  hudiIncDF2
      .select("_hoodie_commit_seqno", "fare.amount", "fare.currency", "tip_history").count())
    assertEquals(150,  hudiIncDF1Skipmerge
      .select("_hoodie_commit_seqno", "fare.amount", "fare.currency", "tip_history").count())

    // select nested columns with order different from the actual schema
    verifySchemaAndTypes(hudiSnapshotDF1)
    verifySchemaAndTypes(hudiSnapshotDF2)
    verifySchemaAndTypes(hudiIncDF1)
    verifySchemaAndTypes(hudiIncDF2)
    verifySchemaAndTypes(hudiIncDF1Skipmerge)

    // make sure show() work
    verifyShow(hudiSnapshotDF1)
    verifyShow(hudiSnapshotDF2)
    verifyShow(hudiIncDF1)
    verifyShow(hudiIncDF2)
    verifyShow(hudiIncDF1Skipmerge)

    val record3 = dataGen.generateUpdatesWithTimestamp("003", hoodieRecords1, -1)
    val inputDF3 = toDataset(spark, record3)
    inputDF3.write.format("org.apache.hudi").options(opts)
      .mode(SaveMode.Append).save(basePath)

    val hudiSnapshotDF3 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath)

    verifyShow(hudiSnapshotDF3);

    assertEquals(100, hudiSnapshotDF3.count())
    assertEquals(0, hudiSnapshotDF3.filter("rider = 'rider-003'").count())
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testVectorizedReader(recordType: HoodieRecordType) {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

    spark.conf.set("spark.sql.parquet.enableVectorizedReader", true)
    assertTrue(spark.conf.get("spark.sql.parquet.enableVectorizedReader").toBoolean)
    // Vectorized Reader will only be triggered with AtomicType schema,
    // which is not null, UDTs, arrays, structs, and maps.
    val schema = HoodieTestDataGenerator.SHORT_TRIP_SCHEMA
    val records1 = recordsToStrings(dataGen.generateInsertsAsPerSchema("001", 100, schema)).asScala.toSeq
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option("hoodie.compact.inline", "false") // else fails due to compaction & deltacommit instant times being same
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val hudiSnapshotDF1 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath)
    assertEquals(100, hudiSnapshotDF1.count())

    val records2 = recordsToStrings(dataGen.generateUniqueUpdatesAsPerSchema("002", 50, schema))
      .asScala.toSeq
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val hudiSnapshotDF2 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath)
    assertEquals(100, hudiSnapshotDF2.count())

    // loading correct type
    val sampleRow = hudiSnapshotDF2
      .select("fare", "driver", "_hoodie_is_deleted")
      .head()
    assertEquals(sampleRow.getDouble(0), sampleRow.get(0))
    assertEquals(sampleRow.getString(1), sampleRow.get(1))
    assertEquals(sampleRow.getBoolean(2), sampleRow.get(2))

    // test show()
    hudiSnapshotDF1.show(1)
    hudiSnapshotDF2.show(1)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testPreCombineFieldForReadMOR(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

    writeData((1, "a0", 10, 100, false), writeOpts)
    checkAnswer((1, "a0", 10, 100, false), readOpts)

    writeData((1, "a0", 12, 99, false), writeOpts)
    // The value has not update, because the version 99 < 100
    checkAnswer((1, "a0", 10, 100, false), readOpts)

    writeData((1, "a0", 12, 101, false), writeOpts)
    // The value has update
    checkAnswer((1, "a0", 12, 101, false), readOpts)

    writeData((1, "a0", 14, 98, false), writeOpts)
    // Latest value should be ignored if preCombine honors ordering
    checkAnswer((1, "a0", 12, 101, false), readOpts)

    writeData((1, "a0", 16, 97, true), writeOpts)
    // Ordering value will be honored, the delete record is considered as obsolete
    // because it has smaller version number (97 < 101)
    checkAnswer((1, "a0", 12, 101, false), readOpts)

    writeData((1, "a0", 18, 96, false), writeOpts)
    // Ordering value will be honored, the data record is considered as obsolete
    // because it has smaller version number (96 < 101)
    checkAnswer((1, "a0", 12, 101, false), readOpts)
  }

  private def writeData(data: (Int, String, Int, Int, Boolean), opts: Map[String, String]): Unit = {
    val _spark = spark
    import _spark.implicits._
    val df = Seq(data).toDF("id", "name", "value", "version", "_hoodie_is_deleted")
    df.write.format("org.apache.hudi")
      .options(opts)
      // use DefaultHoodieRecordPayload here
      .option(PAYLOAD_CLASS_NAME.key, classOf[DefaultHoodieRecordPayload].getCanonicalName)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .option(RECORDKEY_FIELD.key, "id")
      .option(HoodieTableConfig.ORDERING_FIELDS.key, "version")
      .option(PARTITIONPATH_FIELD.key, "")
      .mode(SaveMode.Append)
      .save(basePath)
  }

  private def checkAnswer(expect: (Int, String, Int, Int, Boolean), opts: Map[String, String]): Unit = {
    val readDf = spark.read.format("org.apache.hudi")
      .options(opts)
      .load(basePath)
    if (expect._5) {
      assertTrue(readDf.isEmpty, "Found df " + readDf.collectAsList().get(0).mkString(","))
    } else {
      val row1 = readDf.select("id", "name", "value", "version", "_hoodie_is_deleted").take(1)(0)
      assertEquals(Row(expect.productIterator.toSeq: _*), row1)
    }
  }

  def verifySchemaAndTypes(df: DataFrame): Unit = {
    assertEquals("amount,currency,tip_history,_hoodie_commit_seqno",
      df.select("fare.amount", "fare.currency", "tip_history", "_hoodie_commit_seqno")
        .orderBy(desc("_hoodie_commit_seqno"))
        .columns.mkString(","))
    val sampleRow = df
      .select("begin_lat", "current_date", "fare.currency", "tip_history", "nation")
      .orderBy(desc("_hoodie_commit_time"))
      .head()
    assertEquals(sampleRow.getDouble(0), sampleRow.get(0))
    assertEquals(sampleRow.getDate(1), sampleRow.get(1))
    assertEquals(sampleRow.getString(2), sampleRow.get(2))
    assertEquals(sampleRow.getSeq(3), sampleRow.get(3))
    assertEquals(sampleRow.getAs[Array[Byte]](4), sampleRow.get(4))
  }

  def verifyShow(df: DataFrame): Unit = {
    df.show(1)
    df.select("_hoodie_commit_seqno", "fare.amount", "fare.currency", "tip_history").show(1)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableVersion], names = Array("SIX", "EIGHT"))
  def testIncrementalQueryMORWithCompactionAndClean(tableVersion: HoodieTableVersion): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(HoodieRecordType.AVRO)
    initMetaClient(HoodieTableType.MERGE_ON_READ)
    // delta commit1
    // delta commit2
    // delta commit3
    // commit <--compaction
    // delta commit4
    // delta commit5
    // clean
    for (i <- 1 to 5) {
      val records = recordsToStrings(dataGen.generateInserts("%05d".format(i), 10)).asScala.toList
      val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))

      inputDF.write.format("hudi")
        .options(writeOpts)
        .option(HoodieCompactionConfig.INLINE_COMPACT.key(), "true")
        .option(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), tableVersion.versionCode().toString)
        .option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "3")
        .option(HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key(), "2")
        .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
        .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
        // Use InMemoryIndex to generate log only mor table.
        .option(HoodieIndexConfig.INDEX_TYPE.key, IndexType.INMEMORY.toString)
        .mode(if (i == 1) SaveMode.Overwrite else SaveMode.Append)
        .save(basePath)
    }

    // specify begin time as 000 for the incremental query, and the query will fallback to full table scan.
    val rowCount1 = spark.read.format("hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN.key(), "true")
      .option(DataSourceReadOptions.START_COMMIT.key(), "000")
      .load(basePath)
      .count()
    assertEquals(50, rowCount1)
  }

  @ParameterizedTest
  @CsvSource(Array(
    "true,false,AVRO", "true,true,AVRO", "false,true,AVRO", "false,false,AVRO",
    "true,false,SPARK", "true,true,SPARK", "false,true,SPARK", "false,false,SPARK"
  ))
  def testQueryMORWithBasePathAndFileIndex(partitionEncode: Boolean, isMetadataEnabled: Boolean, recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

    val N = 20
    // Test query with partition prune if URL_ENCODE_PARTITIONING has enable
    val records1 = dataGen.generateInsertsContainsAllPartitions("000", N)
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records1).asScala.toSeq, 2))
    inputDF1.write.format("hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.URL_ENCODE_PARTITIONING.key, partitionEncode)
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val commitInstantTime1 = HoodieDataSourceHelpers.latestCommit(storage, basePath)
    val commitCompletionTime1 = DataSourceTestUtils.latestCommitCompletionTime(storage, basePath)

    val countIn20160315 = records1.asScala.count(record => record.getPartitionPath == "2016/03/15")
    // query the partition by filter
    val count1 = spark.read.format("hudi")
      .options(readOpts)
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .load(basePath)
      .filter("partition = '2016/03/15'")
      .count()
    assertEquals(countIn20160315, count1)

    // query the partition by path
    val partitionPath = if (partitionEncode) "2016%2F03%2F15" else "2016/03/15"
    val count2 = spark.read.format("hudi")
      .options(readOpts)
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .load(basePath + s"/$partitionPath")
      .count()
    assertEquals(countIn20160315, count2)

    // Second write with Append mode
    val records2 = dataGen.generateInsertsContainsAllPartitions("000", N + 1)
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records2).asScala.toSeq, 2))
    inputDF2.write.format("hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.URL_ENCODE_PARTITIONING.key, partitionEncode)
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .mode(SaveMode.Append)
      .save(basePath)
    val commitCompletionTime2 = DataSourceTestUtils.latestCommitCompletionTime(storage, basePath)
    // Incremental query without "*" in path
    val hoodieIncViewDF1 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commitCompletionTime2)
      .load(basePath)
    assertEquals(N + 1, hoodieIncViewDF1.count())
  }

  @ParameterizedTest
  @CsvSource(Array(
    "true, false, AVRO", "false, true, AVRO", "false, false, AVRO", "true, true, AVRO",
    "true, false, SPARK", "false, true, SPARK", "false, false, SPARK", "true, true, SPARK"
  ))
  def testMORPartitionPrune(partitionEncode: Boolean, hiveStylePartition: Boolean, recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

    val partitions = Array("2021/03/01", "2021/03/02", "2021/03/03", "2021/03/04", "2021/03/05")
    val newDataGen =  new HoodieTestDataGenerator(partitions)
    val records1 = newDataGen.generateInsertsContainsAllPartitions("000", 100).asScala
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records1.asJava).asScala.toSeq, 2))

    val partitionCounts = partitions.map(p => p -> records1.count(r => r.getPartitionPath == p)).toMap

    inputDF1.write.format("hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.URL_ENCODE_PARTITIONING.key, partitionEncode)
      .option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key, hiveStylePartition)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val count1 = spark.read.format("hudi")
      .options(readOpts)
      .load(basePath)
      .filter("partition = '2021/03/01'")
      .count()
    assertEquals(partitionCounts("2021/03/01"), count1)

    val count2 = spark.read.format("hudi")
      .options(readOpts)
      .load(basePath)
      .filter("partition > '2021/03/01' and partition < '2021/03/03'")
      .count()
    assertEquals(partitionCounts("2021/03/02"), count2)

    val count3 = spark.read.format("hudi")
      .options(readOpts)
      .load(basePath)
      .filter("partition != '2021/03/01'")
      .count()
    assertEquals(records1.size - partitionCounts("2021/03/01"), count3)

    val count4 = spark.read.format("hudi")
      .options(readOpts)
      .load(basePath)
      .filter("partition like '2021/03/03%'")
      .count()
    assertEquals(partitionCounts("2021/03/03"), count4)

    val count5 = spark.read.format("hudi")
      .options(readOpts)
      .load(basePath)
      .filter("partition like '%2021/03/%'")
      .count()
    assertEquals(records1.size, count5)

    val count6 = spark.read.format("hudi")
      .options(readOpts)
      .load(basePath)
      .filter("partition = '2021/03/01' or partition = '2021/03/05'")
      .count()
    assertEquals(partitionCounts("2021/03/01") + partitionCounts("2021/03/05"), count6)

    val count7 = spark.read.format("hudi")
      .options(readOpts)
      .load(basePath)
      .filter("substr(partition, 9, 10) = '03'")
      .count()

    assertEquals(partitionCounts("2021/03/03"), count7)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testReadPathsForMergeOnReadTable(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

    // Paths only baseFiles
    val records1 = dataGen.generateInserts("001", 100)
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records1).asScala.toSeq, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option("hoodie.compact.inline", "false") // else fails due to compaction & deltacommit instant times being same
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    assertTrue(HoodieDataSourceHelpers.hasNewCommits(storage, basePath, "000"))
    val baseFilePath = storage.listDirectEntries(new StoragePath(basePath, dataGen.getPartitionPaths.head))
      .asScala
      .filter(_.getPath.getName.endsWith("parquet"))
      .map(_.getPath.toString)
      .mkString(",")
    val records2 = dataGen.generateUniqueDeleteRecords("002", 100)
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records2).asScala.toSeq, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val hudiReadPathDF1 = spark.read.options(readOpts).format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .option(DataSourceReadOptions.READ_PATHS.key, baseFilePath)
      .load()

    val expectedCount1 = records1.asScala.count(record => record.getPartitionPath == dataGen.getPartitionPaths.head)
    assertEquals(expectedCount1, hudiReadPathDF1.count())

    // Paths Contains both baseFile and log files
    val logFilePath = storage.listDirectEntries(new StoragePath(basePath, dataGen.getPartitionPaths.head))
      .asScala
      .filter(_.getPath.getName.contains("log"))
      .map(_.getPath.toString)
      .mkString(",")

    val readPaths = baseFilePath + "," + logFilePath
    val hudiReadPathDF2 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .option(DataSourceReadOptions.READ_PATHS.key, readPaths)
      .load()

    assertEquals(0, hudiReadPathDF2.count())
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testReadPathsForOnlyLogFiles(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)
    // enable column stats
    val hudiOpts = writeOpts ++ Map(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true",
      HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key() -> "fare,city_to_state,rider")

    initMetaClient(HoodieTableType.MERGE_ON_READ)
    val records1 = dataGen.generateInsertsContainsAllPartitions("000", 20)
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records1).asScala.toSeq, 2))
    inputDF1.write.format("hudi")
      .options(hudiOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      // Use InMemoryIndex to generate log only mor table.
      .option(HoodieIndexConfig.INDEX_TYPE.key, IndexType.INMEMORY.toString)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    // There should no base file in the file list.
    assertTrue(DataSourceTestUtils.isLogFileOnly(basePath))

    val logFilePath = storage.listDirectEntries(new StoragePath(basePath, dataGen.getPartitionPaths.head))
      .asScala
      .filter(_.getPath.getName.contains("log"))
      .map(_.getPath.toString)
      .mkString(",")

    val records2 = dataGen.generateInsertsContainsAllPartitions("000", 20)
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records2).asScala.toSeq, 2))
    inputDF2.write.format("hudi")
      .options(hudiOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      // Use InMemoryIndex to generate log only mor table.
      .option(HoodieIndexConfig.INDEX_TYPE.key, IndexType.INMEMORY.toString)
      .mode(SaveMode.Append)
      .save(basePath)
    // There should no base file in the file list.
    assertTrue(DataSourceTestUtils.isLogFileOnly(basePath))

    val expectedCount1 = records1.asScala.count(record => record.getPartitionPath == dataGen.getPartitionPaths.head)

    val hudiReadPathDF = spark.read.options(readOpts).format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .option(DataSourceReadOptions.READ_PATHS.key, logFilePath)
      .load()

    assertEquals(expectedCount1, hudiReadPathDF.count())

    if (recordType == HoodieRecordType.SPARK) {
      metaClient = HoodieTableMetaClient.reload(metaClient)
      val metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).withMetadataIndexColumnStats(true).build()
      val columnStatsIndex = new ColumnStatsIndexSupport(spark, inputDF1.schema, metadataConfig, metaClient)
      columnStatsIndex.loadTransposed(Seq("fare","city_to_state", "rider"), shouldReadInMemory = true) { emptyTransposedColStatsDF =>
        assertTrue(!emptyTransposedColStatsDF.columns.contains("fare"))
        assertTrue(!emptyTransposedColStatsDF.columns.contains("city_to_state"))
        // rider is a simple string field, so it should have a min/max value as well as nullCount
        assertTrue(emptyTransposedColStatsDF.filter("rider_minValue IS NOT NULL").count() > 0)
        assertTrue(emptyTransposedColStatsDF.filter("rider_maxValue IS NOT NULL").count() > 0)
        assertTrue(emptyTransposedColStatsDF.filter("rider_nullCount IS NOT NULL").count() > 0)
      }
    }
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testReadLogOnlyMergeOnReadTable(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

    initMetaClient(HoodieTableType.MERGE_ON_READ)
    val records1 = dataGen.generateInsertsContainsAllPartitions("000", 20)
    val inputDF = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records1).asScala.toSeq, 2))
    inputDF.write.format("hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      // Use InMemoryIndex to generate log only mor table.
      .option(HoodieIndexConfig.INDEX_TYPE.key, IndexType.INMEMORY.toString)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    // There should no base file in the file list.
    assertTrue(DataSourceTestUtils.isLogFileOnly(basePath))
    // Test read logs only mor table with glob paths.
    assertEquals(20, spark.read.format("hudi").load(basePath).count())
    // Test read log only mor table.
    assertEquals(20, spark.read.format("hudi").options(readOpts).load(basePath).count())
  }

  @Test
  def testTempFilesCleanForClustering(): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts()

    val records1 = recordsToStrings(dataGen.generateInserts("001", 1000)).asScala.toSeq
    val inputDF1: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key(), DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      // option for clustering
      .option("hoodie.clustering.inline", "true")
      .option("hoodie.clustering.plan.strategy.sort.columns", "begin_lat, begin_lon")
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val tempPath = new Path(basePath, HoodieTableMetaClient.TEMPFOLDER_NAME)
    val fs = tempPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    assertEquals(true, fs.listStatus(tempPath).isEmpty)
  }

  @Disabled
  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testClusteringOnNullableColumn(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

    val records1 = recordsToStrings(dataGen.generateInserts("001", 1000)).asScala.toSeq
    val inputDF1: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records1, 2))
      .withColumn("cluster_id", when(expr("end_lon < 0.2 "), lit(null).cast("string"))
          .otherwise(col("_row_key")))
      .withColumn("struct_cluster_col", when(expr("end_lon < 0.1"), lit(null))
          .otherwise(struct(col("cluster_id"), col("_row_key"))))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key(), DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      // option for clustering
      .option("hoodie.clustering.inline", "true")
      .option("hoodie.clustering.inline.max.commits", "1")
      .option("hoodie.clustering.plan.strategy.target.file.max.bytes", "1073741824")
      .option("hoodie.clustering.plan.strategy.small.file.limit", "629145600")
      .option("hoodie.clustering.plan.strategy.sort.columns", "struct_cluster_col")
      .mode(SaveMode.Overwrite)
      .save(basePath)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testClusteringSamePrecombine(recordType: HoodieRecordType): Unit = {
    var writeOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.OPERATION.key() -> DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      DataSourceWriteOptions.TABLE_TYPE.key()-> DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL,
      "hoodie.clustering.inline"-> "true",
      "hoodie.clustering.inline.max.commits" -> "2",
      "hoodie.clustering.plan.strategy.sort.columns" -> "_row_key",
      "hoodie.metadata.enable" -> "false",
      "hoodie.datasource.write.row.writer.enable" -> "false"
    )
    if (recordType.equals(HoodieRecordType.SPARK)) {
      writeOpts = Map(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key -> classOf[DefaultSparkRecordMerger].getName,
        HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key -> "parquet") ++ writeOpts
    }
    val records1 = recordsToStrings(dataGen.generateInserts("001", 10)).asScala.toSeq
    val inputDF1: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val records2 = recordsToStrings(dataGen.generateUniqueUpdates("002", 5)).asScala.toSeq
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .mode(SaveMode.Append)
      .save(basePath)

    assertEquals(5,
      spark.read.format("hudi").load(basePath)
        .select("_row_key", "partition", "rider")
        .except(inputDF2.select("_row_key", "partition", "rider")).count())
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testClusteringSamePrecombineWithDelete(recordType: HoodieRecordType): Unit = {
    var writeOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.OPERATION.key() -> DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      DataSourceWriteOptions.TABLE_TYPE.key() -> DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL,
      "hoodie.clustering.inline" -> "true",
      "hoodie.clustering.inline.max.commits" -> "2",
      "hoodie.clustering.plan.strategy.sort.columns" -> "_row_key",
      "hoodie.metadata.enable" -> "false",
      "hoodie.datasource.write.row.writer.enable" -> "false"
    )
    if (recordType.equals(HoodieRecordType.SPARK)) {
      writeOpts = Map(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key -> classOf[DefaultSparkRecordMerger].getName,
        HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key -> "parquet") ++ writeOpts
    }
    val records1 = recordsToStrings(dataGen.generateInserts("001", 10)).asScala.toSeq
    val inputDF1: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    writeOpts = writeOpts + (DataSourceWriteOptions.OPERATION.key() -> DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL)
    val records2 = recordsToStrings(dataGen.generateUniqueUpdates("002", 5)).asScala.toSeq
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .mode(SaveMode.Append)
      .save(basePath)

    assertEquals(5,
      spark.read.format("hudi").load(basePath).count())
  }

  /**
   * This tests the case that query by with a specified partition condition on hudi table which is
   * different between the value of the partition field and the actual partition path,
   * like hudi table written by TimestampBasedKeyGenerator.
   *
   * For MOR table, test all the three query modes.
   */
  @ParameterizedTest
  @CsvSource(Array("true,AVRO", "true,SPARK", "false,AVRO", "false,SPARK"))
  def testPrunePartitionForTimestampBasedKeyGenerator(enableFileIndex: Boolean,
                                                      recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType, enableFileIndex = enableFileIndex)

    val options = commonOpts ++ Map(
      "hoodie.compact.inline" -> "false",
      DataSourceWriteOptions.TABLE_TYPE.key -> DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL,
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.TimestampBasedKeyGenerator",
      TIMESTAMP_TYPE_FIELD.key -> "DATE_STRING",
      TIMESTAMP_OUTPUT_DATE_FORMAT.key -> "yyyy/MM/dd",
      TIMESTAMP_TIMEZONE_FORMAT.key -> "GMT+8:00",
      TIMESTAMP_INPUT_DATE_FORMAT.key -> "yyyy-MM-dd"
    ) ++ writeOpts

    val dataGen1 = new HoodieTestDataGenerator(Array("2022-01-01"))
    val records1 = recordsToStrings(dataGen1.generateInserts("001", 50)).asScala.toSeq
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    metaClient = createMetaClient(spark, basePath)
    val commit1Time = metaClient.getActiveTimeline.lastInstant().get().requestedTime

    val dataGen2 = new HoodieTestDataGenerator(Array("2022-01-02"))
    val records2 = recordsToStrings(dataGen2.generateInserts("002", 60)).asScala.toSeq
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)
    val commit2Time = metaClient.reloadActiveTimeline.lastInstant().get().requestedTime

    val records3 = recordsToStrings(dataGen2.generateUniqueUpdates("003", 20)).asScala.toSeq
    val inputDF3 = spark.read.json(spark.sparkContext.parallelize(records3, 2))
    inputDF3.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)
    val commit3Time = metaClient.reloadActiveTimeline.lastInstant().get().requestedTime
    val commit3CompletionTime = metaClient.reloadActiveTimeline.lastInstant().get().getCompletionTime

    val pathForROQuery = getPathForROQuery(basePath, !enableFileIndex, 3)
    // snapshot query
    val snapshotQueryRes = spark.read.format("hudi").options(readOpts).load(basePath)
    assertEquals(snapshotQueryRes.where(s"_hoodie_commit_time = '$commit1Time'").count, 50)
    assertEquals(snapshotQueryRes.where(s"_hoodie_commit_time = '$commit2Time'").count, 40)
    assertEquals(snapshotQueryRes.where(s"_hoodie_commit_time = '$commit3Time'").count, 20)

    assertEquals(snapshotQueryRes.where("partition = '2022-01-01'").count, 50)
    assertEquals(snapshotQueryRes.where("partition = '2022-01-02'").count, 60)

    // read_optimized query
    val readOptimizedQueryRes = spark.read.format("hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
      .load(pathForROQuery)
    assertEquals(readOptimizedQueryRes.where("partition = '2022-01-01'").count, 50)
    assertEquals(readOptimizedQueryRes.where("partition = '2022-01-02'").count, 60)

    // incremental query
    val incrementalQueryRes = spark.read.format("hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commit3CompletionTime)
      .option(DataSourceReadOptions.END_COMMIT.key, commit3CompletionTime)
      .load(basePath)
    assertEquals(0, incrementalQueryRes.where("partition = '2022-01-01'").count)
    assertEquals(20, incrementalQueryRes.where("partition = '2022-01-02'").count)
  }

  /**
   * Test read-optimized query on MOR during inflight compaction.
   *
   * The following scenario is tested:
   * Hudi timeline:
   * > Deltacommit1 (DC1, completed), writing file group 1 (fg1)
   * > Deltacommit2 (DC2, completed), updating fg1
   * > Compaction3 (C3, inflight), compacting fg1
   * > Deltacommit4 (DC4, completed), updating fg1
   *
   * On storage, these are the data files for fg1:
   * file slice v1:
   * - fg1_dc1.parquet (from DC1)
   * - .fg1_dc1.log (from DC2)
   * file slice v2:
   * - fg1_c3.parquet (from C3, inflight)
   * - .fg1_c3.log (from DC4)
   *
   * The read-optimized query should read `fg1_dc1.parquet` only in this case.
   */
  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testReadOptimizedQueryAfterInflightCompactionAndCompletedDeltaCommit(enableFileIndex: Boolean): Unit = {
    val (tableName, tablePath) = ("hoodie_mor_ro_read_test_table", s"${basePath}_mor_test_table")
    val orderingFields = "col3"
    val recordKeyField = "key"
    val dataField = "age"

    val options = Map[String, String](
      DataSourceWriteOptions.TABLE_TYPE.key -> HoodieTableType.MERGE_ON_READ.name,
      DataSourceWriteOptions.OPERATION.key -> UPSERT_OPERATION_OPT_VAL,
      HoodieTableConfig.ORDERING_FIELDS.key -> orderingFields,
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> recordKeyField,
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "",
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
      HoodieWriteConfig.TBL_NAME.key -> tableName,
      "hoodie.insert.shuffle.parallelism" -> "1",
      "hoodie.upsert.shuffle.parallelism" -> "1")
    val pathForROQuery = getPathForROQuery(tablePath, !enableFileIndex, 0)

    val (writeOpts, readOpts) = getWriterReaderOpts(HoodieRecordType.AVRO, options, enableFileIndex)

    // First batch with all inserts
    // Deltacommit1 (DC1, completed), writing file group 1 (fg1)
    // fg1_dc1.parquet written to storage
    // For record key "0", the row is (0, 0, 1000)
    val firstDf = spark.range(0, 10).toDF(recordKeyField)
      .withColumn(orderingFields, expr(recordKeyField))
      .withColumn(dataField, expr(recordKeyField + " + 1000"))

    firstDf.write.format("hudi")
      .options(writeOpts)
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Second batch with all updates
    // Deltacommit2 (DC2, completed), updating fg1
    // .fg1_dc1.log (from DC2) written to storage
    // For record key "0", the row is (0, 0, 2000)
    val secondDf = spark.range(0, 10).toDF(recordKeyField)
      .withColumn(orderingFields, expr(recordKeyField))
      .withColumn(dataField, expr(recordKeyField + " + 2000"))

    secondDf.write.format("hudi")
      .options(writeOpts)
      .mode(SaveMode.Append).save(tablePath)

    val compactionOptions = writeOpts ++ Map(
      HoodieCompactionConfig.INLINE_COMPACT_TRIGGER_STRATEGY.key -> CompactionTriggerStrategy.NUM_COMMITS.name,
      HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key -> "1",
      DataSourceWriteOptions.ASYNC_COMPACT_ENABLE.key -> "false",
      HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key -> classOf[OverwriteWithLatestAvroPayload].getName
    )

    // Schedule and execute compaction, leaving the compaction inflight
    // Compaction3 (C3, inflight), compacting fg1
    // fg1_c3.parquet is written to storage
    val client = DataSourceUtils.createHoodieClient(
      spark.sparkContext, "", tablePath, tableName,
      compactionOptions.asJava).asInstanceOf[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]]

    val compactionInstant = client.scheduleCompaction(Option.empty()).get()

    // NOTE: this executes the compaction to write the compacted base files, and leaves the
    // compaction instant still inflight, emulating a compaction action that is in progress
    client.compact(compactionInstant).getWriteStatuses.collect();
    client.close()

    // Third batch with all updates
    // Deltacommit4 (DC4, completed), updating fg1
    // .fg1_c3.log (from DC4) is written to storage
    // For record key "0", the row is (0, 0, 3000)
    val thirdDf = spark.range(0, 10).toDF(recordKeyField)
      .withColumn(orderingFields, expr(recordKeyField))
      .withColumn(dataField, expr(recordKeyField + " + 3000"))

    thirdDf.write.format("hudi")
      .options(writeOpts)
      // need to disable inline compaction for this test to avoid the compaction instant being completed
      .option(HoodieCompactionConfig.INLINE_COMPACT.key, "false")
      .mode(SaveMode.Append).save(tablePath)

    // Read-optimized query on MOR
    val roDf = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(
        DataSourceReadOptions.QUERY_TYPE.key,
        DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
      .load(pathForROQuery)

    // The base file in the first file slice, i.e., fg1_dc1.parquet, should be read only
    assertEquals(10, roDf.count())
    assertEquals(
      1000L,
      roDf.where(col(recordKeyField) === 0).select(dataField).collect()(0).getLong(0))
  }

  @ParameterizedTest
  @CsvSource(value = Array("true,6", "true,8", "false,6", "false,8"))
//  @ValueSource(booleans = Array(true, false))
  def testSnapshotQueryAfterInflightDeltaCommit(enableFileIndex: Boolean, tableVersion: Int): Unit = {
    val (tableName, tablePath) = ("hoodie_mor_snapshot_read_test_table", s"${basePath}_mor_test_table")
    val orderingFields = "col3"
    val recordKeyField = "key"
    val dataField = "age"

    val options = Map[String, String](
      DataSourceWriteOptions.TABLE_TYPE.key -> HoodieTableType.MERGE_ON_READ.name,
      DataSourceWriteOptions.OPERATION.key -> UPSERT_OPERATION_OPT_VAL,
      HoodieTableConfig.ORDERING_FIELDS.key -> orderingFields,
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> recordKeyField,
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "",
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
      HoodieWriteConfig.TBL_NAME.key -> tableName,
      "hoodie.insert.shuffle.parallelism" -> "1",
      "hoodie.upsert.shuffle.parallelism" -> "1")
    val pathForQuery = getPathForROQuery(tablePath, !enableFileIndex, 0)

    var (writeOpts, readOpts) = getWriterReaderOpts(HoodieRecordType.AVRO, options, enableFileIndex)
    writeOpts = writeOpts ++ Map(
      HoodieTableConfig.VERSION.key() -> tableVersion.toString,
      HoodieWriteConfig.WRITE_TABLE_VERSION.key() -> tableVersion.toString)

    val firstDf = spark.range(0, 10).toDF(recordKeyField)
      .withColumn(orderingFields, expr(recordKeyField))
      .withColumn(dataField, expr(recordKeyField + " + 1000"))
    firstDf.write.format("hudi")
      .options(writeOpts)
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    val secondDf = spark.range(0, 10).toDF(recordKeyField)
      .withColumn(orderingFields, expr(recordKeyField))
      .withColumn(dataField, expr(recordKeyField + " + 2000"))
    secondDf.write.format("hudi")
      .options(writeOpts)
      .mode(SaveMode.Append).save(tablePath)

    // Snapshot query on MOR
    val snapshotDf = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(pathForQuery)

    // Delete last completed instant
    metaClient = createMetaClient(spark, tablePath)
    val files = storage.listDirectEntries(metaClient.getTimelinePath).stream()
      .filter(f => f.getPath.getName.contains(metaClient.getActiveTimeline.lastInstant().get().requestedTime())
        && !f.getPath.getName.contains("inflight")
        && !f.getPath.getName.contains("requested"))
      .collect(Collectors.toList[StoragePathInfo]).asScala
    assertEquals(1, files.size)
    storage.deleteFile(files.head.getPath)

    // verify snapshot query returns data written using firstDf
    assertEquals(10, snapshotDf.count())
    assertEquals(
      1000L,
      snapshotDf.where(col(recordKeyField) === 0).select(dataField).collect()(0).getLong(0))
  }

  @ParameterizedTest
  @CsvSource(Array(
    "AVRO, AVRO, END_MAP",
    "AVRO, SPARK, END_MAP",
    "SPARK, AVRO, END_MAP",
    "AVRO, AVRO, END_ARRAY",
    "AVRO, SPARK, END_ARRAY",
    "SPARK, AVRO, END_ARRAY"))
  def testRecordTypeCompatibilityWithParquetLog(readType: HoodieRecordType,
                                                writeType: HoodieRecordType,
                                                transformMode: String): Unit = {
    def transform(sourceDF: DataFrame, transformed: String): DataFrame = {
      transformed match {
        case "END_MAP" => sourceDF
          .withColumn("obj_ids", array(lit("wk_tenant_id")))
          .withColumn("obj_maps", map(lit("wk_tenant_id"), col("obj_ids")))
        case "END_ARRAY" => sourceDF
          .withColumn("obj_maps", map(lit("wk_tenant_id"), lit("wk_tenant_id")))
          .withColumn("obj_ids", array(col("obj_maps")))
      }
    }

    var (_, readOpts) = getWriterReaderOpts(readType)
    var (writeOpts, _) = getWriterReaderOpts(writeType)
    readOpts = readOpts ++ Map(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key -> "parquet")
    writeOpts = writeOpts ++ Map(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key -> "parquet")
    val records = dataGen.generateInserts("001", 10)

    // End with array
    val inputDF1 = transform(spark.read.json(
      spark.sparkContext.parallelize(recordsToStrings(records).asScala.toSeq, 2))
      .withColumn("wk_tenant_id", lit("wk_tenant_id"))
      .withColumn("ref_id", lit("wk_tenant_id")), transformMode)
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      // CanIndexLogFile=true
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, IndexType.INMEMORY.name())
      .mode(SaveMode.Overwrite)
      .save(basePath)
    assertTrue(HoodieDataSourceHelpers.hasNewCommits(storage, basePath, "000"))

    val snapshotDF1 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath)

    def sort(df: DataFrame): DataFrame = df.sort("_row_key")

    val inputRows = sort(inputDF1).collectAsList()
    val readRows = sort(snapshotDF1.drop(HoodieRecord.HOODIE_META_COLUMNS.asScala.toSeq: _*)).collectAsList()

    assertEquals(inputRows, readRows)
  }

  def getWriterReaderOpts(recordType: HoodieRecordType = HoodieRecordType.AVRO,
                          opt: Map[String, String] = commonOpts,
                          enableFileIndex: Boolean = DataSourceReadOptions.ENABLE_HOODIE_FILE_INDEX.defaultValue()):
  (Map[String, String], Map[String, String]) = {
    val fileIndexOpt: Map[String, String] =
      Map(DataSourceReadOptions.ENABLE_HOODIE_FILE_INDEX.key -> enableFileIndex.toString)

    recordType match {
      case HoodieRecordType.SPARK => (opt ++ sparkOpts, sparkOpts ++ fileIndexOpt)
      case _ => (opt, fileIndexOpt)
    }
  }

  def getPathForROQuery(basePath: String, useGlobbing: Boolean, partitionPathLevel: Int): String = {
    if (useGlobbing) {
      // When explicitly using globbing or not using HoodieFileIndex, we fall back to the old way
      // of reading Hudi table with globbed path
      basePath + "/*" * (partitionPathLevel + 1)
    } else {
      basePath
    }
  }

  @ParameterizedTest
  @CsvSource(Array("8", "9"))
  def testMergerStrategySet(tableVersion: String): Unit = {
    val (writeOpts, _) = getWriterReaderOpts()
    val input = recordsToStrings(dataGen.generateInserts("000", 1)).asScala
    val inputDf= spark.read.json(spark.sparkContext.parallelize(input.toSeq, 1))
    val mergerStrategyName = "example_merger_strategy"
    inputDf.write.format("hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, "MERGE_ON_READ")
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.RECORD_MERGE_STRATEGY_ID.key(), mergerStrategyName)
      .option(DataSourceWriteOptions.RECORD_MERGE_MODE.key(), RecordMergeMode.CUSTOM.name())
      .option(HoodieWriteConfig.WRITE_TABLE_VERSION.key, tableVersion)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    metaClient = createMetaClient(spark, basePath)
    assertEquals(metaClient.getTableConfig.getRecordMergeStrategyId, mergerStrategyName)
  }

  /**
   * Test Read-Optimized and time travel query on MOR table with RECORD_INDEX enabled.
   */
  @Test
  def testReadOptimizedAndTimeTravelWithRecordIndex(): Unit = {
    var (writeOpts, readOpts) = getWriterReaderOpts()
    writeOpts = writeOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL,
      HoodieCompactionConfig.INLINE_COMPACT.key -> "false",
      HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key -> "0",
      HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key -> "true",
      HoodieIndexConfig.INDEX_TYPE.key -> IndexType.RECORD_INDEX.name()
    )
    readOpts = readOpts ++ Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true"
    )
    initMetaClient(HoodieTableType.MERGE_ON_READ)
    // Create a MOR table and add three records to the table.
    val records = recordsToStrings(dataGen.generateInserts("000", 3)).asScala.toSeq
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    inputDF.write.format("org.apache.hudi")
      .options(writeOpts)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    var roDf = spark.read.format("hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
      .load(basePath)
    // assert count
    assertEquals(3, roDf.count())

    // choose a record to delete
    val deleteRecord = recordsToStrings(dataGen.generateUniqueDeleteRecords("002", 1)).asScala.toSeq
    // get the record key from the deleted record records2
    val recordKey = deleteRecord.head.split(",")(1).split(":")(1).trim.replace("\"", "")
    // delete the record
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(deleteRecord, 1))
    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    // load RO view again with data skipping enabled
    roDf = spark.read.format("hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
      .load(basePath)

    // There should still be 3 records in RO view
    assertEquals(3, roDf.count())
    // deleted record should still show in RO view
    assertEquals(1, roDf.where(s"_row_key = '$recordKey'").count())

    // load snapshot view
    val snapshotDF = spark.read.format("hudi")
      .options(readOpts)
      .load(basePath)
    // There should be only 2 records in snapshot view
    assertEquals(2, snapshotDF.count())
    // deleted record should NOT show in snapshot view
    assertEquals(0, snapshotDF.where(s"_row_key = '$recordKey'").count())

    // get the first instant on the timeline
    val firstInstant = metaClient.reloadActiveTimeline().filterCompletedInstants().firstInstant().get()
    // do a time travel query with data skipping enabled
    val timeTravelDF = spark.read.format("hudi")
      .options(readOpts)
      .option("as.of.instant", firstInstant.requestedTime)
      .load(basePath)
    // there should still be 3 records in time travel view
    assertEquals(3, timeTravelDF.count())
    // deleted record should still show in time travel view
    assertEquals(1, timeTravelDF.where(s"_row_key = '$recordKey'").count())
  }

  /**
   * Test Secondary Index creation through datasource and metadata write configs.
   *
   * 1. Insert a batch of data with record_index enabled.
   * 2. Upsert another batch of data with secondary index configs.
   * 3. Validate that secondary index is created.
   */
  @Test
  def testSecondaryIndexCreation(): Unit = {
    var (writeOpts, readOpts) = getWriterReaderOpts()
    writeOpts = writeOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL,
      HoodieCompactionConfig.INLINE_COMPACT.key -> "false",
      HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key -> "0",
      HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key -> "true",
      HoodieIndexConfig.INDEX_TYPE.key -> IndexType.RECORD_INDEX.name()
    )
    readOpts = readOpts ++ Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true"
    )
    initMetaClient(HoodieTableType.MERGE_ON_READ)
    // Create a MOR table and add 10 records to the table.
    val records = recordsToStrings(dataGen.generateInserts("000", 3)).asScala.toSeq
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    inputDF.write.format("org.apache.hudi")
      .options(writeOpts)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val snapshotDF = spark.read.format("org.apache.hudi").options(readOpts).load(basePath)
    assertEquals(3, snapshotDF.count())

    // Upsert another batch with secondary index configs
    val secondaryIndexName = "idx_rider"
    val secondaryIndexColumn = "rider"
    writeOpts = writeOpts ++ Map(
      HoodieMetadataConfig.SECONDARY_INDEX_ENABLE_PROP.key -> "true",
      HoodieMetadataConfig.SECONDARY_INDEX_NAME.key -> secondaryIndexName,
      HoodieMetadataConfig.SECONDARY_INDEX_COLUMN.key -> secondaryIndexColumn
    )
    val records2 = recordsToStrings(dataGen.generateInserts("001", 3)).asScala.toSeq
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 10))
    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .mode(SaveMode.Append)
      .save(basePath)

    // validate that secondary index is created
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertTrue(metadataPartitionExists(basePath, context, PARTITION_NAME_SECONDARY_INDEX_PREFIX + secondaryIndexName))
    assertTrue(metaClient.getTableConfig.getMetadataPartitions.contains(PARTITION_NAME_SECONDARY_INDEX_PREFIX + secondaryIndexName))

    // let us create one more index
    val secondaryIndexName2 = "idx_driver"
    val secondaryIndexColumn2 = "driver"
    writeOpts = writeOpts ++ Map(
      HoodieMetadataConfig.SECONDARY_INDEX_ENABLE_PROP.key -> "true",
      HoodieMetadataConfig.SECONDARY_INDEX_NAME.key -> secondaryIndexName2,
      HoodieMetadataConfig.SECONDARY_INDEX_COLUMN.key -> secondaryIndexColumn2
    )
    val records3 = recordsToStrings(dataGen.generateInserts("002", 3)).asScala.toSeq
    val inputDF3 = spark.read.json(spark.sparkContext.parallelize(records3, 10))
    inputDF3.write.format("org.apache.hudi")
      .options(writeOpts)
      .mode(SaveMode.Append)
      .save(basePath)

    // validate that secondary index is created
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertTrue(metadataPartitionExists(basePath, context, PARTITION_NAME_SECONDARY_INDEX_PREFIX + secondaryIndexName2))
    assertTrue(metaClient.getTableConfig.getMetadataPartitions.contains(PARTITION_NAME_SECONDARY_INDEX_PREFIX + secondaryIndexName2))
  }

  @Test
  def testMultipleOrderingFields() {
    val (writeOpts, readOpts) = getWriterReaderOpts(HoodieRecordType.AVRO)

    // Insert Operation
    var records = recordsToStrings(dataGen.generateInserts("003", 100)).asScala.toList
    var inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    inputDF = inputDF.withColumn("timestamp", lit(10))

    val commonOptsWithMultipleOrderingFields = writeOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL,
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      HoodieTableConfig.ORDERING_FIELDS.key() -> "timestamp,rider",
      DataSourceWriteOptions.RECORD_MERGE_MODE.key() -> RecordMergeMode.EVENT_TIME_ORDERING.name(),
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
    )
    inputDF.write.format("hudi")
      .options(commonOptsWithMultipleOrderingFields)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    records = recordsToStrings(dataGen.generateUniqueUpdates("002", 10)).asScala.toList
    inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    inputDF = inputDF.withColumn("timestamp", lit(10))
    inputDF.write.format("hudi")
      .options(commonOptsWithMultipleOrderingFields)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    assertEquals(0, spark.read.format("org.apache.hudi").load(basePath).filter("rider = 'rider-002'").count())

    records = recordsToStrings(dataGen.generateUniqueUpdates("004", 10)).asScala.toList
    inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    inputDF = inputDF.withColumn("timestamp", lit(10))
    inputDF.write.format("hudi")
      .options(commonOptsWithMultipleOrderingFields)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    assertEquals(10, spark.read.format("org.apache.hudi").load(basePath).filter("rider = 'rider-004'").count())

    records = recordsToStrings(dataGen.generateUniqueUpdates("001", 10)).asScala.toList
    inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    inputDF = inputDF.withColumn("timestamp", lit(20))
    inputDF.write.format("hudi")
      .options(commonOptsWithMultipleOrderingFields)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    assertEquals(10, spark.read.format("org.apache.hudi").load(basePath).filter("rider = 'rider-001'").count())
  }

  @ParameterizedTest
  @CsvSource(Array(
    "6,8,true,UPGRADE", // Normal upgrade: table=6, write=8, autoUpgrade=true → should upgrade
    "6,9,true,UPGRADE", // Normal upgrade: table=6, write=9, autoUpgrade=true → should upgrade
    "6,6,false,NO_UPGRADE", // Auto-upgrade disabled: table=6, write=6, autoUpgrade=false → no upgrade
    "6,8,false,NO_UPGRADE", // Auto-upgrade disabled: table=6, write=8, autoUpgrade=false → no upgrade
    "4,8,true,EXCEPTION", // Auto-upgrade enabled: Should throw exception since table version is less than 6
    "4,8,false,EXCEPTION", // Auto-upgrade disabled: Should throw exception since table version is less than 6
    "8,8,false,NO_UPGRADE", // Auto-upgrade disabled: Should not upgrade as auto-upgrade is disabled and same versions
    "8,8,true,NO_UPGRADE" // Auto-upgrade enabled: Should not upgrade as auto-upgrade is enabled and same versions
  ))
  def testBaseHoodieWriteClientUpgradeDecisionLogic(
    tableVersionStr: String,
    writeVersionStr: String,
    autoUpgrade: Boolean,
    expectedResult: String): Unit = {
    val tableVersion = HoodieTableVersion.fromVersionCode(tableVersionStr.toInt)
    val writeVersion = HoodieTableVersion.fromVersionCode(writeVersionStr.toInt)
    // Create a temporary directory for this test
    val testBasePath = s"${basePath}_upgrade_test_${tableVersionStr}_${writeVersionStr}_${autoUpgrade}_${System.currentTimeMillis()}"

    try {
      loadFixtureTable(testBasePath, tableVersion)

      val testWriteOpts = getFixtureCompatibleWriteOpts() ++ Map(
        HoodieWriteConfig.WRITE_TABLE_VERSION.key -> writeVersion.versionCode().toString,
        HoodieWriteConfig.AUTO_UPGRADE_VERSION.key -> autoUpgrade.toString,
        DataSourceWriteOptions.TABLE_TYPE.key -> DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL,
        DataSourceWriteOptions.OPERATION.key -> DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL
      )

      val testData = createFixtureCompatibleTestData()
      val schema = StructType(Array(
        StructField("id", StringType, false),
        StructField("name", StringType, false),
        StructField("ts", LongType, false),
        StructField("partition", StringType, false)
      ))
      val inputDF = spark.createDataFrame(spark.sparkContext.parallelize(testData), schema)

      // Execute the test based on expected result
      expectedResult match {
        case "UPGRADE" =>
          // Should perform upgrade successfully
          inputDF.write.format("hudi")
            .options(testWriteOpts)
            .mode(SaveMode.Append)
            .save(testBasePath)

          // Verify table was upgraded
          val upgradeMetaClient = HoodieTableMetaClient.builder()
            .setConf(storageConf.newInstance())
            .setBasePath(testBasePath)
            .build()
          assertEquals(
            writeVersion.versionCode(),
            upgradeMetaClient.getTableConfig.getTableVersion.versionCode(),
            s"Table should have been upgraded to at least version ${writeVersion.versionCode()}")

          // Verify data integrity
          val resultDF = spark.read.format("hudi").load(testBasePath)
          assertEquals(11, resultDF.count(), "Data should be preserved after upgrade")

        case "NO_UPGRADE" =>
          // Should complete successfully without upgrade
          inputDF.write.format("hudi")
            .options(testWriteOpts)
            .mode(SaveMode.Append)
            .save(testBasePath)

          // Verify table version remained unchanged
          val noUpgradeMetaClient = HoodieTableMetaClient.builder()
            .setConf(storageConf.newInstance())
            .setBasePath(testBasePath)
            .build()
          assertEquals(tableVersion, noUpgradeMetaClient.getTableConfig.getTableVersion,
            s"Table version should remain at $tableVersion")

          // Verify data integrity
          val resultDF = spark.read.format("hudi").load(testBasePath)
          assertEquals(11, resultDF.count(), "Data should be written successfully without upgrade")

        case "EXCEPTION" =>
          // Should throw HoodieUpgradeDowngradeException
          val exception = assertThrows(classOf[HoodieUpgradeDowngradeException]) {
            inputDF.write.format("hudi")
              .options(testWriteOpts)
              .mode(SaveMode.Append)
              .save(testBasePath)
          }

          // Verify exception message contains expected content
          val expectedMessageFragment = if (tableVersion.versionCode() < HoodieTableVersion.SIX.versionCode()) {
            // For Hudi 1.1.0: any table version < 6 throws exception with this message
            "Hudi 1.x release only supports table version greater than version 6 or above"
          } else {
            "upgrade"
          }
          assertTrue(exception.getMessage.contains(expectedMessageFragment),
            s"Exception message should contain '${expectedMessageFragment}', but was: ${exception.getMessage}")
      }

    } finally {
      // Cleanup test directory
      try {
        val testPath = new StoragePath(testBasePath)
        if (storage.exists(testPath)) {
          storage.deleteDirectory(testPath)
        }
      } catch {
        case _: Exception => // Ignore cleanup errors
      }
    }
  }

  private def loadFixtureTable(testBasePath: String, version: HoodieTableVersion): HoodieTableMetaClient = {
    val fixtureName = getFixtureName(version)
    val resourcePath = s"/upgrade-downgrade-fixtures/mor-tables/$fixtureName"

    // Create temporary directory for fixture extraction
    val tempFixtureDir = Files.createTempDirectory("hudi-fixture-")

    try {
      // Extract fixture to temporary directory
      HoodieTestUtils.extractZipToDirectory(resourcePath, tempFixtureDir, this.getClass)

      // Copy extracted table to test location
      val tableName = fixtureName.replace(".zip", "")
      val extractedTablePath = tempFixtureDir.resolve(tableName).toFile
      val testTablePath = new File(testBasePath)

      // Copy the extracted table contents to our test path
      FileUtils.copyDirectory(extractedTablePath, testTablePath)

      // Verify the table was loaded at the expected version
      val metaClient = HoodieTableMetaClient.builder()
        .setConf(storageConf.newInstance())
        .setBasePath(testBasePath)
        .build()
      assertEquals(version, metaClient.getTableConfig.getTableVersion,
        s"Fixture table should be at version ${version}")
      metaClient
    } finally {
      // Cleanup temporary fixture directory
      try {
        FileUtils.deleteDirectory(tempFixtureDir.toFile)
      } catch {
        case _: Exception => // Ignore cleanup errors
      }
    }
  }

  private def getFixtureCompatibleWriteOpts(): Map[String, String] = {
    Map(
      // Don't override table name - let fixture table configuration take precedence
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",         // Fixture uses 'id' as record key
      HoodieTableConfig.ORDERING_FIELDS.key -> "ts",        // Fixture uses 'ts' as precombine field
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition", // Fixture uses 'partition' as partition field
      "hoodie.upsert.shuffle.parallelism" -> "2",
      "hoodie.insert.shuffle.parallelism" -> "2"
    ) ++ sparkOpts
  }

  private def createFixtureCompatibleTestData(): Seq[Row] = {
    // Create test data that matches fixture schema: (id, name, ts, partition)
    Seq(
      Row("id9", "TestUser1", 9000L, "2023-01-05"),
      Row("id10", "TestUser2", 10000L, "2023-01-05"),
      Row("id11", "TestUser3", 11000L, "2023-01-06")
    )
  }
}
