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


import org.apache.hudi.{AvroConversionUtils, ColumnStatsIndexSupport, DataSourceReadOptions, DataSourceUtils, DataSourceWriteOptions, HoodieDataSourceHelpers, HoodieSparkRecordMerger, HoodieSparkUtils, SparkDatasetMixin}
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.HoodieConversionUtils.toJavaOption
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.common.config.TimestampKeyGeneratorConfig.{TIMESTAMP_INPUT_DATE_FORMAT, TIMESTAMP_OUTPUT_DATE_FORMAT, TIMESTAMP_TIMEZONE_FORMAT, TIMESTAMP_TYPE_FIELD}
import org.apache.hudi.common.config.{HoodieMetadataConfig, HoodieStorageConfig}
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.model.{DefaultHoodieRecordPayload, HoodieRecord, HoodieRecordPayload, HoodieTableType, OverwriteWithLatestAvroPayload}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.testutils.{HoodieTestDataGenerator, HoodieTestUtils}
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.common.util.Option
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.functional.TestCOWDataSource.convertColumnsToNullable
import org.apache.hudi.hadoop.config.HoodieRealtimeConfig
import org.apache.hudi.index.HoodieIndex.IndexType
import org.apache.hudi.storage.{StoragePath, StoragePathInfo}
import org.apache.hudi.table.action.compact.CompactionTriggerStrategy
import org.apache.hudi.testutils.{DataSourceTestUtils, HoodieSparkClientTestBase}
import org.apache.hudi.util.{JavaConversions, JFunction}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.types.BooleanType
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, EnumSource, ValueSource}
import org.slf4j.LoggerFactory

import java.net.URI
import java.nio.file.Paths
import java.sql.Timestamp
import java.util.function.Consumer
import java.util.stream.Collectors
import scala.collection.JavaConverters._

/**
 * Tests on Spark DataSource for MOR table.
 */
class TestMORDataSource extends HoodieSparkClientTestBase with SparkDatasetMixin {

  var spark: SparkSession = null
  private val log = LoggerFactory.getLogger(classOf[TestMORDataSource])
  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
  )
  val sparkOpts = Map(
    HoodieWriteConfig.RECORD_MERGER_IMPLS.key -> classOf[HoodieSparkRecordMerger].getName,
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
  @CsvSource(Array("AVRO, AVRO, avro", "AVRO, SPARK, parquet", "SPARK, AVRO, parquet", "SPARK, SPARK, parquet"))
  def testCount(readType: HoodieRecordType, writeType: HoodieRecordType, logType: String) {
    var (_, readOpts) = getWriterReaderOpts(readType)
    var (writeOpts, _) = getWriterReaderOpts(writeType)
    readOpts = readOpts ++ Map(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key -> logType)
    writeOpts = writeOpts ++ Map(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key -> logType)

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
      .load(basePath + "/*/*/*/*")
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
    val hudiSnapshotDF2 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    assertEquals(100, hudiSnapshotDF2.count()) // still 100, since we only updated
    val commit1Time = hudiSnapshotDF1.select("_hoodie_commit_time").head().get(0).toString
    val commit2Time = hudiSnapshotDF2.select("_hoodie_commit_time").head().get(0).toString
    assertEquals(hudiSnapshotDF2.select("_hoodie_commit_time").distinct().count(), 1)
    assertTrue(commit2Time > commit1Time)
    assertEquals(100, hudiSnapshotDF2.join(hudiSnapshotDF1, Seq("_hoodie_record_key"), "left").count())

    // incremental view
    // base file only
    val hudiIncDF1 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, "000")
      .option(DataSourceReadOptions.END_INSTANTTIME.key, commit1Time)
      .load(basePath)
    assertEquals(100, hudiIncDF1.count())
    assertEquals(1, hudiIncDF1.select("_hoodie_commit_time").distinct().count())
    assertEquals(commit1Time, hudiIncDF1.select("_hoodie_commit_time").head().get(0).toString)
    hudiIncDF1.show(1)
    // log file only
    val hudiIncDF2 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, commit1Time)
      .option(DataSourceReadOptions.END_INSTANTTIME.key, commit2Time)
      .load(basePath)
    assertEquals(100, hudiIncDF2.count())
    assertEquals(1, hudiIncDF2.select("_hoodie_commit_time").distinct().count())
    assertEquals(commit2Time, hudiIncDF2.select("_hoodie_commit_time").head().get(0).toString)
    hudiIncDF2.show(1)

    // base file + log file
    val hudiIncDF3 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, "000")
      .option(DataSourceReadOptions.END_INSTANTTIME.key, commit2Time)
      .load(basePath)
    assertEquals(100, hudiIncDF3.count())
    // log file being load
    assertEquals(1, hudiIncDF3.select("_hoodie_commit_time").distinct().count())
    assertEquals(commit2Time, hudiIncDF3.select("_hoodie_commit_time").head().get(0).toString)

    // Test incremental query has no instant in range
    val emptyIncDF = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, "000")
      .option(DataSourceReadOptions.END_INSTANTTIME.key, "001")
      .load(basePath)
    assertEquals(0, emptyIncDF.count())

    // Unmerge
    val hudiSnapshotSkipMergeDF2 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .option(DataSourceReadOptions.REALTIME_MERGE.key, DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    assertEquals(200, hudiSnapshotSkipMergeDF2.count())
    assertEquals(100, hudiSnapshotSkipMergeDF2.select("_hoodie_record_key").distinct().count())
    assertEquals(200, hudiSnapshotSkipMergeDF2.join(hudiSnapshotDF2, Seq("_hoodie_record_key"), "left").count())

    // Test Read Optimized Query on MOR table
    val hudiRODF2 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
      .load(basePath + "/*/*/*/*")
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
    val hudiSnapshotDF3 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    // still 100, because we only updated the existing records
    assertEquals(100, hudiSnapshotDF3.count())

    // 50 from commit2, 50 from commit3
    assertEquals(hudiSnapshotDF3.select("_hoodie_commit_time").distinct().count(), 2)
    assertEquals(50, hudiSnapshotDF3.filter(col("_hoodie_commit_time") > commit2Time).count())
    assertEquals(50,
      hudiSnapshotDF3.join(hudiSnapshotDF2, Seq("_hoodie_record_key", "_hoodie_commit_time"), "inner").count())

    // incremental query from commit2Time
    val hudiIncDF4 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, commit2Time)
      .load(basePath)
    assertEquals(50, hudiIncDF4.count())

    // skip merge incremental view
    // including commit 2 and commit 3
    val hudiIncDF4SkipMerge = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, "000")
      .option(DataSourceReadOptions.REALTIME_MERGE.key, DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL)
      .load(basePath)
    assertEquals(200, hudiIncDF4SkipMerge.count())

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
    val hudiSnapshotDF4 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    // 200, because we insert 100 records to a new partition
    assertEquals(200, hudiSnapshotDF4.count())
    assertEquals(100,
      hudiSnapshotDF1.join(hudiSnapshotDF4, Seq("_hoodie_record_key"), "inner").count())

    // Incremental query, 50 from log file, 100 from base file of the new partition.
    val hudiIncDF5 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, commit2Time)
      .load(basePath)
    assertEquals(150, hudiIncDF5.count())

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
    val commit5Time = HoodieDataSourceHelpers.latestCommit(storage, basePath)
    val hudiSnapshotDF5 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/*/*/*/*")
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
    val commit6Time = HoodieDataSourceHelpers.latestCommit(storage, basePath)
    val hudiSnapshotDF6 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/2020/01/10/*")
    assertEquals(102, hudiSnapshotDF6.count())
    val hudiIncDF6 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, commit5Time)
      .option(DataSourceReadOptions.END_INSTANTTIME.key, commit6Time)
      .load(basePath)
    // even though compaction updated 150 rows, since preserve commit metadata is true, they won't be part of incremental query.
    // inserted 2 new row
    assertEquals(2, hudiIncDF6.count())
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
    spark.sparkContext.hadoopConfiguration.set(HoodieRealtimeConfig.COMPACTION_MEMORY_FRACTION_PROP, "0.00001")
    val hudiSnapshotDF1 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    assertEquals(100, hudiSnapshotDF1.count()) // still 100, since we only updated
    spark.sparkContext.hadoopConfiguration.set(HoodieRealtimeConfig.COMPACTION_MEMORY_FRACTION_PROP, HoodieRealtimeConfig.DEFAULT_COMPACTION_MEMORY_FRACTION)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testPayloadDelete(recordType: HoodieRecordType) {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

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
      .load(basePath + "/*/*/*/*")
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
    val hudiSnapshotDF2 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/*/*/*/*")
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
      .load(basePath + "/*/*/*/*")
    assertEquals(100, hudiSnapshotDF2Unmerge.count())

    // incremental query, read 50 delete records from log file and get 0 count.
    val hudiIncDF1 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, commit2Time)
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
      .load(basePath + "/*/*/*/*")
    assertEquals(0, hudiSnapshotDF3.count()) // 100 records were deleted, 0 record to load
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testPrunedFiltered(recordType: HoodieRecordType) {

    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

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
      .load(basePath + "/*/*/*/*")
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
    val hudiSnapshotDF2 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    val hudiIncDF1 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, "000")
      .load(basePath)
    val hudiIncDF1Skipmerge = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.REALTIME_MERGE.key, DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, "000")
      .load(basePath)
    val hudiIncDF2 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, commit1Time)
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
      .load(basePath + "/*/*/*/*")

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
      .load(basePath + "/*/*/*/*")
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
      .load(basePath + "/*/*/*/*")
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
  def testNoPrecombine(recordType: HoodieRecordType) {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

    // Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).asScala.toSeq
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))

    val commonOptsNoPreCombine = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
    ) ++ writeOpts
    inputDF.write.format("hudi")
      .options(commonOptsNoPreCombine)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key(), "MERGE_ON_READ")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    spark.read.format("org.apache.hudi").options(readOpts).load(basePath).count()
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testPreCombineFiledForReadMOR(recordType: HoodieRecordType): Unit = {
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
      .option(PRECOMBINE_FIELD.key, "version")
      .option(PARTITIONPATH_FIELD.key, "")
      .mode(SaveMode.Append)
      .save(basePath)
  }

  private def checkAnswer(expect: (Int, String, Int, Int, Boolean), opts: Map[String, String]): Unit = {
    val readDf = spark.read.format("org.apache.hudi")
      .options(opts)
      .load(basePath + "/*")
    if (expect._5) {
      if (!readDf.isEmpty) {
        println("Found df " + readDf.collectAsList().get(0).mkString(","))
      }
      assertTrue(readDf.isEmpty)
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
    // Incremental query without "*" in path
    val hoodieIncViewDF1 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, commitInstantTime1)
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
    var (writeOpts, readOpts) = getWriterReaderOpts(recordType)
    writeOpts += (HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true")

    initMetaClient(HoodieTableType.MERGE_ON_READ)
    val records1 = dataGen.generateInsertsContainsAllPartitions("000", 20)
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records1).asScala.toSeq, 2))
    inputDF1.write.format("hudi")
      .options(writeOpts)
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
      .options(writeOpts)
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

    if (recordType == HoodieRecordType.SPARK && HoodieSparkUtils.gteqSpark3_4) {
      metaClient = HoodieTableMetaClient.reload(metaClient)
      val metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).withMetadataIndexColumnStats(true).build()
      val avroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(inputDF1.schema, "record", "")
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
    assertEquals(20, spark.read.format("hudi").load(basePath + "/*/*/*/*").count())
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
      DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "timestamp",
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
      writeOpts = Map(HoodieWriteConfig.RECORD_MERGER_IMPLS.key -> classOf[HoodieSparkRecordMerger].getName,
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
      DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "timestamp",
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
      writeOpts = Map(HoodieWriteConfig.RECORD_MERGER_IMPLS.key -> classOf[HoodieSparkRecordMerger].getName,
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

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testHoodieIsDeletedMOR(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

    val numRecords = 100
    val numRecordsToDelete = 2
    val schema = HoodieTestDataGenerator.SHORT_TRIP_SCHEMA
    val records0 = recordsToStrings(dataGen.generateInsertsAsPerSchema("000", numRecords, schema)).asScala.toSeq
    val inputDF0 = spark.read.json(spark.sparkContext.parallelize(records0, 2))
    inputDF0.write.format("org.apache.hudi")
      .options(writeOpts)
      .option("hoodie.compact.inline", "false")
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val snapshotDF0 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    assertEquals(numRecords, snapshotDF0.count())

    val df1 = snapshotDF0.limit(numRecordsToDelete)
    val dropDf = df1.drop(df1.columns.filter(_.startsWith("_hoodie_")): _*)

    val df2 = convertColumnsToNullable(
      dropDf.withColumn("_hoodie_is_deleted", lit(true).cast(BooleanType)),
      "_hoodie_is_deleted"
    )
    df2.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val snapshotDF2 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    assertEquals(numRecords - numRecordsToDelete, snapshotDF2.count())
  }

  @CsvSource(Array("avro, 6", "parquet, 6"))
  def testLogicalTypesReadRepair(logBlockFormat: String, tableVersion: Int): Unit = {
    val logBlockString = if (logBlockFormat == "avro") {
      ""
    } else {
      "_parquet_log"
    }
    val prevTimezone = spark.conf.get("spark.sql.session.timeZone")
    val propertyValue: String = System.getProperty("spark.testing")
    try {
      if (HoodieSparkUtils.isSpark3_3) {
        System.setProperty("spark.testing", "true")
      }
      spark.conf.set("spark.sql.session.timeZone", "UTC")
      val tableName = "trips_logical_types_json_mor_read_v" + tableVersion + logBlockString
      val dataPath = "file://" + basePath + "/" + tableName
      val zipOutput = Paths.get(new URI(dataPath))
      HoodieTestUtils.extractZipToDirectory("/" + tableName + ".zip", zipOutput, getClass)
      val tableBasePath = zipOutput.toString

      val df = spark.read.format("hudi").load(tableBasePath)

      val rows = df.collect()
      assertEquals(20, rows.length)
      for (row <- rows) {
        val hash = row.get(6).asInstanceOf[String].hashCode()
        if ((hash & 1)== 0) {
          assertEquals("2020-01-01T00:00:00.001Z", row.get(15).asInstanceOf[Timestamp].toInstant.toString)
          assertEquals("2020-06-01T12:00:00.000001Z", row.get(16).asInstanceOf[Timestamp].toInstant.toString)
          assertEquals("2015-05-20T12:34:56.001", row.get(17).toString)
          assertEquals("2017-07-07T07:07:07.000001", row.get(18).toString)
        } else {
          assertEquals("2019-12-31T23:59:59.999Z", row.get(15).asInstanceOf[Timestamp].toInstant.toString)
          assertEquals("2020-06-01T11:59:59.999999Z", row.get(16).asInstanceOf[Timestamp].toInstant.toString)
          assertEquals("2015-05-20T12:34:55.999", row.get(17).toString)
          assertEquals("2017-07-07T07:07:06.999999", row.get(18).toString)
        }
      }
      assertEquals(10, df.filter("ts_millis > timestamp('2020-01-01 00:00:00Z')").count())
      assertEquals(10, df.filter("ts_millis < timestamp('2020-01-01 00:00:00Z')").count())
      assertEquals(0, df.filter("ts_millis > timestamp('2020-01-01 00:00:00.001Z')").count())
      assertEquals(0, df.filter("ts_millis < timestamp('2019-12-31 23:59:59.999Z')").count())

      assertEquals(10, df.filter("ts_micros > timestamp('2020-06-01 12:00:00Z')").count())
      assertEquals(10, df.filter("ts_micros < timestamp('2020-06-01 12:00:00Z')").count())
      assertEquals(0, df.filter("ts_micros > timestamp('2020-06-01 12:00:00.000001Z')").count())
      assertEquals(0, df.filter("ts_micros < timestamp('2020-06-01 11:59:59.999999Z')").count())

      assertEquals(10, df.filter("local_ts_millis > CAST('2015-05-20 12:34:56' AS TIMESTAMP_NTZ)").count())
      assertEquals(10, df.filter("local_ts_millis < CAST('2015-05-20 12:34:56' AS TIMESTAMP_NTZ)").count())
      assertEquals(0, df.filter("local_ts_millis > CAST('2015-05-20 12:34:56.001' AS TIMESTAMP_NTZ)").count())
      assertEquals(0, df.filter("local_ts_millis < CAST('2015-05-20 12:34:55.999' AS TIMESTAMP_NTZ)").count())

      assertEquals(10, df.filter("local_ts_micros > CAST('2017-07-07 07:07:07' AS TIMESTAMP_NTZ)").count())
      assertEquals(10, df.filter("local_ts_micros < CAST('2017-07-07 07:07:07' AS TIMESTAMP_NTZ)").count())
      assertEquals(0, df.filter("local_ts_micros > CAST('2017-07-07 07:07:07.000001' AS TIMESTAMP_NTZ)").count())
      assertEquals(0, df.filter("local_ts_micros < CAST('2017-07-07 07:07:06.999999' AS TIMESTAMP_NTZ)").count())
    } finally {
      spark.conf.set("spark.sql.session.timeZone", prevTimezone)
      if (HoodieSparkUtils.isSpark3_3) {
        if (propertyValue == null) {
          System.clearProperty("spark.testing")
        } else {
          System.setProperty("spark.testing", propertyValue)
        }
      }
    }
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
    val commit1Time = metaClient.getActiveTimeline.lastInstant().get().getTimestamp

    val dataGen2 = new HoodieTestDataGenerator(Array("2022-01-02"))
    val records2 = recordsToStrings(dataGen2.generateInserts("002", 60)).asScala.toSeq
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)
    val commit2Time = metaClient.reloadActiveTimeline.lastInstant().get().getTimestamp

    val records3 = recordsToStrings(dataGen2.generateUniqueUpdates("003", 20)).asScala.toSeq
    val inputDF3 = spark.read.json(spark.sparkContext.parallelize(records3, 2))
    inputDF3.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)
    val commit3Time = metaClient.reloadActiveTimeline.lastInstant().get().getTimestamp

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
    // TODO(HUDI-3204) we have to revert this to pre-existing behavior from 0.10
    if (enableFileIndex) {
      assertEquals(readOptimizedQueryRes.where("partition = '2022/01/01'").count, 50)
      assertEquals(readOptimizedQueryRes.where("partition = '2022/01/02'").count, 60)
    } else {
      assertEquals(readOptimizedQueryRes.where("partition = '2022-01-01'").count, 50)
      assertEquals(readOptimizedQueryRes.where("partition = '2022-01-02'").count, 60)
    }

    // incremental query
    val incrementalQueryRes = spark.read.format("hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, commit2Time)
      .option(DataSourceReadOptions.END_INSTANTTIME.key, commit3Time)
      .load(basePath)
    assertEquals(incrementalQueryRes.where("partition = '2022-01-01'").count, 0)
    assertEquals(incrementalQueryRes.where("partition = '2022-01-02'").count, 20)
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
    val precombineField = "col3"
    val recordKeyField = "key"
    val dataField = "age"

    val options = Map[String, String](
      DataSourceWriteOptions.TABLE_TYPE.key -> HoodieTableType.MERGE_ON_READ.name,
      DataSourceWriteOptions.OPERATION.key -> UPSERT_OPERATION_OPT_VAL,
      DataSourceWriteOptions.PRECOMBINE_FIELD.key -> precombineField,
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
      .withColumn(precombineField, expr(recordKeyField))
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
      .withColumn(precombineField, expr(recordKeyField))
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
    client.compact(compactionInstant)
    client.close()

    // Third batch with all updates
    // Deltacommit4 (DC4, completed), updating fg1
    // .fg1_c3.log (from DC4) is written to storage
    // For record key "0", the row is (0, 0, 3000)
    val thirdDf = spark.range(0, 10).toDF(recordKeyField)
      .withColumn(precombineField, expr(recordKeyField))
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
  @CsvSource(value = Array("true,6", "false,6"))
  def testSnapshotQueryAfterInflightDeltaCommit(enableFileIndex: Boolean, tableVersion: Int): Unit = {
    if (HoodieSparkUtils.gteqSpark3_4) {
      val (tableName, tablePath) = ("hoodie_mor_snapshot_read_test_table", s"${basePath}_mor_test_table")
      val orderingFields = "col3"
      val recordKeyField = "key"
      val dataField = "age"

      val options = Map[String, String](
        DataSourceWriteOptions.TABLE_TYPE.key -> HoodieTableType.MERGE_ON_READ.name,
        DataSourceWriteOptions.OPERATION.key -> UPSERT_OPERATION_OPT_VAL,
        DataSourceWriteOptions.PRECOMBINE_FIELD.key -> orderingFields,
        DataSourceWriteOptions.RECORDKEY_FIELD.key -> recordKeyField,
        DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "",
        DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
        HoodieWriteConfig.TBL_NAME.key -> tableName,
        "hoodie.insert.shuffle.parallelism" -> "1",
        "hoodie.upsert.shuffle.parallelism" -> "1")
      val pathForQuery = getPathForROQuery(tablePath, !enableFileIndex, 0)

      var (writeOpts, readOpts) = getWriterReaderOpts(HoodieRecordType.AVRO, options, enableFileIndex)
      writeOpts = writeOpts ++ Map(
        HoodieTableConfig.VERSION.key() -> tableVersion.toString)

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
      val files = storage.listDirectEntries(new StoragePath(s"$tablePath/.hoodie")).stream()
        .filter(JavaConversions.getPredicate((f: StoragePathInfo) => f.getPath.getName.contains(metaClient.getActiveTimeline.lastInstant().get().getTimestamp)
          && !f.getPath.getName.contains("inflight")
          && !f.getPath.getName.contains("requested")))
        .collect(Collectors.toList[StoragePathInfo]).asScala
      assertEquals(1, files.size)
      storage.deleteFile(files.head.getPath)

      // verify snapshot query returns data written using firstDf
      assertEquals(10, snapshotDf.count())
      assertEquals(
        1000L,
        snapshotDf.where(col(recordKeyField) === 0).select(dataField).collect()(0).getLong(0))
    }
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
      .load(basePath + "/*/*/*/*")

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

  @Test
  def testMergerStrategySet(): Unit = {
    val (writeOpts, _) = getWriterReaderOpts()
    val input = recordsToStrings(dataGen.generateInserts("000", 1)).asScala
    val inputDf= spark.read.json(spark.sparkContext.parallelize(input.toSeq, 1))
    val mergerStrategyName = "example_merger_strategy"
    inputDf.write.format("hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, "MERGE_ON_READ")
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.RECORD_MERGER_STRATEGY.key(), mergerStrategyName)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    metaClient = createMetaClient(spark, basePath)
    assertEquals(metaClient.getTableConfig.getRecordMergerStrategy, mergerStrategyName)
  }
}
