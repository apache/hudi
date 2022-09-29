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

import org.apache.hadoop.fs.Path
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.HoodieConversionUtils.toJavaOption
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.{DefaultHoodieRecordPayload, HoodieRecordPayload, HoodieTableType, OverwriteWithLatestAvroPayload}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.common.util
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.index.HoodieIndex.IndexType
import org.apache.hudi.keygen.NonpartitionedKeyGenerator
import org.apache.hudi.keygen.constant.KeyGeneratorOptions.Config
import org.apache.hudi.table.action.compact.CompactionTriggerStrategy
import org.apache.hudi.testutils.{DataSourceTestUtils, HoodieClientTestBase}
import org.apache.hudi.util.JFunction
import org.apache.hudi.{DataSourceReadOptions, DataSourceUtils, DataSourceWriteOptions, HoodieDataSourceHelpers, SparkDatasetMixin}
import org.apache.log4j.LogManager
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.types.BooleanType
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

import java.util.function.Consumer
import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConverters._

/**
 * Tests on Spark DataSource for MOR table.
 */
class TestMORDataSource extends HoodieClientTestBase with SparkDatasetMixin {

  var spark: SparkSession = null
  private val log = LogManager.getLogger(classOf[TestMORDataSource])
  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
  )

  val verificationCol: String = "driver"
  val updatedVerificationVal: String = "driver_update"

  @BeforeEach override def setUp() {
    setTableName("hoodie_test")
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initFileSystem()
  }

  @AfterEach override def tearDown() = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
  }

  override def getSparkSessionExtensionsInjector: util.Option[Consumer[SparkSessionExtensions]] =
    toJavaOption(
      Some(
        JFunction.toJava((receiver: SparkSessionExtensions) => new HoodieSparkSessionExtension().apply(receiver)))
    )

  @Test def testCount() {
    // First Operation:
    // Producing parquet files to three default partitions.
    // SNAPSHOT view on MOR table with parquet files only.
    val records1 = recordsToStrings(dataGen.generateInserts("001", 100)).asScala
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
      .option("hoodie.compact.inline", "false") // else fails due to compaction & deltacommit instant times being same
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))
    val hudiSnapshotDF1 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    assertEquals(100, hudiSnapshotDF1.count()) // still 100, since we only updated

    // Second Operation:
    // Upsert the update to the default partitions with duplicate records. Produced a log file for each parquet.
    // SNAPSHOT view should read the log files only with the latest commit time.
    val records2 = recordsToStrings(dataGen.generateUniqueUpdates("002", 100)).asScala
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val hudiSnapshotDF2 = spark.read.format("org.apache.hudi")
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
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, "000")
      .option(DataSourceReadOptions.END_INSTANTTIME.key, "001")
      .load(basePath)
    assertEquals(0, emptyIncDF.count())

    // Unmerge
    val hudiSnapshotSkipMergeDF2 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .option(DataSourceReadOptions.REALTIME_MERGE.key, DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    assertEquals(200, hudiSnapshotSkipMergeDF2.count())
    assertEquals(100, hudiSnapshotSkipMergeDF2.select("_hoodie_record_key").distinct().count())
    assertEquals(200, hudiSnapshotSkipMergeDF2.join(hudiSnapshotDF2, Seq("_hoodie_record_key"), "left").count())

    // Test Read Optimized Query on MOR table
    val hudiRODF2 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    assertEquals(100, hudiRODF2.count())

    // Third Operation:
    // Upsert another update to the default partitions with 50 duplicate records. Produced the second log file for each parquet.
    // SNAPSHOT view should read the latest log files.
    val records3 = recordsToStrings(dataGen.generateUniqueUpdates("003", 50)).asScala
    val inputDF3: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records3, 2))
    inputDF3.write.format("org.apache.hudi")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val hudiSnapshotDF3 = spark.read.format("org.apache.hudi")
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
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, commit2Time)
      .load(basePath)
    assertEquals(50, hudiIncDF4.count())

    // skip merge incremental view
    // including commit 2 and commit 3
    val hudiIncDF4SkipMerge = spark.read.format("org.apache.hudi")
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
    val records4 = recordsToStrings(newDataGen.generateInserts("004", 100)).asScala
    val inputDF4: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records4, 2))
    inputDF4.write.format("org.apache.hudi")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val hudiSnapshotDF4 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    // 200, because we insert 100 records to a new partition
    assertEquals(200, hudiSnapshotDF4.count())
    assertEquals(100,
      hudiSnapshotDF1.join(hudiSnapshotDF4, Seq("_hoodie_record_key"), "inner").count())

    // Incremental query, 50 from log file, 100 from base file of the new partition.
    val hudiIncDF5 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, commit2Time)
      .load(basePath)
    assertEquals(150, hudiIncDF5.count())

    // Fifth Operation:
    // Upsert records to the new partition. Produced a newer version of parquet file.
    // SNAPSHOT view should read the latest log files from the default partition
    // and the latest parquet from the new partition.
    val records5 = recordsToStrings(newDataGen.generateUniqueUpdates("005", 50)).asScala
    val inputDF5: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records5, 2))
    inputDF5.write.format("org.apache.hudi")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val commit5Time = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    val hudiSnapshotDF5 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    assertEquals(200, hudiSnapshotDF5.count())

    // Sixth Operation:
    // Insert 2 records and trigger compaction.
    val records6 = recordsToStrings(newDataGen.generateInserts("006", 2)).asScala
    val inputDF6: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records6, 2))
    inputDF6.write.format("org.apache.hudi")
      .options(commonOpts)
      .option("hoodie.compact.inline", "true")
      .mode(SaveMode.Append)
      .save(basePath)
    val commit6Time = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    val hudiSnapshotDF6 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/2020/01/10/*")
    assertEquals(102, hudiSnapshotDF6.count())
    val hudiIncDF6 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, commit5Time)
      .option(DataSourceReadOptions.END_INSTANTTIME.key, commit6Time)
      .load(basePath)
    // even though compaction updated 150 rows, since preserve commit metadata is true, they won't be part of incremental query.
    // inserted 2 new row
    assertEquals(2, hudiIncDF6.count())
  }

  @Test
  def testPayloadDelete() {
    // First Operation:
    // Producing parquet files to three default partitions.
    // SNAPSHOT view on MOR table with parquet files only.
    val records1 = recordsToStrings(dataGen.generateInserts("001", 100)).asScala
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
      .option("hoodie.compact.inline", "false") // else fails due to compaction & deltacommit instant times being same
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))
    val hudiSnapshotDF1 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    assertEquals(100, hudiSnapshotDF1.count()) // still 100, since we only updated

    // Second Operation:
    // Upsert 50 delete records
    // Snopshot view should only read 50 records
    val records2 = recordsToStrings(dataGen.generateUniqueDeleteRecords("002", 50)).asScala
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val hudiSnapshotDF2 = spark.read.format("org.apache.hudi")
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
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .option(DataSourceReadOptions.REALTIME_MERGE.key, DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    assertEquals(100, hudiSnapshotDF2Unmerge.count())

    // incremental query, read 50 delete records from log file and get 0 count.
    val hudiIncDF1 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, commit2Time)
      .load(basePath)
    assertEquals(0, hudiIncDF1.count())

    // Third Operation:
    // Upsert 50 delete records to delete the reset
    // Snopshot view should read 0 record
    val records3 = recordsToStrings(dataGen.generateUniqueDeleteRecords("003", 50)).asScala
    val inputDF3: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records3, 2))
    inputDF3.write.format("org.apache.hudi")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val hudiSnapshotDF3 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    assertEquals(0, hudiSnapshotDF3.count()) // 100 records were deleted, 0 record to load
  }

  @Test
  def testPrunedFiltered() {
    // First Operation:
    // Producing parquet files to three default partitions.
    // SNAPSHOT view on MOR table with parquet files only.

    // Overriding the partition-path field
    val opts = commonOpts + (DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition_path")

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
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    val hudiIncDF1 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, "000")
      .load(basePath)
    val hudiIncDF1Skipmerge = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.REALTIME_MERGE.key, DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, "000")
      .load(basePath)
    val hudiIncDF2 = spark.read.format("org.apache.hudi")
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

    val record3 = dataGen.generateUpdatesWithTS("003", hoodieRecords1, -1)
    val inputDF3 = toDataset(spark, record3)
    inputDF3.write.format("org.apache.hudi").options(opts)
      .mode(SaveMode.Append).save(basePath)

    val hudiSnapshotDF3 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/*/*/*/*")

    verifyShow(hudiSnapshotDF3);

    assertEquals(100, hudiSnapshotDF3.count())
    assertEquals(0, hudiSnapshotDF3.filter("rider = 'rider-003'").count())
  }

  @Test
  def testVectorizedReader() {
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", true)
    assertTrue(spark.conf.get("spark.sql.parquet.enableVectorizedReader").toBoolean)
    // Vectorized Reader will only be triggered with AtomicType schema,
    // which is not null, UDTs, arrays, structs, and maps.
    val schema = HoodieTestDataGenerator.SHORT_TRIP_SCHEMA
    val records1 = recordsToStrings(dataGen.generateInsertsAsPerSchema("001", 100, schema)).asScala
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
      .option("hoodie.compact.inline", "false") // else fails due to compaction & deltacommit instant times being same
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val hudiSnapshotDF1 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    assertEquals(100, hudiSnapshotDF1.count())

    val records2 = recordsToStrings(dataGen.generateUniqueUpdatesAsPerSchema("002", 50, schema))
      .asScala
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val hudiSnapshotDF2 = spark.read.format("org.apache.hudi")
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

  @Test def testNoPrecombine() {
    // Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).asScala
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))

    val commonOptsNoPreCombine = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
    )
    inputDF.write.format("hudi")
      .options(commonOptsNoPreCombine)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key(), "MERGE_ON_READ")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    spark.read.format("org.apache.hudi").load(basePath).count()
  }

  @Test
  def testPreCombineFiledForReadMOR(): Unit = {
    writeData((1, "a0", 10, 100, false))
    checkAnswer((1, "a0", 10, 100, false))

    writeData((1, "a0", 12, 99, false))
    // The value has not update, because the version 99 < 100
    checkAnswer((1, "a0", 10, 100, false))

    writeData((1, "a0", 12, 101, false))
    // The value has update
    checkAnswer((1, "a0", 12, 101, false))

    writeData((1, "a0", 14, 98, false))
    // Latest value should be ignored if preCombine honors ordering
    checkAnswer((1, "a0", 12, 101, false))

    writeData((1, "a0", 16, 97, true))
    // Ordering value will be honored, the delete record is considered as obsolete
    // because it has smaller version number (97 < 101)
    checkAnswer((1, "a0", 12, 101, false))

    writeData((1, "a0", 18, 96, false))
    // Ordering value will be honored, the data record is considered as obsolete
    // because it has smaller version number (96 < 101)
    checkAnswer((1, "a0", 12, 101, false))
  }

  private def writeData(data: (Int, String, Int, Int, Boolean)): Unit = {
    val _spark = spark
    import _spark.implicits._
    val df = Seq(data).toDF("id", "name", "value", "version", "_hoodie_is_deleted")
    df.write.format("org.apache.hudi")
      .options(commonOpts)
      // use DefaultHoodieRecordPayload here
      .option(PAYLOAD_CLASS_NAME.key, classOf[DefaultHoodieRecordPayload].getCanonicalName)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .option(RECORDKEY_FIELD.key, "id")
      .option(PRECOMBINE_FIELD.key, "version")
      .option(PARTITIONPATH_FIELD.key, "")
      .option(KEYGENERATOR_CLASS_NAME.key, classOf[NonpartitionedKeyGenerator].getName)
      .mode(SaveMode.Append)
      .save(basePath)
  }

  private def checkAnswer(expect: (Int, String, Int, Int, Boolean)): Unit = {
    val readDf = spark.read.format("org.apache.hudi")
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
  @CsvSource(Array("true,false", "true,true", "false,true", "false,false"))
  def testQueryMORWithBasePathAndFileIndex(partitionEncode: Boolean, isMetadataEnabled: Boolean): Unit = {
    val N = 20
    // Test query with partition prune if URL_ENCODE_PARTITIONING has enable
    val records1 = dataGen.generateInsertsContainsAllPartitions("000", N)
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records1).asScala, 2))
    inputDF1.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.URL_ENCODE_PARTITIONING.key, partitionEncode)
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val commitInstantTime1 = HoodieDataSourceHelpers.latestCommit(fs, basePath)

    val countIn20160315 = records1.asScala.count(record => record.getPartitionPath == "2016/03/15")
    // query the partition by filter
    val count1 = spark.read.format("hudi")
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .load(basePath)
      .filter("partition = '2016/03/15'")
      .count()
    assertEquals(countIn20160315, count1)

    // query the partition by path
    val partitionPath = if (partitionEncode) "2016%2F03%2F15" else "2016/03/15"
    val count2 = spark.read.format("hudi")
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .load(basePath + s"/$partitionPath")
      .count()
    assertEquals(countIn20160315, count2)

    // Second write with Append mode
    val records2 = dataGen.generateInsertsContainsAllPartitions("000", N + 1)
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records2).asScala, 2))
    inputDF2.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.URL_ENCODE_PARTITIONING.key, partitionEncode)
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .mode(SaveMode.Append)
      .save(basePath)
    // Incremental query without "*" in path
    val hoodieIncViewDF1 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, commitInstantTime1)
      .load(basePath)
    assertEquals(N + 1, hoodieIncViewDF1.count())
  }

  @ParameterizedTest
  @CsvSource(Array("true, false", "false, true", "false, false", "true, true"))
  def testMORPartitionPrune(partitionEncode: Boolean, hiveStylePartition: Boolean): Unit = {
    val partitions = Array("2021/03/01", "2021/03/02", "2021/03/03", "2021/03/04", "2021/03/05")
    val newDataGen =  new HoodieTestDataGenerator(partitions)
    val records1 = newDataGen.generateInsertsContainsAllPartitions("000", 100).asScala
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records1.asJava).asScala, 2))

    val partitionCounts = partitions.map(p => p -> records1.count(r => r.getPartitionPath == p)).toMap

    inputDF1.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.URL_ENCODE_PARTITIONING.key, partitionEncode)
      .option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key, hiveStylePartition)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val count1 = spark.read.format("hudi")
      .load(basePath)
      .filter("partition = '2021/03/01'")
      .count()
    assertEquals(partitionCounts("2021/03/01"), count1)

    val count2 = spark.read.format("hudi")
      .load(basePath)
      .filter("partition > '2021/03/01' and partition < '2021/03/03'")
      .count()
    assertEquals(partitionCounts("2021/03/02"), count2)

    val count3 = spark.read.format("hudi")
      .load(basePath)
      .filter("partition != '2021/03/01'")
      .count()
    assertEquals(records1.size - partitionCounts("2021/03/01"), count3)

    val count4 = spark.read.format("hudi")
      .load(basePath)
      .filter("partition like '2021/03/03%'")
      .count()
    assertEquals(partitionCounts("2021/03/03"), count4)

    val count5 = spark.read.format("hudi")
      .load(basePath)
      .filter("partition like '%2021/03/%'")
      .count()
    assertEquals(records1.size, count5)

    val count6 = spark.read.format("hudi")
      .load(basePath)
      .filter("partition = '2021/03/01' or partition = '2021/03/05'")
      .count()
    assertEquals(partitionCounts("2021/03/01") + partitionCounts("2021/03/05"), count6)

    val count7 = spark.read.format("hudi")
      .load(basePath)
      .filter("substr(partition, 9, 10) = '03'")
      .count()

    assertEquals(partitionCounts("2021/03/03"), count7)
  }

  @Test
  def testReadPathsForMergeOnReadTable(): Unit = {
    // Paths only baseFiles
    val records1 = dataGen.generateInserts("001", 100)
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records1).asScala, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
      .option("hoodie.compact.inline", "false") // else fails due to compaction & deltacommit instant times being same
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))
    val baseFilePath = fs.listStatus(new Path(basePath, dataGen.getPartitionPaths.head))
      .filter(_.getPath.getName.endsWith("parquet"))
      .map(_.getPath.toString)
      .mkString(",")
    val records2 = dataGen.generateUniqueDeleteRecords("002", 100)
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records2).asScala, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val hudiReadPathDF1 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .option(DataSourceReadOptions.READ_PATHS.key, baseFilePath)
      .load()

    val expectedCount1 = records1.asScala.count(record => record.getPartitionPath == dataGen.getPartitionPaths.head)
    assertEquals(expectedCount1, hudiReadPathDF1.count())

    // Paths Contains both baseFile and log files
    val logFilePath = fs.listStatus(new Path(basePath, dataGen.getPartitionPaths.head))
      .filter(_.getPath.getName.contains("log"))
      .map(_.getPath.toString)
      .mkString(",")

    val readPaths = baseFilePath + "," + logFilePath
    val hudiReadPathDF2 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .option(DataSourceReadOptions.READ_PATHS.key, readPaths)
      .load()

    assertEquals(0, hudiReadPathDF2.count())
  }

  @Test
  def testReadPathsForOnlyLogFiles(): Unit = {
    initMetaClient(HoodieTableType.MERGE_ON_READ)
    val records1 = dataGen.generateInsertsContainsAllPartitions("000", 20)
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records1).asScala, 2))
    inputDF1.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      // Use InMemoryIndex to generate log only mor table.
      .option(HoodieIndexConfig.INDEX_TYPE.key, IndexType.INMEMORY.toString)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    // There should no base file in the file list.
    assertTrue(DataSourceTestUtils.isLogFileOnly(basePath))

    val logFilePath = fs.listStatus(new Path(basePath, dataGen.getPartitionPaths.head))
      .filter(_.getPath.getName.contains("log"))
      .map(_.getPath.toString)
      .mkString(",")

    val records2 = dataGen.generateInsertsContainsAllPartitions("000", 20)
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records2).asScala, 2))
    inputDF2.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      // Use InMemoryIndex to generate log only mor table.
      .option(HoodieIndexConfig.INDEX_TYPE.key, IndexType.INMEMORY.toString)
      .mode(SaveMode.Append)
      .save(basePath)
    // There should no base file in the file list.
    assertTrue(DataSourceTestUtils.isLogFileOnly(basePath))

    val expectedCount1 = records1.asScala.count(record => record.getPartitionPath == dataGen.getPartitionPaths.head)

    val hudiReadPathDF = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .option(DataSourceReadOptions.READ_PATHS.key, logFilePath)
      .load()

    assertEquals(expectedCount1, hudiReadPathDF.count())
  }

  @Test
  def testReadLogOnlyMergeOnReadTable(): Unit = {
    initMetaClient(HoodieTableType.MERGE_ON_READ)
    val records1 = dataGen.generateInsertsContainsAllPartitions("000", 20)
    val inputDF = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records1).asScala, 2))
    inputDF.write.format("hudi")
      .options(commonOpts)
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
    assertEquals(20, spark.read.format("hudi").load(basePath).count())
  }

  @Test
  def testTempFilesCleanForClustering(): Unit = {
    val records1 = recordsToStrings(dataGen.generateInserts("001", 1000)).asScala
    val inputDF1: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
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

  @Test
  def testClusteringOnNullableColumn(): Unit = {
    val records1 = recordsToStrings(dataGen.generateInserts("001", 1000)).asScala
    val inputDF1: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records1, 2))
      .withColumn("cluster_id", when(expr("end_lon < 0.2 "), lit(null).cast("string"))
          .otherwise(col("_row_key")))
      .withColumn("struct_cluster_col", when(expr("end_lon < 0.1"), lit(null))
          .otherwise(struct(col("cluster_id"), col("_row_key"))))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
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

  @Test
  def testHoodieIsDeletedMOR(): Unit =  {
    val numRecords = 100
    val numRecordsToDelete = 2
    val schema = HoodieTestDataGenerator.SHORT_TRIP_SCHEMA
    val records0 = recordsToStrings(dataGen.generateInsertsAsPerSchema("000", numRecords, schema)).asScala
    val inputDF0 = spark.read.json(spark.sparkContext.parallelize(records0, 2))
    inputDF0.write.format("org.apache.hudi")
      .options(commonOpts)
      .option("hoodie.compact.inline", "false")
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val snapshotDF0 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    assertEquals(numRecords, snapshotDF0.count())

    val df1 = snapshotDF0.limit(numRecordsToDelete)
    val dropDf = df1.drop(df1.columns.filter(_.startsWith("_hoodie_")): _*)

    val df2 = dropDf.withColumn("_hoodie_is_deleted", lit(true).cast(BooleanType))
    df2.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val snapshotDF2 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    assertEquals(numRecords - numRecordsToDelete, snapshotDF2.count())
  }

  /**
   * This tests the case that query by with a specified partition condition on hudi table which is
   * different between the value of the partition field and the actual partition path,
   * like hudi table written by TimestampBasedKeyGenerator.
   *
   * For MOR table, test all the three query modes.
   */
  @Test
  def testPrunePartitionForTimestampBasedKeyGenerator(): Unit = {
    val options = commonOpts ++ Map(
      "hoodie.compact.inline" -> "false",
      DataSourceWriteOptions.TABLE_TYPE.key -> DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL,
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.TimestampBasedKeyGenerator",
      Config.TIMESTAMP_TYPE_FIELD_PROP -> "DATE_STRING",
      Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP -> "yyyy/MM/dd",
      Config.TIMESTAMP_TIMEZONE_FORMAT_PROP -> "GMT+8:00",
      Config.TIMESTAMP_INPUT_DATE_FORMAT_PROP -> "yyyy-MM-dd"
    )

    val dataGen1 = new HoodieTestDataGenerator(Array("2022-01-01"))
    val records1 = recordsToStrings(dataGen1.generateInserts("001", 50)).asScala
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(spark.sessionState.newHadoopConf)
      .build()
    val commit1Time = metaClient.getActiveTimeline.lastInstant().get().getTimestamp

    val dataGen2 = new HoodieTestDataGenerator(Array("2022-01-02"))
    val records2 = recordsToStrings(dataGen2.generateInserts("002", 60)).asScala
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)
    val commit2Time = metaClient.reloadActiveTimeline.lastInstant().get().getTimestamp

    val records3 = recordsToStrings(dataGen2.generateUniqueUpdates("003", 20)).asScala
    val inputDF3 = spark.read.json(spark.sparkContext.parallelize(records3, 2))
    inputDF3.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)
    val commit3Time = metaClient.reloadActiveTimeline.lastInstant().get().getTimestamp

    // snapshot query
    val snapshotQueryRes = spark.read.format("hudi").load(basePath)
      assertEquals(snapshotQueryRes.where(s"_hoodie_commit_time = '$commit1Time'").count, 50)
    assertEquals(snapshotQueryRes.where(s"_hoodie_commit_time = '$commit2Time'").count, 40)
    assertEquals(snapshotQueryRes.where(s"_hoodie_commit_time = '$commit3Time'").count, 20)

    assertEquals(snapshotQueryRes.where("partition = '2022-01-01'").count, 50)
    assertEquals(snapshotQueryRes.where("partition = '2022-01-02'").count, 60)

    // read_optimized query
    val readOptimizedQueryRes = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
      .load(basePath)
    // TODO(HUDI-3204) we have to revert this to pre-existing behavior from 0.10
    //assertEquals(readOptimizedQueryRes.where("partition = '2022-01-01'").count, 50)
    //assertEquals(readOptimizedQueryRes.where("partition = '2022-01-02'").count, 60)
    assertEquals(readOptimizedQueryRes.where("partition = '2022/01/01'").count, 50)
    assertEquals(readOptimizedQueryRes.where("partition = '2022/01/02'").count, 60)

    // incremental query
    val incrementalQueryRes = spark.read.format("hudi")
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
  @Test
  def testReadOptimizedQueryAfterInflightCompactionAndCompletedDeltaCommit(): Unit = {
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

    // First batch with all inserts
    // Deltacommit1 (DC1, completed), writing file group 1 (fg1)
    // fg1_dc1.parquet written to storage
    // For record key "0", the row is (0, 0, 1000)
    val firstDf = spark.range(0, 10).toDF(recordKeyField)
      .withColumn(precombineField, expr(recordKeyField))
      .withColumn(dataField, expr(recordKeyField + " + 1000"))

    firstDf.write.format("hudi")
      .options(options)
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
      .options(options)
      .mode(SaveMode.Append).save(tablePath)

    val compactionOptions = options ++ Map(
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
      mapAsJavaMap(compactionOptions)).asInstanceOf[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]]

    val compactionInstant = client.scheduleCompaction(org.apache.hudi.common.util.Option.empty()).get()

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
      .options(options)
      .mode(SaveMode.Append).save(tablePath)

    // Read-optimized query on MOR
    val roDf = spark.read.format("org.apache.hudi")
      .option(
        DataSourceReadOptions.QUERY_TYPE.key,
        DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
      .load(tablePath)

    // The base file in the first file slice, i.e., fg1_dc1.parquet, should be read only
    assertEquals(10, roDf.count())
    assertEquals(
      1000L,
      roDf.where(col(recordKeyField) === 0).select(dataField).collect()(0).getLong(0))
  }
}
