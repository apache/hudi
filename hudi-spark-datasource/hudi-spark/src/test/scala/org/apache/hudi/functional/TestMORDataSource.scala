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

import org.apache.hudi.DataSourceWriteOptions.{KEYGENERATOR_CLASS_OPT_KEY, PARTITIONPATH_FIELD_OPT_KEY, PAYLOAD_CLASS_OPT_KEY, PRECOMBINE_FIELD_OPT_KEY, RECORDKEY_FIELD_OPT_KEY}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.DefaultHoodieRecordPayload
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieDataSourceHelpers}
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.keygen.NonpartitionedKeyGenerator
import org.apache.hudi.testutils.HoodieClientTestBase
import org.apache.log4j.LogManager
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import scala.collection.JavaConversions._

/**
 * Tests on Spark DataSource for MOR table.
 */
class TestMORDataSource extends HoodieClientTestBase {

  var spark: SparkSession = null
  private val log = LogManager.getLogger(classOf[TestMORDataSource])
  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY -> "partition",
    DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> "timestamp",
    HoodieWriteConfig.TABLE_NAME -> "hoodie_test"
  )

  val verificationCol: String = "driver"
  val updatedVerificationVal: String = "driver_update"

  @BeforeEach override def setUp() {
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

  @Test def testMergeOnReadStorage() {

    val fs = FSUtils.getFs(basePath, spark.sparkContext.hadoopConfiguration)
    // Bulk Insert Operation
    val records1 = recordsToStrings(dataGen.generateInserts("001", 100)).toList
    val inputDF1: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
      .option("hoodie.compact.inline", "false") // else fails due to compaction & deltacommit instant times being same
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))

    // Read RO View
    val hudiRODF1 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    assertEquals(100, hudiRODF1.count()) // still 100, since we only updated
    val insertCommitTime = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    val insertCommitTimes = hudiRODF1.select("_hoodie_commit_time").distinct().collectAsList().map(r => r.getString(0)).toList
    assertEquals(List(insertCommitTime), insertCommitTimes)

    // Upsert operation without Hudi metadata columns
    val records2 = recordsToStrings(dataGen.generateUniqueUpdates("002", 100)).toList
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)

    // Read Snapshot query
    val updateCommitTime = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    val hudiSnapshotDF2 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    val updateCommitTimes = hudiSnapshotDF2.select("_hoodie_commit_time").distinct().collectAsList().map(r => r.getString(0)).toList
    assertEquals(List(updateCommitTime), updateCommitTimes)

    // Upsert based on the written table with Hudi metadata columns
    val verificationRowKey = hudiSnapshotDF2.limit(1).select("_row_key").first.getString(0)
    val inputDF3 = hudiSnapshotDF2.filter(col("_row_key") === verificationRowKey).withColumn(verificationCol, lit(updatedVerificationVal))

    inputDF3.write.format("org.apache.hudi")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)

    val hudiSnapshotDF3 = spark.read.format("hudi").load(basePath + "/*/*/*/*")
    assertEquals(100, hudiSnapshotDF3.count())
    assertEquals(updatedVerificationVal, hudiSnapshotDF3.filter(col("_row_key") === verificationRowKey).select(verificationCol).first.getString(0))
  }

  @Test def testCount() {
    // First Operation:
    // Producing parquet files to three default partitions.
    // SNAPSHOT view on MOR table with parquet files only.
    val records1 = recordsToStrings(dataGen.generateInserts("001", 100)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
      .option("hoodie.compact.inline", "false") // else fails due to compaction & deltacommit instant times being same
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))
    val hudiSnapshotDF1 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    assertEquals(100, hudiSnapshotDF1.count()) // still 100, since we only updated

    // Second Operation:
    // Upsert the update to the default partitions with duplicate records. Produced a log file for each parquet.
    // SNAPSHOT view should read the log files only with the latest commit time.
    val records2 = recordsToStrings(dataGen.generateUniqueUpdates("002", 100)).toList
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val hudiSnapshotDF2 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
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
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "000")
      .option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY, commit1Time)
      .load(basePath)
    assertEquals(100, hudiIncDF1.count())
    assertEquals(1, hudiIncDF1.select("_hoodie_commit_time").distinct().count())
    assertEquals(commit1Time, hudiIncDF1.select("_hoodie_commit_time").head().get(0).toString)
    hudiIncDF1.show(1)
    // log file only
    val hudiIncDF2 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, commit1Time)
      .option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY, commit2Time)
      .load(basePath)
    assertEquals(100, hudiIncDF2.count())
    assertEquals(1, hudiIncDF2.select("_hoodie_commit_time").distinct().count())
    assertEquals(commit2Time, hudiIncDF2.select("_hoodie_commit_time").head().get(0).toString)
    hudiIncDF2.show(1)

    // base file + log file
    val hudiIncDF3 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "000")
      .option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY, commit2Time)
      .load(basePath)
    assertEquals(100, hudiIncDF3.count())
    // log file being load
    assertEquals(1, hudiIncDF3.select("_hoodie_commit_time").distinct().count())
    assertEquals(commit2Time, hudiIncDF3.select("_hoodie_commit_time").head().get(0).toString)

    // Unmerge
    val hudiSnapshotSkipMergeDF2 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .option(DataSourceReadOptions.REALTIME_MERGE_OPT_KEY, DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    assertEquals(200, hudiSnapshotSkipMergeDF2.count())
    assertEquals(100, hudiSnapshotSkipMergeDF2.select("_hoodie_record_key").distinct().count())
    assertEquals(200, hudiSnapshotSkipMergeDF2.join(hudiSnapshotDF2, Seq("_hoodie_record_key"), "left").count())

    // Test Read Optimized Query on MOR table
    val hudiRODF2 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    assertEquals(100, hudiRODF2.count())

    // Third Operation:
    // Upsert another update to the default partitions with 50 duplicate records. Produced the second log file for each parquet.
    // SNAPSHOT view should read the latest log files.
    val records3 = recordsToStrings(dataGen.generateUniqueUpdates("003", 50)).toList
    val inputDF3: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records3, 2))
    inputDF3.write.format("org.apache.hudi")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val hudiSnapshotDF3 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
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
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, commit2Time)
      .load(basePath)
    assertEquals(50, hudiIncDF4.count())

    // skip merge incremental view
    // including commit 2 and commit 3
    val hudiIncDF4SkipMerge = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "000")
      .option(DataSourceReadOptions.REALTIME_MERGE_OPT_KEY, DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL)
      .load(basePath)
    assertEquals(200, hudiIncDF4SkipMerge.count())

    // Fourth Operation:
    // Insert records to a new partition. Produced a new parquet file.
    // SNAPSHOT view should read the latest log files from the default partition and parquet from the new partition.
    val partitionPaths = new Array[String](1)
    partitionPaths.update(0, "2020/01/10")
    val newDataGen = new HoodieTestDataGenerator(partitionPaths)
    val records4 = recordsToStrings(newDataGen.generateInserts("004", 100)).toList
    val inputDF4: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records4, 2))
    inputDF4.write.format("org.apache.hudi")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val hudiSnapshotDF4 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    // 200, because we insert 100 records to a new partition
    assertEquals(200, hudiSnapshotDF4.count())
    assertEquals(100,
      hudiSnapshotDF1.join(hudiSnapshotDF4, Seq("_hoodie_record_key"), "inner").count())

    // Incremental query, 50 from log file, 100 from base file of the new partition.
    val hudiIncDF5 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, commit2Time)
      .load(basePath)
    assertEquals(150, hudiIncDF5.count())

    // Fifth Operation:
    // Upsert records to the new partition. Produced a newer version of parquet file.
    // SNAPSHOT view should read the latest log files from the default partition
    // and the latest parquet from the new partition.
    val records5 = recordsToStrings(newDataGen.generateUniqueUpdates("005", 50)).toList
    val inputDF5: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records5, 2))
    inputDF5.write.format("org.apache.hudi")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val commit5Time = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    val hudiSnapshotDF5 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    assertEquals(200, hudiSnapshotDF5.count())

    // Sixth Operation:
    // Insert 2 records and trigger compaction.
    val records6 = recordsToStrings(newDataGen.generateInserts("006", 2)).toList
    val inputDF6: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records6, 2))
    inputDF6.write.format("org.apache.hudi")
      .options(commonOpts)
      .option("hoodie.compact.inline", "true")
      .mode(SaveMode.Append)
      .save(basePath)
    val commit6Time = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    val hudiSnapshotDF6 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/2020/01/10/*")
    assertEquals(102, hudiSnapshotDF6.count())
    val hudiIncDF6 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, commit5Time)
      .option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY, commit6Time)
      .load(basePath)
    // compaction updated 150 rows + inserted 2 new row
    assertEquals(152, hudiIncDF6.count())
  }

  @Test
  def testPayloadDelete() {
    // First Operation:
    // Producing parquet files to three default partitions.
    // SNAPSHOT view on MOR table with parquet files only.
    val records1 = recordsToStrings(dataGen.generateInserts("001", 100)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
      .option("hoodie.compact.inline", "false") // else fails due to compaction & deltacommit instant times being same
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))
    val hudiSnapshotDF1 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    assertEquals(100, hudiSnapshotDF1.count()) // still 100, since we only updated

    // Second Operation:
    // Upsert 50 delete records
    // Snopshot view should only read 50 records
    val records2 = recordsToStrings(dataGen.generateUniqueDeleteRecords("002", 50)).toList
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val hudiSnapshotDF2 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    assertEquals(50, hudiSnapshotDF2.count()) // 50 records were deleted
    assertEquals(hudiSnapshotDF2.select("_hoodie_commit_time").distinct().count(), 1)
    val commit1Time = hudiSnapshotDF1.select("_hoodie_commit_time").head().get(0).toString
    val commit2Time = hudiSnapshotDF2.select("_hoodie_commit_time").head().get(0).toString
    assertTrue(commit1Time.equals(commit2Time))
    assertEquals(50, hudiSnapshotDF2.join(hudiSnapshotDF1, Seq("_hoodie_record_key"), "left").count())

    // unmerge query, skip the delete records
    val hudiSnapshotDF2Unmerge = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .option(DataSourceReadOptions.REALTIME_MERGE_OPT_KEY, DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    assertEquals(100, hudiSnapshotDF2Unmerge.count())

    // incremental query, read 50 delete records from log file and get 0 count.
    val hudiIncDF1 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, commit2Time)
      .load(basePath)
    assertEquals(0, hudiIncDF1.count())

    // Third Operation:
    // Upsert 50 delete records to delete the reset
    // Snopshot view should read 0 record
    val records3 = recordsToStrings(dataGen.generateUniqueDeleteRecords("003", 50)).toList
    val inputDF3: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records3, 2))
    inputDF3.write.format("org.apache.hudi")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val hudiSnapshotDF3 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    assertEquals(0, hudiSnapshotDF3.count()) // 100 records were deleted, 0 record to load
  }

  @Test
  def testPrunedFiltered() {
    // First Operation:
    // Producing parquet files to three default partitions.
    // SNAPSHOT view on MOR table with parquet files only.
    val records1 = recordsToStrings(dataGen.generateInserts("001", 100)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
      .option("hoodie.compact.inline", "false") // else fails due to compaction & deltacommit instant times being same
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val hudiSnapshotDF1 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
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
    val records2 = recordsToStrings(dataGen.generateUniqueUpdates("002", 50))
      .toList
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val hudiSnapshotDF2 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    val hudiIncDF1 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "000")
      .load(basePath)
    val hudiIncDF1Skipmerge = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.REALTIME_MERGE_OPT_KEY, DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "000")
      .load(basePath)
    val hudiIncDF2 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, commit1Time)
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
  }

  @Test
  def testVectorizedReader() {
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", true)
    assertTrue(spark.conf.get("spark.sql.parquet.enableVectorizedReader").toBoolean)
    // Vectorized Reader will only be triggered with AtomicType schema,
    // which is not null, UDTs, arrays, structs, and maps.
    val schema = HoodieTestDataGenerator.SHORT_TRIP_SCHEMA
    val records1 = recordsToStrings(dataGen.generateInsertsAsPerSchema("001", 100, schema)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
      .option("hoodie.compact.inline", "false") // else fails due to compaction & deltacommit instant times being same
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val hudiSnapshotDF1 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    assertEquals(100, hudiSnapshotDF1.count())

    val records2 = recordsToStrings(dataGen.generateUniqueUpdatesAsPerSchema("002", 50, schema))
      .toList
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val hudiSnapshotDF2 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
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

  @Test
  def testPreCombineFiledForReadMOR(): Unit = {
    writeData((1, "a0",10, 100))
    checkAnswer((1, "a0",10, 100))

    writeData((1, "a0", 12, 99))
    // The value has not update, because the version 99 < 100
    checkAnswer((1, "a0",10, 100))

    writeData((1, "a0", 12, 101))
    // The value has update
    checkAnswer((1, "a0", 12, 101))
  }

  private def writeData(data: (Int, String, Int, Int)): Unit = {
    val _spark = spark
    import _spark.implicits._
    val df = Seq(data).toDF("id", "name", "value", "version")
    df.write.format("org.apache.hudi")
      .options(commonOpts)
      // use DefaultHoodieRecordPayload here
      .option(PAYLOAD_CLASS_OPT_KEY, classOf[DefaultHoodieRecordPayload].getCanonicalName)
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .option(RECORDKEY_FIELD_OPT_KEY, "id")
      .option(PRECOMBINE_FIELD_OPT_KEY, "version")
      .option(PARTITIONPATH_FIELD_OPT_KEY, "")
      .option(KEYGENERATOR_CLASS_OPT_KEY, classOf[NonpartitionedKeyGenerator].getName)
      .mode(SaveMode.Append)
      .save(basePath)
  }

  private def checkAnswer(expect: (Int, String, Int, Int)): Unit = {
    val readDf = spark.read.format("org.apache.hudi")
      .load(basePath + "/*")
    val row1 = readDf.select("id", "name", "value", "version").take(1)(0)
    assertEquals(Row(expect.productIterator.toSeq: _*), row1)
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
    assertEquals(sampleRow.getLong(1), sampleRow.get(1))
    assertEquals(sampleRow.getString(2), sampleRow.get(2))
    assertEquals(sampleRow.getSeq(3), sampleRow.get(3))
    assertEquals(sampleRow.getStruct(4), sampleRow.get(4))
  }

  def verifyShow(df: DataFrame): Unit = {
    df.show(1)
    df.select("_hoodie_commit_seqno", "fare.amount", "fare.currency", "tip_history").show(1)
  }
}
