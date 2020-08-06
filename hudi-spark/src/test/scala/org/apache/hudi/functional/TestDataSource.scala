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
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.TableNotFoundException
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieDataSourceHelpers}
import org.apache.log4j.LogManager
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.{BeforeEach, Test}

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
 * Basic tests on the spark datasource
 */
class TestDataSource {
  private val log = LogManager.getLogger(getClass)

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

  @Test def testShortNameStorage() {
    // Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records, 2))
    inputDF.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))
  }

  @Test def testCopyOnWriteStorage() {
    // Insert Operation
    val records1 = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF1: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))
    val commitInstantTime1: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)

    // Read RO View
    val hoodieROViewDF1 = spark.read.format("org.apache.hudi")
      .load(basePath + "/*/*/*/*");
    assertEquals(100, hoodieROViewDF1.count())

    val records2 = recordsToStrings(dataGen.generateUpdates("001", 100)).toList
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    val uniqueKeyCnt = inputDF2.select("_row_key").distinct().count()

    // Upsert Operation
    inputDF2.write.format("org.apache.hudi")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)

    val commitInstantTime2: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(2, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").size())

    // Read RO View
    val hoodieROViewDF2 = spark.read.format("org.apache.hudi")
      .load(basePath + "/*/*/*/*");
    assertEquals(100, hoodieROViewDF2.count()) // still 100, since we only updated

    // Read Incremental View
    // we have 2 commits, try pulling the first commit (which is not the latest)
    val firstCommit = HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").get(0);
    val hoodieIncViewDF1 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "000")
      .option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY, firstCommit)
      .load(basePath);
    assertEquals(100, hoodieIncViewDF1.count()) // 100 initial inserts must be pulled
    var countsPerCommit = hoodieIncViewDF1.groupBy("_hoodie_commit_time").count().collect();
    assertEquals(1, countsPerCommit.length)
    assertEquals(firstCommit, countsPerCommit(0).get(0))

    // Upsert an empty dataFrame
    val emptyRecords = recordsToStrings(dataGen.generateUpdates("002", 0)).toList
    val emptyDF: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(emptyRecords, 1))
    emptyDF.write.format("org.apache.hudi")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)

    // pull the latest commit
    val hoodieIncViewDF2 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, commitInstantTime1)
      .load(basePath);

    assertEquals(uniqueKeyCnt, hoodieIncViewDF2.count()) // 100 records must be pulled
    countsPerCommit = hoodieIncViewDF2.groupBy("_hoodie_commit_time").count().collect();
    assertEquals(1, countsPerCommit.length)
    assertEquals(commitInstantTime2, countsPerCommit(0).get(0))

    // pull the latest commit within certain partitions
    val hoodieIncViewDF3 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, commitInstantTime1)
      .option(DataSourceReadOptions.INCR_PATH_GLOB_OPT_KEY, "/2016/*/*/*")
      .load(basePath);
    assertEquals(hoodieIncViewDF2.filter(col("_hoodie_partition_path").contains("2016")).count(), hoodieIncViewDF3.count())
  }

  @Test def testMergeOnReadStorage() {
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
    val hoodieROViewDF1 = spark.read.format("org.apache.hudi").load(basePath + "/*/*/*/*")
    assertEquals(100, hoodieROViewDF1.count()) // still 100, since we only updated
  }

  @Test def testDropInsertDup(): Unit = {
    val insert1Cnt = 10
    val insert2DupKeyCnt = 9
    val insert2NewKeyCnt = 2

    val totalUniqueKeyToGenerate = insert1Cnt + insert2NewKeyCnt
    val allRecords =  dataGen.generateInserts("001", totalUniqueKeyToGenerate)
    val inserts1 = allRecords.subList(0, insert1Cnt)
    val inserts2New = dataGen.generateSameKeyInserts("002", allRecords.subList(insert1Cnt, insert1Cnt + insert2NewKeyCnt))
    val inserts2Dup = dataGen.generateSameKeyInserts("002", inserts1.subList(0, insert2DupKeyCnt))

    val records1 = recordsToStrings(inserts1).toList
    val inputDF1: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val hoodieROViewDF1 = spark.read.format("org.apache.hudi")
      .load(basePath + "/*/*/*/*")
    assertEquals(insert1Cnt, hoodieROViewDF1.count())

    val commitInstantTime1 = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    val records2 = recordsToStrings(inserts2Dup ++ inserts2New).toList
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.INSERT_DROP_DUPS_OPT_KEY, "true")
      .mode(SaveMode.Append)
      .save(basePath)
    val hoodieROViewDF2 = spark.read.format("org.apache.hudi")
      .load(basePath + "/*/*/*/*")
    assertEquals(hoodieROViewDF2.count(), totalUniqueKeyToGenerate)

    val hoodieIncViewDF2 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, commitInstantTime1)
      .load(basePath)
    assertEquals(hoodieIncViewDF2.count(), insert2NewKeyCnt)
  }

  @Test
  def testStructuredStreaming(): Unit = {
    fs.delete(new Path(basePath), true)
    val sourcePath = basePath + "/source"
    val destPath = basePath + "/dest"
    fs.mkdirs(new Path(sourcePath))

    // First chunk of data
    val records1 = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF1: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records1, 2))

    // Second chunk of data
    val records2 = recordsToStrings(dataGen.generateUpdates("001", 100)).toList
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    val uniqueKeyCnt = inputDF2.select("_row_key").distinct().count()

    // define the source of streaming
    val streamingInput =
      spark.readStream
      .schema(inputDF1.schema)
      .json(sourcePath)

    val f1 = Future {
      println("streaming starting")
    //'writeStream' can be called only on streaming Dataset/DataFrame
      streamingInput
        .writeStream
        .format("org.apache.hudi")
        .options(commonOpts)
        .trigger(new ProcessingTime(100))
        .option("checkpointLocation", basePath + "/checkpoint")
        .outputMode(OutputMode.Append)
        .start(destPath)
        .awaitTermination(10000)
      println("streaming ends")
    }

    val f2 = Future {
      inputDF1.write.mode(SaveMode.Append).json(sourcePath)
      // wait for spark streaming to process one microbatch
      val currNumCommits = waitTillAtleastNCommits(fs, destPath, 1, 120, 5);
      assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, destPath, "000"))
      val commitInstantTime1: String = HoodieDataSourceHelpers.latestCommit(fs, destPath)
      // Read RO View
      val hoodieROViewDF1 = spark.read.format("org.apache.hudi")
        .load(destPath + "/*/*/*/*")
      assert(hoodieROViewDF1.count() == 100)

      inputDF2.write.mode(SaveMode.Append).json(sourcePath)
      // wait for spark streaming to process one microbatch
      waitTillAtleastNCommits(fs, destPath, currNumCommits + 1, 120, 5);
      val commitInstantTime2: String = HoodieDataSourceHelpers.latestCommit(fs, destPath)
      assertEquals(2, HoodieDataSourceHelpers.listCommitsSince(fs, destPath, "000").size())
      // Read RO View
      val hoodieROViewDF2 = spark.read.format("org.apache.hudi")
        .load(destPath + "/*/*/*/*")
      assertEquals(100, hoodieROViewDF2.count()) // still 100, since we only updated


      // Read Incremental View
      // we have 2 commits, try pulling the first commit (which is not the latest)
      val firstCommit = HoodieDataSourceHelpers.listCommitsSince(fs, destPath, "000").get(0)
      val hoodieIncViewDF1 = spark.read.format("org.apache.hudi")
        .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
        .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "000")
        .option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY, firstCommit)
        .load(destPath)
      assertEquals(100, hoodieIncViewDF1.count())
      // 100 initial inserts must be pulled
      var countsPerCommit = hoodieIncViewDF1.groupBy("_hoodie_commit_time").count().collect()
      assertEquals(1, countsPerCommit.length)
      assertEquals(firstCommit, countsPerCommit(0).get(0))

      // pull the latest commit
      val hoodieIncViewDF2 = spark.read.format("org.apache.hudi")
        .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
        .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, commitInstantTime1)
        .load(destPath)

      assertEquals(uniqueKeyCnt, hoodieIncViewDF2.count()) // 100 records must be pulled
      countsPerCommit = hoodieIncViewDF2.groupBy("_hoodie_commit_time").count().collect()
      assertEquals(1, countsPerCommit.length)
      assertEquals(commitInstantTime2, countsPerCommit(0).get(0))
    }
    Await.result(Future.sequence(Seq(f1, f2)), Duration.Inf)
  }

  @throws[InterruptedException]
  private def waitTillAtleastNCommits(fs: FileSystem, tablePath: String,
                               numCommits: Int, timeoutSecs: Int, sleepSecsAfterEachRun: Int): Int = {
    val beginTime = System.currentTimeMillis
    var currTime = beginTime
    val timeoutMsecs = timeoutSecs * 1000
    var numInstants = 0
    var success: Boolean = false
    while ({!success && (currTime - beginTime) < timeoutMsecs}) try {
      val timeline = HoodieDataSourceHelpers.allCompletedCommitsCompactions(fs, tablePath)
      log.info("Timeline :" + timeline.getInstants.toArray)
      if (timeline.countInstants >= numCommits) {
        numInstants = timeline.countInstants
        success = true
      }
      val metaClient = new HoodieTableMetaClient(fs.getConf, tablePath, true)
    } catch {
      case te: TableNotFoundException =>
        log.info("Got table not found exception. Retrying")
    } finally {
      Thread.sleep(sleepSecsAfterEachRun * 1000)
      currTime = System.currentTimeMillis
    }
    if (!success) {
      throw new IllegalStateException("Timed-out waiting for " + numCommits + " commits to appear in " + tablePath)
    }
    numInstants
  }
}
