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
import org.apache.hudi.common.model.{FileSlice, HoodieTableType}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.common.testutils.{HoodieTestDataGenerator, HoodieTestTable}
import org.apache.hudi.common.util.CollectionUtils
import org.apache.hudi.common.config.HoodieStorageConfig
import org.apache.hudi.config.{HoodieClusteringConfig, HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.exception.TableNotFoundException
import org.apache.hudi.testutils.HoodieClientTestBase
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieDataSourceHelpers}
import org.apache.log4j.LogManager

import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{EnumSource, ValueSource}

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.hudi.common.config.HoodieStorageConfig

/**
 * Basic tests on the spark datasource for structured streaming sink
 */
class TestStructuredStreaming extends HoodieClientTestBase {
  private val log = LogManager.getLogger(getClass)
  var spark: SparkSession = null
  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
  )

  @BeforeEach override def setUp() {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initFileSystem()
    initTimelineService()
  }

  @AfterEach override def tearDown() = {
    cleanupTimelineService()
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
  }

  def initStreamingWriteFuture(schema: StructType, sourcePath: String, destPath: String, hudiOptions: Map[String, String]): Future[Unit] = {
    // define the source of streaming
    val streamingInput =
      spark.readStream
        .schema(schema)
        .json(sourcePath)
    Future {
      println("streaming starting")
      //'writeStream' can be called only on streaming Dataset/DataFrame
      streamingInput
        .writeStream
        .format("org.apache.hudi")
        .options(hudiOptions)
        .trigger(Trigger.ProcessingTime(100))
        .option("checkpointLocation", basePath + "/checkpoint")
        .outputMode(OutputMode.Append)
        .start(destPath)
        .awaitTermination(10000)
      println("streaming ends")
    }
  }

  def initStreamingSourceAndDestPath(sourceDirName: String, destDirName: String): (String, String) = {
    fs.delete(new Path(basePath), true)
    val sourcePath = basePath + "/" + sourceDirName
    val destPath = basePath + "/" + destDirName
    fs.mkdirs(new Path(sourcePath))
    (sourcePath, destPath)
  }

  def getOptsWithTableType(tableType: HoodieTableType): Map[String, String] = {
    commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name())
  }

  def getClusteringOpts(tableType: HoodieTableType, isInlineClustering: String,
                        isAsyncClustering: String, clusteringNumCommit: String,
                        fileMaxRecordNum: Int): Map[String, String] = {
    getOptsWithTableType(tableType) + (
      HoodieClusteringConfig.INLINE_CLUSTERING.key -> isInlineClustering,
      HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key -> clusteringNumCommit,
      DataSourceWriteOptions.ASYNC_CLUSTERING_ENABLE.key -> isAsyncClustering,
      HoodieClusteringConfig.ASYNC_CLUSTERING_MAX_COMMITS.key -> clusteringNumCommit,
      HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.key -> dataGen.getEstimatedFileSizeInBytes(fileMaxRecordNum).toString
    )
  }

  def getCompactionOpts(tableType: HoodieTableType, isAsyncCompaction: Boolean): Map[String, String] = {
    getOptsWithTableType(tableType) + (
      DataSourceWriteOptions.ASYNC_COMPACT_ENABLE.key -> isAsyncCompaction.toString,
      HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key -> "1"
    )
  }

  def structuredStreamingTestRunner(tableType: HoodieTableType, addCompactionConfigs: Boolean, isAsyncCompaction: Boolean): Unit = {
    val (sourcePath, destPath) = initStreamingSourceAndDestPath("source", "dest")
    // First chunk of data
    val records1 = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))

    // Second chunk of data
    val records2 = recordsToStrings(dataGen.generateUpdates("001", 100)).toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    val uniqueKeyCnt = inputDF2.select("_row_key").distinct().count()

    val hudiOptions = if (addCompactionConfigs) {
      getCompactionOpts(tableType, isAsyncCompaction)
    } else {
      getOptsWithTableType(tableType)
    }
    val f1 = initStreamingWriteFuture(inputDF1.schema, sourcePath, destPath, hudiOptions)

    val f2 = Future {
      inputDF1.coalesce(1).write.mode(SaveMode.Append).json(sourcePath)
      // wait for spark streaming to process one microbatch
      val currNumCommits = waitTillAtleastNCommits(fs, destPath, 1, 120, 5)
      assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, destPath, "000"))
      val commitInstantTime1 = HoodieDataSourceHelpers.latestCommit(fs, destPath)
      // Read RO View
      val hoodieROViewDF1 = spark.read.format("org.apache.hudi")
        .load(destPath + "/*/*/*/*")
      assert(hoodieROViewDF1.count() == 100)

      inputDF2.coalesce(1).write.mode(SaveMode.Append).json(sourcePath)
      // When the compaction configs are added, one more commit of the compaction is expected
      val numExpectedCommits = if (addCompactionConfigs) currNumCommits + 2 else currNumCommits + 1
      waitTillAtleastNCommits(fs, destPath, numExpectedCommits, 120, 5)

      val commitInstantTime2 = if (tableType == HoodieTableType.MERGE_ON_READ) {
        // For the records that are processed by the compaction in MOR table
        // the "_hoodie_commit_time" still reflects the latest delta commit
        latestInstant(fs, destPath, HoodieTimeline.DELTA_COMMIT_ACTION)
      } else {
        HoodieDataSourceHelpers.latestCommit(fs, destPath)
      }
      assertEquals(numExpectedCommits, HoodieDataSourceHelpers.listCommitsSince(fs, destPath, "000").size())
      // Read RO View
      val hoodieROViewDF2 = spark.read.format("org.apache.hudi")
        .load(destPath + "/*/*/*/*")
      assertEquals(100, hoodieROViewDF2.count()) // still 100, since we only updated

      // Read Incremental View
      // we have 2 commits, try pulling the first commit (which is not the latest)
      val firstCommit = HoodieDataSourceHelpers.listCommitsSince(fs, destPath, "000").get(0)
      val hoodieIncViewDF1 = spark.read.format("org.apache.hudi")
        .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
        .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, "000")
        .option(DataSourceReadOptions.END_INSTANTTIME.key, firstCommit)
        .load(destPath)
      assertEquals(100, hoodieIncViewDF1.count())
      // 100 initial inserts must be pulled
      var countsPerCommit = hoodieIncViewDF1.groupBy("_hoodie_commit_time").count().collect()
      assertEquals(1, countsPerCommit.length)
      assertEquals(firstCommit, countsPerCommit(0).get(0))

      // pull the latest commit
      val hoodieIncViewDF2 = spark.read.format("org.apache.hudi")
        .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
        .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, commitInstantTime1)
        .load(destPath)

      assertEquals(uniqueKeyCnt, hoodieIncViewDF2.count()) // 100 records must be pulled
      countsPerCommit = hoodieIncViewDF2.groupBy("_hoodie_commit_time").count().collect()
      assertEquals(1, countsPerCommit.length)
      assertEquals(commitInstantTime2, countsPerCommit(0).get(0))
    }
    Await.result(Future.sequence(Seq(f1, f2)), Duration.Inf)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testStructuredStreaming(tableType: HoodieTableType): Unit = {
    structuredStreamingTestRunner(tableType, false, false)
  }

  @throws[InterruptedException]
  private def waitTillAtleastNCommits(fs: FileSystem, tablePath: String,
                                      numCommits: Int, timeoutSecs: Int, sleepSecsAfterEachRun: Int) = {
    val beginTime = System.currentTimeMillis
    var currTime = beginTime
    val timeoutMsecs = timeoutSecs * 1000
    var numInstants = 0
    var success = false
    while ({!success && (currTime - beginTime) < timeoutMsecs}) try {
      val timeline = HoodieDataSourceHelpers.allCompletedCommitsCompactions(fs, tablePath)
      log.info("Timeline :" + timeline.getInstants.toArray)
      if (timeline.countInstants >= numCommits) {
        numInstants = timeline.countInstants
        success = true
      }
    } catch {
      case _: TableNotFoundException =>
        log.info("Got table not found exception. Retrying")
    } finally {
      if (!success) {
        Thread.sleep(sleepSecsAfterEachRun * 1000)
        currTime = System.currentTimeMillis
      }
    }
    if (!success) throw new IllegalStateException("Timed-out waiting for " + numCommits + " commits to appear in " + tablePath)
    numInstants
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testStructuredStreamingWithClustering(isAsyncClustering: Boolean): Unit = {
    val (sourcePath, destPath) = initStreamingSourceAndDestPath("source", "dest")

    def checkClusteringResult(destPath: String): Unit = {
      // check have schedule clustering and clustering file group to one
      waitTillHasCompletedReplaceInstant(destPath, 120, 1)
      metaClient.reloadActiveTimeline()
      assertEquals(1, getLatestFileGroupsFileId(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH).size)
    }

    structuredStreamingForTestClusteringRunner(sourcePath, destPath, HoodieTableType.COPY_ON_WRITE,
      !isAsyncClustering, isAsyncClustering, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, checkClusteringResult)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testStructuredStreamingWithCompaction(isAsyncCompaction: Boolean): Unit = {
    structuredStreamingTestRunner(HoodieTableType.MERGE_ON_READ, true, isAsyncCompaction)
  }

  def structuredStreamingForTestClusteringRunner(sourcePath: String, destPath: String, tableType: HoodieTableType,
                                                 isInlineClustering: Boolean, isAsyncClustering: Boolean,
                                                 partitionOfRecords: String, checkClusteringResult: String => Unit): Unit = {
    // First insert of data
    val records1 = recordsToStrings(dataGen.generateInsertsForPartition("000", 100, partitionOfRecords)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))

    // Second insert of data
    val records2 = recordsToStrings(dataGen.generateInsertsForPartition("001", 100, partitionOfRecords)).toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))

    val hudiOptions = getClusteringOpts(
      tableType, isInlineClustering.toString, isAsyncClustering.toString, "2", 100)
    val f1 = initStreamingWriteFuture(inputDF1.schema, sourcePath, destPath, hudiOptions)

    val f2 = Future {
      inputDF1.coalesce(1).write.mode(SaveMode.Append).json(sourcePath)
      // wait for spark streaming to process one microbatch
      var currNumCommits = waitTillAtleastNCommits(fs, destPath, 1, 120, 5)
      assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, destPath, "000"))

      inputDF2.coalesce(1).write.mode(SaveMode.Append).json(sourcePath)
      // wait for spark streaming to process second microbatch
      currNumCommits = waitTillAtleastNCommits(fs, destPath, currNumCommits + 1, 120, 5)

      // Wait for the clustering to finish
      this.metaClient = HoodieTableMetaClient.builder().setConf(fs.getConf).setBasePath(destPath)
        .setLoadActiveTimelineOnLoad(true).build()
      checkClusteringResult(destPath)

      assertEquals(3, HoodieDataSourceHelpers.listCommitsSince(fs, destPath, "000").size())
      // Check have at least one file group
      assertTrue(getLatestFileGroupsFileId(partitionOfRecords).size > 0)

      // Validate data after clustering
      val hoodieROViewDF2 = spark.read.format("org.apache.hudi")
        .load(destPath + "/*/*/*/*")
      assertEquals(200, hoodieROViewDF2.count())
      val countsPerCommit = hoodieROViewDF2.groupBy("_hoodie_commit_time").count().collect()
      assertEquals(2, countsPerCommit.length)
      val commitInstantTime2 = latestInstant(fs, destPath, HoodieTimeline.COMMIT_ACTION)
      assertEquals(commitInstantTime2, countsPerCommit.maxBy(row => row.getAs[String](0)).get(0))
    }
    Await.result(Future.sequence(Seq(f1, f2)), Duration.Inf)
  }

  private def getLatestFileGroupsFileId(partition: String):Array[String] = {
    getHoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline,
      HoodieTestTable.of(metaClient).listAllBaseFiles())
    tableView.getLatestFileSlices(partition)
      .toArray().map(slice => slice.asInstanceOf[FileSlice].getFileGroupId.getFileId)
  }

  @throws[InterruptedException]
  private def waitTillHasCompletedReplaceInstant(tablePath: String,
                                                 timeoutSecs: Int, sleepSecsAfterEachRun: Int) = {
    val beginTime = System.currentTimeMillis
    var currTime = beginTime
    val timeoutMsecs = timeoutSecs * 1000
    var success = false
    while ({!success && (currTime - beginTime) < timeoutMsecs}) try {
      this.metaClient.reloadActiveTimeline()
      val completeReplaceSize = this.metaClient.getActiveTimeline.getCompletedReplaceTimeline().getInstants.toArray.size
      println("completeReplaceSize:" + completeReplaceSize)
      if (completeReplaceSize > 0) {
        success = true
      }
    } catch {
      case te: TableNotFoundException =>
        log.info("Got table not found exception. Retrying")
    } finally {
      Thread.sleep(sleepSecsAfterEachRun * 1000)
      currTime = System.currentTimeMillis
    }
    if (!success) throw new IllegalStateException("Timed-out waiting for completing replace instant appear in " + tablePath)
  }

  private def latestInstant(fs: FileSystem, basePath: String, instantAction: String): String = {
    val metaClient = HoodieTableMetaClient.builder
      .setConf(fs.getConf).setBasePath(basePath).setLoadActiveTimelineOnLoad(true).build
    metaClient.getActiveTimeline
      .getTimelineOfActions(CollectionUtils.createSet(instantAction))
      .filterCompletedInstants
      .lastInstant
      .get.getTimestamp
  }
}
