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

import org.apache.hudi.{DataSourceReadOptions, DataSourceUtils, DataSourceWriteOptions, DefaultSparkRecordMerger, HoodieDataSourceHelpers, HoodieStreamingSink}
import org.apache.hudi.DataSourceWriteOptions.{STREAMING_CHECKPOINT_IDENTIFIER, UPSERT_OPERATION_OPT_VAL}
import org.apache.hudi.HoodieStreamingSink.SINK_CHECKPOINT_KEY
import org.apache.hudi.async.AsyncCompactService
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.common.config.{HoodieStorageConfig, TypedProperties}
import org.apache.hudi.common.engine.RecordContext
import org.apache.hudi.common.model.{FileSlice, HoodieFailedWritesCleaningPolicy, HoodieTableType, WriteConcurrencyMode}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.marker.MarkerType
import org.apache.hudi.common.table.read.BufferedRecord
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.table.view.{FileSystemViewStorageConfig, FileSystemViewStorageType}
import org.apache.hudi.common.testutils.{HoodieTestDataGenerator, HoodieTestTable, HoodieTestUtils}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings
import org.apache.hudi.common.util.{CollectionUtils, CommitUtils, MarkerUtils, Option => HOption}
import org.apache.hudi.config.{HoodieCleanConfig, HoodieClusteringConfig, HoodieCompactionConfig, HoodieLockConfig, HoodieWriteConfig}
import org.apache.hudi.core.transaction.lock.InProcessLockProvider
import org.apache.hudi.exception.{HoodieException, TableNotFoundException}
import org.apache.hudi.storage.{HoodieStorage, StoragePath}
import org.apache.hudi.testutils.{DataSourceTestUtils, HoodieSparkClientTestBase, HoodieSparkDeleteRecordMerger}

import org.apache.spark.TaskContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, StreamingQueryException, Trigger}
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.{BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, EnumSource, ValueSource}
import org.slf4j.LoggerFactory

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

/**
 * Basic tests on the spark datasource for structured streaming sink
 */
class TestStructuredStreaming extends HoodieSparkClientTestBase {
  private val log = LoggerFactory.getLogger(getClass)

  var spark: SparkSession = _

  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
  )

  @BeforeEach override def setUp() {
    super.setUp()
    spark = sqlContext.sparkSession
    // We set stop to timeout after 30s to avoid blocking things indefinitely
    spark.conf.set("spark.sql.streaming.stopTimeout", 30000)
  }

  def initWritingStreamingQuery(schema: StructType,
                                sourcePath: String,
                                destPath: String,
                                hudiOptions: Map[String, String]): StreamingQuery = {
    val streamingInput =
      spark.readStream
        .schema(schema)
        .json(sourcePath)

    streamingInput
      .writeStream
      .format("org.apache.hudi")
      .options(hudiOptions)
      .trigger(Trigger.ProcessingTime(1000))
      .option("checkpointLocation", basePath + "/checkpoint")
      .outputMode(OutputMode.Append)
      .start(destPath)
  }

  def initStreamingSourceAndDestPath(sourceDirName: String, destDirName: String): (String, String) = {
    storage.deleteDirectory(new StoragePath(basePath))
    val sourcePath = basePath + "/" + sourceDirName
    val destPath = basePath + "/" + destDirName
    storage.createDirectory(new StoragePath(sourcePath))
    (sourcePath, destPath)
  }

  /**
   * Lazy failed-writes cleaning, where failed instants are rolled back once their heartbeat expires; failed
   * micro-batches are ignored so that the query keeps running.
   */
  private val lazyFailedWritesCleaningOpts = Map(
    HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key -> WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.name,
    HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key -> HoodieFailedWritesCleaningPolicy.LAZY.name,
    HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key -> classOf[InProcessLockProvider].getName,
    STREAMING_CHECKPOINT_IDENTIFIER.key -> "streaming_writer",
    DataSourceWriteOptions.STREAMING_IGNORE_FAILED_BATCH.key -> "true",
    DataSourceWriteOptions.STREAMING_RETRY_CNT.key -> "1")

  def getOptsWithTableType(tableType: HoodieTableType): Map[String, String] = {
    commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name())
  }

  def getClusteringOpts(tableType: HoodieTableType, isInlineClustering: String,
                        isAsyncClustering: String, clusteringNumCommit: String,
                        fileMaxRecordNum: Int): Map[String, String] = {
    getOptsWithTableType(tableType) ++ Map(
      HoodieClusteringConfig.INLINE_CLUSTERING.key -> isInlineClustering,
      HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key -> clusteringNumCommit,
      DataSourceWriteOptions.ASYNC_CLUSTERING_ENABLE.key -> isAsyncClustering,
      HoodieClusteringConfig.ASYNC_CLUSTERING_MAX_COMMITS.key -> clusteringNumCommit,
      HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.key -> dataGen.getEstimatedFileSizeInBytes(fileMaxRecordNum).toString
    )
  }

  def getCompactionOpts(tableType: HoodieTableType, isAsyncCompaction: Boolean): Map[String, String] = {
    getOptsWithTableType(tableType) ++ Map(
      DataSourceWriteOptions.ASYNC_COMPACT_ENABLE.key -> isAsyncCompaction.toString,
      HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key -> "1"
    )
  }

  def structuredStreamingTestRunner(tableType: HoodieTableType, addCompactionConfigs: Boolean, isAsyncCompaction: Boolean): Unit = {
    val (sourcePath, destPath) = initStreamingSourceAndDestPath("source", "dest")
    // First chunk of data
    val records1 = recordsToStrings(dataGen.generateInserts("000", 100)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))

    // Second chunk of data
    val records2 = recordsToStrings(dataGen.generateUpdates("001", 100)).asScala.toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    val uniqueKeyCnt = inputDF2.select("_row_key").distinct().count()

    val hudiOptions = if (addCompactionConfigs) {
      getCompactionOpts(tableType, isAsyncCompaction)
    } else {
      getOptsWithTableType(tableType)
    }

    val streamingQuery = initWritingStreamingQuery(inputDF1.schema, sourcePath, destPath, hudiOptions)

    val f2 = Future {
      inputDF1.coalesce(1).write.mode(SaveMode.Append).json(sourcePath)
      // wait for spark streaming to process one microbatch
      val currNumCommits = waitTillAtleastNCommits(storage, destPath, 1, 120, 5)
      assertTrue(HoodieDataSourceHelpers.hasNewCommits(storage, destPath, "000"))
      val commitCompletionTime1 = DataSourceTestUtils.latestCommitCompletionTime(storage, destPath)
      // Read RO View
      val hoodieROViewDF1 = spark.read.format("org.apache.hudi").load(destPath)
      assert(hoodieROViewDF1.count() == 100)

      inputDF2.coalesce(1).write.mode(SaveMode.Append).json(sourcePath)
      // When the compaction configs are added, one more commit of the compaction is expected
      val numExpectedCommits = if (addCompactionConfigs) currNumCommits + 2 else currNumCommits + 1
      waitTillAtleastNCommits(storage, destPath, numExpectedCommits, 120, 5)

      val commitInstantTime2 = if (tableType == HoodieTableType.MERGE_ON_READ) {
        // For the records that are processed by the compaction in MOR table
        // the "_hoodie_commit_time" still reflects the latest delta commit
        latestInstant(storage, destPath, HoodieTimeline.DELTA_COMMIT_ACTION)
      } else {
        HoodieDataSourceHelpers.latestCommit(storage, destPath)
      }
      val commitCompletionTime2 = if (tableType == HoodieTableType.MERGE_ON_READ) {
        DataSourceTestUtils.latestDeltaCommitCompletionTime(storage, destPath)
      } else {
        DataSourceTestUtils.latestCommitCompletionTime(storage, destPath)
      }
      assertEquals(numExpectedCommits, HoodieDataSourceHelpers.listCommitsSince(storage, destPath, "000").size())
      // Read RO View
      val hoodieROViewDF2 = spark.read.format("org.apache.hudi").load(destPath)
      assertEquals(100, hoodieROViewDF2.count()) // still 100, since we only updated

      // Read Incremental View
      // we have 2 commits, try pulling the first commit (which is not the latest)
      val firstCommit = HoodieDataSourceHelpers.listCommitsSince(storage, destPath, "000").get(0)
      val hoodieIncViewDF1 = spark.read.format("org.apache.hudi")
        .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
        .option(DataSourceReadOptions.START_COMMIT.key, "000")
        .option(DataSourceReadOptions.END_COMMIT.key, commitCompletionTime1)
        .load(destPath)
      assertEquals(100, hoodieIncViewDF1.count())
      // 100 initial inserts must be pulled
      var countsPerCommit = hoodieIncViewDF1.groupBy("_hoodie_commit_time").count().collect()
      assertEquals(1, countsPerCommit.length)
      assertEquals(firstCommit, countsPerCommit(0).get(0))

      // pull the latest commit
      val hoodieIncViewDF2 = spark.read.format("org.apache.hudi")
        .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
        .option(DataSourceReadOptions.START_COMMIT.key, commitCompletionTime1)
        .load(destPath)

      assertEquals(uniqueKeyCnt, hoodieIncViewDF2.count()) // 100 records must be pulled
      countsPerCommit = hoodieIncViewDF2.groupBy("_hoodie_commit_time").count().collect()
      assertEquals(1, countsPerCommit.length)
      assertEquals(commitInstantTime2, countsPerCommit(0).get(0))

      streamingQuery.stop()
    }

    Await.result(f2, Duration("120s"))
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testStructuredStreaming(tableType: HoodieTableType): Unit = {
    structuredStreamingTestRunner(
      tableType, addCompactionConfigs = false, isAsyncCompaction = false)
  }

  @throws[InterruptedException]
  private def waitTillAtleastNCommits(storage: HoodieStorage, tablePath: String,
                                      numCommits: Int, timeoutSecs: Int, sleepSecsAfterEachRun: Int) = {
    val beginTime = System.currentTimeMillis
    var currTime = beginTime
    val timeoutMsecs = timeoutSecs * 1000
    var numInstants = 0
    var success = false
    while ( {
      !success && (currTime - beginTime) < timeoutMsecs
    }) try {
      val timeline = HoodieDataSourceHelpers.allCompletedCommitsCompactions(storage, tablePath)
      log.info("Timeline :" + timeline.getInstants.toArray.mkString("Array(", ", ", ")"))
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
      assertEquals(1, getLatestFileGroupsFileId(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH).length)
    }

    structuredStreamingForTestClusteringRunner(sourcePath, destPath, HoodieTableType.COPY_ON_WRITE,
      !isAsyncClustering, isAsyncClustering, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, checkClusteringResult)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testStructuredStreamingWithCompaction(isAsyncCompaction: Boolean): Unit = {
    structuredStreamingTestRunner(
      HoodieTableType.MERGE_ON_READ, addCompactionConfigs = true, isAsyncCompaction = isAsyncCompaction)
  }

  @Test
  def testStructuredStreamingWithCheckpoint(): Unit = {
    val (sourcePath, destPath) = initStreamingSourceAndDestPath("source", "dest")

    val opts: Map[String, String] = commonOpts ++ Map(
      HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key -> WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.name,
      HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key -> classOf[InProcessLockProvider].getName
    )

    val records1 = recordsToStrings(dataGen.generateInsertsForPartition("000", 100, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    val schema = inputDF1.schema

    inputDF1.coalesce(1).write.mode(SaveMode.Append).json(sourcePath)

    val query1 = spark.readStream
      .schema(schema)
      .json(sourcePath)
      .writeStream
      .format("org.apache.hudi")
      .options(opts)
      .outputMode(OutputMode.Append)
      .option(STREAMING_CHECKPOINT_IDENTIFIER.key(), "streaming_identifier1")
      .option("checkpointLocation", s"$basePath/checkpoint1")
      .start(destPath)

    query1.processAllAvailable()
    var metaClient = HoodieTestUtils.createMetaClient(storage, destPath)

    assertLatestCheckpointInfoMatched(metaClient, "streaming_identifier1", "0")

    // Add another identifier checkpoint info to the commit.
    val records2 = recordsToStrings(dataGen.generateInsertsForPartition("001", 100, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)).asScala.toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))

    inputDF2.coalesce(1).write.mode(SaveMode.Append).json(sourcePath)

    val query2 = spark.readStream
      .schema(schema)
      .json(sourcePath)
      .writeStream
      .format("org.apache.hudi")
      .options(opts)
      .outputMode(OutputMode.Append)
      .option(STREAMING_CHECKPOINT_IDENTIFIER.key(), "streaming_identifier2")
      .option("checkpointLocation", s"$basePath/checkpoint2")
      .start(destPath)
    query2.processAllAvailable()
    query1.processAllAvailable()

    query1.stop()
    query2.stop()

    assertLatestCheckpointInfoMatched(metaClient, "streaming_identifier2", "0")
    assertLatestCheckpointInfoMatched(metaClient, "streaming_identifier1", "1")


    inputDF1.coalesce(1).write.mode(SaveMode.Append).json(sourcePath)

    val query3 = spark.readStream
      .schema(schema)
      .json(sourcePath)
      .writeStream
      .format("org.apache.hudi")
      .options(commonOpts)
      .outputMode(OutputMode.Append)
      .option(STREAMING_CHECKPOINT_IDENTIFIER.key(), "streaming_identifier1")
      .option("checkpointLocation", s"${basePath}/checkpoint1")
      .start(destPath)

    query3.processAllAvailable()
    query3.stop()
    metaClient = HoodieTestUtils.createMetaClient(storage, destPath)

    assertLatestCheckpointInfoMatched(metaClient, "streaming_identifier1", "2")
    assertLatestCheckpointInfoMatched(metaClient, "streaming_identifier2", "0")
  }

  @Test
  def testStructuredStreamingForDefaultIdentifier(): Unit = {
    testStructuredStreamingInternal()
  }

  @Test
  def testStructuredStreamingWithBulkInsert(): Unit = {
    testStructuredStreamingInternal("bulk_insert")
  }

  def testStructuredStreamingInternal(operation : String = "upsert"): Unit = {
    val (sourcePath, destPath) = initStreamingSourceAndDestPath("source", "dest")
    val records1 = recordsToStrings(dataGen.generateInsertsForPartition("000", 100, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    val schema = inputDF1.schema
    inputDF1.coalesce(1).write.mode(SaveMode.Append).json(sourcePath)

    val query1 = spark.readStream
      .schema(schema)
      .json(sourcePath)
      .writeStream
      .format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key(), operation)
      .outputMode(OutputMode.Append)
      .option("checkpointLocation", s"$basePath/checkpoint1")
      .start(destPath)

    query1.processAllAvailable()
    val metaClient = HoodieTestUtils.createMetaClient(storage, destPath)

    assertLatestCheckpointInfoMatched(metaClient, STREAMING_CHECKPOINT_IDENTIFIER.defaultValue(), "0")
    query1.stop()
  }

  def assertLatestCheckpointInfoMatched(metaClient: HoodieTableMetaClient,
                                        identifier: String,
                                        expectBatchId: String): Unit = {
    metaClient.reloadActiveTimeline()
    val lastCheckpoint = CommitUtils.getValidCheckpointForCurrentWriter(
      metaClient.getActiveTimeline.getCommitsTimeline, SINK_CHECKPOINT_KEY, identifier)
    assertEquals(lastCheckpoint.get(), expectBatchId)
  }

  def structuredStreamingForTestClusteringRunner(sourcePath: String, destPath: String, tableType: HoodieTableType,
                                                 isInlineClustering: Boolean, isAsyncClustering: Boolean,
                                                 partitionOfRecords: String, checkClusteringResult: String => Unit): Unit = {
    // First insert of data
    val records1 = recordsToStrings(dataGen.generateInsertsForPartition("000", 100, partitionOfRecords)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))

    // Second insert of data
    val records2 = recordsToStrings(dataGen.generateInsertsForPartition("001", 100, partitionOfRecords)).asScala.toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))

    val hudiOptions = getClusteringOpts(
      tableType, isInlineClustering.toString, isAsyncClustering.toString, "2", 100)

    val streamingQuery = initWritingStreamingQuery(inputDF1.schema, sourcePath, destPath, hudiOptions)

    val f2 = Future {
      inputDF1.coalesce(1).write.mode(SaveMode.Append).json(sourcePath)
      // wait for spark streaming to process one microbatch
      var currNumCommits = waitTillAtleastNCommits(storage, destPath, 1, 120, 5)
      assertTrue(HoodieDataSourceHelpers.hasNewCommits(storage, destPath, "000"))

      inputDF2.coalesce(1).write.mode(SaveMode.Append).json(sourcePath)
      // wait for spark streaming to process second microbatch
      currNumCommits = waitTillAtleastNCommits(storage, destPath, currNumCommits + 1, 120, 5)

      // Wait for the clustering to finish
      this.metaClient = HoodieTestUtils.createMetaClient(storage, destPath)
      checkClusteringResult(destPath)

      assertEquals(3, HoodieDataSourceHelpers.listCommitsSince(storage, destPath, "000").size())
      // Check have at least one file group
      assertTrue(getLatestFileGroupsFileId(partitionOfRecords).size > 0)

      // Validate data after clustering
      val hoodieROViewDF2 = spark.read.format("org.apache.hudi").load(destPath)
      assertEquals(200, hoodieROViewDF2.count())
      val countsPerCommit = hoodieROViewDF2.groupBy("_hoodie_commit_time").count().collect()
      assertEquals(2, countsPerCommit.length)
      val commitInstantTime2 = latestInstant(storage, destPath, HoodieTimeline.COMMIT_ACTION)
      assertEquals(commitInstantTime2, countsPerCommit.maxBy(row => row.getAs[String](0)).get(0))

      streamingQuery.stop()
    }

    Await.result(f2, Duration("120s"))
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
      val completeReplaceSize = this.metaClient.getActiveTimeline.getCompletedReplaceTimeline.getInstants.toArray.length
      if (completeReplaceSize > 0) {
        success = true
      }
    } catch {
      case _: TableNotFoundException =>
        log.info("Got table not found exception. Retrying")
    } finally {
      Thread.sleep(sleepSecsAfterEachRun * 1000)
      currTime = System.currentTimeMillis
    }
    if (!success) throw new IllegalStateException("Timed-out waiting for completing replace instant appear in " + tablePath)
  }

  private def latestInstant(storage: HoodieStorage, basePath: String, instantAction: String): String = {
    val metaClient = HoodieTestUtils.createMetaClient(storage, basePath)
    metaClient.getActiveTimeline
      .getTimelineOfActions(CollectionUtils.createSet(instantAction))
      .filterCompletedInstants
      .lastInstant
      .get.requestedTime
  }

  private def streamingWrite(schema: StructType, sourcePath: String, destPath: String, hudiOptions: Map[String, String]): Unit = {
    val query = spark.readStream
      .schema(schema)
      .json(sourcePath)
      .writeStream
      .format("org.apache.hudi")
      .options(hudiOptions)
      .trigger(Trigger.Once())
      .option("checkpointLocation", basePath + "/checkpoint")
      .outputMode(OutputMode.Append)
      .start(destPath)
    query.processAllAvailable()
    query.stop()
  }

  @Test
  def testStructuredStreamingWithDisabledCompaction(): Unit = {
    val (sourcePath, destPath) = initStreamingSourceAndDestPath("source", "dest")
    // First chunk of data
    val records1 = recordsToStrings(dataGen.generateInserts("000", 10)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.coalesce(1).write.mode(SaveMode.Append).json(sourcePath)
    val opts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> HoodieTableType.MERGE_ON_READ.name()) + (DataSourceWriteOptions.STREAMING_DISABLE_COMPACTION.key -> "true")
    streamingWrite(inputDF1.schema, sourcePath, destPath, opts)
    for (i <- 1 to 24) {
      val id = String.format("%03d", new Integer(i))
      val records = recordsToStrings(dataGen.generateUpdates(id, 10)).asScala.toList
      val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
      inputDF.coalesce(1).write.mode(SaveMode.Append).json(sourcePath)
      streamingWrite(inputDF.schema, sourcePath, destPath, opts)
    }
    val metaClient = HoodieTestUtils.createMetaClient(storage, destPath)
    assertTrue(metaClient.getActiveTimeline.getCommitAndReplaceTimeline.empty())
    assertEquals(25, metaClient.getActiveTimeline.countInstants())
  }

  /**
   * The sink owns one embedded timeline server for the lifetime of the streaming query and reuses it across
   * micro-batches. With a REMOTE_ONLY file system view and no backup view, every micro-batch after the first used
   * to fail because the timeline server was stopped along with the first micro-batch's write client
   * (https://github.com/apache/hudi/issues/12652). Failed batches are ignored here so that a regression shows up
   * as missing commits rather than the sink exiting the JVM.
   */
  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testStructuredStreamingReusesTimelineServerAcrossBatches(tableType: HoodieTableType): Unit = {
    val (sourcePath, destPath) = initStreamingSourceAndDestPath("source", "dest")
    val opts = getOptsWithTableType(tableType) ++ Map(
      HoodieWriteConfig.EMBEDDED_TIMELINE_SERVER_ENABLE.key -> "true",
      FileSystemViewStorageConfig.VIEW_TYPE.key -> FileSystemViewStorageType.REMOTE_ONLY.name,
      FileSystemViewStorageConfig.REMOTE_BACKUP_VIEW_ENABLE.key -> "false",
      DataSourceWriteOptions.STREAMING_IGNORE_FAILED_BATCH.key -> "true",
      DataSourceWriteOptions.STREAMING_RETRY_CNT.key -> "1")

    val records1 = recordsToStrings(dataGen.generateInserts("000", 100)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.coalesce(1).write.mode(SaveMode.Append).json(sourcePath)
    val query = initWritingStreamingQuery(inputDF1.schema, sourcePath, destPath, opts)
    try {
      query.processAllAvailable()
      for (i <- 1 to 2) {
        val records = recordsToStrings(dataGen.generateUpdates("%03d".format(i), 50)).asScala.toList
        spark.read.json(spark.sparkContext.parallelize(records, 2)).coalesce(1)
          .write.mode(SaveMode.Append).json(sourcePath)
        query.processAllAvailable()
      }
    } finally {
      query.stop()
    }

    assertEquals(3, HoodieDataSourceHelpers.allCompletedCommitsCompactions(storage, destPath).countInstants())
  }

  /**
   * The sink reuses its write client across micro-batches, so it must re-create the client when the writer schema
   * changes under a running query, as HoodieStreamer does. Here a batch writer adds a column between two
   * micro-batches; the next micro-batch fills it with nulls and must commit with the evolved schema.
   */
  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testStructuredStreamingWithTableSchemaEvolvedMidQuery(tableType: HoodieTableType): Unit = {
    val (sourcePath, destPath) = initStreamingSourceAndDestPath("source", "dest")
    val opts = getOptsWithTableType(tableType) ++ Map(
      DataSourceWriteOptions.SET_NULL_FOR_MISSING_COLUMNS.key -> "true")

    val records1 = recordsToStrings(dataGen.generateInserts("000", 100)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.coalesce(1).write.mode(SaveMode.Append).json(sourcePath)
    val query = initWritingStreamingQuery(inputDF1.schema, sourcePath, destPath, opts)
    val evolvedDF = spark.read.json(spark.sparkContext.parallelize(
      recordsToStrings(dataGen.generateUniqueUpdates("001", 10)).asScala.toList, 2))
      .withColumn("new_col", lit("evolved"))
    val streamedDF = spark.read.json(spark.sparkContext.parallelize(
      recordsToStrings(dataGen.generateUniqueUpdates("002", 50)).asScala.toList, 2))
    try {
      query.processAllAvailable()
      // evolve the table schema outside of the streaming query
      evolvedDF.write.format("org.apache.hudi").options(opts)
        .option(DataSourceWriteOptions.OPERATION.key, UPSERT_OPERATION_OPT_VAL)
        .mode(SaveMode.Append).save(destPath)
      streamedDF.coalesce(1).write.mode(SaveMode.Append).json(sourcePath)
      query.processAllAvailable()
    } finally {
      query.stop()
    }

    val metaClient = HoodieTestUtils.createMetaClient(storage, destPath)
    assertEquals(3, metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants().countInstants())
    val tableSchema = new TableSchemaResolver(metaClient).getTableSchema(false)
    assertTrue(tableSchema.getField("new_col").isPresent, s"new_col missing from table schema: $tableSchema")
    val tableDF = spark.read.format("org.apache.hudi").load(destPath)
    assertEquals(100, tableDF.count())
    // rows last written by the batch writer keep their value, rows the stream updated afterwards are null
    val evolvedOnlyKeys = evolvedDF.select("_row_key").except(streamedDF.select("_row_key")).count()
    assertEquals(evolvedOnlyKeys, tableDF.filter(col("new_col") === "evolved").count())
  }

  /**
   * Same as [[testStructuredStreamingWithTableSchemaEvolvedMidQuery]] for a delete stream: delete commits record
   * the schema of the write client, so a stale client would drop the new column from the table schema.
   */
  @Test
  def testStructuredStreamingDeletesWithTableSchemaEvolvedMidQuery(): Unit = {
    val (sourcePath, destPath) = initStreamingSourceAndDestPath("source", "dest")
    val opts = getOptsWithTableType(HoodieTableType.COPY_ON_WRITE)
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).asScala.toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    inputDF.write.format("org.apache.hudi").options(opts).mode(SaveMode.Overwrite).save(destPath)

    def recordsDF(from: Int, until: Int): DataFrame =
      spark.read.schema(inputDF.schema).json(spark.sparkContext.parallelize(records.slice(from, until), 1))

    recordsDF(0, 10).write.mode(SaveMode.Append).json(sourcePath)
    val query = initWritingStreamingQuery(inputDF.schema, sourcePath, destPath,
      opts + (DataSourceWriteOptions.OPERATION.key -> DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL))
    try {
      query.processAllAvailable()
      // evolve the table schema outside of the streaming query
      recordsDF(20, 30).withColumn("new_col", lit("evolved")).write.format("org.apache.hudi").options(opts)
        .option(DataSourceWriteOptions.OPERATION.key, UPSERT_OPERATION_OPT_VAL)
        .mode(SaveMode.Append).save(destPath)
      recordsDF(10, 20).write.mode(SaveMode.Append).json(sourcePath)
      query.processAllAvailable()
    } finally {
      query.stop()
    }

    val metaClient = HoodieTestUtils.createMetaClient(storage, destPath)
    assertEquals(4, metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants().countInstants())
    val tableSchema = new TableSchemaResolver(metaClient).getTableSchema(false)
    assertTrue(tableSchema.getField("new_col").isPresent, s"new_col missing from table schema: $tableSchema")
    val tableDF = spark.read.format("org.apache.hudi").load(destPath)
    assertEquals(80, tableDF.count())
    assertEquals(10, tableDF.filter(col("new_col") === "evolved").count())
  }

  /**
   * Same as [[testStructuredStreamingReleasesHeartbeatOfFailedBatch]] for a delete stream, which fails inside the
   * delete operation: a record without a record key fails the key generation after the commit started.
   */
  @Test
  def testStructuredStreamingReleasesHeartbeatOfFailedDelete(): Unit = {
    val (sourcePath, destPath) = initStreamingSourceAndDestPath("source", "dest")
    val opts = getOptsWithTableType(HoodieTableType.COPY_ON_WRITE) ++ lazyFailedWritesCleaningOpts
    val inputDF = spark.read.json(spark.sparkContext.parallelize(
      recordsToStrings(dataGen.generateInserts("000", 10)).asScala.toList, 1))
    inputDF.write.format("org.apache.hudi").options(opts).mode(SaveMode.Overwrite).save(destPath)

    inputDF.limit(1).withColumn("_row_key", lit(null).cast("string")).write.mode(SaveMode.Append).json(sourcePath)
    val query = initWritingStreamingQuery(inputDF.schema, sourcePath, destPath,
      opts + (DataSourceWriteOptions.OPERATION.key -> DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL))
    try {
      query.processAllAvailable()
    } finally {
      query.stop()
    }

    val failedInstant = HoodieTestUtils.createMetaClient(storage, destPath).getActiveTimeline
      .getCommitsTimeline.filterInflightsAndRequested().lastInstant()
    assertTrue(failedInstant.isPresent, "the failed micro-batch must leave an inflight instant")
    assertFalse(storage.exists(new StoragePath(HoodieTableMetaClient.getHeartbeatFolderPath(destPath),
      failedInstant.get().requestedTime())), "the heartbeat of the failed instant must be stopped")
  }

  /**
   * The sink keeps its write client open across micro-batches, so a micro-batch that fails after its commit started
   * must still stop that instant's heartbeat. Otherwise, with the lazy failed-writes clean policy, the failed instant
   * looks alive for as long as the query runs and is never rolled back. The failed write's markers also show that
   * the sink uses timeline-server-based markers.
   */
  @Test
  def testStructuredStreamingReleasesHeartbeatOfFailedBatch(): Unit = {
    val (sourcePath, destPath) = initStreamingSourceAndDestPath("source", "dest")
    val opts = getOptsWithTableType(HoodieTableType.COPY_ON_WRITE) ++ Map(
      DataSourceWriteOptions.OPERATION.key -> UPSERT_OPERATION_OPT_VAL,
      DataSourceWriteOptions.RECORD_MERGE_MODE.key -> "CUSTOM",
      DataSourceWriteOptions.RECORD_MERGE_STRATEGY_ID.key -> FailingSparkRecordMerger.STRATEGY_ID,
      DataSourceWriteOptions.RECORD_MERGE_IMPL_CLASSES.key -> classOf[FailingSparkRecordMerger].getName
    ) ++ lazyFailedWritesCleaningOpts

    val records1 = recordsToStrings(dataGen.generateInserts("000", 100)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.coalesce(1).write.mode(SaveMode.Append).json(sourcePath)
    val query = initWritingStreamingQuery(inputDF1.schema, sourcePath, destPath, opts)
    try {
      query.processAllAvailable()

      // the updates of the next micro-batch fail while merging, after the commit started
      FailingSparkRecordMerger.shouldFail = true
      try {
        spark.read.json(spark.sparkContext.parallelize(
          recordsToStrings(dataGen.generateUniqueUpdates("001", 50)).asScala.toList, 2)).coalesce(1)
          .write.mode(SaveMode.Append).json(sourcePath)
        query.processAllAvailable()
      } finally {
        FailingSparkRecordMerger.shouldFail = false
      }
      val metaClient = HoodieTestUtils.createMetaClient(storage, destPath)
      val failedInstant = metaClient.getActiveTimeline.getCommitsTimeline.filterInflightsAndRequested().lastInstant()
      assertTrue(failedInstant.isPresent, "the failed micro-batch must leave an inflight instant")
      assertFalse(storage.exists(new StoragePath(HoodieTableMetaClient.getHeartbeatFolderPath(destPath),
        failedInstant.get().requestedTime())), "the heartbeat of the failed instant must be stopped")
      // the sink does not force direct markers: the failed write's markers went through the sink's timeline server
      assertEquals(HOption.of(MarkerType.TIMELINE_SERVER_BASED),
        MarkerUtils.readMarkerType(storage, metaClient.getMarkerFolderPath(failedInstant.get().requestedTime())))

      // the query recovers on the next micro-batch
      spark.read.json(spark.sparkContext.parallelize(
        recordsToStrings(dataGen.generateUniqueUpdates("002", 50)).asScala.toList, 2)).coalesce(1)
        .write.mode(SaveMode.Append).json(sourcePath)
      query.processAllAvailable()
    } finally {
      query.stop()
    }
    assertEquals(2, HoodieDataSourceHelpers.allCompletedCommitsCompactions(storage, destPath).countInstants())
  }

  /**
   * When a micro-batch exhausts its retries, the sink resets while holding its lock: it shuts down the async table
   * services and waits for them. The services' shutdown callbacks must not block on the sink in turn, otherwise the
   * reset never finishes and the query hangs. The reset is driven directly here, with async clustering running,
   * since a micro-batch that exhausts its retries exits the JVM.
   */
  @Test
  def testSinkResetWithAsyncServiceRunningDoesNotHang(): Unit = {
    val (_, destPath) = initStreamingSourceAndDestPath("source", "dest")
    val opts = getOptsWithTableType(HoodieTableType.COPY_ON_WRITE)
    spark.read.json(spark.sparkContext.parallelize(recordsToStrings(dataGen.generateInserts("000", 10)).asScala.toList, 1))
      .write.format("org.apache.hudi").options(opts).mode(SaveMode.Overwrite).save(destPath)

    val sink = new ResettableStreamingSink(sqlContext, opts + ("path" -> destPath))
    val client = DataSourceUtils.createHoodieClient(jsc, HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, destPath,
      commonOpts(HoodieWriteConfig.TBL_NAME.key), opts.asJava)
    try {
      sink.startAsyncClustering(client)
      Await.result(Future(sink.resetHoldingSinkLock()), Duration(2, "minutes"))
    } finally {
      client.close()
    }
  }

  private def compactionControlledOpts: Map[String, String] =
    getCompactionOpts(HoodieTableType.MERGE_ON_READ, isAsyncCompaction = true) ++ Map(
      DataSourceWriteOptions.OPERATION.key -> UPSERT_OPERATION_OPT_VAL,
      DataSourceWriteOptions.RECORD_MERGE_MODE.key -> "CUSTOM",
      DataSourceWriteOptions.RECORD_MERGE_STRATEGY_ID.key -> CompactionControlledSparkRecordMerger.STRATEGY_ID,
      DataSourceWriteOptions.RECORD_MERGE_IMPL_CLASSES.key -> classOf[CompactionControlledSparkRecordMerger].getName,
      HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key -> "parquet")

  private def writeUpdatesToSource(sourcePath: String, instantTime: String, numRecords: Int): Unit = {
    spark.read.json(spark.sparkContext.parallelize(
      recordsToStrings(dataGen.generateUniqueUpdates(instantTime, numRecords)).asScala.toList, 1))
      .write.mode(SaveMode.Append).json(sourcePath)
  }

  /**
   * An async compaction running on the sink's write client when a schema change makes the sink re-create its client
   * may fail once that client is closed. Its service retries it with the new client, and the query keeps running.
   */
  @Test
  def testStructuredStreamingRetriesAsyncCompactionFailedByWriteClientSwap(): Unit = {
    val (sourcePath, destPath) = initStreamingSourceAndDestPath("source", "dest")
    val opts = compactionControlledOpts + (DataSourceWriteOptions.SET_NULL_FOR_MISSING_COLUMNS.key -> "true")
    val inputDF = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(dataGen.generateInsertsForPartition(
      "000", 20, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)).asScala.toList, 1))
    inputDF.write.mode(SaveMode.Append).json(sourcePath)
    val query = initWritingStreamingQuery(inputDF.schema, sourcePath, destPath, opts)
    val compactionStarted = new CountDownLatch(1)
    val releaseCompaction = new CountDownLatch(1)
    try {
      query.processAllAvailable()
      CompactionControlledSparkRecordMerger.holdNextCompactionThenFail(compactionStarted, releaseCompaction)
      writeUpdatesToSource(sourcePath, "001", 10)
      query.processAllAvailable()
      assertTrue(compactionStarted.await(2, TimeUnit.MINUTES), "the async compaction must start")

      // evolve the table schema outside of the query, so that the next micro-batch re-creates the sink's write client
      spark.read.json(spark.sparkContext.parallelize(
        recordsToStrings(dataGen.generateUniqueUpdates("002", 5)).asScala.toList, 1))
        .withColumn("new_col", lit("evolved"))
        .write.format("org.apache.hudi").options(opts).mode(SaveMode.Append).save(destPath)
      writeUpdatesToSource(sourcePath, "003", 10)
      query.processAllAvailable()
      // the micro-batch that replaced the write client completed without waiting for the running compaction
      assertTrue(CompactionControlledSparkRecordMerger.holdingCompaction,
        "the micro-batch must not wait for the running async compaction")

      // the compaction held on the previous write client now fails, and is retried with the new one
      releaseCompaction.countDown()
      waitTillAtleastNCompletedCompactions(destPath, 1)
      writeUpdatesToSource(sourcePath, "004", 10)
      query.processAllAvailable()
      assertTrue(query.isActive, "the streaming query must keep running")
    } finally {
      releaseCompaction.countDown()
      CompactionControlledSparkRecordMerger.reset()
      query.stop()
    }
  }

  /**
   * An async compaction failure the service cannot recover from fails the query, with that failure as the cause.
   */
  @Test
  def testStructuredStreamingFailsWithCauseWhenAsyncCompactionFails(): Unit = {
    val (sourcePath, destPath) = initStreamingSourceAndDestPath("source", "dest")
    val inputDF = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(dataGen.generateInsertsForPartition(
      "000", 20, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)).asScala.toList, 1))
    inputDF.write.mode(SaveMode.Append).json(sourcePath)
    val query = initWritingStreamingQuery(inputDF.schema, sourcePath, destPath, compactionControlledOpts)
    var failure: Option[StreamingQueryException] = None
    try {
      query.processAllAvailable()
      CompactionControlledSparkRecordMerger.failCompaction = true
      // the compaction scheduled by an update fails in the background; a later micro-batch then fails the query
      for (i <- 1 to 10 if failure.isEmpty) {
        writeUpdatesToSource(sourcePath, "%03d".format(i), 10)
        try {
          query.processAllAvailable()
          Thread.sleep(2000)
        } catch {
          case e: StreamingQueryException => failure = Some(e)
        }
      }
    } finally {
      CompactionControlledSparkRecordMerger.reset()
      query.stop()
    }
    assertTrue(failure.isDefined, "the streaming query must fail")
    assertTrue(causeChainContains(failure.get, "Async Compactor shutdown unexpectedly"), s"unexpected failure: ${failure.get}")
    assertTrue(causeChainContains(failure.get, CompactionControlledSparkRecordMerger.FAILURE_MESSAGE),
      s"the compaction failure must be the cause: ${failure.get}")
  }

  private def causeChainContains(throwable: Throwable, message: String): Boolean =
    Iterator.iterate(throwable)(_.getCause).takeWhile(_ != null).exists(t => t.getMessage != null && t.getMessage.contains(message))

  private def waitTillAtleastNCompletedCompactions(tablePath: String, numCompactions: Int): Unit = {
    val deadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2)
    while (HoodieTestUtils.createMetaClient(storage, tablePath).getActiveTimeline.getCommitTimeline
      .filterCompletedInstants().countInstants() < numCompactions) {
      assertTrue(System.currentTimeMillis() < deadline, s"expected $numCompactions completed compactions")
      Thread.sleep(500)
    }
  }

  /**
   * Injects a failing micro-batch into [[HoodieStreamingSink]] by pointing the record key at a
   * non-existent column so that every Hudi write throws. With STREAMING_IGNORE_FAILED_BATCH
   * enabled the sink must swallow the failure (unpersisting the write RDDs), let the streaming
   * query complete without crashing, and produce no commit.
   */
  @Test
  def testStructuredStreamingIgnoreFailedBatch(): Unit = {
    val (sourcePath, destPath) = initStreamingSourceAndDestPath("source", "dest")
    val records1 = recordsToStrings(dataGen.generateInsertsForPartition(
      "000", 100, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    val schema = inputDF1.schema
    inputDF1.coalesce(1).write.mode(SaveMode.Append).json(sourcePath)

    val failingOpts = commonOpts ++ Map(
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "non_existent_field",
      DataSourceWriteOptions.STREAMING_IGNORE_FAILED_BATCH.key -> "true",
      DataSourceWriteOptions.STREAMING_RETRY_CNT.key -> "1"
    )

    val query = spark.readStream
      .schema(schema)
      .json(sourcePath)
      .writeStream
      .format("org.apache.hudi")
      .options(failingOpts)
      .outputMode(OutputMode.Append)
      .option("checkpointLocation", s"$basePath/checkpoint_ignore")
      .start(destPath)

    // Must not throw even though the micro-batch fails; the failure is ignored.
    query.processAllAvailable()
    query.stop()

    // No completed commit must exist since every batch failed and was ignored.
    val completedCommits =
      try {
        HoodieTestUtils.createMetaClient(storage, destPath)
          .getActiveTimeline.getCommitsTimeline.filterCompletedInstants().countInstants()
      } catch {
        case _: TableNotFoundException => 0
      }
    assertEquals(0, completedCommits)
  }

  @ParameterizedTest
  @CsvSource(Array(
    "COPY_ON_WRITE,EVENT_TIME_ORDERING",
    "MERGE_ON_READ,EVENT_TIME_ORDERING",
    "COPY_ON_WRITE,COMMIT_TIME_ORDERING",
    "MERGE_ON_READ,COMMIT_TIME_ORDERING",
    "COPY_ON_WRITE,CUSTOM",
    "MERGE_ON_READ,CUSTOM"))
  def testStructuredStreamingWithMergeMode(tableType: String, mergeMode: String): Unit = {
    val (sourcePath, destPath) = initStreamingSourceAndDestPath("source", "dest")
    // First chunk of data
    val records1 = recordsToStrings(dataGen.generateInserts("000", 100)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.coalesce(1).write.mode(SaveMode.Append).json(sourcePath)
    var opts = commonOpts ++ Map(DataSourceWriteOptions.OPERATION.key -> UPSERT_OPERATION_OPT_VAL,
      DataSourceWriteOptions.TABLE_TYPE.key() -> tableType,
      DataSourceWriteOptions.RECORD_MERGE_MODE.key() -> mergeMode,
      HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key() -> "parquet",
      HoodieTableConfig.ORDERING_FIELDS.key -> "weight")
    if (mergeMode == "CUSTOM") {
      opts = opts ++ Map(DataSourceWriteOptions.RECORD_MERGE_STRATEGY_ID.key() -> HoodieSparkDeleteRecordMerger.DELETE_MERGER_STRATEGY,
        DataSourceWriteOptions.RECORD_MERGE_IMPL_CLASSES.key() -> classOf[HoodieSparkDeleteRecordMerger].getName)
    }
    streamingWrite(inputDF1.schema, sourcePath, destPath, opts)

    val records2 = recordsToStrings(dataGen.generateUniqueUpdates("001", 50)).asScala.toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.coalesce(1).write.mode(SaveMode.Append).json(sourcePath)
    streamingWrite(inputDF2.schema, sourcePath, destPath, opts)

    if (mergeMode == "CUSTOM") {
      //merger will delete any records in records2 so we remove those from the original batch using except
      val expectedFinalRecords = inputDF1.select("_row_key", "partition_path").except(inputDF2.select("_row_key", "partition_path"))
      val finalRecords = spark.read.format("hudi")
        .option(DataSourceWriteOptions.RECORD_MERGE_IMPL_CLASSES.key(), classOf[HoodieSparkDeleteRecordMerger].getName)
        .load(destPath).select("_row_key", "partition_path")
      assertEquals(expectedFinalRecords.count(), finalRecords.count())
      assertEquals(0, expectedFinalRecords.except(finalRecords).count())
    } else {
      val metaClient = HoodieTestUtils.createMetaClient(destPath)
      val instants = metaClient.getActiveTimeline.getCommitsTimeline.getInstants
      assertEquals(2, instants.size())
      spark.read.format("hudi").load(destPath).createOrReplaceTempView("finalRecords")
      val updatedRecords = spark.sql(s"select _row_key, partition_path, weight from finalRecords "
        + s"where _hoodie_commit_time = ${instants.get(1).requestedTime()}")
      if (mergeMode == "COMMIT_TIME_ORDERING") {
        assertEquals(inputDF2.count(), updatedRecords.count())
        assertEquals(0, inputDF2.select("_row_key", "partition_path", "weight").except(updatedRecords).count())
      } else if (mergeMode == "EVENT_TIME_ORDERING") {
        inputDF1.createOrReplaceTempView("input1")
        inputDF2.createOrReplaceTempView("input2")
        val expectedUpdatedRecords = spark.sql("SELECT input2._row_key, input2.partition_path, input2.weight FROM "
          + "input1 JOIN input2 ON input1._row_key = input2._row_key AND input1.partition_path = input2.partition_path "
          + "WHERE input1.weight < input2.weight")
        assertEquals(expectedUpdatedRecords.count(), updatedRecords.count())
        assertEquals(0, expectedUpdatedRecords.except(updatedRecords).count())
      }
    }
  }
}

/**
 * Event-time merger that fails every merge while [[FailingSparkRecordMerger.shouldFail]] is set, to fail a write
 * after its commit started.
 */
class FailingSparkRecordMerger extends DefaultSparkRecordMerger {
  override def getMergingStrategy: String = FailingSparkRecordMerger.STRATEGY_ID

  override def merge[T](older: BufferedRecord[T], newer: BufferedRecord[T], recordContext: RecordContext[T],
                        props: TypedProperties): BufferedRecord[T] = {
    if (FailingSparkRecordMerger.shouldFail) {
      throw new HoodieException("Injected merge failure")
    }
    super.merge(older, newer, recordContext, props)
  }
}

object FailingSparkRecordMerger {
  val STRATEGY_ID = "6b3a2b4e-2f4f-4f0c-9a55-3c3f7d0b8e21"
  @volatile var shouldFail: Boolean = false
}

/**
 * Streaming sink that exposes its async clustering service and reset to tests.
 */
class ResettableStreamingSink(sqlContext: SQLContext, options: Map[String, String])
  extends HoodieStreamingSink(sqlContext, options, Seq.empty, OutputMode.Append) {

  def startAsyncClustering(client: SparkRDDWriteClient[_]): Unit = triggerAsyncClustering(client)

  // a micro-batch that exhausts its retries resets the sink from addBatch, which holds the sink lock
  def resetHoldingSinkLock(): Unit = this.synchronized {
    reset(false)
  }
}

/**
 * Event-time merger that, inside async compaction (Spark tasks of the compaction scheduler pool), either fails every
 * merge or holds the next merge until released and then fails it.
 */
class CompactionControlledSparkRecordMerger extends DefaultSparkRecordMerger {
  override def getMergingStrategy: String = CompactionControlledSparkRecordMerger.STRATEGY_ID

  override def merge[T](older: BufferedRecord[T], newer: BufferedRecord[T], recordContext: RecordContext[T],
                        props: TypedProperties): BufferedRecord[T] = {
    val taskContext = TaskContext.get()
    if (taskContext != null && AsyncCompactService.COMPACT_POOL_NAME == taskContext.getLocalProperty("spark.scheduler.pool")) {
      CompactionControlledSparkRecordMerger.beforeCompactionMerge()
    }
    super.merge(older, newer, recordContext, props)
  }
}

object CompactionControlledSparkRecordMerger {
  val STRATEGY_ID = "0f6c2f0e-6a52-4b8e-9d2f-2b8b4c3e5a17"
  val FAILURE_MESSAGE = "Injected async compaction failure"
  @volatile var failCompaction: Boolean = false
  @volatile var holdingCompaction: Boolean = false
  private val heldCompaction = new AtomicReference[(CountDownLatch, CountDownLatch)]()

  def holdNextCompactionThenFail(started: CountDownLatch, release: CountDownLatch): Unit = heldCompaction.set((started, release))

  def beforeCompactionMerge(): Unit = {
    val hold = heldCompaction.getAndSet(null)
    if (hold != null) {
      holdingCompaction = true
      hold._1.countDown()
      try {
        hold._2.await(2, TimeUnit.MINUTES)
      } finally {
        holdingCompaction = false
      }
      throw new HoodieException(FAILURE_MESSAGE)
    }
    if (failCompaction) {
      throw new HoodieException(FAILURE_MESSAGE)
    }
  }

  def reset(): Unit = {
    failCompaction = false
    holdingCompaction = false
    heldCompaction.set(null)
  }
}
