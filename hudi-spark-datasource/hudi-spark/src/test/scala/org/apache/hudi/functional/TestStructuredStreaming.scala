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

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieDataSourceHelpers}
import org.apache.hudi.DataSourceWriteOptions.{STREAMING_CHECKPOINT_IDENTIFIER, UPSERT_OPERATION_OPT_VAL}
import org.apache.hudi.HoodieStreamingSink.SINK_CHECKPOINT_KEY
import org.apache.hudi.client.transaction.lock.InProcessLockProvider
import org.apache.hudi.common.config.HoodieStorageConfig
import org.apache.hudi.common.model.{FileSlice, HoodieTableType, WriteConcurrencyMode}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.testutils.{HoodieTestDataGenerator, HoodieTestTable, HoodieTestUtils}
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.common.util.{CollectionUtils, CommitUtils}
import org.apache.hudi.config.{HoodieClusteringConfig, HoodieCompactionConfig, HoodieLockConfig, HoodieWriteConfig}
import org.apache.hudi.exception.TableNotFoundException
import org.apache.hudi.storage.{HoodieStorage, StoragePath}
import org.apache.hudi.testutils.{DataSourceTestUtils, HoodieSparkClientTestBase, HoodieSparkDeleteRecordMerger}

import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.{BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, EnumSource, ValueSource}
import org.slf4j.LoggerFactory

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
        .option(DataSourceReadOptions.START_COMMIT.key, commitCompletionTime1)
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
        .option(DataSourceReadOptions.START_COMMIT.key, commitCompletionTime2)
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
