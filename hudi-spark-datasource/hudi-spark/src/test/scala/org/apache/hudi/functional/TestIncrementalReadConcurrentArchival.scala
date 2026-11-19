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

import org.apache.hudi.{DataSourceOptionsHelper, DataSourceReadOptions, DataSourceWriteOptions, DefaultSource}
import org.apache.hudi.common.model.{HoodieRecord, HoodieTableType}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.{ExceptionUtil, HoodieIncrementalPathNotFoundException, HoodieIOException}
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.sources.BaseRelation
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

import java.io.FileNotFoundException
import java.util.Properties

import scala.collection.JavaConverters._

/**
 * Incremental queries list the active timeline once and read each included instant's commit metadata later. These
 * tests archive or remove instants in between, the way a concurrent writer does: they build the relation from a meta
 * client that has already listed the timeline, change the timeline, then plan and run the scan.
 *
 * Tables are written either at version 6, read through the V1 incremental relations (instant-time ranges), or at the
 * current version, read through the V2 relations (completion-time ranges). V1 relations treat an instant archived
 * after the listing like an archived start instant: they fall back to a full table scan if enabled and fail with
 * HoodieIncrementalPathNotFoundException otherwise. V2 relations read it from the archived timeline.
 */
class TestIncrementalReadConcurrentArchival extends HoodieSparkClientTestBase {

  private val testTableName = "hoodie_test"
  private val recordsPerBatch = 20
  private var spark: SparkSession = _

  @BeforeEach override def setUp(): Unit = {
    setTableName(testTableName)
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initHoodieStorage()
  }

  @AfterEach override def tearDown(): Unit = {
    spark = null
    cleanupResources()
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testLegacyTableFallsBackToFullTableScanWhenIncludedInstantArchivedAfterListing(tableType: HoodieTableType): Unit = {
    val batches = writeBatches(tableType, legacyTableVersion = true, 6)
    val listedMetaClient = loadMetaClient()
    val relation = createRelation(listedMetaClient, startCommit(batches.head, legacyTableVersion = true), fallbackToFullTableScan = true)

    archiveIncludedInstants(listedMetaClient, batches, legacyTableVersion = true)

    // V1 relations do not read the archived timeline, so only the full table scan can return these rows.
    assertEquals(expectedRows(batches.tail), readRows(relation))
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testLegacyTableFailsWithPathNotFoundWhenIncludedInstantArchivedAfterListingWithoutFallback(tableType: HoodieTableType): Unit = {
    val batches = writeBatches(tableType, legacyTableVersion = true, 6)
    val listedMetaClient = loadMetaClient()
    val relation = createRelation(listedMetaClient, startCommit(batches.head, legacyTableVersion = true), fallbackToFullTableScan = false)

    archiveIncludedInstants(listedMetaClient, batches, legacyTableVersion = true)

    val thrown = assertThrows(classOf[Exception], new Executable {
      override def execute(): Unit = readRows(relation)
    })
    assertTrue(ExceptionUtil.isCausedBy(thrown, classOf[HoodieIncrementalPathNotFoundException]),
      s"Expected a HoodieIncrementalPathNotFoundException in the cause chain of $thrown")
    assertTrue(ExceptionUtil.isCausedBy(thrown, classOf[FileNotFoundException]),
      s"Expected the FileNotFoundException of the archived instant in the cause chain of $thrown")
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testReadsInstantsArchivedAfterListingFromArchivedTimeline(tableType: HoodieTableType): Unit = {
    val batches = writeBatches(tableType, legacyTableVersion = false, 6)
    val listedMetaClient = loadMetaClient()
    val relation = createRelation(listedMetaClient, startCommit(batches.head, legacyTableVersion = false), fallbackToFullTableScan = false)

    archiveIncludedInstants(listedMetaClient, batches, legacyTableVersion = false)

    assertEquals(expectedRows(batches.tail), readRows(relation))
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testFailsWhenIncludedInstantIsNeitherActiveNorArchived(tableType: HoodieTableType): Unit = {
    val legacyTableVersion = false
    val batches = writeBatches(tableType, legacyTableVersion, 3)
    val listedMetaClient = loadMetaClient()
    val relation = createRelation(listedMetaClient, startCommit(batches.head, legacyTableVersion), fallbackToFullTableScan = false)
    val removedInstant = batches(1).instant
    // Remove every state file of the instant, as archival does, but without writing an archive entry.
    val removedFiles = storage.listDirectEntries(listedMetaClient.getTimelinePath).asScala.map(_.getPath)
      .filter(path => path.getName.startsWith(removedInstant.requestedTime + ".")
        || path.getName.startsWith(removedInstant.requestedTime + "_"))
    assertEquals(3, removedFiles.size, s"Expected requested, inflight and completed files, found ${removedFiles.mkString(", ")}")
    removedFiles.foreach(path => assertTrue(storage.deleteFile(path)))

    val thrown = assertThrows(classOf[Exception], new Executable {
      override def execute(): Unit = readRows(relation)
    })

    val ioException = Iterator.iterate[Throwable](thrown)(_.getCause).takeWhile(_ != null)
      .collectFirst { case e: HoodieIOException => e }
    assertTrue(ioException.isDefined, s"Expected a HoodieIOException in the cause chain of $thrown")
    assertEquals(s"Instant $removedInstant of ${listedMetaClient.getBasePath} is missing from both the active and the archived timeline",
      ioException.get.getMessage)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testReadsReplaceCommitArchivedAfterListingFromArchivedTimeline(tableType: HoodieTableType): Unit = {
    // Batch 3 overwrites the table, replacing the file groups of batches 1 and 2.
    val overwriteBatch = 2
    val batches = writeBatches(tableType, legacyTableVersion = false, 6, overwriteTableBatch = Some(overwriteBatch))
    val replaceInstant = batches(overwriteBatch).instant
    assertEquals(HoodieTimeline.REPLACE_COMMIT_ACTION, replaceInstant.getAction)
    val listedMetaClient = loadMetaClient()
    val relation = createRelation(listedMetaClient, startCommit(batches.head, legacyTableVersion = false), fallbackToFullTableScan = false)

    // The archiver keeps a replace commit until the file groups it replaced are cleaned.
    archiveIncludedInstants(listedMetaClient, batches, legacyTableVersion = false, cleanFirst = true)
    assertTrue(loadMetaClient().getArchivedTimeline.containsInstant(replaceInstant.requestedTime),
      s"Expected archival to move the replace commit $replaceInstant to the archived timeline")

    // Batch 2 is in the queried range but its file group was replaced, which the relation can only know from the
    // replace commit's metadata, now read back from the archived timeline.
    assertEquals(expectedRows(batches.drop(overwriteBatch)), readRows(relation))
  }

  /**
   * Archives instants the way a concurrent writer does, and checks that some of the instants the relation included
   * when it listed the timeline left the active timeline.
   */
  private def archiveIncludedInstants(listedMetaClient: HoodieTableMetaClient, batches: Seq[Batch], legacyTableVersion: Boolean,
                                      cleanFirst: Boolean = false): Unit = {
    archive(legacyTableVersion, keepMinCommits = 2, keepMaxCommits = 3, cleanFirst)

    val archivedTimestamps = loadMetaClient().getArchivedTimeline.getInstants.asScala.map(_.requestedTime).toSet
    val activeTimestamps = loadMetaClient().getCommitsTimeline.filterCompletedInstants.getInstants.asScala
      .map(_.requestedTime).toSet
    val includedArchivedTimestamps = batches.tail.map(_.instant.requestedTime).filter(archivedTimestamps.contains)
    assertTrue(includedArchivedTimestamps.nonEmpty, s"Expected archival to remove some of ${batches.tail.map(_.instant)}")
    assertTrue(includedArchivedTimestamps.forall(ts => !activeTimestamps.contains(ts)))
    assertTrue(includedArchivedTimestamps.forall(ts => listedMetaClient.getActiveTimeline.containsInstant(ts)))
  }

  private case class Batch(instant: HoodieInstant, keys: Set[String])

  /**
   * Writes `numBatches` insert batches and returns, per batch, its completed instant and record keys. The batch at
   * index `overwriteTableBatch`, if any, is an insert_overwrite_table instead.
   */
  private def writeBatches(tableType: HoodieTableType, legacyTableVersion: Boolean, numBatches: Int,
                           overwriteTableBatch: Option[Int] = None): Seq[Batch] = {
    (1 to numBatches).map { batch =>
      val operation = if (overwriteTableBatch.contains(batch - 1)) {
        DataSourceWriteOptions.INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL
      } else {
        DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL
      }
      val records = dataGen.generateInserts("%05d".format(batch), recordsPerBatch).asScala
      val inputDF = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records.asJava).asScala.toSeq, 2))
      inputDF.write.format("org.apache.hudi")
        .options(tableVersionOptions(legacyTableVersion))
        .option("hoodie.insert.shuffle.parallelism", "2")
        .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType.name())
        .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "_row_key")
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "partition")
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key, "timestamp")
        .option(DataSourceWriteOptions.OPERATION.key, operation)
        .option(HoodieWriteConfig.TBL_NAME.key, testTableName)
        .option("hoodie.metadata.enable", "false")
        .option("hoodie.archive.automatic", "false")
        .option("hoodie.clean.automatic", "false")
        .option("hoodie.clean.commits.retained", "2")
        .option("hoodie.keep.min.commits", "3")
        .option("hoodie.keep.max.commits", "4")
        .mode(SaveMode.Append)
        .save(basePath)
      Batch(loadMetaClient().getCommitsTimeline.filterCompletedInstants.lastInstant.get, records.map(_.getRecordKey).toSet)
    }
  }

  private def tableVersionOptions(legacyTableVersion: Boolean): Map[String, String] = {
    if (legacyTableVersion) {
      Map(HoodieWriteConfig.WRITE_TABLE_VERSION.key -> "6", HoodieWriteConfig.AUTO_UPGRADE_VERSION.key -> "false")
    } else {
      Map.empty
    }
  }

  /**
   * V1 relations take an exclusive instant-time start; V2 relations take an exclusive completion-time start.
   */
  private def startCommit(batch: Batch, legacyTableVersion: Boolean): String = {
    if (legacyTableVersion) batch.instant.requestedTime else batch.instant.getCompletionTime
  }

  private def archive(legacyTableVersion: Boolean, keepMinCommits: Int, keepMaxCommits: Int, cleanFirst: Boolean): Unit = {
    val props = new Properties()
    tableVersionOptions(legacyTableVersion).foreach { case (key, value) => props.setProperty(key, value) }
    props.setProperty("hoodie.metadata.enable", "false")
    props.setProperty("hoodie.clean.commits.retained", (keepMinCommits - 1).toString)
    props.setProperty("hoodie.keep.min.commits", keepMinCommits.toString)
    props.setProperty("hoodie.keep.max.commits", keepMaxCommits.toString)
    // Keeps MERGE_ON_READ archival from retaining delta commits for a compaction that never runs here.
    props.setProperty("hoodie.compact.inline.max.delta.commits", "1")
    val config = HoodieWriteConfig.newBuilder().withPath(basePath).forTable(testTableName).withProps(props).build()
    val client = getHoodieWriteClient(config)
    try {
      if (cleanFirst) {
        client.clean()
      }
      client.archive()
    } finally {
      client.close()
    }
  }

  private def createRelation(metaClient: HoodieTableMetaClient, startCommit: String,
                             fallbackToFullTableScan: Boolean): BaseRelation = {
    val parameters = DataSourceOptionsHelper.parametersWithReadDefaults(Map(
      "path" -> basePath,
      DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
      DataSourceReadOptions.START_COMMIT.key -> startCommit,
      DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN.key -> fallbackToFullTableScan.toString))
    DefaultSource.createRelation(sqlContext, metaClient, null, parameters)
  }

  private def readRows(relation: BaseRelation): Set[(String, String)] = {
    sqlContext.baseRelationToDataFrame(relation)
      .select(HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.RECORD_KEY_METADATA_FIELD)
      .collect()
      .map(row => (row.getString(0), row.getString(1)))
      .toSet
  }

  private def expectedRows(batches: Seq[Batch]): Set[(String, String)] = {
    batches.flatMap(batch => batch.keys.map(key => (batch.instant.requestedTime, key))).toSet
  }

  private def loadMetaClient(): HoodieTableMetaClient = {
    val metaClient = createMetaClient(spark, basePath)
    metaClient.getActiveTimeline
    metaClient
  }
}
