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

import org.apache.hudi.common.model.{HoodieRecord, HoodieTableType}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.{ExceptionUtil, HoodieIncrementalPathNotFoundException}
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.hudi.{DataSourceOptionsHelper, DataSourceReadOptions, DataSourceWriteOptions, DefaultSource}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

import java.io.FileNotFoundException
import java.util.Properties

import scala.collection.JavaConverters._

/**
 * Incremental queries list the active timeline once and read each included instant's commit metadata later. These
 * tests archive instants in between, the way a concurrent writer does: they build the relation and let it list the
 * timeline (the file index reloads the meta client's active timeline on first use), then archive, then plan and run
 * the scan. An instant archived after the listing is treated like an archived begin instant: the query falls back to
 * a full table scan if enabled, and fails with HoodieIncrementalPathNotFoundException otherwise. Planning reads commit
 * metadata twice for MERGE_ON_READ: the file index lists the written partitions while Spark optimizes the plan, and
 * the relation reads the included commits while it collects file splits.
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
  def testFallsBackToFullTableScanWhenIncludedInstantArchivedAfterListing(tableType: HoodieTableType): Unit = {
    val batches = writeBatches(tableType, 6)
    val listedMetaClient = loadMetaClient()
    val relation = createListedRelation(listedMetaClient, batches.head._1, fallbackToFullTableScan = true)

    archiveIncludedInstants(listedMetaClient, batches)

    // The archived instants' metadata is no longer readable, so only the full table scan can return these rows.
    assertEquals(expectedRows(batches.tail), readRows(relation))
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testFailsWithPathNotFoundWhenIncludedInstantArchivedAfterListingWithoutFallback(tableType: HoodieTableType): Unit = {
    val batches = writeBatches(tableType, 6)
    val listedMetaClient = loadMetaClient()
    val relation = createListedRelation(listedMetaClient, batches.head._1, fallbackToFullTableScan = false)

    archiveIncludedInstants(listedMetaClient, batches)

    val thrown = assertThrows(classOf[Exception], new Executable {
      override def execute(): Unit = readRows(relation)
    })
    assertTrue(ExceptionUtil.hasCause(thrown, classOf[HoodieIncrementalPathNotFoundException]),
      s"Expected a HoodieIncrementalPathNotFoundException in the cause chain of $thrown")
    assertTrue(ExceptionUtil.hasCause(thrown, classOf[FileNotFoundException]),
      s"Expected the FileNotFoundException of the archived instant in the cause chain of $thrown")
  }

  /**
   * Archives instants the way a concurrent writer does, and checks that some of the instants the relation included
   * when it listed the timeline left the active timeline.
   */
  private def archiveIncludedInstants(listedMetaClient: HoodieTableMetaClient, batches: Seq[(String, Set[String])]): Unit = {
    archive(keepMinCommits = 2, keepMaxCommits = 3)

    val archivedTimestamps = new HoodieArchivedTimeline(loadMetaClient()).getInstantsAsStream.iterator().asScala
      .map(_.getTimestamp).toSet
    val activeTimestamps = loadMetaClient().getCommitsTimeline.filterCompletedInstants.getInstantsAsStream.iterator().asScala
      .map(_.getTimestamp).toSet
    val includedArchivedTimestamps = batches.tail.map(_._1).filter(archivedTimestamps.contains)
    assertTrue(includedArchivedTimestamps.nonEmpty, s"Expected archival to remove some of ${batches.tail.map(_._1)}")
    assertTrue(includedArchivedTimestamps.forall(ts => !activeTimestamps.contains(ts)))
    assertTrue(includedArchivedTimestamps.forall(ts => listedMetaClient.getActiveTimeline.containsInstant(ts)))
  }

  /**
   * Writes `numBatches` insert batches and returns, per batch, its commit time and record keys.
   */
  private def writeBatches(tableType: HoodieTableType, numBatches: Int): Seq[(String, Set[String])] = {
    (1 to numBatches).map { batch =>
      val records = dataGen.generateInserts("%05d".format(batch), recordsPerBatch).asScala
      val inputDF = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records.asJava).asScala.toSeq, 2))
      inputDF.write.format("org.apache.hudi")
        .option("hoodie.insert.shuffle.parallelism", "2")
        .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType.name())
        .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "_row_key")
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "partition")
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key, "timestamp")
        .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
        .option(HoodieWriteConfig.TBL_NAME.key, testTableName)
        .option("hoodie.metadata.enable", "false")
        .option("hoodie.archive.automatic", "false")
        .option("hoodie.clean.automatic", "false")
        .option("hoodie.cleaner.commits.retained", "2")
        .option("hoodie.keep.min.commits", "3")
        .option("hoodie.keep.max.commits", "4")
        .mode(SaveMode.Append)
        .save(basePath)
      val commitTime = loadMetaClient().getCommitsTimeline.filterCompletedInstants.lastInstant.get.getTimestamp
      (commitTime, records.map(_.getRecordKey).toSet)
    }
  }

  private def archive(keepMinCommits: Int, keepMaxCommits: Int): Unit = {
    val props = new Properties()
    props.setProperty("hoodie.metadata.enable", "false")
    props.setProperty("hoodie.cleaner.commits.retained", (keepMinCommits - 1).toString)
    props.setProperty("hoodie.keep.min.commits", keepMinCommits.toString)
    props.setProperty("hoodie.keep.max.commits", keepMaxCommits.toString)
    // Keeps MERGE_ON_READ archival from retaining delta commits for a compaction that never runs here.
    props.setProperty("hoodie.compact.inline.max.delta.commits", "1")
    val config = HoodieWriteConfig.newBuilder().withPath(basePath).forTable(testTableName).withProps(props).build()
    val client = getHoodieWriteClient(config)
    try {
      client.archive()
    } finally {
      client.close()
    }
  }

  /**
   * Creates an incremental relation and makes it list the active timeline, as Spark does when it sizes the relation
   * during analysis. Commit metadata is read later, when the scan is planned.
   */
  private def createListedRelation(metaClient: HoodieTableMetaClient, beginInstant: String,
                                   fallbackToFullTableScan: Boolean): BaseRelation = {
    val parameters = DataSourceOptionsHelper.parametersWithReadDefaults(Map(
      "path" -> basePath,
      DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
      DataSourceReadOptions.BEGIN_INSTANTTIME.key -> beginInstant,
      DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN.key -> fallbackToFullTableScan.toString))
    val relation = DefaultSource.createRelation(sqlContext, metaClient, null, Seq.empty, parameters)
    relation.sizeInBytes
    relation
  }

  private def readRows(relation: BaseRelation): Set[(String, String)] = {
    sqlContext.baseRelationToDataFrame(relation)
      .select(HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.RECORD_KEY_METADATA_FIELD)
      .collect()
      .map(row => (row.getString(0), row.getString(1)))
      .toSet
  }

  private def expectedRows(batches: Seq[(String, Set[String])]): Set[(String, String)] = {
    batches.flatMap { case (commitTime, keys) => keys.map(key => (commitTime, key)) }.toSet
  }

  private def loadMetaClient(): HoodieTableMetaClient = {
    HoodieTableMetaClient.builder().setConf(HadoopFSUtils.getStorageConf(spark.sparkContext.hadoopConfiguration)).setBasePath(basePath)
      .setLoadActiveTimelineOnLoad(true).build()
  }
}
