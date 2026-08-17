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

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieSparkUtils}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieInstantTimeGenerator, InstantComparison}
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator.instantTimeMinusMillis
import org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.SparkException
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, EnumSource}

import java.time.Instant
import java.util.Date

import scala.collection.JavaConverters._

class TestIncrementalReadWithFullTableScan extends HoodieSparkClientTestBase {

  var spark: SparkSession = _
  private val perBatchSize = 100

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
    spark = null
    cleanupResources()
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testFailEarlyForIncrViewQueryForNonExistingFiles(tableType: HoodieTableType): Unit = {
    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key -> "1"
    )

    // Create 10 commits
    for (i <- 1 to 10) {
      val records = recordsToStrings(dataGen.generateInserts("%05d".format(i), perBatchSize)).asScala.toList
      val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
      inputDF.write.format("org.apache.hudi")
        .options(commonOpts)
        .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType.name())
        .option("hoodie.clean.commits.retained", "3")
        .option("hoodie.keep.min.commits", "4")
        .option("hoodie.keep.max.commits", "7")
        .option(DataSourceWriteOptions.OPERATION.key(), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
        .mode(SaveMode.Append)
        .save(basePath)
    }

    val hoodieMetaClient = createMetaClient(spark, basePath)
    /**
     * State of timeline after 10 commits
     * +------------------+--------------------------------------+
     * |     Archived     |            Active Timeline           |
     * +------------------+--------------+-----------------------+
     * | C0   C1   C2  C3 |    C4   C5   |   C6    C7   C8   C9  |
     * +------------------+--------------+-----------------------+
     * |          Data cleaned           |  Data exists in table |
     * +---------------------------------+-----------------------+
     */

    val completedCommits = hoodieMetaClient.getCommitsTimeline.filterCompletedInstants() // C4 to C9
    val archivedInstants = hoodieMetaClient.getArchivedTimeline.filterCompletedInstants()
      .getInstantsAsStream.distinct().toArray // C0 to C3
    val nCompletedCommits = completedCommits.getInstants.size
    val nArchivedInstants = archivedInstants.size
    assertTrue(nCompletedCommits >= 3)
    assertTrue(nArchivedInstants >= 3)

    //Anything less than 2 is a valid commit in the sense no cleanup has been done for those commit files
    val startUnarchivedCompletionTs = completedCommits.nthInstant(0).get().getCompletionTime //C5 completion
    val endUnarchivedCompletionTs = completedCommits.nthInstant(1).get().getCompletionTime //C5 completion

    val startArchivedCompletionTs = archivedInstants(0).asInstanceOf[HoodieInstant].getCompletionTime //C1 completion
    val endArchivedCompletionTs = archivedInstants(1).asInstanceOf[HoodieInstant].getCompletionTime //C1 completion

    val instant = Instant.now()
    val startOutOfRangeCommitTs = HoodieInstantTimeGenerator.formatDate(Date.from(instant))
    val endOutOfRangeCommitTs = HoodieInstantTimeGenerator.formatDate(Date.from(instant.plusMillis(1)))

    assertTrue(compareTimestamps(startOutOfRangeCommitTs, InstantComparison.GREATER_THAN, completedCommits.lastInstant().get().requestedTime))
    assertTrue(compareTimestamps(endOutOfRangeCommitTs, InstantComparison.GREATER_THAN, completedCommits.lastInstant().get().requestedTime))

    // Test both start and end commits are archived
    runIncrementalQueryAndCompare(startArchivedCompletionTs, endArchivedCompletionTs, 1, true)
    // Test start commit is archived, end commit is not archived
    shouldThrowSparkExceptionIfFallbackIsFalse(
      () => runIncrementalQueryAndCompare(startArchivedCompletionTs, endUnarchivedCompletionTs, nArchivedInstants + 1, false))
    runIncrementalQueryAndCompare(startArchivedCompletionTs, endUnarchivedCompletionTs, nArchivedInstants + 1, true)

    // Test both start commit and end commits are not archived but got cleaned
    shouldThrowSparkExceptionIfFallbackIsFalse(
      () => runIncrementalQueryAndCompare(startUnarchivedCompletionTs, endUnarchivedCompletionTs, 1, false))
    runIncrementalQueryAndCompare(startUnarchivedCompletionTs, endUnarchivedCompletionTs, 1, true)

    // Test start commit is not archived, end commits is out of the timeline
    runIncrementalQueryAndCompare(startUnarchivedCompletionTs, endOutOfRangeCommitTs, nCompletedCommits - 1, true)

    // Test both start commit and end commits are out of the timeline
    runIncrementalQueryAndCompare(startOutOfRangeCommitTs, endOutOfRangeCommitTs, 0, false)
    runIncrementalQueryAndCompare(startOutOfRangeCommitTs, endOutOfRangeCommitTs, 0, true)

    // Test end commit is smaller than the start commit
    runIncrementalQueryAndCompare(
      startUnarchivedCompletionTs, instantTimeMinusMillis(startUnarchivedCompletionTs, 1), 0, false)
    runIncrementalQueryAndCompare(
      startUnarchivedCompletionTs, instantTimeMinusMillis(startUnarchivedCompletionTs, 1), 0, true)

    // Test both start commit and end commits is not archived and not cleaned
    val reversedCommits = completedCommits.getReverseOrderedInstants.toArray
    val startUncleanedCompletionTs = reversedCommits.apply(1).asInstanceOf[HoodieInstant].getCompletionTime
    val endUncleanedCompletionTs = reversedCommits.apply(0).asInstanceOf[HoodieInstant].getCompletionTime
    runIncrementalQueryAndCompare(startUncleanedCompletionTs, endUncleanedCompletionTs, 1, true)
    runIncrementalQueryAndCompare(startUncleanedCompletionTs, endUncleanedCompletionTs, 1, false)
  }

  /**
   * Pins the divergent DEFAULT of {@code hoodie.datasource.read.incr.fallback.fulltablescan.enable}
   * across table versions. {@code MergeOnReadIncrementalRelationV1} (selected for table version
   * below 8) hardcodes "false" when the option is absent, while
   * {@code MergeOnReadIncrementalRelationV2} (table version 8 and above) reads the ConfigProperty
   * default, which is "true" since HUDI-8624. Batch incremental reads route through these two
   * relations for both COW and MOR (see the {@code HoodieCopyOnWriteIncrementalHadoopFsRelationFactory}
   * and {@code HoodieMergeOnReadIncrementalHadoopFsRelationFactory} V1/V2 subclasses), so the
   * divergence is table-version driven, not table-type driven. Note that
   * {@code INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN_FOR_NON_EXISTING_FILES} is an
   * alias for the very same ConfigProperty (same key, same default), so a single key governs both
   * versions and only the in-code default differs.
   *
   * The timeline shape here is the one proven by {@code testFailEarlyForIncrViewQueryForNonExistingFiles}:
   * 10 insert commits with cleaning and archival configured so that the earliest commits are both
   * cleaned and archived. The discriminating query is a narrow span over the two oldest commits that
   * are still on the active timeline but whose data files have already been cleaned; there the
   * default decides between failing and silently degrading into a full table scan.
   *
   * A span that starts at "000" does NOT discriminate: both versions return every record. With an
   * archived start instant V1 keeps only the latest merged file slice of each affected file group
   * (which still exists) and its record filter degenerates to {@code _hoodie_commit_time > "000"},
   * so no file is missing and no fallback is needed. That case is asserted too, so nobody
   * "simplifies" the discriminating span away and silently loses the pin.
   */
  @ParameterizedTest
  @CsvSource(value = Array(
    "COPY_ON_WRITE,6",
    "COPY_ON_WRITE,8",
    "MERGE_ON_READ,6",
    "MERGE_ON_READ,8"))
  def testDefaultFallbackBehaviorAcrossTableVersions(tableType: String, tableVersion: Int): Unit = {
    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key -> "1",
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
      HoodieWriteConfig.WRITE_TABLE_VERSION.key -> tableVersion.toString,
      HoodieWriteConfig.AUTO_UPGRADE_VERSION.key -> "false"
    )

    val numCommits = 10
    for (i <- 1 to numCommits) {
      val records = recordsToStrings(dataGen.generateInserts("%05d".format(i), perBatchSize)).asScala.toList
      val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
      inputDF.write.format("org.apache.hudi")
        .options(commonOpts)
        .option("hoodie.clean.commits.retained", "3")
        .option("hoodie.keep.min.commits", "4")
        .option("hoodie.keep.max.commits", "7")
        .option(DataSourceWriteOptions.OPERATION.key(), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
        .mode(SaveMode.Append)
        .save(basePath)
    }

    val metaClient = createMetaClient(spark, basePath)
    assertEquals(tableVersion, metaClient.getTableConfig.getTableVersion.versionCode(),
      "table should be written at the requested version to select the V1/V2 relation")
    assertTrue(metaClient.getArchivedTimeline.filterCompletedInstants().countInstants() > 0,
      "early commits must be archived so the timeline matches testFailEarlyForIncrViewQueryForNonExistingFiles")

    // The two oldest commits still on the active timeline: not archived, but already cleaned.
    val completedCommits = metaClient.getCommitsTimeline.filterCompletedInstants()
    val cleanedStart = completedCommits.nthInstant(0).get()
    val cleanedEnd = completedCommits.nthInstant(1).get()
    // V1 ranges over requested time, V2 over completion time.
    val (startTs, endTs) = if (tableVersion < 8) {
      (cleanedStart.requestedTime, cleanedEnd.requestedTime)
    } else {
      (cleanedStart.getCompletionTime, cleanedEnd.getCompletionTime)
    }

    // No fallback option is set on any of these queries: what happens is purely the in-code default
    // of the relation that the table version selects.
    def readSpan(start: String, end: Option[String]): Long = {
      var reader = spark.read.format("org.apache.hudi")
        .option(DataSourceReadOptions.QUERY_TYPE.key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
        .option(DataSourceReadOptions.START_COMMIT.key(), start)
      end.foreach(e => reader = reader.option(DataSourceReadOptions.END_COMMIT.key(), e))
      reader.load(basePath).count()
    }

    if (tableVersion < 8) {
      // V1 hardcodes the default to false, so nothing falls back and the query fails on the cleaned
      // data files of the span.
      shouldThrowSparkExceptionIfFallbackIsFalse(() => readSpan(startTs, Some(endTs)))
    } else {
      // V2 takes the config default of true, so the same span silently degrades into a full table
      // scan and still returns the single batch the span covers.
      assertEquals(perBatchSize, readSpan(startTs, Some(endTs)),
        "table version 8 must silently fall back to a full table scan for a cleaned span")
    }

    // Both versions agree once the span starts before the timeline: see the class comment on why
    // this query needs no fallback at all.
    assertEquals(numCommits * perBatchSize, readSpan("000", None),
      "a span starting at 000 returns every record on both table versions")
  }

  private def runIncrementalQueryAndCompare(
      startTs: String,
      endTs: String,
      batchNum: Int,
      fallBackFullTableScan: Boolean): Unit = {

  val fallbackKeys = Seq(
    DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN.key(),
    DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN_FOR_NON_EXISTING_FILES.key()
  )

  fallbackKeys.foreach { key =>
    val hoodieIncViewDF = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key(), startTs)
      .option(DataSourceReadOptions.END_COMMIT.key(), endTs)
      .option(key, fallBackFullTableScan.toString)
      .load(basePath)

    assertEquals(perBatchSize * batchNum, hoodieIncViewDF.count(), s"with fallback‐key=$key")
  }
}

  private def shouldThrowSparkExceptionIfFallbackIsFalse(fn: () => Unit): Unit = {
    val msg = "Should fail with Path does not exist"
    val exp = assertThrows(classOf[SparkException], new Executable {
      override def execute(): Unit = {
        fn()
      }
    }, msg)
    val expected = if (HoodieSparkUtils.gteqSpark4_0)
      "[FAILED_READ_FILE.FILE_NOT_EXIST]"
    else
      "FileNotFoundException"
    assertTrue(exp.getMessage.contains(expected),
      "Expected to contain: " + expected + ", but got: " + exp.getMessage)
  }
}
