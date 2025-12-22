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

import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.timeline.HoodieTimeline.GREATER_THAN
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieInstantTimeGenerator, HoodieTimeline}
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.HoodieIncrementalPathNotFoundException
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}

import org.apache.spark.SparkException
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

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
      DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "timestamp",
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
        .option("hoodie.cleaner.commits.retained", "3")
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
    val startUnarchivedCommitTs = completedCommits.nthInstant(0).get().getTimestamp //C4
    val endUnarchivedCommitTs = completedCommits.nthInstant(1).get().getTimestamp //C5

    val startArchivedCommitTs = archivedInstants(0).asInstanceOf[HoodieInstant].getTimestamp //C0
    val endArchivedCommitTs = archivedInstants(1).asInstanceOf[HoodieInstant].getTimestamp //C1

    val startOutOfRangeCommitTs = HoodieInstantTimeGenerator.createNewInstantTime(0)
    val endOutOfRangeCommitTs = HoodieInstantTimeGenerator.createNewInstantTime(0)

    assertTrue(HoodieTimeline.compareTimestamps(startOutOfRangeCommitTs, GREATER_THAN, completedCommits.lastInstant().get().getTimestamp))
    assertTrue(HoodieTimeline.compareTimestamps(endOutOfRangeCommitTs, GREATER_THAN, completedCommits.lastInstant().get().getTimestamp))

    // Test both start and end commits are archived
    runIncrementalQueryAndCompare(startArchivedCommitTs, endArchivedCommitTs, 1, true)

    // Test start commit is archived, end commit is not archived
    shouldThrowIfFallbackIsFalse(tableType,
      () => runIncrementalQueryAndCompare(startArchivedCommitTs, endUnarchivedCommitTs, nArchivedInstants + 1, false))
    runIncrementalQueryAndCompare(startArchivedCommitTs, endUnarchivedCommitTs, nArchivedInstants + 1, true)

    // Test both start commit and end commits are not archived but got cleaned
    shouldThrowIfFallbackIsFalse(tableType,
      () => runIncrementalQueryAndCompare(startUnarchivedCommitTs, endUnarchivedCommitTs, 1, false))
    runIncrementalQueryAndCompare(startUnarchivedCommitTs, endUnarchivedCommitTs, 1, true)

    // Test start commit is not archived, end commits is out of the timeline
    runIncrementalQueryAndCompare(startUnarchivedCommitTs, endOutOfRangeCommitTs, nCompletedCommits - 1, true)

    // Test both start commit and end commits are out of the timeline
    runIncrementalQueryAndCompare(startOutOfRangeCommitTs, endOutOfRangeCommitTs, 0, false)
    runIncrementalQueryAndCompare(startOutOfRangeCommitTs, endOutOfRangeCommitTs, 0, true)

    // Test end commit is smaller than the start commit
    runIncrementalQueryAndCompare(endUnarchivedCommitTs, startUnarchivedCommitTs, 0, false)
    runIncrementalQueryAndCompare(endUnarchivedCommitTs, startUnarchivedCommitTs, 0, true)

    // Test both start commit and end commits is not archived and not cleaned
    val reversedCommits = completedCommits.getReverseOrderedInstants.toArray
    val startUncleanedCommitTs = reversedCommits.apply(1).asInstanceOf[HoodieInstant].getTimestamp
    val endUncleanedCommitTs = reversedCommits.apply(0).asInstanceOf[HoodieInstant].getTimestamp
    runIncrementalQueryAndCompare(startUncleanedCommitTs, endUncleanedCommitTs, 1, true)
    runIncrementalQueryAndCompare(startUncleanedCommitTs, endUncleanedCommitTs, 1, false)
  }

  private def runIncrementalQueryAndCompare(
      startTs: String,
      endTs: String,
      batchNum: Int,
      fallBackFullTableScan: Boolean): Unit = {
    val hoodieIncViewDF = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key(), startTs)
      .option(DataSourceReadOptions.END_INSTANTTIME.key(), endTs)
      .option(DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN.key(), fallBackFullTableScan)
      .load(basePath)
    assertEquals(perBatchSize * batchNum, hoodieIncViewDF.count())
  }

  private def shouldThrowIfFallbackIsFalse(tableType: HoodieTableType, fn: () => Unit): Unit = {
    val msg = "Should fail with Path does not exist"
    tableType match {
      case HoodieTableType.COPY_ON_WRITE =>
        assertThrows(classOf[HoodieIncrementalPathNotFoundException], new Executable {
          override def execute(): Unit = {
            fn()
          }
        }, msg)
      case HoodieTableType.MERGE_ON_READ =>
        val exp = assertThrows(classOf[SparkException], new Executable {
          override def execute(): Unit = {
            fn()
          }
        }, msg)
        assertTrue(exp.getMessage.contains("FileNotFoundException"))
    }
  }
}
