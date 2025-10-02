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

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.hudi.common.config.{HoodieMetadataConfig, HoodieReaderConfig}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Tag}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.collection.JavaConverters._

@Tag("functional")
class TestIncrementalQueriesV2WithCompleteTime extends HoodieSparkClientTestBase {

  var spark: SparkSession = _

  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    DataSourceWriteOptions.RECORDKEY_FIELD.key() -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key() -> "partition",
    HoodieTableConfig.ORDERING_FIELDS.key() -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
    HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key -> "1",
    HoodieTableConfig.VERSION.key() -> "9"
  )

  @BeforeEach
  override def setUp() {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initHoodieStorage()
  }

  @AfterEach
  override def tearDown() = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
  }

  @ParameterizedTest
  @ValueSource(strings = Array("COPY_ON_WRITE", "MERGE_ON_READ"))
  def testCompleteTimeFilteringSemantics(tableTypeStr: String): Unit = {
    testBasicIncrementalQuery(tableTypeStr, enableFileGroupReader = true)
    // testBasicIncrementalQuery(tableTypeStr, enableFileGroupReader = false) // Base MOR doesn't support this mode
  }

  private def testBasicIncrementalQuery(tableTypeStr: String, enableFileGroupReader: Boolean): Unit = {
    val testBasePath = s"${basePath}_${tableTypeStr}_fgr_${enableFileGroupReader}"
    val testOpts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key() -> tableTypeStr
    )

    val baseTime = System.currentTimeMillis()
    val instant1 = (baseTime + 1000).toString
    val instant2 = (baseTime + 2000).toString

    val firstBatchRecords = recordsToStrings(dataGen.generateInserts(instant1, 5)).asScala.toList
    val firstBatchDF: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(firstBatchRecords, 2))

    firstBatchDF.write.format("org.apache.hudi")
      .options(testOpts)
      .option(DataSourceWriteOptions.OPERATION.key(), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(testBasePath)

    Thread.sleep(50)

    val secondBatchRecords = recordsToStrings(dataGen.generateUniqueUpdates(instant2, 3)).asScala.toList
    val secondBatchDF: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(secondBatchRecords, 2))

    secondBatchDF.write.format("org.apache.hudi")
      .options(testOpts)
      .option(DataSourceWriteOptions.OPERATION.key(), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(testBasePath)

    val metaClient = createMetaClient(spark, testBasePath)
    val commits = metaClient.getActiveTimeline.filterCompletedInstants().getInstantsAsStream.toArray()
    val lastCommit = commits(commits.length - 1).asInstanceOf[HoodieInstant]

    val completionTimeQueryView = metaClient.getTableFormat.getTimelineFactory.createCompletionTimeQueryView(metaClient)

    val requestedToCompletionMap = scala.collection.mutable.Map[String, String]()
    commits.foreach { commit =>
      val instant = commit.asInstanceOf[HoodieInstant]
      val requestedTime = instant.requestedTime()
      val completionTimeOpt = completionTimeQueryView.getCompletionTime(requestedTime)
      val completionTime = if (completionTimeOpt.isPresent) completionTimeOpt.get() else requestedTime
      requestedToCompletionMap(requestedTime) = completionTime
    }

    completionTimeQueryView.close()

    val firstCommitCompletionTime = commits(0).asInstanceOf[HoodieInstant].getCompletionTime
    val startTime = (firstCommitCompletionTime.toLong - 1).toString

    val incrementalDF = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key(), startTime)
      .option(DataSourceReadOptions.END_COMMIT.key(), lastCommit.getCompletionTime)
      .option(DataSourceReadOptions.INCREMENTAL_READ_TABLE_VERSION.key, "9")
      .option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key, enableFileGroupReader.toString)
      .load(testBasePath)

    val tempViewName = s"hudi_incremental_test_${tableTypeStr}_${enableFileGroupReader}"
    println(s"incrementalDF.schema.fieldNames: ${incrementalDF.schema.fieldNames.mkString(", ")}")
    incrementalDF.createOrReplaceTempView(tempViewName)
    incrementalDF.show(false)

    val completionTimesDF = spark.sql(s"""
      SELECT DISTINCT _hoodie_commit_completion_time
      FROM $tempViewName
      ORDER BY _hoodie_commit_completion_time
    """)

    val actualCompletionTimes = completionTimesDF.collect().map(_.getString(0))
    assertTrue(actualCompletionTimes.length == 5, s"Should have length of 5 but got ${actualCompletionTimes.length}")

    val expectedCompletionTimes = commits.map(_.asInstanceOf[HoodieInstant].getCompletionTime).sorted

    val actualTimesSorted = actualCompletionTimes.sorted
    val completionTimeColumnWorking = actualTimesSorted.sameElements(expectedCompletionTimes)
    assertTrue(completionTimeColumnWorking,
      s"V2 relation should show completion times in _hoodie_completion_time field. " +
      s"Expected: ${expectedCompletionTimes.mkString(", ")}, " +
      s"Got: ${actualTimesSorted.mkString(", ")}")
  }
}
