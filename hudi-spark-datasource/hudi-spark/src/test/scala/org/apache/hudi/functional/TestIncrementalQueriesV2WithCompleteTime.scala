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
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Tag}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

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
  @CsvSource(Array("COPY_ON_WRITE", "MERGE_ON_READ"))
  def testCompleteTimeFilteringSemantics(tableTypeStr: String): Unit = {
    val testOpts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key() -> tableTypeStr
    )
    val firstBatchRecords = recordsToStrings(dataGen.generateInserts("001", 10)).asScala.toList
    val firstBatchDF: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(firstBatchRecords, 2))

    firstBatchDF.write.format("org.apache.hudi")
      .options(testOpts)
      .option(DataSourceWriteOptions.OPERATION.key(), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val secondBatchRecords = recordsToStrings(dataGen.generateUniqueUpdates("002", 5)).asScala.toList
    val secondBatchDF: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(secondBatchRecords, 2))

    secondBatchDF.write.format("org.apache.hudi")
      .options(testOpts)
      .option(DataSourceWriteOptions.OPERATION.key(), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val thirdBatchRecords = recordsToStrings(dataGen.generateUniqueUpdates("003", 8)).asScala.toList
    val thirdBatchDF: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(thirdBatchRecords, 2))

    thirdBatchDF.write.format("org.apache.hudi")
      .options(testOpts)
      .option(DataSourceWriteOptions.OPERATION.key(), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val metaClient = createMetaClient(spark, basePath)
    val commits = metaClient.getActiveTimeline.filterCompletedInstants().getInstantsAsStream.toArray()
    val lastCommit = commits(commits.length - 1).asInstanceOf[org.apache.hudi.common.table.timeline.HoodieInstant]

    val incrementalDF = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key(), "000")
      .option(DataSourceReadOptions.END_COMMIT.key(), lastCommit.getCompletionTime)
      .option(DataSourceReadOptions.INCREMENTAL_READ_TABLE_VERSION.key, "9")
      .load(basePath)

    incrementalDF.createOrReplaceTempView("hudi_incremental_test")

    val completeTimesDF = spark.sql("""
      SELECT DISTINCT _hoodie_commit_time 
      FROM hudi_incremental_test 
      ORDER BY _hoodie_commit_time
    """)

    completeTimesDF.show(false)
    
    val completeTimesArray = completeTimesDF.collect().map(_.getString(0))
    assertTrue(completeTimesArray.length == 3, "Should have 3 distinct completion times")

    val secondCompleteTime = completeTimesArray(1)

    val geqSQL = s"""
      SELECT _hoodie_commit_time, _row_key, driver 
      FROM hudi_incremental_test 
      WHERE _hoodie_commit_time >= '$secondCompleteTime'
      ORDER BY _hoodie_commit_time, _row_key
    """
    val geqDF = spark.sql(geqSQL)
    geqDF.show(false)
    val geqCount = geqDF.count()

    val gtSQL = s"""
      SELECT _hoodie_commit_time, _row_key, driver 
      FROM hudi_incremental_test 
      WHERE _hoodie_commit_time > '$secondCompleteTime'
      ORDER BY _hoodie_commit_time, _row_key
    """
    val gtDF = spark.sql(gtSQL)
    gtDF.show(false)
    val gtCount = gtDF.count()

    val exactSQL = s"""
      SELECT _hoodie_commit_time, _row_key, driver 
      FROM hudi_incremental_test 
      WHERE _hoodie_commit_time = '$secondCompleteTime'
      ORDER BY _row_key
    """
    val exactDF = spark.sql(exactSQL)
    exactDF.show(false)
    val exactMatchCount = exactDF.count()

    val diff = geqCount - gtCount
    assertTrue(diff == 1)
    assertEquals(diff, exactMatchCount)
  }
}