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
import org.apache.hudi.common.table.timeline.TimelineUtils.HollowCommitHandling.USE_TRANSITION_TIME
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.jupiter.api.{AfterEach, Assertions, BeforeEach}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

import scala.collection.JavaConverters._

class TestIncrementalReadByStateTransitionTime extends HoodieSparkClientTestBase  {

  var spark: SparkSession = null

  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
    HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key -> "1"
  )

  @BeforeEach
  override def setUp(): Unit = {
    setTableName("hoodie_test")
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initHoodieStorage()
  }

  @AfterEach
  override def tearDown(): Unit = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testReadingWithStateTransitionTime(tableType: HoodieTableType): Unit = {
    val records = recordsToStrings(dataGen.generateInserts("001", 100)).asScala.toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    inputDF.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType.name())
      .option(DataSourceWriteOptions.OPERATION.key(), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val metaClient = createMetaClient(spark, basePath)

    val firstInstant = metaClient.getActiveTimeline.filterCompletedInstants().getInstantsOrderedByStateTransitionTime
      .findFirst().get()

    val result1 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key(), "000")
      .option(DataSourceReadOptions.INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT.key(), USE_TRANSITION_TIME.name())
      .option(DataSourceReadOptions.END_INSTANTTIME.key(), firstInstant.getTimestamp)
      .load(basePath)
      .count()

    Assertions.assertEquals(result1, 0)
    val result2 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key(), "000")
      .option(DataSourceReadOptions.INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT.key(), USE_TRANSITION_TIME.name())
      .option(DataSourceReadOptions.END_INSTANTTIME.key(), firstInstant.getStateTransitionTime)
      .load(basePath)
      .count()
    Assertions.assertEquals(result2, 100)
  }
}
