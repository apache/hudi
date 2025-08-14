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

import org.apache.hudi.{DataSourceWriteOptions, SparkDatasetMixin}
import org.apache.hudi.HoodieConversionUtils.toJavaOption
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.common.util.Option
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.config.metrics.{HoodieMetricsConfig, HoodieMetricsDatadogConfig}
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.hudi.util.JFunction

import org.apache.spark.sql._
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.junit.jupiter.api.{AfterEach, Assertions, BeforeEach, Test}
import org.junit.jupiter.api.function.Executable
import org.slf4j.LoggerFactory

import java.util.function.Consumer

import scala.collection.JavaConverters._

/**
 * Tests on Spark DataSource for MOR table.
 */
class TestMetricsReporter extends HoodieSparkClientTestBase with SparkDatasetMixin {
  var spark: SparkSession = null
  private val log = LoggerFactory.getLogger(classOf[TestMORDataSource])
  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
  )

  @BeforeEach override def setUp() {
    setTableName("hoodie_test")
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initHoodieStorage()
  }

  @AfterEach override def tearDown() = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
  }

  override def getSparkSessionExtensionsInjector: Option[Consumer[SparkSessionExtensions]] =
    toJavaOption(
      Some(
        JFunction.toJavaConsumer((receiver: SparkSessionExtensions) => new HoodieSparkSessionExtension().apply(receiver)))
    )

  @Test
  def testSmokeDatadogReporter() {
    val records1 = recordsToStrings(dataGen.generateInserts("001", 100)).asScala
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1.toSeq, 2))
    val writeOpts: Map[String, String] = commonOpts ++ Map(
      DataSourceWriteOptions.OPERATION.key -> DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      DataSourceWriteOptions.TABLE_TYPE.key -> DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL,
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetricsConfig.TURN_METRICS_ON.key -> "true",
      HoodieMetricsConfig.METRICS_REPORTER_TYPE_VALUE.key -> "DATADOG",
      HoodieMetricsDatadogConfig.API_KEY_SKIP_VALIDATION.key -> "true",
      HoodieMetricsDatadogConfig.METRIC_PREFIX_VALUE.key -> "hudi",
      HoodieMetricsDatadogConfig.API_SITE_VALUE.key -> "US",
      HoodieMetricsDatadogConfig.API_KEY.key -> "dummykey")

    Assertions.assertDoesNotThrow(new Executable {
      override def execute(): Unit =
        inputDF1.write.format("org.apache.hudi")
          .options(writeOpts)
          .mode(SaveMode.Overwrite)
          .save(basePath)

    })
  }
}
