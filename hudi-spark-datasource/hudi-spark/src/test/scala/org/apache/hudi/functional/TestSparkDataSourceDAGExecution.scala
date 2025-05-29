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

import org.apache.hudi.{DataSourceWriteOptions, DefaultSparkRecordMerger, ScalaAssertionSupport}
import org.apache.hudi.HoodieConversionUtils.toJavaOption
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.common.util.Option
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.hudi.util.JFunction

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}
import org.apache.spark.sql.{SaveMode, SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.slf4j.LoggerFactory

import java.util.function.Consumer

import scala.collection.JavaConverters._

/**
 * Tests around Dag execution for Spark DataSource.
 */
class TestSparkDataSourceDAGExecution extends HoodieSparkClientTestBase with ScalaAssertionSupport {
  private val log = LoggerFactory.getLogger(getClass)
  var spark: SparkSession = null
  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    "hoodie.bulkinsert.shuffle.parallelism" -> "2",
    "hoodie.delete.shuffle.parallelism" -> "1",
    HoodieTableConfig.PARTITION_METAFILE_USE_BASE_FORMAT.key() -> "true",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
    HoodieMetadataConfig.ENABLE.key -> "false"
  )
  val sparkOpts = Map(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key -> classOf[DefaultSparkRecordMerger].getName)

  val verificationCol: String = "driver"
  val updatedVerificationVal: String = "driver_update"

  override def getSparkSessionExtensionsInjector: Option[Consumer[SparkSessionExtensions]] =
    toJavaOption(
      Some(
        JFunction.toJavaConsumer((receiver: SparkSessionExtensions) => new HoodieSparkSessionExtension().apply(receiver)))
    )

  @BeforeEach
  override def setUp(): Unit = {
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
    FileSystem.closeAll()
    System.gc()
  }

  @ParameterizedTest
  @CsvSource(Array(
    "upsert,org.apache.hudi.client.SparkRDDWriteClient.commit",
    "insert,org.apache.hudi.client.SparkRDDWriteClient.commit",
    "bulk_insert,org.apache.hudi.HoodieSparkSqlWriterInternal.bulkInsertAsRow"))
  def testWriteOperationDoesNotTriggerRepeatedDAG(operation: String, event: String): Unit = {
    // register stage event listeners
    val stageListener = new StageListener(event)
    spark.sparkContext.addSparkListener(stageListener)

    var structType: StructType = null
    val records = recordsToStrings(dataGen.generateInserts("%05d".format(1), 10)).asScala.toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    structType = inputDF.schema
    inputDF.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, operation)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    // verify that operation is not trigered more than once.
    assertEquals(1, stageListener.triggerCount)
  }

  @Test
  def testClusteringDoesNotTriggerRepeatedDAG(): Unit = {
    // register stage event listeners
    val stageListener = new StageListener("org.apache.hudi.client.BaseHoodieTableServiceClient.cluster")
    spark.sparkContext.addSparkListener(stageListener)

    var structType: StructType = null
    for (i <- 1 to 2) {
      val records = recordsToStrings(dataGen.generateInserts("%05d".format(i), 100)).asScala.toList
      val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
      structType = inputDF.schema
      inputDF.write.format("hudi")
        .options(commonOpts)
        .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
        .mode(if (i == 1) SaveMode.Overwrite else SaveMode.Append)
        .save(basePath)
    }

    // trigger clustering.
    val records = recordsToStrings(dataGen.generateInserts("%05d".format(4), 100)).asScala.toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    structType = inputDF.schema
    inputDF.write.format("hudi")
      .options(commonOpts)
      .option("hoodie.clean.commits.retained", "0")
      .option("hoodie.parquet.small.file.limit", "0")
      .option("hoodie.clustering.inline", "true")
      .option("hoodie.clustering.inline.max.commits", "2")
      .mode(SaveMode.Append)
      .save(basePath)

    // verify that clustering is not trigered more than once.
    assertEquals(1, stageListener.triggerCount)
  }

  @Test
  def testCompactionDoesNotTriggerRepeatedDAG(): Unit = {
    // register stage event listeners
    val stageListener = new StageListener("org.apache.hudi.client.BaseHoodieTableServiceClient.commitCompaction")
    spark.sparkContext.addSparkListener(stageListener)

    var structType: StructType = null
    for (i <- 1 to 2) {
      val records = recordsToStrings(dataGen.generateInserts("%05d".format(i), 100)).asScala.toList
      val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
      structType = inputDF.schema
      inputDF.write.format("hudi")
        .options(commonOpts)
        .option(DataSourceWriteOptions.TABLE_TYPE.key(), DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
        .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
        .mode(if (i == 1) SaveMode.Overwrite else SaveMode.Append)
        .save(basePath)
    }

    // trigger compaction
    val records = recordsToStrings(dataGen.generateUniqueUpdates("%05d".format(4), 100)).asScala.toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    structType = inputDF.schema
    inputDF.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.TABLE_TYPE.key(), DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .option("hoodie.compact.inline.max.delta.commits", "1")
      .option("hoodie.compact.inline", "true")
      .mode(SaveMode.Append)
      .save(basePath)

    // verify that compaction is not trigered more than once.
    assertEquals(1, stageListener.triggerCount)
  }

  /** ************ Stage Event Listener ************* */
  class StageListener(eventToTrack: String) extends SparkListener() {
    var triggerCount = 0

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      if (stageCompleted.stageInfo.details.contains(eventToTrack)) {
        triggerCount += 1
      }
    }
  }
}
