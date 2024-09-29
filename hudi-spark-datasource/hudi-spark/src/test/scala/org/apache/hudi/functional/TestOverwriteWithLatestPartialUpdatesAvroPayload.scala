/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.functional

import java.util.function.Consumer
import org.apache.hadoop.fs.FileSystem
import org.apache.hudi.HoodieConversionUtils.toJavaOption
import org.apache.hudi.{DataSourceReadOptions, DataSourceUtils, DataSourceWriteOptions, QuickstartUtils}
import org.apache.hudi.QuickstartUtils.{convertToStringList, getQuickstartWriteConfigs}
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.common.model.{HoodieRecordPayload, HoodieTableType, OverwriteWithLatestAvroPayload}
import org.apache.hudi.common.util
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.table.action.compact.CompactionTriggerStrategy
import org.apache.hudi.testutils.HoodieClientTestBase
import org.apache.hudi.util.JFunction
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{lit, typedLit}
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.types.StringType
import org.junit.jupiter.api.Assertions.{assertEquals, assertNull}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

import scala.collection.JavaConverters._

class TestOverwriteWithLatestPartialUpdatesAvroPayload extends HoodieClientTestBase {
  var spark: SparkSession = null

  override def getSparkSessionExtensionsInjector: util.Option[Consumer[SparkSessionExtensions]] =
    toJavaOption(
      Some(
        JFunction.toJavaConsumer((receiver: SparkSessionExtensions) => new HoodieSparkSessionExtension().apply(receiver)))
    )

  @BeforeEach override def setUp() {
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
    FileSystem.closeAll()
    System.gc()
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testOverwriteWithLatestPartialUpdatesAvroPayload(): Unit = {
    val hoodieTableType = HoodieTableType.COPY_ON_WRITE
    val dataGenerator = new QuickstartUtils.DataGenerator()
    val records = convertToStringList(dataGenerator.generateInserts(5))
    val recordsRDD = spark.sparkContext.parallelize(records.asScala.toSeq, 2)
    val inputDF = spark.read.json(sparkSession.createDataset(recordsRDD)(Encoders.STRING)).withColumn("_hoodie_change_columns", lit(null).cast(StringType)).withColumn("ts", lit(1L))
    inputDF.write.format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(DataSourceWriteOptions.TABLE_TYPE.key(), hoodieTableType.name())
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "uuid")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "partitionpath")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key, "ts")
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key, "org.apache.hudi.common.model.OverwriteWithLatestPartialUpdatesAvroPayload")
      .option(HoodieWriteConfig.TBL_NAME.key, "hoodie_test")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val records2 = convertToStringList(dataGenerator.generateUniqueUpdates(5))
    val inputDF2 = spark.read.json(sparkSession.createDataset(records2)(Encoders.STRING)).withColumn("_hoodie_change_columns", lit("rider")).withColumn("ts", lit(2L))
    inputDF2.write.format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(DataSourceWriteOptions.TABLE_TYPE.key(), hoodieTableType.name())
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "uuid")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "partitionpath")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key, "ts")
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option(HoodieWriteConfig.TBL_NAME.key, "hoodie_test")
      .option(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key, "org.apache.hudi.common.model.OverwriteWithLatestPartialUpdatesAvroPayload")
      .mode(SaveMode.Append)
      .save(basePath)

    val finalDF = spark.read.format("hudi").load(basePath)

    // Validate only "rider" is updated to inputDF2 value
    assertEquals(finalDF.select("rider").collectAsList().get(0).getString(0), inputDF2.select("rider").collectAsList().get(0).getString(0))
    assertEquals(finalDF.select("rider").collectAsList().get(1).getString(0), inputDF2.select("rider").collectAsList().get(1).getString(0))
    assertEquals(finalDF.select("rider").collectAsList().get(2).getString(0), inputDF2.select("rider").collectAsList().get(2).getString(0))
    assertEquals(finalDF.select("rider").collectAsList().get(3).getString(0), inputDF2.select("rider").collectAsList().get(3).getString(0))
    assertEquals(finalDF.select("rider").collectAsList().get(4).getString(0), inputDF2.select("rider").collectAsList().get(4).getString(0))
    assertEquals(finalDF.select("driver").collectAsList().get(0).getString(0), inputDF.select("driver").collectAsList().get(0).getString(0))
    assertEquals(finalDF.select("driver").collectAsList().get(1).getString(0), inputDF.select("driver").collectAsList().get(1).getString(0))
    assertEquals(finalDF.select("driver").collectAsList().get(2).getString(0), inputDF.select("driver").collectAsList().get(2).getString(0))
    assertEquals(finalDF.select("driver").collectAsList().get(3).getString(0), inputDF.select("driver").collectAsList().get(3).getString(0))
    assertEquals(finalDF.select("driver").collectAsList().get(4).getString(0), inputDF.select("driver").collectAsList().get(4).getString(0))
    assertNull(finalDF.select("_hoodie_change_columns").collectAsList().get(0).getString(0))
    assertNull(finalDF.select("_hoodie_change_columns").collectAsList().get(1).getString(0))
    assertNull(finalDF.select("_hoodie_change_columns").collectAsList().get(2).getString(0))
    assertNull(finalDF.select("_hoodie_change_columns").collectAsList().get(3).getString(0))
    assertNull(finalDF.select("_hoodie_change_columns").collectAsList().get(4).getString(0))
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testOverwriteWithLatestPartialUpdatesAvroPayloadOutOfOrder(hoodieTableType: HoodieTableType): Unit = {
    val dataGenerator = new QuickstartUtils.DataGenerator()
    val records = convertToStringList(dataGenerator.generateInserts(1))
    val recordsRDD = spark.sparkContext.parallelize(records.asScala.toSeq, 2)
    val inputDF = spark.read.json(sparkSession.createDataset(recordsRDD)(Encoders.STRING)).withColumn("_hoodie_change_columns", lit(null).cast(StringType)).withColumn("ts", lit(2L))
    inputDF.write.format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(DataSourceWriteOptions.TABLE_TYPE.key(), hoodieTableType.name())
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "uuid")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "partitionpath")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key, "ts")
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key, "org.apache.hudi.common.model.OverwriteWithLatestPartialUpdatesAvroPayload")
      .option(HoodieWriteConfig.TBL_NAME.key, "hoodie_test")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val records2 = convertToStringList(dataGenerator.generateUniqueUpdates(1))
    val inputDF2 = spark.read.json(sparkSession.createDataset(records2)(Encoders.STRING)).withColumn("_hoodie_change_columns", lit("rider")).withColumn("ts", lit(1L))
    inputDF2.write.format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(DataSourceWriteOptions.TABLE_TYPE.key(), hoodieTableType.name())
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "uuid")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "partitionpath")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key, "ts")
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option(HoodieWriteConfig.TBL_NAME.key, "hoodie_test")
      .option(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key, "org.apache.hudi.common.model.OverwriteWithLatestPartialUpdatesAvroPayload")
      .mode(SaveMode.Append)
      .save(basePath)

    val finalDF = spark.read.format("hudi").load(basePath)

    // Validate rider is not updated
    assertEquals(finalDF.select("rider").collectAsList().get(0).getString(0), inputDF.select("rider").collectAsList().get(0).getString(0))
    assertEquals(finalDF.select("driver").collectAsList().get(0).getString(0), inputDF.select("driver").collectAsList().get(0).getString(0))
    assertEquals(finalDF.select("fare").collectAsList().get(0).getDouble(0), inputDF.select("fare").collectAsList().get(0).getDouble(0))
    assertNull(finalDF.select("_hoodie_change_columns").collectAsList().get(0).getString(0))
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testOverwriteWithLatestPartialUpdatesAvroPayloadPrecombine(hoodieTableType: HoodieTableType): Unit = {
    val dataGenerator = new QuickstartUtils.DataGenerator()
    val records = convertToStringList(dataGenerator.generateInserts(1))
    val recordsRDD = spark.sparkContext.parallelize(records.asScala.toSeq, 2)
    val inputDF = spark.read.json(sparkSession.createDataset(recordsRDD)(Encoders.STRING)).withColumn("ts", lit(1L)).withColumn("_hoodie_change_columns", typedLit(null).cast(StringType))
    inputDF.write.format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(DataSourceWriteOptions.TABLE_TYPE.key(), hoodieTableType.name())
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "uuid")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "partitionpath")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key, "ts")
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key, "org.apache.hudi.common.model.OverwriteWithLatestPartialUpdatesAvroPayload")
      .option(HoodieWriteConfig.TBL_NAME.key, "hoodie_test")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val upsert1 = convertToStringList(dataGenerator.generateUniqueUpdates(1))
    val upsert1DF = spark.read.json(sparkSession.createDataset(upsert1)(Encoders.STRING)).withColumn("ts", lit(4L)).withColumn("_hoodie_change_columns", lit("rider,driver,ts"))

    val upsert2 = convertToStringList(dataGenerator.generateUniqueUpdates(1))
    val upsert2DF = spark.read.json(sparkSession.createDataset(upsert2)(Encoders.STRING)).withColumn("ts", lit(6L)).withColumn("_hoodie_change_columns", lit("rider,ts"))

    val upsert3 = convertToStringList(dataGenerator.generateUniqueUpdates(1))
    val upsert3DF = spark.read.json(sparkSession.createDataset(upsert3)(Encoders.STRING)).withColumn("ts", lit(3L)).withColumn("_hoodie_change_columns", lit("driver,ts"))

    val mergedDF = upsert1DF.union(upsert2DF).union(upsert3DF)

    mergedDF.write.format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(DataSourceWriteOptions.TABLE_TYPE.key(), hoodieTableType.name())
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "uuid")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "partitionpath")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key, "ts")
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option(HoodieWriteConfig.TBL_NAME.key, "hoodie_test")
      .option(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key, "org.apache.hudi.common.model.OverwriteWithLatestPartialUpdatesAvroPayload")
      .mode(SaveMode.Append)
      .save(basePath)

    val finalDF = spark.read.format("hudi").load(basePath)

    assertEquals(finalDF.select("driver").collectAsList().get(0).getString(0), upsert1DF.select("driver").collectAsList().get(0).getString(0))
    assertEquals(finalDF.select("rider").collectAsList().get(0).getString(0), upsert2DF.select("rider").collectAsList().get(0).getString(0))
    assertEquals(finalDF.select("fare").collectAsList().get(0).getDouble(0), inputDF.select("fare").collectAsList().get(0).getDouble(0))
    assertNull(finalDF.select("_hoodie_change_columns").collectAsList().get(0).getString(0))
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testOverwriteWithLatestPartialUpdatesAvroPayloadDelete(hoodieTableType: HoodieTableType): Unit = {
    val dataGenerator = new QuickstartUtils.DataGenerator()
    val records = convertToStringList(dataGenerator.generateInserts(1))
    val recordsRDD = spark.sparkContext.parallelize(records.asScala.toSeq, 2)
    val inputDF = spark.read.json(sparkSession.createDataset(recordsRDD)(Encoders.STRING)).withColumn("ts", lit(1L))
        .withColumn("_hoodie_is_deleted", lit(false))
        .withColumn("_hoodie_change_columns", typedLit(null).cast(StringType))
    inputDF.write.format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(DataSourceWriteOptions.TABLE_TYPE.key(), hoodieTableType.name())
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "uuid")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "partitionpath")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key, "ts")
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key, "org.apache.hudi.common.model.OverwriteWithLatestPartialUpdatesAvroPayload")
      .option(HoodieWriteConfig.TBL_NAME.key, "hoodie_test")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val upsert1 = convertToStringList(dataGenerator.generateUniqueUpdates(1))
    val upsert1DF = spark.read.json(sparkSession.createDataset(upsert1)(Encoders.STRING)).withColumn("ts", lit(4L))
      .withColumn("_hoodie_change_columns", typedLit(null).cast(StringType)).withColumn("_hoodie_is_deleted", lit(true))

    val upsert2 = convertToStringList(dataGenerator.generateUniqueUpdates(1))
    val upsert2DF = spark.read.json(sparkSession.createDataset(upsert2)(Encoders.STRING)).withColumn("ts", lit(6L))
      .withColumn("_hoodie_change_columns", lit("rider")).withColumn("_hoodie_is_deleted", lit(false))

    val upsert3 = convertToStringList(dataGenerator.generateUniqueUpdates(1))
    val upsert3DF = spark.read.json(sparkSession.createDataset(upsert3)(Encoders.STRING)).withColumn("ts", lit(3L))
      .withColumn("_hoodie_change_columns", lit("driver")).withColumn("_hoodie_is_deleted", lit(false))

    val mergedDF = upsert1DF.union(upsert2DF).union(upsert3DF)

    mergedDF.write.format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(DataSourceWriteOptions.TABLE_TYPE.key(), hoodieTableType.name())
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "uuid")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "partitionpath")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key, "ts")
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option(HoodieWriteConfig.TBL_NAME.key, "hoodie_test")
      .option(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key, "org.apache.hudi.common.model.OverwriteWithLatestPartialUpdatesAvroPayload")
      .mode(SaveMode.Append)
      .save(basePath)

    val finalDF = spark.read.format("hudi").load(basePath)
    assertEquals(finalDF.collect().length, 0)
  }

  @Test
  def testOverwriteWithLatestPartialUpdatesAvroPayloadCompactionMOR(): Unit = {
    val (tableName, tablePath) = ("hoodie_mor_test_table", s"${basePath}_mor_test_table")

    val dataGenerator = new QuickstartUtils.DataGenerator()
    val records = convertToStringList(dataGenerator.generateInserts(1))
    val recordsRDD = spark.sparkContext.parallelize(records.asScala.toSeq, 2)
    val inputDF = spark.read.json(sparkSession.createDataset(recordsRDD)(Encoders.STRING)).withColumn("ts", lit(1L)).withColumn("_hoodie_change_columns", typedLit(null).cast(StringType))
    inputDF.write.format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(HoodieWriteConfig.TBL_NAME.key, tableName)
      .option(DataSourceWriteOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name())
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "uuid")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "partitionpath")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key, "ts")
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key, "org.apache.hudi.common.model.OverwriteWithLatestPartialUpdatesAvroPayload")
      .option(HoodieWriteConfig.TBL_NAME.key, "hoodie_test")
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    val upsert1 = convertToStringList(dataGenerator.generateUniqueUpdates(1))
    val upsert1DF = spark.read.json(sparkSession.createDataset(upsert1)(Encoders.STRING)).withColumn("ts", lit(4L)).withColumn("_hoodie_change_columns", lit("rider,driver,ts"))

    val upsert2 = convertToStringList(dataGenerator.generateUniqueUpdates(1))
    val upsert2DF = spark.read.json(sparkSession.createDataset(upsert2)(Encoders.STRING)).withColumn("ts", lit(6L)).withColumn("_hoodie_change_columns", lit("rider,ts"))

    val upsert3 = convertToStringList(dataGenerator.generateUniqueUpdates(1))
    val upsert3DF = spark.read.json(sparkSession.createDataset(upsert3)(Encoders.STRING)).withColumn("ts", lit(3L)).withColumn("_hoodie_change_columns", lit("driver,ts"))

    val mergedDF = upsert1DF.union(upsert2DF).union(upsert3DF)

    mergedDF.write.format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(HoodieWriteConfig.TBL_NAME.key, tableName)
      .option(DataSourceWriteOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name())
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "uuid")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "partitionpath")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key, "ts")
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option(HoodieWriteConfig.TBL_NAME.key, "hoodie_test")
      .option(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key, "org.apache.hudi.common.model.OverwriteWithLatestPartialUpdatesAvroPayload")
      .mode(SaveMode.Append)
      .save(tablePath)

    val snapshotDF = spark.read.format("hudi").load(tablePath)
    val readOptimizedDF = spark.read.format("hudi").option(DataSourceReadOptions.QUERY_TYPE.key,
       DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL).load(tablePath)

    assertEquals(snapshotDF.select("driver").collectAsList().get(0).getString(0), upsert1DF.select("driver").collectAsList().get(0).getString(0))
    assertEquals(snapshotDF.select("rider").collectAsList().get(0).getString(0), upsert2DF.select("rider").collectAsList().get(0).getString(0))
    assertEquals(snapshotDF.select("fare").collectAsList().get(0).getDouble(0), inputDF.select("fare").collectAsList().get(0).getDouble(0))
    assertNull(snapshotDF.select("_hoodie_change_columns").collectAsList().get(0).getString(0))

    assertEquals(readOptimizedDF.select("driver").collectAsList().get(0).getString(0), inputDF.select("driver").collectAsList().get(0).getString(0))
    assertEquals(readOptimizedDF.select("rider").collectAsList().get(0).getString(0), inputDF.select("rider").collectAsList().get(0).getString(0))
    assertEquals(readOptimizedDF.select("fare").collectAsList().get(0).getDouble(0), inputDF.select("fare").collectAsList().get(0).getDouble(0))
    assertNull(readOptimizedDF.select("_hoodie_change_columns").collectAsList().get(0).getString(0))

    val compactionOptions = getQuickstartWriteConfigs.asScala ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key() -> HoodieTableType.MERGE_ON_READ.name(),
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "uuid",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key ->"partitionpath",
      DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "ts",
      DataSourceWriteOptions.OPERATION.key -> DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      HoodieWriteConfig.TBL_NAME.key -> tableName,
      HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key -> "org.apache.hudi.common.model.OverwriteWithLatestPartialUpdatesAvroPayload",
      HoodieCompactionConfig.INLINE_COMPACT_TRIGGER_STRATEGY.key -> CompactionTriggerStrategy.NUM_COMMITS.name,
      HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key -> "1",
      DataSourceWriteOptions.ASYNC_COMPACT_ENABLE.key -> "false",
      HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key -> classOf[OverwriteWithLatestAvroPayload].getName
    )
    val client = DataSourceUtils.createHoodieClient(
      spark.sparkContext, "", tablePath, tableName, compactionOptions.asJava)
      .asInstanceOf[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]]
    val compactionInstant = client.scheduleCompaction(org.apache.hudi.common.util.Option.empty()).get()

    // NOTE: this executes the compaction to write the compacted base files, and leaves the
    // compaction instant still inflight, emulating a compaction action that is in progress
    client.commitCompaction(compactionInstant, client.compact(compactionInstant).getCommitMetadata.get(), org.apache.hudi.common.util.Option.empty())

    val snapshotDF2 = spark.read.format("hudi").load(tablePath)
    val readOptimizedDF2 = spark.read.format("hudi").option(DataSourceReadOptions.QUERY_TYPE.key,
      DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL).load(tablePath)

    assertEquals(snapshotDF2.select("driver").collectAsList().get(0).getString(0), upsert1DF.select("driver").collectAsList().get(0).getString(0))
    assertEquals(snapshotDF2.select("rider").collectAsList().get(0).getString(0), upsert2DF.select("rider").collectAsList().get(0).getString(0))
    assertEquals(snapshotDF2.select("fare").collectAsList().get(0).getDouble(0), inputDF.select("fare").collectAsList().get(0).getDouble(0))
    assertNull(snapshotDF2.select("_hoodie_change_columns").collectAsList().get(0).getString(0))

    assertEquals(readOptimizedDF2.select("driver").collectAsList().get(0).getString(0), upsert1DF.select("driver").collectAsList().get(0).getString(0))
    assertEquals(readOptimizedDF2.select("rider").collectAsList().get(0).getString(0), upsert2DF.select("rider").collectAsList().get(0).getString(0))
    assertEquals(readOptimizedDF2.select("fare").collectAsList().get(0).getDouble(0), inputDF.select("fare").collectAsList().get(0).getDouble(0))
    assertNull(readOptimizedDF2.select("_hoodie_change_columns").collectAsList().get(0).getString(0))
  }
}
