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

package org.apache.hudi

import org.apache.hudi.common.config.{RecordMergeMode, TypedProperties}
import org.apache.hudi.common.model.{HoodieRecordMerger, HoodieTableType, WriteOperationType}
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.hudi.testutils.TaskPayloadTestUtils.serializedClasses

import org.apache.spark.SparkException
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types._
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

import scala.collection.JavaConverters._

/**
 * Test cases for {@link HoodieCreateRecordUtils}.
 */
class TestHoodieCreateRecordUtils {

  private val SPARK_SCHEMA = StructType(Seq(
    StructField("uuid", StringType, nullable = false),
    StructField("name", StringType, nullable = false),
    StructField("age", IntegerType, nullable = false),
    StructField("ts", LongType, nullable = true),
    StructField("partition", StringType, nullable = false)
  ))

  // Common test constants
  private val TEST_TABLE_NAME = "test_table"
  private val RECORD_NAME = "TestRecord"
  private val RECORD_NAMESPACE = "org.apache.hudi.test"
  private val INSTANT_TIME = "20231031000000"
  private val RECORD_KEY_FIELD = "uuid"
  private val PARTITION_FIELD = "partition"
  private val PRECOMBINE_FIELD = "ts"

  /**
   * Helper method to create DataFrame from Row data
   */
  private def createTestDataFrame(rows: Row*): org.apache.spark.sql.DataFrame = {
    val spark = TestHoodieCreateRecordUtils.spark
    spark.createDataFrame(spark.sparkContext.parallelize(rows), SPARK_SCHEMA)
  }

  /**
   * Helper method to get the root cause of an exception.
   * Iterative implementation to avoid stack overflow and handle circular references.
   *
   * @param t The throwable to extract root cause from
   * @return The root cause throwable
   */
  private def getRootCause(t: Throwable): Throwable = {
    var current = t
    val visited = scala.collection.mutable.Set[Throwable]()

    while (current.getCause != null && !visited.contains(current)) {
      visited += current
      current = current.getCause
    }

    current
  }

  /**
   * Helper method to create base parameters common to all tests.
   * These are the mandatory properties required by SimpleKeyGenerator.
   */
  private def createBaseParameters(): Map[String, String] = {
    Map(
      // KeyGeneratorOptions (used by some parts of the pipeline)
      KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key() -> RECORD_KEY_FIELD,
      KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key() -> PARTITION_FIELD,
      // DataSourceWriteOptions (required by SimpleKeyGenerator)
      DataSourceWriteOptions.RECORDKEY_FIELD.key() -> RECORD_KEY_FIELD,
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key() -> PARTITION_FIELD
    )
  }

  /**
   * Helper method to create common parameters for tests with precombine
   */
  private def createParametersWithPrecombine(payloadClass: String = "org.apache.hudi.common.model.DefaultHoodieRecordPayload"): Map[String, String] = {
    createBaseParameters() ++ Map(
      DataSourceWriteOptions.PRECOMBINE_FIELD.key() -> PRECOMBINE_FIELD,
      DataSourceWriteOptions.PAYLOAD_CLASS_NAME.key() -> payloadClass,
      HoodieWriteConfig.COMBINE_BEFORE_UPSERT.key() -> "true",
      DataSourceWriteOptions.INSERT_DROP_DUPS.key() -> "false"
    )
  }

  /**
   * Helper method to create parameters for tests without precombine
   */
  private def createParametersWithoutPrecombine(): Map[String, String] = {
    createBaseParameters() ++ Map(
      DataSourceWriteOptions.PAYLOAD_CLASS_NAME.key() -> "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
      HoodieWriteConfig.COMBINE_BEFORE_INSERT.key() -> "false",
      DataSourceWriteOptions.INSERT_DROP_DUPS.key() -> "false"
    )
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testRecordCreationTasksCaptureNoWriteState(recordType: HoodieRecordType): Unit = {
    val df = createTestDataFrame(Row("id1", "Alice", 25, 1000L, "par1"), Row("id2", "Bob", 30, 2000L, "par2"))
    val schema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(df.schema, RECORD_NAME, RECORD_NAMESPACE)
    val parameters = createBaseParameters()
    val mergerProps = if (recordType == HoodieRecordType.SPARK) {
      Map(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key -> classOf[DefaultSparkRecordMerger].getName,
        HoodieWriteConfig.RECORD_MERGE_STRATEGY_ID.key -> HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID)
    } else {
      Map.empty[String, String]
    }
    val basePath = TestHoodieCreateRecordUtils.tempDir + "/test_task_payload_" + recordType
    val config = HoodieWriteConfig.newBuilder.withPath(basePath).withSchema(schema.toString).forTable(TEST_TABLE_NAME)
      .withProps((parameters ++ mergerProps).asJava).build
    assertEquals(recordType, config.getRecordMerger.getRecordType)
    val tableConfig = HoodieTableMetaClient.newTableBuilder.setTableType(HoodieTableType.COPY_ON_WRITE)
      .setTableName(TEST_TABLE_NAME).setRecordKeyFields(RECORD_KEY_FIELD).setPartitionFields(PARTITION_FIELD)
      .initTable(HadoopFSUtils.getStorageConf(TestHoodieCreateRecordUtils.spark.sparkContext.hadoopConfiguration), basePath)
      .getTableConfig

    val records = HoodieCreateRecordUtils.createHoodieRecordRdd(HoodieCreateRecordUtils.createHoodieRecordRddArgs(
      df, config, parameters, RECORD_NAME, RECORD_NAMESPACE, schema, schema, WriteOperationType.UPSERT, INSTANT_TIME,
      preppedSparkSqlWrites = false, preppedSparkSqlMergeInto = false, preppedWriteOperation = false, tableConfig))

    val heavy = serializedClasses(records.rdd).asScala
      .filter(clazz => Seq(classOf[Dataset[_]], classOf[SparkSession], classOf[HoodieWriteConfig], classOf[HoodieTableConfig])
        .exists(_.isAssignableFrom(clazz)))
      .map(_.getName)
    assertTrue(heavy.isEmpty, s"Record creation tasks carry $heavy")
    assertEquals(Seq("id1" -> "par1", "id2" -> "par2"),
      records.collect().asScala.map(record => record.getRecordKey -> record.getPartitionPath).sorted)

    // prepped writes read the keys from the meta fields, so the tasks need no key generator properties
    val preppedRecords = HoodieCreateRecordUtils.createHoodieRecordRdd(HoodieCreateRecordUtils.createHoodieRecordRddArgs(
      df, config, parameters, RECORD_NAME, RECORD_NAMESPACE, schema, schema, WriteOperationType.UPSERT_PREPPED, INSTANT_TIME,
      preppedSparkSqlWrites = false, preppedSparkSqlMergeInto = false, preppedWriteOperation = true, tableConfig))
    assertTrue(!serializedClasses(preppedRecords.rdd).contains(classOf[TypedProperties]),
      "Prepped record creation tasks carry the key generator properties")
  }

  @Test
  def testNullPrecombineFieldThrowsClearError(): Unit = {
    val df = createTestDataFrame(Row("id1", "Alice", 25, null, "par1"))
    val parameters = createParametersWithPrecombine()

    val exception = try {
      // Attempt to write which will trigger HoodieCreateRecordUtils
      df.write
        .format("hudi")
        .options(parameters)
        .option(DataSourceWriteOptions.TABLE_NAME.key(), TEST_TABLE_NAME)
        .option("hoodie.table.name", TEST_TABLE_NAME)
        .option("path", TestHoodieCreateRecordUtils.tempDir + "/test_null_precombine")
        .mode("overwrite")
        .save()
      null
    } catch {
      case e: SparkException =>
        getRootCause(e) match {
          case iae: IllegalArgumentException => iae
          case other => other
        }
      case e: IllegalArgumentException => e
      case e: Exception =>
        getRootCause(e) match {
          case iae: IllegalArgumentException => iae
          case _ => throw e
        }
    }

    assertNotNull(exception, "Expected IllegalArgumentException for null precombine field")
    assertTrue(exception.isInstanceOf[IllegalArgumentException],
      s"Expected IllegalArgumentException but got ${exception.getClass.getName}")
    assertTrue(exception.getMessage.contains("has null value for record key"),
      s"Exception message should mention null value for record key. Actual: ${exception.getMessage}")
    assertTrue(exception.getMessage.contains("Please ensure all records have non-null values for the ordering field"),
      s"Exception message should provide guidance. Actual: ${exception.getMessage}")
    assertTrue(exception.getMessage.contains("OverwriteWithLatestAvroPayload"),
      s"Exception message should suggest alternative payload class. Actual: ${exception.getMessage}")
  }

  @Test
  def testValidPrecombineFieldSucceeds(): Unit = {
    val df = createTestDataFrame(Row("id1", "Alice", 25, 1000L, "par1"))
    val parameters = createParametersWithPrecombine()

    // Should not throw exception
    df.write
      .format("hudi")
      .options(parameters)
      .option(DataSourceWriteOptions.TABLE_NAME.key(), TEST_TABLE_NAME)
      .option("hoodie.table.name", TEST_TABLE_NAME)
      .option("path", TestHoodieCreateRecordUtils.tempDir + "/test_valid_precombine")
      .mode("overwrite")
      .save()

    // Verify data was written
    val result = TestHoodieCreateRecordUtils.spark.read
      .format("hudi")
      .load(TestHoodieCreateRecordUtils.tempDir + "/test_valid_precombine")
    assertTrue(result.count() > 0, "Data should have been written successfully")
  }

  @Test
  def testNullPrecombineFieldErrorContainsRecordKey(): Unit = {
    val testRecordKey = "test_key_123"
    val df = createTestDataFrame(Row(testRecordKey, "Bob", 30, null, "par2"))
    val parameters = createParametersWithPrecombine()

    val exception = try {
      df.write
        .format("hudi")
        .options(parameters)
        .option(DataSourceWriteOptions.TABLE_NAME.key(), TEST_TABLE_NAME)
        .option("hoodie.table.name", TEST_TABLE_NAME)
        .option("path", TestHoodieCreateRecordUtils.tempDir + "/test_null_precombine_key")
        .mode("overwrite")
        .save()
      null
    } catch {
      case e: Exception =>
        getRootCause(e) match {
          case iae: IllegalArgumentException => iae
          case _ => throw e
        }
    }

    assertNotNull(exception)
    assertTrue(exception.getMessage.contains(testRecordKey),
      s"Exception message should contain the record key '$testRecordKey' to help identify the problematic record. Actual: ${exception.getMessage}")
  }

  @Test
  def testNullPrecombineFieldWithOverwritePayloadSucceeds(): Unit = {
    // OverwriteWithLatestAvroPayload should allow null precombine values
    val df = createTestDataFrame(Row("id1", "Alice", 25, null, "par1"))
    val parameters = createParametersWithPrecombine(
      payloadClass = "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload")

    // Should not throw exception - OverwriteWithLatestAvroPayload doesn't require ordering values
    df.write
      .format("hudi")
      .options(parameters)
      .option(DataSourceWriteOptions.TABLE_NAME.key(), TEST_TABLE_NAME)
      .option("hoodie.table.name", TEST_TABLE_NAME)
      .option("path", TestHoodieCreateRecordUtils.tempDir + "/test_null_precombine_overwrite")
      .mode("overwrite")
      .save()

    // Verify data was written
    val result = TestHoodieCreateRecordUtils.spark.read
      .format("hudi")
      .load(TestHoodieCreateRecordUtils.tempDir + "/test_null_precombine_overwrite")
    assertTrue(result.count() > 0, "Data should have been written successfully with null precombine using OverwriteWithLatestAvroPayload")
  }

  @Test
  def testNullPrecombineFieldWithCommitTimeOrderingSucceeds(): Unit = {
    // COMMIT_TIME_ORDERING merge mode should allow null precombine values
    val df = createTestDataFrame(Row("id1", "Alice", 25, null, "par1"))
    val parameters = createBaseParameters() ++ Map(
      DataSourceWriteOptions.PRECOMBINE_FIELD.key() -> PRECOMBINE_FIELD,
      HoodieWriteConfig.COMBINE_BEFORE_UPSERT.key() -> "true",
      DataSourceWriteOptions.INSERT_DROP_DUPS.key() -> "false",
      DataSourceWriteOptions.RECORD_MERGE_MODE.key() -> RecordMergeMode.COMMIT_TIME_ORDERING.name()
    )

    // Should not throw exception - COMMIT_TIME_ORDERING doesn't require ordering values
    df.write
      .format("hudi")
      .options(parameters)
      .option(DataSourceWriteOptions.TABLE_NAME.key(), TEST_TABLE_NAME)
      .option("hoodie.table.name", TEST_TABLE_NAME)
      .option("path", TestHoodieCreateRecordUtils.tempDir + "/test_null_precombine_commit_time")
      .mode("overwrite")
      .save()

    // Verify data was written
    val result = TestHoodieCreateRecordUtils.spark.read
      .format("hudi")
      .load(TestHoodieCreateRecordUtils.tempDir + "/test_null_precombine_commit_time")
    assertTrue(result.count() > 0, "Data should have been written successfully with null precombine using COMMIT_TIME_ORDERING")
  }
}

object TestHoodieCreateRecordUtils {
  var spark: SparkSession = _
  var tempDir: String = _

  @BeforeAll
  def setupSpark(): Unit = {
    tempDir = java.nio.file.Files.createTempDirectory("hudi_test_").toFile.getAbsolutePath
    spark = SparkSession.builder()
      .appName("TestHoodieCreateRecordUtils")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .getOrCreate()
  }

  @AfterAll
  def teardownSpark(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    // Clean up temp directory
    if (tempDir != null) {
      org.apache.commons.io.FileUtils.deleteQuietly(new java.io.File(tempDir))
    }
  }
}
