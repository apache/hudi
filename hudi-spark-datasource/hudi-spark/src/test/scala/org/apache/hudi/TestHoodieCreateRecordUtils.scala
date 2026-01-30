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

import org.apache.hudi.common.model.WriteOperationType
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen.constant.KeyGeneratorOptions

import org.apache.spark.SparkException
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions.{assertNotNull, assertTrue}
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}

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
    assertTrue(exception.getMessage.contains("Please ensure all records have non-null values for the precombine field"),
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
