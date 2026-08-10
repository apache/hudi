/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.functional

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.common.model.HoodieFileFormat
import org.apache.hudi.common.testutils.{HoodieTestDataGenerator, HoodieTestUtils}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings
import org.apache.hudi.common.util.FileFormatUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.core.io.storage.HoodieIOFactory
import org.apache.hudi.keygen.{KeyGenerator, KeyGenUtils}
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.collection.JavaConverters._

/**
 * Tests for ComplexKeyGenerator auto-deduction of encoding format using Spark DataFrame writes.
 */
class TestComplexKeyGenAutoDeduction extends HoodieSparkClientTestBase {

  var commonOpts: Map[String, String] = Map(
    "hoodie.write.table.version" -> "8", // the encoding fix only applies to table version 8 and below
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
  )

  @BeforeEach
  override def setUp(): Unit = {
    initPath()
    initSparkContexts()
    initTestDataGenerator()
    initHoodieStorage()
  }

  @AfterEach
  override def tearDown(): Unit = {
    cleanupResources()
  }

  /**
   * Test auto-deduction with two commits where first commit uses specified encoding
   * and second commit auto-deduces and maintains the same encoding.
   */
  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testAutoDeductionWithTwoCommits(useNewEncoding: Boolean): Unit = {
    val recordKeyField = "_row_key"
    val partitionPathField = "partition"

    // First commit: Disable auto-deduction, explicitly set encoding format
    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val records1 = recordsToStrings(dataGen.generateInserts("001", 100)).asScala.toList
    val inputDF1 = sparkSession.read.json(sparkSession.sparkContext.parallelize(records1, 2))

    val options1 = commonOpts ++ Map(
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> recordKeyField,
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> partitionPathField,
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.ComplexKeyGenerator",
      HoodieWriteConfig.COMPLEX_KEYGEN_NEW_ENCODING.key -> useNewEncoding.toString,
      HoodieWriteConfig.COMPLEX_KEYGEN_AUTO_DEDUCE_ENCODING.key -> "false",
      HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key -> "false"
    )

    inputDF1.write.format("org.apache.hudi")
      .options(options1)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    // Verify first commit used the specified encoding
    verifyRecordKeyEncoding(basePath, recordKeyField, useNewEncoding)

    // Verify no aux file exists yet
    val auxFilePath = KeyGenUtils.getComplexKeyEncodingFilePath(new StoragePath(basePath))
    val storage = HoodieTestUtils.getStorage(new StoragePath(basePath))
    assertFalse(storage.exists(auxFilePath), "Aux file should not exist after first commit")

    // Second commit: Enable auto-deduction
    val records2 = recordsToStrings(dataGen.generateInserts("002", 100)).asScala.toList
    val inputDF2 = sparkSession.read.json(sparkSession.sparkContext.parallelize(records2, 2))

    val options2 = commonOpts ++ Map(
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> recordKeyField,
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> partitionPathField,
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.ComplexKeyGenerator",
      HoodieWriteConfig.COMPLEX_KEYGEN_AUTO_DEDUCE_ENCODING.key -> "true"
    )

    inputDF2.write.format("org.apache.hudi")
      .options(options2)
      .mode(SaveMode.Append)
      .save(basePath)

    // Verify aux file was created after second commit
    assertTrue(storage.exists(auxFilePath), "Aux file should exist after second commit with auto-deduction")

    // Verify cached encoding matches first commit's encoding
    val cachedEncoding = KeyGenUtils.readComplexKeyEncodingFromAuxFile(storage, new StoragePath(basePath))
    assertTrue(cachedEncoding.isPresent, "Cached encoding should be present")
    assertEquals(useNewEncoding, cachedEncoding.get(), "Cached encoding should match first commit's encoding")

    // Verify second commit used the same encoding as first commit
    verifyRecordKeyEncoding(basePath, recordKeyField, useNewEncoding)
  }

  /**
   * Test that specifically verifies older encoding (field:value format) is correctly
   * detected and maintained across commits.
   */
  @Test
  def testOlderEncodingAutoDeduction(): Unit = {
    val recordKeyField = "_row_key"
    val partitionPathField = "partition"

    // First commit: Use older encoding (useNewEncoding = false)
    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val records1 = recordsToStrings(dataGen.generateInserts("001", 100)).asScala.toList
    val inputDF1 = sparkSession.read.json(sparkSession.sparkContext.parallelize(records1, 2))

    val options1 = commonOpts ++ Map(
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> recordKeyField,
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> partitionPathField,
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.ComplexKeyGenerator",
      HoodieWriteConfig.COMPLEX_KEYGEN_NEW_ENCODING.key -> "false",
      HoodieWriteConfig.COMPLEX_KEYGEN_AUTO_DEDUCE_ENCODING.key -> "false",
      HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key -> "false"
    )

    inputDF1.write.format("org.apache.hudi")
      .options(options1)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    // Verify first commit used older encoding (field:value format)
    val storage = HoodieTestUtils.getStorage(new StoragePath(basePath))
    val fileFormatUtils = HoodieIOFactory.getIOFactory(storage)
      .getFileFormatUtils(HoodieFileFormat.PARQUET)

    val parquetFiles = storage.globEntries(new StoragePath(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH + "/*.parquet"))
    assertTrue(parquetFiles.size() > 0, "Should have at least one parquet file")

    val firstCommitFile = parquetFiles.get(0)
    val keyIterator = fileFormatUtils.getHoodieKeyIterator(storage, firstCommitFile.getPath)
    try {
      assertTrue(keyIterator.hasNext, "Should have at least one record")
      val hoodieKey = keyIterator.next()
      val hoodieRecordKey = hoodieKey.getRecordKey

      // Verify older encoding format: field:value
      val expectedPrefix = recordKeyField + KeyGenerator.DEFAULT_COLUMN_VALUE_SEPARATOR
      assertTrue(hoodieRecordKey.startsWith(expectedPrefix),
        s"First commit should use older encoding (field:value). hoodieRecordKey=$hoodieRecordKey")
    } finally {
      keyIterator.close()
    }

    // Second commit: Enable auto-deduction
    val records2 = recordsToStrings(dataGen.generateInserts("002", 100)).asScala.toList
    val inputDF2 = sparkSession.read.json(sparkSession.sparkContext.parallelize(records2, 2))

    val options2 = commonOpts ++ Map(
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> recordKeyField,
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> partitionPathField,
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.ComplexKeyGenerator",
      HoodieWriteConfig.COMPLEX_KEYGEN_AUTO_DEDUCE_ENCODING.key -> "true"
    )

    inputDF2.write.format("org.apache.hudi")
      .options(options2)
      .mode(SaveMode.Append)
      .save(basePath)

    // Verify aux file was created with older encoding (useNewEncoding=false)
    val auxFilePath = KeyGenUtils.getComplexKeyEncodingFilePath(new StoragePath(basePath))
    assertTrue(storage.exists(auxFilePath), "Aux file should exist after second commit")

    val cachedEncoding = KeyGenUtils.readComplexKeyEncodingFromAuxFile(storage, new StoragePath(basePath))
    assertTrue(cachedEncoding.isPresent, "Cached encoding should be present")
    assertEquals(false, cachedEncoding.get(), "Cached encoding should be false (older encoding)")

    // Verify second commit also uses older encoding
    val allParquetFiles = storage.globEntries(new StoragePath(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH + "/*.parquet"))

    // Check a file from the second commit
    assertTrue(allParquetFiles.size() > parquetFiles.size(), "Should have at least one parquet file from second commit")
    val secondCommitFile = allParquetFiles.stream().filter(parquetFile => !parquetFiles.contains(parquetFile)).findFirst().get()

    val keyIterator2 = fileFormatUtils.getHoodieKeyIterator(storage, secondCommitFile.getPath)
    try {
      assertTrue(keyIterator2.hasNext, "Should have at least one record")
      val hoodieKey = keyIterator2.next()
      val hoodieRecordKey = hoodieKey.getRecordKey

      // Verify second commit also uses older encoding format: field:value
      val expectedPrefix = recordKeyField + KeyGenerator.DEFAULT_COLUMN_VALUE_SEPARATOR
      assertTrue(hoodieRecordKey.startsWith(expectedPrefix),
        s"Second commit should use older encoding (field:value) after auto-deduction. hoodieRecordKey=$hoodieRecordKey")
    } finally {
      keyIterator2.close()
    }
  }

  private def verifyRecordKeyEncoding(basePath: String, recordKeyFieldName: String, useNewEncoding: Boolean): Unit = {
    val storage = HoodieTestUtils.getStorage(new StoragePath(basePath))
    val fileFormatUtils = HoodieIOFactory.getIOFactory(storage)
      .getFileFormatUtils(HoodieFileFormat.PARQUET)

    // Get all parquet files
    val parquetFiles = storage.globEntries(new StoragePath(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH + "/*.parquet"))

    assertTrue(parquetFiles.size() > 0, "Should have at least one parquet file")

    // Check the first parquet file
    val parquetFile = parquetFiles.get(0)
    val keyIterator = fileFormatUtils.getHoodieKeyIterator(storage, parquetFile.getPath)
    try {
      assertTrue(keyIterator.hasNext, "Should have at least one record")
      val hoodieKey = keyIterator.next()
      val hoodieRecordKey = hoodieKey.getRecordKey

      val expectedPrefix = recordKeyFieldName + KeyGenerator.DEFAULT_COLUMN_VALUE_SEPARATOR
      val actualUsesNewEncoding = !hoodieRecordKey.startsWith(expectedPrefix)

      assertEquals(useNewEncoding, actualUsesNewEncoding,
        s"Record key encoding mismatch. Expected useNewEncoding=$useNewEncoding, hoodieRecordKey=$hoodieRecordKey")
    } finally {
      keyIterator.close()
    }
  }
}
