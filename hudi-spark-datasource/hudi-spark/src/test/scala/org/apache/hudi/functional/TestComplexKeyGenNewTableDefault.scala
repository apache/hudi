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
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.io.storage.HoodieIOFactory
import org.apache.hudi.keygen.KeyGenUtils
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import scala.collection.JavaConverters._

/**
 * Writes to a BRAND NEW table using ComplexKeyGenerator with a single record key field
 * and a single partition path field, using PURE DEFAULTS for the three keygen-related
 * write configs (new.encoding, auto.deduce.encoding, validation.enable are NOT set).
 *
 * Goal: empirically determine the default _hoodie_record_key format produced by this branch
 * when a fresh table is written directly.
 */
class TestComplexKeyGenNewTableDefault extends HoodieSparkClientTestBase {

  var commonOpts: Map[String, String] = Map(
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

  @Test
  def testNewTableDefaultKeyFormat(): Unit = {
    val recordKeyField = "_row_key"
    val partitionPathField = "partition"

    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val records = recordsToStrings(dataGen.generateInserts("001", 100)).asScala.toList
    val inputDF = sparkSession.read.json(sparkSession.sparkContext.parallelize(records, 2))

    // PURE DEFAULTS: only set keygen class + record key + partition path.
    // Do NOT set hoodie.write.complex.keygen.new.encoding
    // Do NOT set hoodie.write.complex.keygen.auto.deduce.encoding
    // Do NOT set hoodie.write.complex.keygen.validation.enable
    val options = commonOpts ++ Map(
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> recordKeyField,
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> partitionPathField,
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.ComplexKeyGenerator"
    )

    println("========== NEW TABLE DEFAULT WRITE: starting ==========")
    println(s"recordKeyField=$recordKeyField partitionPathField=$partitionPathField " +
      s"keygen=org.apache.hudi.keygen.ComplexKeyGenerator (no encoding/deduce/validation overrides)")

    try {
      inputDF.write.format("org.apache.hudi")
        .options(options)
        .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
        .mode(SaveMode.Overwrite)
        .save(basePath)

      println("========== NEW TABLE DEFAULT WRITE: SUCCEEDED ==========")

      val storage = HoodieTestUtils.getStorage(new StoragePath(basePath))
      val fileFormatUtils = HoodieIOFactory.getIOFactory(storage)
        .getFileFormatUtils(HoodieFileFormat.PARQUET)
      val parquetFiles = storage.globEntries(
        new StoragePath(basePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH + "/*.parquet"))
      println(s"parquet files found: ${parquetFiles.size()}")

      val keyIterator = fileFormatUtils.getHoodieKeyIterator(storage, parquetFiles.get(0).getPath)
      try {
        var i = 0
        while (keyIterator.hasNext && i < 5) {
          val key = keyIterator.next()
          println(s"SAMPLE _hoodie_record_key[$i] = '${key.getRecordKey}'  | partition = '${key.getPartitionPath}'")
          i += 1
        }
      } finally {
        keyIterator.close()
      }

      // Report whether the aux file was written
      val auxFilePath = KeyGenUtils.getComplexKeyEncodingFilePath(new StoragePath(basePath))
      println(s"aux encoding file exists: ${storage.exists(auxFilePath)} (path=$auxFilePath)")
      val cached = KeyGenUtils.readComplexKeyEncodingFromAuxFile(storage, new StoragePath(basePath))
      println(s"cached new.encoding value: ${if (cached.isPresent) cached.get().toString else "<none>"}")
      println("========== END ==========")
    } catch {
      case e: Throwable =>
        println("========== NEW TABLE DEFAULT WRITE: FAILED ==========")
        println(s"Exception type: ${e.getClass.getName}")
        println(s"Exception message: ${e.getMessage}")
        var cause = e.getCause
        var depth = 0
        while (cause != null && depth < 8) {
          println(s"  caused by[$depth]: ${cause.getClass.getName}: ${cause.getMessage}")
          cause = cause.getCause
          depth += 1
        }
        println("========== END ==========")
        throw e
    }
  }
}
