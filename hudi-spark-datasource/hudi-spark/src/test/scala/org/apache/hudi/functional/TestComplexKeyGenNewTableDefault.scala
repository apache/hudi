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
import org.apache.hudi.common.testutils.{HoodieTestDataGenerator, HoodieTestUtils}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen.KeyGenUtils
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertFalse, assertTrue}

import scala.collection.JavaConverters._

/**
 * Writes to a BRAND NEW table using ComplexKeyGenerator with a single record key field and a single
 * partition path field, using PURE DEFAULTS for the three keygen-related write configs
 * (new.encoding, auto.deduce.encoding, validation.enable are NOT set).
 *
 * On a brand-new table there is no data to deduce the encoding from, so auto-deduction leaves the
 * configured encoding untouched and the write falls back to the COMPLEX_KEYGEN_NEW_ENCODING default
 * (false => legacy field:value encoding). This test asserts that default behavior and that no
 * encoding is cached to the aux file when it could not be deduced.
 */
class TestComplexKeyGenNewTableDefault extends HoodieSparkClientTestBase {

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

    inputDF.write.format("org.apache.hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    // A brand-new table written with pure defaults must use the legacy field:value encoding
    // ("<recordKeyField>:<value>") for every record key, matching the COMPLEX_KEYGEN_NEW_ENCODING
    // default (false). Auto-deduction has no data to learn from here, so it must not change this.
    val recordKeys = sparkSession.read.format("org.apache.hudi").load(basePath)
      .select("_hoodie_record_key").collect().map(_.getString(0))
    assertTrue(recordKeys.nonEmpty, "Expected records to be written to the new table")
    val expectedPrefix = recordKeyField + ":"
    assertTrue(recordKeys.forall(_.startsWith(expectedPrefix)),
      s"New-table default must use field:value encoding ($expectedPrefix<value>); " +
        s"got sample: ${recordKeys.take(5).mkString(", ")}")

    // Auto-deduction cannot determine an encoding on a brand-new (empty) table, so it must NOT cache
    // a guess to the aux file (a later write re-deduces once base files exist).
    val storage = HoodieTestUtils.getStorage(new StoragePath(basePath))
    val auxFilePath = KeyGenUtils.getComplexKeyEncodingFilePath(new StoragePath(basePath))
    assertFalse(storage.exists(auxFilePath),
      s"Encoding aux file must not be cached for a new table where the encoding could not be deduced: $auxFilePath")
  }
}
