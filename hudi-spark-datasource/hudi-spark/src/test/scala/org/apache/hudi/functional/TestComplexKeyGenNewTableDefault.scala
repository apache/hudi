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
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.testutils.{HoodieTestDataGenerator, HoodieTestUtils}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen.KeyGenUtils
import org.apache.hudi.keygen.constant.ComplexKeyGenEncoding
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}

import scala.collection.JavaConverters._

/**
 * Writes to a BRAND NEW table using ComplexKeyGenerator with a single record key field and a single
 * partition path field, using PURE DEFAULTS for the keygen-related write configs
 * (new.encoding and validation.enable are NOT set).
 *
 * A table created directly at the current table version is born at version 9 or above, where
 * field:value is the one canonical encoding. Nothing has to be deduced and nothing is recorded: the
 * encoding property is stamped only by the 8 to 9 upgrade, so its absence here is what marks the
 * table as "born at version 9+" rather than migrated. This test pins that contract.
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
    // default (false) and the fixed encoding of table version 9 and above.
    val recordKeys = sparkSession.read.format("org.apache.hudi").load(basePath)
      .select("_hoodie_record_key").collect().map(_.getString(0))
    assertTrue(recordKeys.nonEmpty, "Expected records to be written to the new table")
    val expectedPrefix = recordKeyField + ":"
    assertTrue(recordKeys.forall(_.startsWith(expectedPrefix)),
      s"New-table default must use field:value encoding ($expectedPrefix<value>); " +
        s"got sample: ${recordKeys.take(5).mkString(", ")}")

    val storage = HoodieTestUtils.getStorage(new StoragePath(basePath))
    // The encoding table property is only ever stamped by the 8 -> 9 upgrade; a table created directly at the
    // current version carries no property and is read as FIELD_PREFIXED by convention.
    val metaClient = HoodieTableMetaClient.builder().setConf(storage.getConf.newInstance()).setBasePath(basePath).build()
    assertFalse(metaClient.getTableConfig.contains(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING),
      "A table created at the current version must not carry hoodie.table.complex.keygen.encoding")
    assertEquals(ComplexKeyGenEncoding.FIELD_PREFIXED, KeyGenUtils.resolveComplexKeyGenEncoding(metaClient.getTableConfig).get)
  }
}
