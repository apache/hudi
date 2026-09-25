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
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.common.testutils.{HoodieTestDataGenerator, HoodieTestUtils}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings
import org.apache.hudi.common.util.Option
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen.KeyGenUtils
import org.apache.hudi.keygen.constant.ComplexKeyGenEncoding
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

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

  private val recordKeyField = "_row_key"
  private val partitionPathField = "partition"

  private def writeNewTable(extraOpts: Map[String, String]): Unit = {
    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val records = recordsToStrings(dataGen.generateInserts("001", 100)).asScala.toList
    val inputDF = sparkSession.read.json(sparkSession.sparkContext.parallelize(records, 2))
    val options = commonOpts ++ Map(
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> recordKeyField,
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> partitionPathField,
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.ComplexKeyGenerator"
    ) ++ extraOpts
    inputDF.write.format("org.apache.hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)
  }

  /** Writes into an existing table, so its recorded configuration is what drives the key generation. */
  private def appendToTable(): Unit = {
    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val records = recordsToStrings(dataGen.generateInserts("001", 100)).asScala.toList
    val inputDF = sparkSession.read.json(sparkSession.sparkContext.parallelize(records, 2))
    inputDF.write.format("org.apache.hudi")
      .options(commonOpts ++ Map(
        DataSourceWriteOptions.RECORDKEY_FIELD.key -> recordKeyField,
        DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> partitionPathField,
        DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.ComplexKeyGenerator"))
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
  }

  private def storedRecordKeys(): Array[String] = {
    val recordKeys = sparkSession.read.format("org.apache.hudi").load(basePath)
      .select("_hoodie_record_key").collect().map(_.getString(0))
    assertTrue(recordKeys.nonEmpty, "Expected records to be written to the new table")
    recordKeys
  }

  private def loadMetaClient(): HoodieTableMetaClient = {
    val storage = HoodieTestUtils.getStorage(new StoragePath(basePath))
    HoodieTableMetaClient.builder().setConf(storage.getConf.newInstance()).setBasePath(basePath).build()
  }

  /** A new table written with pure defaults records FIELD_PREFIXED and stores `<field>:<value>` keys. */
  @ParameterizedTest
  @ValueSource(ints = Array(8, 10))
  def testNewTableRecordsFieldPrefixedEncoding(tableVersion: Int): Unit = {
    writeNewTable(Map(HoodieWriteConfig.WRITE_TABLE_VERSION.key -> tableVersion.toString))
    val recordKeys = storedRecordKeys()
    val expectedPrefix = recordKeyField + ":"
    assertTrue(recordKeys.forall(_.startsWith(expectedPrefix)),
      s"New-table default must use field:value encoding ($expectedPrefix<value>); got sample: ${recordKeys.take(5).mkString(", ")}")
    val metaClient = loadMetaClient()
    assertEquals(HoodieTableVersion.fromVersionCode(tableVersion), metaClient.getTableConfig.getTableVersion)
    assertEquals(Option.of(ComplexKeyGenEncoding.FIELD_PREFIXED), metaClient.getTableConfig.getComplexKeyGenEncoding)
    assertEquals(Option.of(ComplexKeyGenEncoding.FIELD_PREFIXED), KeyGenUtils.resolveComplexKeyGenEncoding(metaClient))
  }

  /**
   * An explicitly requested encoding is honored on creation and by the keys written. VALUE_ONLY describes keys
   * that only releases up to 1.0.2 wrote, so it is accepted for a table created at version 8 and refused above.
   */
  @Test
  def testNewTableHonorsExplicitEncoding(): Unit = {
    writeNewTable(Map(
      HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key -> ComplexKeyGenEncoding.VALUE_ONLY.name,
      HoodieWriteConfig.WRITE_TABLE_VERSION.key -> "8"))
    val recordKeys = storedRecordKeys()
    assertTrue(recordKeys.forall(!_.startsWith(recordKeyField + ":")), s"Keys must be bare; got sample: ${recordKeys.take(5).mkString(", ")}")
    assertEquals(HoodieTableVersion.EIGHT, loadMetaClient().getTableConfig.getTableVersion)
    assertEquals(Option.of(ComplexKeyGenEncoding.VALUE_ONLY), loadMetaClient().getTableConfig.getComplexKeyGenEncoding)

    writeNewTable(Map(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key -> ComplexKeyGenEncoding.FIELD_PREFIXED.name))
    assertTrue(storedRecordKeys().forall(_.startsWith(recordKeyField + ":")))
    assertEquals(Option.of(ComplexKeyGenEncoding.FIELD_PREFIXED), loadMetaClient().getTableConfig.getComplexKeyGenEncoding)
  }

  /**
   * The encoding is a table property, never a write option: a table already carrying VALUE_ONLY at the current
   * version, the state an upgraded version 8 table is left in, keys its records bare without the writer asking.
   */
  @Test
  def testWriterFollowsEncodingRecordedOnTheTable(): Unit = {
    HoodieTableMetaClient.newTableBuilder()
      .setTableType(HoodieTableType.COPY_ON_WRITE.name)
      .setTableName(commonOpts(HoodieWriteConfig.TBL_NAME.key))
      .setTableVersion(HoodieTableVersion.current())
      .setRecordKeyFields(recordKeyField)
      .setPartitionFields(partitionPathField)
      .setKeyGeneratorClassProp("org.apache.hudi.keygen.ComplexKeyGenerator")
      .setComplexKeyGenEncoding(ComplexKeyGenEncoding.VALUE_ONLY)
      .initTable(HoodieTestUtils.getStorage(new StoragePath(basePath)).getConf.newInstance(), basePath)
    assertEquals(Option.of(ComplexKeyGenEncoding.VALUE_ONLY), loadMetaClient().getTableConfig.getComplexKeyGenEncoding)

    appendToTable()

    assertTrue(storedRecordKeys().forall(!_.startsWith(recordKeyField + ":")),
      "The writer must follow the encoding the table records, without being given it as a write option")
    assertEquals(HoodieTableVersion.current(), loadMetaClient().getTableConfig.getTableVersion)
    assertEquals(Option.of(ComplexKeyGenEncoding.VALUE_ONLY), loadMetaClient().getTableConfig.getComplexKeyGenEncoding)
  }

  /** Without a stored record key there is no encoding to track, so nothing is recorded. */
  @Test
  def testNewTableWithoutRecordKeyMetaFieldRecordsNothing(): Unit = {
    writeNewTable(Map(HoodieTableConfig.POPULATE_META_FIELDS.key -> "false", "hoodie.index.type" -> "SIMPLE"))
    val metaClient = loadMetaClient()
    assertFalse(metaClient.getTableConfig.isRecordKeyPopulated)
    assertFalse(metaClient.getTableConfig.getComplexKeyGenEncoding.isPresent)
  }
}
