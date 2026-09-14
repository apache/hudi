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
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen.KeyGenerator
import org.apache.hudi.keygen.constant.ComplexKeyGenEncoding
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.table.upgrade.{SparkUpgradeDowngradeHelper, UpgradeDowngrade}
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}

import scala.collection.JavaConverters._
import scala.io.Source

/**
 * Reproduces the existing-table upgrade scenario:
 *   - existing tables created/written with Hudi 0.14.1 (single-field ComplexKeyGenerator,
 *     so _hoodie_record_key is stored as the BARE value, e.g. "prod-001"),
 *   - single record key field, single partition path field, ComplexKeyGenerator passed explicitly,
 *   - then the upgraded app keeps UPSERTING the SAME records.
 *
 * Goal: show that crossing table version 8 -> 9 records the encoding the data carries in
 * hoodie.properties, so continued writes keep the key format stable (no duplicates), and that a
 * mismatched encoding below version 9 reproduces duplicates.
 */
class TestComplexKeyGenExistingTableUpgrade extends HoodieSparkClientTestBase {

  private val recordKeyField = "_row_key"
  private val partitionPathField = "partition"

  var commonOpts: Map[String, String] = Map(
    "hoodie.write.table.version" -> "8", // the encoding fix only applies to table version 8 and below
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "timestamp",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> recordKeyField,
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> partitionPathField,
    DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.ComplexKeyGenerator",
    HoodieWriteConfig.TBL_NAME.key -> "existing_table_upgrade_test"
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
   * Writes the initial legacy commit at the given table version: bare-value record keys when `bareKeys` is true
   * (what 0.14.1 / 0.15.0 / 1.0.0-1.0.2 stored), field-prefixed otherwise (0.14.0 and older).
   */
  private def writeInitialLegacyTable(dataGen: HoodieTestDataGenerator, bareKeys: Boolean, tableVersion: String = "8") = {
    val records = dataGen.generateInserts("001", 100)
    val inputDF = sparkSession.read.json(
      sparkSession.sparkContext.parallelize(recordsToStrings(records).asScala.toList, 2))
    inputDF.write.format("org.apache.hudi")
      .options(commonOpts + (HoodieWriteConfig.WRITE_TABLE_VERSION.key -> tableVersion))
      .option(HoodieWriteConfig.COMPLEX_KEYGEN_NEW_ENCODING.key, bareKeys.toString)
      .option(HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key, "false")
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    records
  }

  /** Writes the initial 0.14.1-style commit: bare-value record keys (new.encoding=true). */
  private def writeInitial0141Table(dataGen: HoodieTestDataGenerator) = writeInitialLegacyTable(dataGen, bareKeys = true)

  /** Options of the upgraded application: no table version pin, so the writer's default (current) version auto-upgrades. */
  private def upgradedOpts: Map[String, String] = commonOpts - HoodieWriteConfig.WRITE_TABLE_VERSION.key

  /** Upserts the same records again (updated values) with the given options. */
  private def upsertSameRecords(dataGen: HoodieTestDataGenerator, inserted: java.util.List[org.apache.hudi.common.model.HoodieRecord[_]],
                                opts: Map[String, String], commitId: String = "002"): Unit = {
    val updates = dataGen.generateUpdates(commitId, inserted)
    val updDF = sparkSession.read.json(
      sparkSession.sparkContext.parallelize(recordsToStrings(updates).asScala.toList, 2))
    updDF.write.format("org.apache.hudi")
      .options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
  }

  private def loadMetaClient(): HoodieTableMetaClient =
    HoodieTableMetaClient.builder().setConf(storage.getConf.newInstance()).setBasePath(basePath).build()

  /** The encoding persisted in hoodie.properties, if any. */
  private def persistedEncoding(): Option[String] = {
    val tableConfig = loadMetaClient().getTableConfig
    if (tableConfig.contains(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING)) Some(tableConfig.getString(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING)) else None
  }

  private def hoodiePropertiesLines(): Seq[String] = {
    val in = storage.open(new StoragePath(loadMetaClient().getMetaPath, HoodieTableConfig.HOODIE_PROPERTIES_FILE))
    try Source.fromInputStream(in).getLines().toList finally in.close()
  }

  private def rootCauseMessages(t: Throwable): Seq[String] =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null).map(e => Option(e.getMessage).getOrElse("")).toList

  private def readTable(): DataFrame =
    sparkSession.read.format("org.apache.hudi").load(basePath)

  /** DANGER PATH: safety nets OFF -> default encoding (field:value) differs from existing bare data -> duplicates. */
  @Test
  def testUpgradeWithSafetyNetsOffReproducesDuplicates(): Unit = {
    val dataGen = new HoodieTestDataGenerator(0xDEED)

    // Initial table written with the BARE-value encoding (i.e. created on 0.14.1/0.15.0), by setting
    // new.encoding=true explicitly. This is the encoding that differs from the current default
    // (COMPLEX_KEYGEN_NEW_ENCODING defaults to false => field:value).
    val records1 = dataGen.generateInserts("001", 100)
    val inputDF1 = sparkSession.read.json(
      sparkSession.sparkContext.parallelize(recordsToStrings(records1).asScala.toList, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(HoodieWriteConfig.COMPLEX_KEYGEN_NEW_ENCODING.key, "true")
      .option(HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key, "false")
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val (t0, _, bare0, pref0) = keyStatsRaw()
    println(s"[DANGER] after bare-value insert: total=$t0 bareKeys=$bare0 prefixedKeys=$pref0")
    assertEquals(100L, bare0, "Initial table must store bare-value keys")

    // A writer that stays at version 8 with validation OFF falls back to the default encoding
    // (new.encoding=false => field:value). That differs from the existing bare-value data, so the upsert
    // can't match -> duplicates. This is the misconfigured upgrade the table property prevents.
    val updates = dataGen.generateUpdates("002", records1)
    val updDF = sparkSession.read.json(
      sparkSession.sparkContext.parallelize(recordsToStrings(updates).asScala.toList, 2))
    updDF.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key, "false")
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val (t1, d1, bare1, pref1) = keyStatsRaw()
    println(s"[DANGER] after upgrade upsert: total=$t1 distinctKeys=$d1 bareKeys=$bare1 prefixedKeys=$pref1")

    assertEquals(200L, t1, "Validation off at version 8: default encoding differs from existing data -> upsert can't match -> DUPLICATES")
    assertEquals(100L, bare1, "Original bare-value rows remain")
    assertEquals(100L, pref1, "New writes use the default field:value format")
    println("[DANGER] RESULT: 200 rows for 100 logical keys -> DUPLICATES reproduced (the upgrade concern).")

    // A table that already carries both encodings is NOT repaired by moving to the current version. The
    // upgrade samples one stored record key from the newest base file, but that file is itself a merge of
    // both encodings, so which one it reports is not meaningful here - whichever it records, the rows in the
    // other encoding stay orphaned. Assert only that an encoding was pinned; the repair is a rewrite of the
    // record keys, not a config.
    upsertSameRecords(dataGen, records1, upgradedOpts, "003")
    val (t2, _, bare2, pref2) = keyStatsRaw()
    assertEquals(HoodieTableVersion.current(), loadMetaClient().getTableConfig.getTableVersion)
    assertTrue(persistedEncoding().exists(ComplexKeyGenEncoding.values.map(_.name).contains),
      "The upgrade must pin one of the two encodings even for an already-mixed table")
    assertEquals(200L, t2, "An already-mixed table is not repaired by the upgrade")
    assertEquals(100L, bare2)
    assertEquals(100L, pref2)
  }


  /** The real crossing: a bare-key version 8 table, upserted by a writer at the current version with pure defaults. */
  @Test
  def testUpgradeToCurrentVersionWithDefaultsNoDuplicates(): Unit = {
    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val inserted = writeInitial0141Table(dataGen)
    assertEquals((100L, 100L, 100L, 0L), keyStatsRaw())
    assertEquals(HoodieTableVersion.EIGHT, loadMetaClient().getTableConfig.getTableVersion)

    // This very write performs the 8 -> current upgrade AND keys the updates: both must agree on the bare encoding.
    upsertSameRecords(dataGen, inserted, upgradedOpts)

    assertEquals((100L, 100L, 100L, 0L), keyStatsRaw(), "Upgrade write must update in place with bare keys, no duplicates")
    val tableConfig = loadMetaClient().getTableConfig
    assertEquals(HoodieTableVersion.current(), tableConfig.getTableVersion)
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())
    assertEquals(ComplexKeyGenEncoding.VALUE_ONLY, tableConfig.getComplexKeyGenEncoding.get)
    assertTrue(hoodiePropertiesLines().contains(s"${HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key}=VALUE_ONLY"),
      "hoodie.properties must carry the encoding line verbatim")

    // Steady state: later writes read the property from hoodie.properties and keep matching.
    upsertSameRecords(dataGen, inserted, upgradedOpts, "003")
    assertEquals((100L, 100L, 100L, 0L), keyStatsRaw())
  }

  /** A legacy table with field-prefixed keys (0.14.0 and older) records FIELD_PREFIXED. */
  @Test
  def testUpgradeFieldPrefixedLegacyTableStampsFieldPrefixed(): Unit = {
    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val inserted = writeInitialLegacyTable(dataGen, bareKeys = false)
    assertEquals((100L, 100L, 0L, 100L), keyStatsRaw())

    upsertSameRecords(dataGen, inserted, upgradedOpts)

    assertEquals((100L, 100L, 0L, 100L), keyStatsRaw())
    assertEquals(HoodieTableVersion.current(), loadMetaClient().getTableConfig.getTableVersion)
    assertEquals(Some(ComplexKeyGenEncoding.FIELD_PREFIXED.name), persistedEncoding())
  }

  /** A version 6 table (0.14.x) upgraded straight to the current version in one write: the 7 -> 8 guard must not block. */
  @Test
  def testSixToCurrentVersionUpgradeDoesNotBlock(): Unit = {
    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val inserted = writeInitialLegacyTable(dataGen, bareKeys = true, tableVersion = "6")
    assertEquals(HoodieTableVersion.SIX, loadMetaClient().getTableConfig.getTableVersion)
    assertEquals((100L, 100L, 100L, 0L), keyStatsRaw())

    upsertSameRecords(dataGen, inserted, upgradedOpts)

    assertEquals((100L, 100L, 100L, 0L), keyStatsRaw(), "6 -> current upgrade must keep bare keys, no duplicates")
    assertEquals(HoodieTableVersion.current(), loadMetaClient().getTableConfig.getTableVersion)
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())
  }

  /**
   * An upgrade that stops at version 8 leaves the encoding unrecorded -- a writer there still cannot tell what
   * the data carries -- so the validation must refuse it and leave the table untouched.
   */
  @Test
  def testUpgradeStoppingAtVersionEightWithValidationOnFails(): Unit = {
    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val inserted = writeInitialLegacyTable(dataGen, bareKeys = true, tableVersion = "6")
    assertEquals(HoodieTableVersion.SIX, loadMetaClient().getTableConfig.getTableVersion)

    val thrown = assertThrows(classOf[Throwable], () =>
      upsertSameRecords(dataGen, inserted, commonOpts + (HoodieWriteConfig.WRITE_TABLE_VERSION.key -> "8")))
    assertTrue(rootCauseMessages(thrown).exists(_.contains("complex key generator with a single record key field")),
      s"Expected the complex keygen guidance, got: ${rootCauseMessages(thrown).mkString(" | ")}")

    assertEquals(HoodieTableVersion.SIX, loadMetaClient().getTableConfig.getTableVersion, "A refused upgrade leaves the table alone")
    assertEquals(None, persistedEncoding())
    assertEquals((100L, 100L, 100L, 0L), keyStatsRaw())
  }

  /**
   * Downgrading back to version 8 drops the property. Below version 9 there is nowhere to record the encoding,
   * so the guard refuses the move unless the user turns the validation off and configures the encoding
   * themselves -- which is what the downgraded writer then has to do to keep matching the bare keys.
   */
  @Test
  def testDowngradeToEightRemovesProperty(): Unit = {
    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val inserted = writeInitial0141Table(dataGen)
    upsertSameRecords(dataGen, inserted, upgradedOpts)
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())

    val downgradeOpts = commonOpts ++ Map(
      HoodieWriteConfig.WRITE_TABLE_VERSION.key -> "8",
      HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key -> "false")
    val writeConfig = HoodieWriteConfig.newBuilder().withPath(basePath).withProps(downgradeOpts.asJava).build()
    new UpgradeDowngrade(loadMetaClient(), writeConfig, context, SparkUpgradeDowngradeHelper.getInstance)
      .run(HoodieTableVersion.EIGHT, null)

    assertEquals(HoodieTableVersion.EIGHT, loadMetaClient().getTableConfig.getTableVersion)
    assertEquals(None, persistedEncoding(), "The property is only defined at version 9 and above")
    // The downgrading writer carries the encoding it knew about, so it can keep writing bare keys.
    assertEquals("true", writeConfig.getString(HoodieWriteConfig.COMPLEX_KEYGEN_NEW_ENCODING))

    // A version 8 writer now has to be told the encoding explicitly.
    upsertSameRecords(dataGen, inserted, downgradeOpts + (HoodieWriteConfig.COMPLEX_KEYGEN_NEW_ENCODING.key -> "true"), "003")
    assertEquals((100L, 100L, 100L, 0L), keyStatsRaw())
  }

  /** With auto-upgrade disabled the table stays at version 8, where the encoding stays a per-write decision. */
  @Test
  def testAutoUpgradeDisabledStaysAtVersionEight(): Unit = {
    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val inserted = writeInitial0141Table(dataGen)

    upsertSameRecords(dataGen, inserted, upgradedOpts ++ Map(
      HoodieWriteConfig.AUTO_UPGRADE_VERSION.key -> "false",
      HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key -> "false",
      HoodieWriteConfig.COMPLEX_KEYGEN_NEW_ENCODING.key -> "true"))

    assertEquals(HoodieTableVersion.EIGHT, loadMetaClient().getTableConfig.getTableVersion)
    assertEquals((100L, 100L, 100L, 0L), keyStatsRaw())
    assertEquals(None, persistedEncoding())
  }

  /** insert_overwrite_table keeps the table (and its property) while skipping the table-config merge: keys must stay bare. */
  @Test
  def testInsertOverwriteTableKeepsPersistedEncoding(): Unit = {
    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val inserted = writeInitial0141Table(dataGen)
    upsertSameRecords(dataGen, inserted, upgradedOpts)
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())

    val fresh = dataGen.generateInserts("003", 50)
    val freshDF = sparkSession.read.json(
      sparkSession.sparkContext.parallelize(recordsToStrings(fresh).asScala.toList, 2))
    freshDF.write.format("org.apache.hudi")
      .options(upgradedOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val (total, distinct, bare, prefixed) = keyStatsRaw()
    assertEquals(50L, total)
    assertEquals(50L, distinct)
    assertEquals(50L, bare, "Rewritten data must follow the table's persisted VALUE_ONLY encoding")
    assertEquals(0L, prefixed)
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())
  }

  // Reads the table and returns (total, distinctKeys, bareKeys, prefixedKeys).
  private def keyStatsRaw(): (Long, Long, Long, Long) = {
    val rk = readTable().selectExpr("_hoodie_record_key as k").collect().map(_.getString(0))
    val total = rk.length.toLong
    val distinct = rk.distinct.length.toLong
    val prefix = recordKeyField + KeyGenerator.DEFAULT_COLUMN_VALUE_SEPARATOR
    (total, distinct, rk.count(k => !k.startsWith(prefix)).toLong, rk.count(k => k.startsWith(prefix)).toLong)
  }
}
