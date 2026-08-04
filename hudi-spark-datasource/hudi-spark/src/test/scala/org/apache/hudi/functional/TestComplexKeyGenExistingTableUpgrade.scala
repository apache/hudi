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
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen.{KeyGenerator, KeyGenUtils}
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}

import scala.collection.JavaConverters._

/**
 * Reproduces the existing-table upgrade scenario:
 *   - existing tables created/written with Hudi 0.14.1 (single-field ComplexKeyGenerator,
 *     so _hoodie_record_key is stored as the BARE value, e.g. "prod-001"),
 *   - single record key field, single partition path field, ComplexKeyGenerator passed explicitly,
 *   - then the upgraded app keeps UPSERTING the SAME records.
 *
 * Goal: show whether continued writes keep the key format stable (no duplicates) under the
 * default (auto-deduce ON) path, and that turning the safety nets OFF reproduces duplicates.
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

  /** Writes the initial 0.14.1-style commit: bare-value record keys (new.encoding=true), no auto-deduce. */
  private def writeInitial0141Table(dataGen: HoodieTestDataGenerator) = {
    val records = dataGen.generateInserts("001", 100)
    val inputDF = sparkSession.read.json(
      sparkSession.sparkContext.parallelize(recordsToStrings(records).asScala.toList, 2))
    inputDF.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(HoodieWriteConfig.COMPLEX_KEYGEN_NEW_ENCODING.key, "true")            // 0.14.1 stored bare value
      .option(HoodieWriteConfig.COMPLEX_KEYGEN_AUTO_DEDUCE_ENCODING.key, "false")
      .option(HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key, "false")
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    records
  }

  private def readTable(): DataFrame =
    sparkSession.read.format("org.apache.hudi").load(basePath)

  /** SAFE PATH: upgrade app keeps defaults (auto-deduce ON) -> bare value preserved, no duplicates. */
  @Test
  def testUpgradeWithDefaultsNoDuplicates(): Unit = {
    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val inserted = writeInitial0141Table(dataGen)

    val (t0, d0, bare0, pref0) = keyStatsRaw()
    println(s"[DEFAULTS] after 0.14.1 insert: total=$t0 distinctKeys=$d0 bareKeys=$bare0 prefixedKeys=$pref0")
    assertEquals(100L, t0)
    assertEquals(100L, bare0, "0.14.1 commit must store bare-value keys")

    // Upgraded app (0.15.0.3) upserts the SAME 100 records, with DEFAULTS (auto-deduce defaults ON).
    val updates = dataGen.generateUpdates("002", inserted)
    val updDF = sparkSession.read.json(
      sparkSession.sparkContext.parallelize(recordsToStrings(updates).asScala.toList, 2))
    updDF.write.format("org.apache.hudi")
      .options(commonOpts) // NOTE: no new.encoding / auto.deduce / validation overrides
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val (t1, d1, bare1, pref1) = keyStatsRaw()
    val auxFile = KeyGenUtils.getComplexKeyEncodingFilePath(new StoragePath(basePath))
    val cached = KeyGenUtils.readComplexKeyEncodingFromAuxFile(storage, new StoragePath(basePath))
    println(s"[DEFAULTS] after 0.15.0.3 upsert: total=$t1 distinctKeys=$d1 bareKeys=$bare1 prefixedKeys=$pref1")
    println(s"[DEFAULTS] aux exists=${storage.exists(auxFile)} cached new.encoding=${if (cached.isPresent) cached.get else "<none>"}")

    assertEquals(100L, t1, "Upsert with defaults (auto-deduce) must NOT create duplicates")
    assertEquals(100L, d1)
    assertEquals(100L, bare1, "Keys must stay in bare-value format after upgrade")
    assertEquals(0L, pref1, "No field:value keys should appear")
    assertTrue(cached.isPresent && cached.get, "auto-deduce should cache new.encoding=true")
    println("[DEFAULTS] RESULT: no duplicates, key format stable -> existing tables safe with defaults.")
  }

  /** DANGER PATH: safety nets OFF -> default encoding flips to field:value -> duplicates. */
  @Test
  def testUpgradeWithSafetyNetsOffReproducesDuplicates(): Unit = {
    val dataGen = new HoodieTestDataGenerator(0xDEED)

    // Initial table written with the OLD field:value encoding (i.e. created on 0.14.0), by setting
    // new.encoding=false explicitly. This is the encoding that differs from the current default.
    val records1 = dataGen.generateInserts("001", 100)
    val inputDF1 = sparkSession.read.json(
      sparkSession.sparkContext.parallelize(recordsToStrings(records1).asScala.toList, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(HoodieWriteConfig.COMPLEX_KEYGEN_NEW_ENCODING.key, "false")
      .option(HoodieWriteConfig.COMPLEX_KEYGEN_AUTO_DEDUCE_ENCODING.key, "false")
      .option(HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key, "false")
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val (t0, _, bare0, pref0) = keyStatsRaw()
    println(s"[DANGER] after 0.14.0 (field:value) insert: total=$t0 bareKeys=$bare0 prefixedKeys=$pref0")
    assertEquals(100L, pref0, "Initial table must store field:value keys")

    // Upgraded app upserts the SAME 100 records but with auto-deduce OFF and validation OFF, so it
    // falls back to the default encoding (new.encoding=true => bare value). That differs from the
    // existing field:value data, so the upsert can't match -> duplicates. This is the misconfigured upgrade.
    val updates = dataGen.generateUpdates("002", records1)
    val updDF = sparkSession.read.json(
      sparkSession.sparkContext.parallelize(recordsToStrings(updates).asScala.toList, 2))
    updDF.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(HoodieWriteConfig.COMPLEX_KEYGEN_AUTO_DEDUCE_ENCODING.key, "false")
      .option(HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key, "false")
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val (t1, d1, bare1, pref1) = keyStatsRaw()
    println(s"[DANGER] after upgrade upsert: total=$t1 distinctKeys=$d1 bareKeys=$bare1 prefixedKeys=$pref1")

    assertEquals(200L, t1, "Safety nets off: default encoding differs from existing data -> upsert can't match -> DUPLICATES")
    assertEquals(100L, pref1, "Original field:value rows remain")
    assertEquals(100L, bare1, "New writes use the default bare-value format")
    println("[DANGER] RESULT: 200 rows for 100 logical keys -> DUPLICATES reproduced (the upgrade concern).")
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
