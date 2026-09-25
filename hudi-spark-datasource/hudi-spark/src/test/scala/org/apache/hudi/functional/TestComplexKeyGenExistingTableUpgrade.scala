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

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.HoodieDuplicateKeyException
import org.apache.hudi.functional.ComplexKeyGenFixtures._
import org.apache.hudi.keygen.{KeyGenerator, KeyGenUtils}
import org.apache.hudi.keygen.constant.ComplexKeyGenEncoding
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.table.upgrade.{SparkUpgradeDowngradeHelper, UpgradeDowngrade}
import org.apache.hudi.table.upgrade.TestUpgradeDowngrade.getFixtureName
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}

import scala.collection.JavaConverters._
import scala.io.Source

/**
 * Existing single-field ComplexKeyGenerator tables that predate hoodie.table.complex.keygenerator.encoding: the
 * next write or upgrade must deduce the encoding the data carries, record it, and keep matching the existing
 * keys. The legacy tables are the checked-in fixtures written by the actual releases, so the stored keys stay
 * what those releases produced rather than what a current writer would choose by default.
 */
class TestComplexKeyGenExistingTableUpgrade extends HoodieSparkClientTestBase {

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

  /** Extracts a fixture table into the temp dir, points the test's base path at it and returns its write options. */
  private def loadFixture(version: HoodieTableVersion, suffix: String = COMPLEX_KEYGEN_FIXTURE_SUFFIX): Map[String, String] = {
    val fixtureName = getFixtureName(version, suffix)
    HoodieTestUtils.extractZipToDirectory(COMPLEX_KEYGEN_FIXTURES_PATH + fixtureName, tempDir, getClass)
    val tableName = fixtureName.replace(".zip", "")
    basePath = tempDir.resolve(tableName).toString
    assertEquals(version, loadMetaClient().getTableConfig.getTableVersion)
    assertEquals(None, persistedEncoding(), "Fixture tables predate the encoding property")
    fixtureWriteOpts(tableName)
  }

  private def rows(ids: Seq[String], ts: Long): DataFrame = fixtureRows(sparkSession, ids, ts)

  /** Upserts the given fixture records again with a newer ordering value. */
  private def upsertFixtureRecords(opts: Map[String, String], ts: Long, ids: Seq[String] = FIXTURE_IDS): Unit = {
    rows(ids, ts).write.format("org.apache.hudi")
      .options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
  }

  private def loadMetaClient(): HoodieTableMetaClient =
    HoodieTableMetaClient.builder().setConf(storage.getConf.newInstance()).setBasePath(basePath).build()

  private def persistedEncoding(): Option[String] = {
    val tableConfig = loadMetaClient().getTableConfig
    if (tableConfig.contains(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING)) Some(tableConfig.getString(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING)) else None
  }

  private def removeEncodingProperty(): Unit = {
    val metaClient = loadMetaClient()
    HoodieTableConfig.delete(storage, metaClient.getMetaPath, Set(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key).asJava)
    assertEquals(None, persistedEncoding())
  }

  private def hoodiePropertiesLines(): Seq[String] = {
    val in = storage.open(new StoragePath(loadMetaClient().getMetaPath, HoodieTableConfig.HOODIE_PROPERTIES_FILE))
    try Source.fromInputStream(in).getLines().toList finally in.close()
  }

  private def rootCauses(t: Throwable): Seq[Throwable] =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null).toList

  private def readTable(): DataFrame =
    sparkSession.read.format("org.apache.hudi").load(basePath)

  /** Overwrites the data files whose name matches the predicate with garbage, so no record key can be read from them. */
  private def corruptDataFiles(predicate: String => Boolean): Int = {
    val files = storage.listFiles(new StoragePath(basePath)).asScala
      .filter(f => !f.getPath.toString.contains("/.hoodie/") && predicate(f.getPath.getName))
    files.foreach { f =>
      val out = storage.create(f.getPath, true)
      try out.write("not a data file".getBytes) finally out.close()
    }
    files.size
  }

  /** The bare-key 1.0.2 table brought to the current version, then stripped of the property again (e.g. a manual edit). */
  private def currentVersionBareTableWithoutProperty(): Map[String, String] = {
    val opts = loadFixture(HoodieTableVersion.EIGHT)
    assertEquals((8L, 8L, 8L, 0L), keyStatsRaw())
    upsertFixtureRecords(opts, 10000L)
    assertEquals(HoodieTableVersion.current(), loadMetaClient().getTableConfig.getTableVersion)
    assertEquals((8L, 8L, 8L, 0L), keyStatsRaw())
    removeEncodingProperty()
    opts
  }

  /** The real crossing: a bare-key version 8 table, upserted by a writer at the current version with pure defaults. */
  @Test
  def testUpgradeToCurrentVersionWithDefaultsNoDuplicates(): Unit = {
    val opts = loadFixture(HoodieTableVersion.EIGHT)
    assertEquals((8L, 8L, 8L, 0L), keyStatsRaw())

    // This very write performs the 8 -> current upgrade AND keys the updates: both must agree on the bare encoding.
    upsertFixtureRecords(opts, 10000L)

    assertEquals((8L, 8L, 8L, 0L), keyStatsRaw(), "Upgrade write must update in place with bare keys, no duplicates")
    val tableConfig = loadMetaClient().getTableConfig
    assertEquals(HoodieTableVersion.current(), tableConfig.getTableVersion)
    assertEquals(ComplexKeyGenEncoding.VALUE_ONLY, tableConfig.getComplexKeyGenEncoding.get)
    assertTrue(hoodiePropertiesLines().contains(s"${HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key}=VALUE_ONLY"),
      "hoodie.properties must carry the encoding line verbatim")
    assertEquals(Set(10000L), readTable().select("ts").collect().map(_.getLong(0)).toSet, "Every record was updated in place")

    // Steady state: later writes read the property from hoodie.properties and keep matching.
    upsertFixtureRecords(opts, 20000L)
    assertEquals((8L, 8L, 8L, 0L), keyStatsRaw())
  }

  /** A version 9 table whose data carries bare keys keeps them: the property follows the data, not the version. */
  @Test
  def testNineToCurrentVersionUpgradeKeepsBareKeys(): Unit = {
    val opts = loadFixture(HoodieTableVersion.NINE)
    assertEquals((8L, 8L, 8L, 0L), keyStatsRaw())

    upsertFixtureRecords(opts, 10000L)

    assertEquals((8L, 8L, 8L, 0L), keyStatsRaw(), "9 -> current upgrade must keep bare keys, no duplicates")
    assertEquals(HoodieTableVersion.current(), loadMetaClient().getTableConfig.getTableVersion)
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())
  }

  /** A legacy table with field-prefixed keys (0.14.0 and older) records FIELD_PREFIXED. */
  @Test
  def testUpgradeFieldPrefixedLegacyTableStampsFieldPrefixed(): Unit = {
    val opts = loadFixture(HoodieTableVersion.SIX)
    assertEquals((8L, 8L, 0L, 8L), keyStatsRaw())

    upsertFixtureRecords(opts, 10000L)

    assertEquals((8L, 8L, 0L, 8L), keyStatsRaw())
    assertEquals(HoodieTableVersion.current(), loadMetaClient().getTableConfig.getTableVersion)
    assertEquals(Some(ComplexKeyGenEncoding.FIELD_PREFIXED.name), persistedEncoding())
  }

  /** A version 6 table written by 0.14.1 (bare keys) upgraded straight to the current version in one write: no hop may block. */
  @Test
  def testSixToCurrentVersionUpgradeDoesNotBlock(): Unit = {
    val opts = loadFixture(HoodieTableVersion.SIX, COMPLEX_KEYGEN_BARE_FIXTURE_SUFFIX)
    assertEquals((8L, 8L, 8L, 0L), keyStatsRaw())

    upsertFixtureRecords(opts, 10000L)

    assertEquals((8L, 8L, 8L, 0L), keyStatsRaw(), "6 -> current upgrade must keep bare keys, no duplicates")
    assertEquals(HoodieTableVersion.current(), loadMetaClient().getTableConfig.getTableVersion)
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())
  }

  /** An upgrade that stops at version 8 records the encoding just the same. */
  @Test
  def testUpgradeStoppingAtVersionEightRecordsEncoding(): Unit = {
    val opts = loadFixture(HoodieTableVersion.SIX, COMPLEX_KEYGEN_BARE_FIXTURE_SUFFIX)
    assertEquals((8L, 8L, 8L, 0L), keyStatsRaw())

    upsertFixtureRecords(opts + (HoodieWriteConfig.WRITE_TABLE_VERSION.key -> "8"), 10000L)

    assertEquals(HoodieTableVersion.EIGHT, loadMetaClient().getTableConfig.getTableVersion)
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())
    assertEquals((8L, 8L, 8L, 0L), keyStatsRaw())
  }

  /** The recorded encoding is authoritative on every table version: a downgrade keeps it and needs no override. */
  @Test
  def testDowngradeToEightKeepsProperty(): Unit = {
    val opts = loadFixture(HoodieTableVersion.EIGHT)
    assertEquals((8L, 8L, 8L, 0L), keyStatsRaw())
    upsertFixtureRecords(opts, 10000L)
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())

    val downgradeOpts = opts + (HoodieWriteConfig.WRITE_TABLE_VERSION.key -> "8")
    val writeConfig = HoodieWriteConfig.newBuilder().withPath(basePath).withProps(downgradeOpts.asJava).build()
    new UpgradeDowngrade(loadMetaClient(), writeConfig, context, SparkUpgradeDowngradeHelper.getInstance)
      .run(HoodieTableVersion.EIGHT, null)

    assertEquals(HoodieTableVersion.EIGHT, loadMetaClient().getTableConfig.getTableVersion)
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())

    upsertFixtureRecords(downgradeOpts, 20000L)
    assertEquals(HoodieTableVersion.EIGHT, loadMetaClient().getTableConfig.getTableVersion)
    assertEquals((8L, 8L, 8L, 0L), keyStatsRaw())
  }

  /** With auto-upgrade disabled the table stays at version 8, and the write still records the encoding it found. */
  @Test
  def testAutoUpgradeDisabledBackfillsEncoding(): Unit = {
    val opts = loadFixture(HoodieTableVersion.EIGHT)
    assertEquals((8L, 8L, 8L, 0L), keyStatsRaw())

    upsertFixtureRecords(opts + (HoodieWriteConfig.AUTO_UPGRADE_VERSION.key -> "false"), 10000L)

    assertEquals(HoodieTableVersion.EIGHT, loadMetaClient().getTableConfig.getTableVersion)
    assertEquals((8L, 8L, 8L, 0L), keyStatsRaw())
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())
  }

  /** A table already at the current version but without the property is backfilled from its data too. */
  @Test
  def testTableAtCurrentVersionWithoutPropertyBackfillsFromData(): Unit = {
    val opts = currentVersionBareTableWithoutProperty()

    upsertFixtureRecords(opts, 20000L)

    assertEquals((8L, 8L, 8L, 0L), keyStatsRaw(), "Bare keys must keep matching once the encoding is backfilled")
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())
  }

  /** The insert dedup lookup keys the records before the write's own initTable runs; it must see the deduced encoding. */
  @Test
  def testInsertWithDropDuplicatesOnTableWithoutProperty(): Unit = {
    val opts = currentVersionBareTableWithoutProperty()

    rows(FIXTURE_IDS, 20000L).write.format("org.apache.hudi")
      .options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.INSERT_DUP_POLICY.key, DataSourceWriteOptions.DROP_INSERT_DUP_POLICY)
      .mode(SaveMode.Append)
      .save(basePath)

    assertEquals((8L, 8L, 8L, 0L), keyStatsRaw(), "Every incoming record already exists and must be dropped")
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())
  }

  /** The fail-duplicates policy proves the dedup lookup saw the deduced encoding: every incoming key is found. */
  @Test
  def testInsertWithFailDuplicatesOnTableWithoutProperty(): Unit = {
    val opts = currentVersionBareTableWithoutProperty()

    val thrown = assertThrows(classOf[Throwable], () =>
      rows(FIXTURE_IDS, 20000L).write.format("org.apache.hudi")
        .options(opts)
        .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
        .option(DataSourceWriteOptions.INSERT_DUP_POLICY.key, DataSourceWriteOptions.FAIL_INSERT_DUP_POLICY)
        .mode(SaveMode.Append)
        .save(basePath))
    assertTrue(rootCauses(thrown).exists(_.isInstanceOf[HoodieDuplicateKeyException]),
      s"Expected a duplicate key failure, got: ${rootCauses(thrown).map(_.getClass.getSimpleName).mkString(" | ")}")
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding(), "The encoding is recorded during ingestion setup")
    assertEquals((8L, 8L, 8L, 0L), keyStatsRaw())
  }

  /** Data files that yield no record key leave the encoding undetermined: the write fails unless the validation is off. */
  @Test
  def testUnreadableDataFilesFailTheWriteUnlessValidationIsOff(): Unit = {
    val opts = loadFixture(HoodieTableVersion.EIGHT)
    assertTrue(corruptDataFiles(name => name.endsWith(".parquet") || name.contains(".log.")) > 0)

    val thrown = assertThrows(classOf[Throwable], () => upsertFixtureRecords(opts, 10000L))
    assertTrue(rootCauses(thrown).exists(t => Option(t.getMessage).exists(_.contains("complex key generator with a single record key field"))),
      s"Expected the complex keygen guidance, got: ${rootCauses(thrown).map(_.getMessage).mkString(" | ")}")
    assertEquals(None, persistedEncoding(), "Nothing is recorded when the encoding stays undetermined")

    val metaClient = loadMetaClient()
    val validationOn = HoodieWriteConfig.newBuilder().withPath(basePath).withProps(opts.asJava).build()
    assertFalse(KeyGenUtils.resolveComplexKeyGenEncodingForWrite(metaClient, validationOn).isPresent)
    val validationOff = HoodieWriteConfig.newBuilder().withPath(basePath).withProps((opts ++ Map(
      HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key -> "false",
      HoodieWriteConfig.COMPLEX_KEYGEN_NEW_ENCODING.key -> "true")).asJava).build()
    assertEquals(org.apache.hudi.common.util.Option.of(ComplexKeyGenEncoding.VALUE_ONLY),
      KeyGenUtils.resolveComplexKeyGenEncodingForWrite(metaClient, validationOff))
  }

  /** When no base file yields a record key, the log files of the MOR fixture decide the encoding. */
  @Test
  def testEncodingDeducedFromLogFilesWhenBaseFilesAreUnreadable(): Unit = {
    val opts = loadFixture(HoodieTableVersion.EIGHT)
    assertTrue(corruptDataFiles(_.endsWith(".parquet")) > 0)
    assertTrue(storage.listFiles(new StoragePath(basePath)).asScala.exists(_.getPath.getName.contains(".log.")),
      "The fixture must carry log files")

    val metaClient = loadMetaClient()
    assertEquals(org.apache.hudi.common.util.Option.of(ComplexKeyGenEncoding.VALUE_ONLY),
      KeyGenUtils.deduceComplexKeyGenEncodingFromData(metaClient), "The encoding must come from a log block")
    // the ingestion setup records what the log block says, without touching the (unreadable) base files
    val writeConfig = HoodieWriteConfig.newBuilder().withPath(basePath).withProps(opts.asJava).build()
    KeyGenUtils.recordComplexKeygenEncodingIfMissing(metaClient, writeConfig)
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())
  }

  /** insert_overwrite_table keeps the table (and its property) while skipping the table-config merge: keys must stay bare. */
  @Test
  def testInsertOverwriteTableKeepsPersistedEncoding(): Unit = {
    val opts = loadFixture(HoodieTableVersion.EIGHT)
    assertEquals((8L, 8L, 8L, 0L), keyStatsRaw())
    upsertFixtureRecords(opts, 10000L)
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())

    rows(Seq("id9", "id10", "id11"), 20000L).write.format("org.apache.hudi")
      .options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertEquals((3L, 3L, 3L, 0L), keyStatsRaw(), "Rewritten data must follow the table's persisted VALUE_ONLY encoding")
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())
  }

  /** The datasource delete keys its records from its own config: as the upgrading write it must still match the bare keys. */
  @Test
  def testDeleteAsUpgradingWriteRemovesRows(): Unit = {
    val opts = loadFixture(HoodieTableVersion.EIGHT)
    assertEquals((8L, 8L, 8L, 0L), keyStatsRaw())

    rows(FIXTURE_IDS.take(3), 10000L).write.format("org.apache.hudi")
      .options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    assertEquals(HoodieTableVersion.current(), loadMetaClient().getTableConfig.getTableVersion)
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())
    assertEquals((5L, 5L, 5L, 0L), keyStatsRaw(), "The delete must find the bare keys it was given")
    assertEquals(FIXTURE_IDS.drop(3).toSet, readTable().select("id").collect().map(_.getString(0)).toSet)
  }

  // Reads the table and returns (total, distinctKeys, bareKeys, prefixedKeys).
  private def keyStatsRaw(): (Long, Long, Long, Long) = {
    val rk = readTable().selectExpr("_hoodie_record_key as k").collect().map(_.getString(0))
    val total = rk.length.toLong
    val distinct = rk.distinct.length.toLong
    val prefix = FIXTURE_RECORD_KEY_FIELD + KeyGenerator.DEFAULT_COLUMN_VALUE_SEPARATOR
    (total, distinct, rk.count(k => !k.startsWith(prefix)).toLong, rk.count(k => k.startsWith(prefix)).toLong)
  }
}
