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
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.HoodieDuplicateKeyException
import org.apache.hudi.keygen.{KeyGenerator, KeyGenUtils}
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
 * Existing single-field ComplexKeyGenerator tables that predate hoodie.table.complex.keygen.encoding: the next
 * write or upgrade must deduce the encoding the data carries, record it, and keep matching the existing keys.
 * Legacy tables are simulated by creating them with an explicit encoding and then removing the property, the way
 * a table written by an older release looks.
 */
class TestComplexKeyGenExistingTableUpgrade extends HoodieSparkClientTestBase {

  private val recordKeyField = "_row_key"
  private val partitionPathField = "partition"

  var commonOpts: Map[String, String] = Map(
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

  private def toDF(records: java.util.List[HoodieRecord[_]]): DataFrame =
    sparkSession.read.json(sparkSession.sparkContext.parallelize(recordsToStrings(records).asScala.toList, 2))

  /**
   * Writes the initial commit of a legacy table at the given table version with the given key encoding, then
   * removes the encoding property so the table looks like one written by a release that did not record it.
   */
  private def writeInitialLegacyTable(dataGen: HoodieTestDataGenerator, bareKeys: Boolean, tableVersion: String = "8",
                                      extraOpts: Map[String, String] = Map.empty): java.util.List[HoodieRecord[_]] = {
    val records = dataGen.generateInserts("001", 100)
    val encoding = if (bareKeys) ComplexKeyGenEncoding.VALUE_ONLY else ComplexKeyGenEncoding.FIELD_PREFIXED
    toDF(records).write.format("org.apache.hudi")
      .options(commonOpts ++ extraOpts)
      .option(HoodieWriteConfig.WRITE_TABLE_VERSION.key, tableVersion)
      .option(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key, encoding.name)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    assertEquals(Some(encoding.name), persistedEncoding())
    removeEncodingProperty()
    records
  }

  /** Writes the initial 0.14.1-style commit: bare-value record keys. */
  private def writeInitial0141Table(dataGen: HoodieTestDataGenerator) = writeInitialLegacyTable(dataGen, bareKeys = true)

  /** Upserts the same records again (updated values) with the given options. */
  private def upsertSameRecords(dataGen: HoodieTestDataGenerator, inserted: java.util.List[HoodieRecord[_]],
                                opts: Map[String, String], commitId: String = "002"): Unit = {
    toDF(dataGen.generateUpdates(commitId, inserted)).write.format("org.apache.hudi")
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

  private def baseFileCount(): Int =
    storage.listFiles(new StoragePath(basePath)).asScala
      .count(f => f.getPath.getName.endsWith(".parquet") && !f.getPath.toString.contains("/.hoodie/"))

  /** The real crossing: a bare-key version 8 table, upserted by a writer at the current version with pure defaults. */
  @Test
  def testUpgradeToCurrentVersionWithDefaultsNoDuplicates(): Unit = {
    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val inserted = writeInitial0141Table(dataGen)
    assertEquals((100L, 100L, 100L, 0L), keyStatsRaw())
    assertEquals(HoodieTableVersion.EIGHT, loadMetaClient().getTableConfig.getTableVersion)

    // This very write performs the 8 -> current upgrade AND keys the updates: both must agree on the bare encoding.
    upsertSameRecords(dataGen, inserted, commonOpts)

    assertEquals((100L, 100L, 100L, 0L), keyStatsRaw(), "Upgrade write must update in place with bare keys, no duplicates")
    val tableConfig = loadMetaClient().getTableConfig
    assertEquals(HoodieTableVersion.current(), tableConfig.getTableVersion)
    assertEquals(ComplexKeyGenEncoding.VALUE_ONLY, tableConfig.getComplexKeyGenEncoding.get)
    assertTrue(hoodiePropertiesLines().contains(s"${HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key}=VALUE_ONLY"),
      "hoodie.properties must carry the encoding line verbatim")

    // Steady state: later writes read the property from hoodie.properties and keep matching.
    upsertSameRecords(dataGen, inserted, commonOpts, "003")
    assertEquals((100L, 100L, 100L, 0L), keyStatsRaw())
  }

  /** A legacy table with field-prefixed keys (0.14.0 and older) records FIELD_PREFIXED. */
  @Test
  def testUpgradeFieldPrefixedLegacyTableStampsFieldPrefixed(): Unit = {
    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val inserted = writeInitialLegacyTable(dataGen, bareKeys = false)
    assertEquals((100L, 100L, 0L, 100L), keyStatsRaw())

    upsertSameRecords(dataGen, inserted, commonOpts)

    assertEquals((100L, 100L, 0L, 100L), keyStatsRaw())
    assertEquals(HoodieTableVersion.current(), loadMetaClient().getTableConfig.getTableVersion)
    assertEquals(Some(ComplexKeyGenEncoding.FIELD_PREFIXED.name), persistedEncoding())
  }

  /** A version 6 table (0.14.x) upgraded straight to the current version in one write: no hop may block. */
  @Test
  def testSixToCurrentVersionUpgradeDoesNotBlock(): Unit = {
    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val inserted = writeInitialLegacyTable(dataGen, bareKeys = true, tableVersion = "6")
    assertEquals(HoodieTableVersion.SIX, loadMetaClient().getTableConfig.getTableVersion)
    assertEquals((100L, 100L, 100L, 0L), keyStatsRaw())

    upsertSameRecords(dataGen, inserted, commonOpts)

    assertEquals((100L, 100L, 100L, 0L), keyStatsRaw(), "6 -> current upgrade must keep bare keys, no duplicates")
    assertEquals(HoodieTableVersion.current(), loadMetaClient().getTableConfig.getTableVersion)
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())
  }

  /** An upgrade that stops at version 8 records the encoding just the same. */
  @Test
  def testUpgradeStoppingAtVersionEightRecordsEncoding(): Unit = {
    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val inserted = writeInitialLegacyTable(dataGen, bareKeys = true, tableVersion = "6")

    upsertSameRecords(dataGen, inserted, commonOpts + (HoodieWriteConfig.WRITE_TABLE_VERSION.key -> "8"))

    assertEquals(HoodieTableVersion.EIGHT, loadMetaClient().getTableConfig.getTableVersion)
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())
    assertEquals((100L, 100L, 100L, 0L), keyStatsRaw())
  }

  /** The recorded encoding is authoritative on every table version: a downgrade keeps it and needs no override. */
  @Test
  def testDowngradeToEightKeepsProperty(): Unit = {
    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val inserted = writeInitial0141Table(dataGen)
    upsertSameRecords(dataGen, inserted, commonOpts)
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())

    val downgradeOpts = commonOpts + (HoodieWriteConfig.WRITE_TABLE_VERSION.key -> "8")
    val writeConfig = HoodieWriteConfig.newBuilder().withPath(basePath).withProps(downgradeOpts.asJava).build()
    new UpgradeDowngrade(loadMetaClient(), writeConfig, context, SparkUpgradeDowngradeHelper.getInstance)
      .run(HoodieTableVersion.EIGHT, null)

    assertEquals(HoodieTableVersion.EIGHT, loadMetaClient().getTableConfig.getTableVersion)
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())

    upsertSameRecords(dataGen, inserted, downgradeOpts, "003")
    assertEquals(HoodieTableVersion.EIGHT, loadMetaClient().getTableConfig.getTableVersion)
    assertEquals((100L, 100L, 100L, 0L), keyStatsRaw())
  }

  /** With auto-upgrade disabled the table stays at version 8, and the write still records the encoding it found. */
  @Test
  def testAutoUpgradeDisabledBackfillsEncoding(): Unit = {
    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val inserted = writeInitial0141Table(dataGen)

    upsertSameRecords(dataGen, inserted, commonOpts + (HoodieWriteConfig.AUTO_UPGRADE_VERSION.key -> "false"))

    assertEquals(HoodieTableVersion.EIGHT, loadMetaClient().getTableConfig.getTableVersion)
    assertEquals((100L, 100L, 100L, 0L), keyStatsRaw())
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())
  }

  /** A table already at the current version but without the property (e.g. bare keys written by Flink) is backfilled too. */
  @Test
  def testTableAtCurrentVersionWithoutPropertyBackfillsFromData(): Unit = {
    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val inserted = writeInitialLegacyTable(dataGen, bareKeys = true, tableVersion = HoodieTableVersion.current().versionCode().toString)
    assertEquals(HoodieTableVersion.current(), loadMetaClient().getTableConfig.getTableVersion)

    upsertSameRecords(dataGen, inserted, commonOpts)

    assertEquals((100L, 100L, 100L, 0L), keyStatsRaw(), "Bare keys must keep matching once the encoding is backfilled")
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())
  }

  /** The insert dedup lookup keys the records before the write's own initTable runs; it must see the deduced encoding. */
  @Test
  def testInsertWithDropDuplicatesOnTableWithoutProperty(): Unit = {
    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val inserted = writeInitialLegacyTable(dataGen, bareKeys = true, tableVersion = HoodieTableVersion.current().versionCode().toString)

    toDF(inserted).write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.INSERT_DUP_POLICY.key, DataSourceWriteOptions.DROP_INSERT_DUP_POLICY)
      .mode(SaveMode.Append)
      .save(basePath)

    assertEquals((100L, 100L, 100L, 0L), keyStatsRaw(), "Every incoming record already exists and must be dropped")
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())
  }

  /** The fail-duplicates policy proves the dedup lookup saw the deduced encoding: every incoming key is found. */
  @Test
  def testInsertWithFailDuplicatesOnTableWithoutProperty(): Unit = {
    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val inserted = writeInitialLegacyTable(dataGen, bareKeys = true, tableVersion = HoodieTableVersion.current().versionCode().toString)

    val thrown = assertThrows(classOf[Throwable], () =>
      toDF(inserted).write.format("org.apache.hudi")
        .options(commonOpts)
        .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
        .option(DataSourceWriteOptions.INSERT_DUP_POLICY.key, DataSourceWriteOptions.FAIL_INSERT_DUP_POLICY)
        .mode(SaveMode.Append)
        .save(basePath))
    assertTrue(rootCauses(thrown).exists(_.isInstanceOf[HoodieDuplicateKeyException]),
      s"Expected a duplicate key failure, got: ${rootCauses(thrown).map(_.getClass.getSimpleName).mkString(" | ")}")
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding(), "The encoding is recorded when the commit starts")
    assertEquals((100L, 100L, 100L, 0L), keyStatsRaw())
  }

  /** Data files that yield no record key leave the encoding undetermined: the write fails unless the validation is off. */
  @Test
  def testUnreadableDataFilesFailTheWriteUnlessValidationIsOff(): Unit = {
    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val inserted = writeInitial0141Table(dataGen)
    storage.listFiles(new StoragePath(basePath)).asScala
      .filter(f => f.getPath.getName.endsWith(".parquet") && !f.getPath.toString.contains("/.hoodie/"))
      .foreach { f =>
        val out = storage.create(f.getPath, true)
        try out.write("not a parquet file".getBytes) finally out.close()
      }

    val thrown = assertThrows(classOf[Throwable], () => upsertSameRecords(dataGen, inserted, commonOpts))
    assertTrue(rootCauses(thrown).exists(t => Option(t.getMessage).exists(_.contains("complex key generator with a single record key field"))),
      s"Expected the complex keygen guidance, got: ${rootCauses(thrown).map(_.getMessage).mkString(" | ")}")
    assertEquals(None, persistedEncoding(), "Nothing is recorded when the encoding stays undetermined")

    val metaClient = loadMetaClient()
    val validationOn = HoodieWriteConfig.newBuilder().withPath(basePath).withProps(commonOpts.asJava).build()
    assertFalse(KeyGenUtils.resolveComplexKeyGenEncodingForWrite(metaClient, validationOn).isPresent)
    val validationOff = HoodieWriteConfig.newBuilder().withPath(basePath).withProps((commonOpts ++ Map(
      HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key -> "false",
      HoodieWriteConfig.COMPLEX_KEYGEN_NEW_ENCODING.key -> "true")).asJava).build()
    assertEquals(org.apache.hudi.common.util.Option.of(ComplexKeyGenEncoding.VALUE_ONLY),
      KeyGenUtils.resolveComplexKeyGenEncodingForWrite(metaClient, validationOff))
  }

  /** A MOR table whose only data files are log files (log-indexing index, no compaction yet) is deduced from a log block. */
  @Test
  def testUpgradeDeducesEncodingFromLogFiles(): Unit = {
    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val morLogOnlyOpts = Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL,
      "hoodie.index.type" -> "INMEMORY",
      "hoodie.compact.inline" -> "false")
    val inserted = writeInitialLegacyTable(dataGen, bareKeys = true, extraOpts = morLogOnlyOpts)
    assertEquals(0, baseFileCount(), "The legacy commit must have landed in log files only")
    assertEquals((100L, 100L, 100L, 0L), keyStatsRaw())

    upsertSameRecords(dataGen, inserted, commonOpts ++ morLogOnlyOpts)

    assertEquals(HoodieTableVersion.current(), loadMetaClient().getTableConfig.getTableVersion)
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())
    assertEquals((100L, 100L, 100L, 0L), keyStatsRaw(), "The encoding read from the log file must keep the upsert matching")
  }

  /** insert_overwrite_table keeps the table (and its property) while skipping the table-config merge: keys must stay bare. */
  @Test
  def testInsertOverwriteTableKeepsPersistedEncoding(): Unit = {
    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val inserted = writeInitial0141Table(dataGen)
    upsertSameRecords(dataGen, inserted, commonOpts)
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())

    toDF(dataGen.generateInserts("003", 50)).write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertEquals((50L, 50L, 50L, 0L), keyStatsRaw(), "Rewritten data must follow the table's persisted VALUE_ONLY encoding")
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())
  }

  /** The datasource delete keys its records from its own config: as the upgrading write it must still match the bare keys. */
  @Test
  def testDeleteAsUpgradingWriteRemovesRows(): Unit = {
    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val inserted = writeInitial0141Table(dataGen)

    toDF(inserted.subList(0, 40)).write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    assertEquals(HoodieTableVersion.current(), loadMetaClient().getTableConfig.getTableVersion)
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())
    assertEquals((60L, 60L, 60L, 0L), keyStatsRaw(), "The delete must find the bare keys it was given")
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
