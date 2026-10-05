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

import org.apache.hudi.{DataSourceUtils, DataSourceWriteOptions, HoodieWriterUtils}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.functional.ComplexKeyGenFixtures._
import org.apache.hudi.keygen.KeyGenUtils
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
 * Single-field ComplexKeyGenerator tables and hoodie.table.complex.keygenerator.encoding (HUDI-7001).
 *
 * The legacy tables are the checked-in fixtures written by the actual 0.14.0 (`id:<value>` keys) and 0.14.1
 * (bare `<value>` keys) releases, so the stored keys stay what those releases produced rather than what a current
 * writer would choose. The next write must deduce the encoding the data carries, record it, and keep matching the
 * existing keys; new tables record the encoding their writer produces.
 */
class TestComplexKeyGenEncoding extends HoodieSparkClientTestBase {

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

  /** Extracts a version 6 fixture table into the temp dir, points the test's base path at it and returns its write options. */
  private def loadFixture(suffix: String): Map[String, String] = {
    val fixtureName = s"hudi-v6-table$suffix.zip"
    HoodieTestUtils.extractZipToDirectory(COMPLEX_KEYGEN_FIXTURES_PATH + fixtureName, tempDir, getClass)
    val tableName = fixtureName.replace(".zip", "")
    basePath = tempDir.resolve(tableName).toString
    assertEquals(HoodieTableVersion.SIX, loadMetaClient().getTableConfig.getTableVersion)
    assertEquals(None, persistedEncoding(), "Fixture tables predate the encoding property")
    fixtureWriteOpts(tableName)
  }

  private def loadBareFixture(): Map[String, String] = {
    val opts = loadFixture(COMPLEX_KEYGEN_BARE_FIXTURE_SUFFIX)
    assertEquals((8L, 8L, 8L, 0L), keyStatsRaw(), "The 0.14.1 fixture stores bare keys")
    opts
  }

  private def rows(ids: Seq[String], ts: Long): DataFrame = fixtureRows(sparkSession, ids, ts)

  private def write(opts: Map[String, String], operation: String, ids: Seq[String], ts: Long,
                    mode: SaveMode = SaveMode.Append): Unit = {
    rows(ids, ts).write.format("org.apache.hudi")
      .options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, operation)
      .mode(mode)
      .save(basePath)
  }

  /** Upserts the given fixture records again with a newer ordering value. */
  private def upsertFixtureRecords(opts: Map[String, String], ts: Long, ids: Seq[String] = FIXTURE_IDS): Unit =
    write(opts, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL, ids, ts)

  private def loadMetaClient(): HoodieTableMetaClient =
    HoodieTableMetaClient.builder().setConf(storage.getConf.newInstance()).setBasePath(basePath).build()

  private def persistedEncoding(): Option[String] = {
    val tableConfig = loadMetaClient().getTableConfig
    if (tableConfig.contains(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING)) Some(tableConfig.getString(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING)) else None
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

  /** A 0.14.1 table upserted with pure defaults: the write records VALUE_ONLY and updates in place. */
  @Test
  def testBareLegacyTableUpsertWithDefaultsNoDuplicates(): Unit = {
    val opts = loadBareFixture()

    upsertFixtureRecords(opts, 10000L)

    assertEquals((8L, 8L, 8L, 0L), keyStatsRaw(), "The upsert must update in place with bare keys, no duplicates")
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())
    assertTrue(hoodiePropertiesLines().contains(s"${HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key}=VALUE_ONLY"),
      "hoodie.properties must carry the encoding line verbatim")
    assertEquals(Set(10000L), readTable().select("ts").collect().map(_.getLong(0)).toSet, "Every record was updated in place")

    // Steady state: later writes read the property from hoodie.properties and keep matching.
    upsertFixtureRecords(opts, 20000L)
    assertEquals((8L, 8L, 8L, 0L), keyStatsRaw())
  }

  /**
   * With the validation off, the writer used to key with hoodie.write.complex.keygen.new.encoding (default false,
   * i.e. `id:<value>`) and duplicate every record of a 0.14.1 table. The data now decides.
   */
  @Test
  def testBareLegacyTableWithValidationOffDeducesFromData(): Unit = {
    val opts = loadBareFixture() + (HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key -> "false")

    upsertFixtureRecords(opts, 10000L)

    assertEquals((8L, 8L, 8L, 0L), keyStatsRaw(), "The data's bare keys win over the configured new.encoding")
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())
  }

  /** A legacy table with field-prefixed keys (0.14.0 and older) records FIELD_PREFIXED. */
  @Test
  def testPrefixedLegacyTableRecordsFieldPrefixed(): Unit = {
    val opts = loadFixture(COMPLEX_KEYGEN_FIXTURE_SUFFIX)
    assertEquals((8L, 8L, 0L, 8L), keyStatsRaw())

    // new.encoding=true would key bare values: the recorded encoding of the data must win
    upsertFixtureRecords(opts + (HoodieWriteConfig.COMPLEX_KEYGEN_NEW_ENCODING.key -> "true"), 10000L)

    assertEquals((8L, 8L, 0L, 8L), keyStatsRaw())
    assertEquals(Some(ComplexKeyGenEncoding.FIELD_PREFIXED.name), persistedEncoding())
  }

  /** A write option named like the table property does not override what the table's data carries. */
  @Test
  def testEncodingWriteOptionDoesNotOverrideTheData(): Unit = {
    val opts = loadBareFixture()

    upsertFixtureRecords(opts + (HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key -> ComplexKeyGenEncoding.FIELD_PREFIXED.name), 10000L)
    assertEquals((8L, 8L, 8L, 0L), keyStatsRaw())
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())

    // once recorded, a contradicting option is a table config conflict and nothing is written
    val thrown = assertThrows(classOf[Throwable], () =>
      upsertFixtureRecords(opts + (HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key -> ComplexKeyGenEncoding.FIELD_PREFIXED.name), 20000L))
    assertTrue(rootCauses(thrown).exists(t => Option(t.getMessage).exists(_.contains(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key))),
      s"Expected a config conflict on the encoding, got: ${rootCauses(thrown).map(_.getMessage).mkString(" | ")}")
    assertEquals((8L, 8L, 8L, 0L), keyStatsRaw())
    assertEquals(Set(10000L), readTable().select("ts").collect().map(_.getLong(0)).toSet)
  }

  /** The insert dedup lookup keys the records before the client writes; it must see the deduced encoding. */
  @Test
  def testInsertWithDropDuplicatesOnLegacyTable(): Unit = {
    val opts = loadBareFixture()

    write(opts + (DataSourceWriteOptions.INSERT_DROP_DUPS.key -> "true"),
      DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL, FIXTURE_IDS :+ "id9", 20000L)

    assertEquals((9L, 9L, 9L, 0L), keyStatsRaw(), "Every existing record must be dropped, only id9 is new")
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())
  }

  /** The row writer builds its own write config: a bulk insert into a legacy table must still key bare values. */
  @Test
  def testRowWriterBulkInsertFollowsDeducedEncoding(): Unit = {
    val opts = loadBareFixture()

    write(opts + (DataSourceWriteOptions.ENABLE_ROW_WRITER.key -> "true"),
      DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL, Seq("id9", "id10"), 20000L)

    assertEquals((10L, 10L, 10L, 0L), keyStatsRaw(), "Bulk-inserted records must carry bare keys like the rest of the table")
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())
  }

  /** The datasource delete keys its records from its own config: as the first write it must still match the bare keys. */
  @Test
  def testDeleteAsFirstWriteRemovesRows(): Unit = {
    val opts = loadBareFixture()

    write(opts, DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL, FIXTURE_IDS.take(3), 10000L)

    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())
    assertEquals((5L, 5L, 5L, 0L), keyStatsRaw(), "The delete must find the bare keys it was given")
    assertEquals(FIXTURE_IDS.drop(3).toSet, readTable().select("id").collect().map(_.getString(0)).toSet)
  }

  /** insert_overwrite_table keeps the table (and its property): rewritten data must keep the recorded encoding. */
  @Test
  def testInsertOverwriteTableKeepsPersistedEncoding(): Unit = {
    val opts = loadBareFixture()
    upsertFixtureRecords(opts, 10000L)
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())

    write(opts, DataSourceWriteOptions.INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL, Seq("id9", "id10", "id11"), 20000L, SaveMode.Overwrite)

    assertEquals((3L, 3L, 3L, 0L), keyStatsRaw(), "Rewritten data must follow the table's persisted VALUE_ONLY encoding")
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())
  }

  /** Data files that yield no record key leave the encoding undetermined: the write fails unless the validation is off. */
  @Test
  def testUnreadableDataFilesFailTheWriteUnlessValidationIsOff(): Unit = {
    val opts = loadBareFixture()
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
    val opts = loadBareFixture()
    assertTrue(corruptDataFiles(_.endsWith(".parquet")) > 0)
    assertTrue(storage.listFiles(new StoragePath(basePath)).asScala.exists(_.getPath.getName.contains(".log.")),
      "The fixture must carry log files")

    val metaClient = loadMetaClient()
    assertEquals(org.apache.hudi.common.util.Option.of(ComplexKeyGenEncoding.VALUE_ONLY),
      KeyGenUtils.deduceComplexKeyGenEncodingFromData(metaClient), "The encoding must come from a log block")
    val writeConfig = HoodieWriteConfig.newBuilder().withPath(basePath).withProps(opts.asJava).build()
    KeyGenUtils.recordComplexKeygenEncodingIfMissing(metaClient, writeConfig)
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())
  }

  /** A table version change records the encoding of a table that lacks it, so later writes key the way the data does. */
  @Test
  def testDowngradeRecordsEncoding(): Unit = {
    val opts = loadBareFixture()
    val writeConfig = HoodieWriteConfig.newBuilder().withPath(basePath).withProps(opts.asJava).build()

    new UpgradeDowngrade(loadMetaClient(), writeConfig, context, SparkUpgradeDowngradeHelper.getInstance)
      .run(HoodieTableVersion.FIVE, null)

    assertEquals(HoodieTableVersion.FIVE, loadMetaClient().getTableConfig.getTableVersion)
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())
  }

  /**
   * Clients for table services and DDL (procedures, ALTER TABLE) key no records, so they leave a missing encoding
   * unrecorded: recording it from a batch they are about to roll back would misdescribe the remaining data.
   */
  @Test
  def testNonWritingClientsDoNotRecordEncoding(): Unit = {
    val opts = HoodieWriterUtils.parametersWithWriteDefaults(loadBareFixture())

    val client = DataSourceUtils.createHoodieClient(jsc, null, basePath, "hudi-v6-table-complex-keygen-bare", opts.asJava)
    client.close()
    assertEquals(None, persistedEncoding())

    val writingClient = DataSourceUtils.createHoodieClient(jsc, null, basePath, "hudi-v6-table-complex-keygen-bare", opts.asJava, true)
    writingClient.close()
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())
  }

  /**
   * A commit that packs inserts into an existing small file rewrites the file with its older records first, keyed the
   * way their writer keyed them. The encoding must come from a record the newest commit wrote, not from the file's
   * first record.
   */
  @Test
  def testEncodingDeducedFromTheRecordsTheNewestCommitWrote(): Unit = {
    basePath = tempDir.resolve("mixed_cow").toString
    val opts = fixtureWriteOpts("mixed_cow") + (DataSourceWriteOptions.TABLE_TYPE.key -> DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
    val partitionRows = (ids: Seq[String], ts: Long) => sparkSession.createDataFrame(ids.map(id => (id, s"${id}_$ts", ts, "2023-01-01", "a")))
      .toDF("id", "name", "ts", "partition", "category")
    // a writer keying `id:<value>`, as 0.14.0 did
    partitionRows((1 to 8).map(i => s"id$i"), 1000L).write.format("org.apache.hudi").options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL).mode(SaveMode.Overwrite).save(basePath)
    assertEquals(Some(ComplexKeyGenEncoding.FIELD_PREFIXED.name), persistedEncoding())
    // a writer keying bare values, as 0.14.1 did: its inserts are packed into the existing small file
    setEncoding(ComplexKeyGenEncoding.VALUE_ONLY)
    partitionRows(Seq("id9", "id10"), 2000L).write.format("org.apache.hudi").options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL).mode(SaveMode.Append).save(basePath)
    assertEquals((10L, 10L, 2L, 8L), keyStatsRaw())
    val baseFiles = storage.listFiles(new StoragePath(basePath)).asScala.filter(_.getPath.getName.endsWith(".parquet"))
    val latestFile = baseFiles.maxBy(_.getModificationTime).getPath
    val latestFileKeys = sparkSession.read.parquet(latestFile.toString).select("_hoodie_record_key").collect().map(_.getString(0))
    assertEquals(10, latestFileKeys.length, "The newest commit must have rewritten the small file")
    assertTrue(latestFileKeys.head.startsWith("id:"), s"The rewritten file must start with an older record: ${latestFileKeys.mkString(",")}")

    HoodieTableConfig.delete(storage, loadMetaClient().getMetaPath, Set(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key).asJava)
    assertEquals(org.apache.hudi.common.util.Option.of(ComplexKeyGenEncoding.VALUE_ONLY),
      KeyGenUtils.deduceComplexKeyGenEncodingFromData(loadMetaClient()))
  }

  private def setEncoding(encoding: ComplexKeyGenEncoding): Unit = {
    val props = new java.util.Properties()
    props.setProperty(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key, encoding.name)
    HoodieTableConfig.update(storage, loadMetaClient().getMetaPath, props)
  }

  /** A new table records the encoding its writer produces: `id:<value>` by default. */
  @Test
  def testNewTableRecordsFieldPrefixedByDefault(): Unit = {
    val opts = fixtureWriteOpts("new_table")

    write(opts, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL, FIXTURE_IDS, 1000L, SaveMode.Overwrite)

    assertEquals(Some(ComplexKeyGenEncoding.FIELD_PREFIXED.name), persistedEncoding())
    assertEquals((8L, 8L, 0L, 8L), keyStatsRaw())
  }

  /** A new table written with new.encoding=true records VALUE_ONLY and keeps bare keys even when the option changes. */
  @Test
  def testNewTableRecordsTheConfiguredNewEncoding(): Unit = {
    val opts = fixtureWriteOpts("new_table")

    write(opts + (HoodieWriteConfig.COMPLEX_KEYGEN_NEW_ENCODING.key -> "true"),
      DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL, FIXTURE_IDS, 1000L, SaveMode.Overwrite)
    assertEquals(Some(ComplexKeyGenEncoding.VALUE_ONLY.name), persistedEncoding())
    assertEquals((8L, 8L, 8L, 0L), keyStatsRaw())

    upsertFixtureRecords(opts, 2000L)
    assertEquals((8L, 8L, 8L, 0L), keyStatsRaw(), "The recorded VALUE_ONLY wins over the default new.encoding")
  }

  // Reads the table and returns (total, distinctKeys, bareKeys, prefixedKeys).
  private def keyStatsRaw(): (Long, Long, Long, Long) = {
    val rk = readTable().selectExpr("_hoodie_record_key as k").collect().map(_.getString(0))
    val total = rk.length.toLong
    val distinct = rk.distinct.length.toLong
    val prefix = FIXTURE_RECORD_KEY_FIELD + KeyGenUtils.DEFAULT_COLUMN_VALUE_SEPARATOR
    (total, distinct, rk.count(k => !k.startsWith(prefix)).toLong, rk.count(k => k.startsWith(prefix)).toLong)
  }
}
