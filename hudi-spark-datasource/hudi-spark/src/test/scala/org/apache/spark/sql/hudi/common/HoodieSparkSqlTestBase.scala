/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.common

import org.apache.hudi.{DefaultSparkRecordMerger, HoodieSparkUtils}
import org.apache.hudi.HoodieFileIndex.DataSkippingFailureMode
import org.apache.hudi.common.config.{HoodieCommonConfig, HoodieMetadataConfig, HoodieStorageConfig}
import org.apache.hudi.common.engine.HoodieLocalEngineContext
import org.apache.hudi.common.model.{FileSlice, HoodieAvroRecordMerger, HoodieLogFile, HoodieRecord, HoodieReplaceCommitMetadata}
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.log.HoodieLogFormat
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock
import org.apache.hudi.common.table.view.{FileSystemViewManager, FileSystemViewStorageConfig, SyncableFileSystemView}
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.common.util.OrderingValues
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.index.HoodieInMemoryHashIndex
import org.apache.hudi.storage.{HoodieStorage, StoragePath}
import org.apache.hudi.testutils.HoodieClientTestUtils.{createMetaClient, getSparkConfForTest}

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.hudi.catalog.HoodieCatalog
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase.{checkMessageContains, sharedSessionEnabled}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructField
import org.apache.spark.util.Utils
import org.joda.time.DateTimeZone
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.scalactic.source
import org.scalatest.{Args, BeforeAndAfterAll, FunSuite, Status, Tag}
import org.scalatest.Assertions.assertResult
import org.slf4j.LoggerFactory

import java.io.File
import java.util.{Optional, TimeZone}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.regex.Pattern
import java.util.stream.Collectors

import scala.util.Try

class HoodieSparkSqlTestBase extends FunSuite with BeforeAndAfterAll {
  org.apache.log4j.Logger.getRootLogger.setLevel(org.apache.log4j.Level.WARN)
  private val LOG = LoggerFactory.getLogger(getClass)

  private lazy val sparkWareHouse: File = if (sharedSessionEnabled) {
    HoodieSparkSqlTestBase.sharedWarehouse
  } else {
    val dir = Utils.createTempDir()
    Utils.deleteRecursively(dir)
    dir
  }

  // NOTE: We need to set "spark.testing" property to make sure Spark can appropriately
  //       recognize environment as testing
  System.setProperty("spark.testing", "true")
  // NOTE: We have to fix the timezone to make sure all date-/timestamp-bound utilities output
  //       is consistent with the fixtures
  DateTimeZone.setDefault(DateTimeZone.UTC)
  TimeZone.setDefault(DateTimeUtils.getTimeZone("UTC"))
  protected lazy val spark: SparkSession = if (sharedSessionEnabled) {
    val session = HoodieSparkSqlTestBase.sharedBaseSession().newSession()
    SparkSession.setActiveSession(session)
    applySuiteConfToSharedSession(session)
    session
  } else {
    HoodieSparkSqlTestBase.sessionBuilder(sparkWareHouse, sparkConf()).getOrCreate()
  }

  private var tableId = new AtomicInteger(0)

  private var extraConf = Map[String, String]()

  // Shared mode: spark.hadoop.* keys this suite set on the shared Hadoop conf, with the value they replaced.
  private var hadoopConfOverrides: Seq[(String, String)] = Seq.empty

  def sparkConf(): SparkConf = {
    val conf = getSparkConfForTest("Hoodie SQL Test")
    conf.setAll(extraConf)
    conf
  }

  /**
   * Shared mode: the context-level SparkConf is fixed, so the deltas a suite adds through extraConf or a
   * sparkConf() override go to its session conf (hoodie.* and spark.sql.* keys), except spark.hadoop.*
   * keys, which the write client reads from sparkContext.hadoopConfiguration and which are restored in
   * afterAll. Any other spark.* key, and any static SQL conf, is a setting a child session cannot change,
   * so it is rejected before anything is mutated: a partial apply would leave the shared Hadoop conf
   * changed for every later suite in the JVM.
   */
  private def applySuiteConfToSharedSession(session: SparkSession): Unit = {
    val defaults = getSparkConfForTest("Hoodie SQL Test").getAll.toMap
    val deltas = sparkConf().getAll.filterNot { case (k, v) => defaults.get(k).contains(v) }
    val (hadoopKeys, sessionKeys) = deltas.partition { case (k, _) => k.startsWith("spark.hadoop.") }
    val rejected = sessionKeys.collect {
      case (k, _) if (k.startsWith("spark.") && !k.startsWith("spark.sql.")) || SQLConf.isStaticConfigKey(k) => k
    }
    if (rejected.nonEmpty) {
      throw new IllegalArgumentException(
        s"${rejected.mkString(", ")}: SparkContext-level or static settings; shared session mode cannot apply them per suite")
    }
    val hadoopConf = session.sparkContext.hadoopConfiguration
    hadoopKeys.foreach { case (k, v) =>
      val key = k.stripPrefix("spark.hadoop.")
      hadoopConfOverrides :+= (key -> hadoopConf.get(key))
      hadoopConf.set(key, v)
    }
    sessionKeys.foreach { case (k, v) => session.conf.set(k, v) }
  }

  protected def initQueryIndexConf(): Unit = {
    extraConf = extraConf ++ Map(
      DataSkippingFailureMode.configName -> DataSkippingFailureMode.Strict.value
    )
  }

  protected def withTempDir(f: File => Unit): Unit = {
    val tempDir = Utils.createTempDir()
    try f(tempDir) finally {
      Utils.deleteRecursively(tempDir)
    }
  }

  protected def getTableStoragePath(tableName: String): String = {
    new File(sparkWareHouse, tableName).getCanonicalPath
  }

  /**
   * Holds the read side of [[HoodieSparkSqlTestBase.suiteLock]] for the whole suite, beforeAll and afterAll
   * included, so an [[ExclusiveSuite]] never overlaps any part of another suite. ExclusiveSuite takes the
   * write side around this method; ReentrantReadWriteLock lets a writer also acquire the read side.
   */
  override def run(testName: Option[String], args: Args): Status = {
    val readLock = HoodieSparkSqlTestBase.suiteLock.readLock()
    readLock.lock()
    try {
      super.run(testName, args)
    } finally {
      readLock.unlock()
    }
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any /* Assertion */)(implicit pos: source.Position): Unit = {
    super.test(testName, testTags: _*)({
      try {
        if (sharedSessionEnabled) {
          bindSuiteSession()
        }
        testFun
      } finally {
        // The INMEMORY index keeps a JVM-static record-location map; reset it after every test so
        // stale keys from an earlier test cannot misroute writes in a later one. withRecordType
        // clears it between record-type iterations, but only on success and only for tests that use
        // it, so a throwing or non-withRecordType INMEMORY test would otherwise leak state here.
        // Runs before the catalog cleanup so it holds even if a drop throws. In shared mode this is
        // a no-op, see clearInMemoryIndex.
        clearInMemoryIndex()
        dropSuiteTables()
      }
    })
  }

  /**
   * Per-suite mode: reset the JVM-static INMEMORY index between tests (see the note in test()).
   * Shared mode: the index is keyed per table and dropSuiteTables clears each dropped table's
   * entry, so a global clear would only wipe other suites' tables. A test that drops and re-creates
   * a table at the same path inside one test must clear that path itself
   * (HoodieInMemoryHashIndex.clear(basePath)); tagLocation's commit-time check accepts entries from
   * an earlier table at the same path.
   */
  protected def clearInMemoryIndex(): Unit = {
    if (!sharedSessionEnabled) {
      HoodieInMemoryHashIndex.clear()
    }
  }

  /**
   * Shared mode: scalatest runs each suite on its own thread and Hudi reads SparkSession.active in a few
   * places (HoodieCatalog captures it when the session's catalog is first built, BaseProcedure per CALL),
   * so pin this suite's child session to the test thread and fail fast if the session's HoodieCatalog was
   * built against another session.
   */
  private def bindSuiteSession(): Unit = {
    SparkSession.setActiveSession(spark)
    spark.sessionState.catalogManager.catalog(CatalogManager.SESSION_CATALOG_NAME) match {
      case hoodieCatalog: HoodieCatalog =>
        assert(hoodieCatalog.spark eq spark,
          s"HoodieCatalog of ${getClass.getSimpleName} is bound to another SparkSession")
      case _ =>
    }
  }

  /**
   * Drops the tables a test left behind. Per-suite mode owns the whole catalog. Shared mode shares the
   * external catalog with every other suite in the JVM and those suites may be mid-test, so it drops
   * only the tables whose name carries this suite's generateTableName prefix, plus its own temp views.
   */
  private def dropSuiteTables(): Unit = {
    val catalog = spark.sessionState.catalog
    // Concurrent suites may drop their own databases at any time, so every per-database step is best effort.
    catalog.listDatabases().foreach { db =>
      val tables = Try(catalog.listTables(db)).getOrElse(Seq.empty)
      val toDrop = if (sharedSessionEnabled) tables.filter(table => ownsTable(catalog, table)) else tables
      toDrop.foreach { table =>
        if (sharedSessionEnabled && !catalog.isTempView(table)) {
          // Shared mode keeps one index map per table; drop this table's entry with the table.
          Try(catalog.getTableMetadata(table).location.getPath)
            .foreach(path => HoodieInMemoryHashIndex.clear(path))
        }
        Try(catalog.dropTable(table, true, true))
      }
    }
  }

  /**
   * Shared mode: a table is this suite's if generateTableName produced its name (derived names such as
   * `s"${generateTableName}_pt"` keep the prefix), or if it is a temp view, which is session-scoped.
   */
  private def ownsTable(catalog: SessionCatalog, table: TableIdentifier): Boolean = {
    catalog.isTempView(table) || table.table.startsWith(tableNamePrefix)
  }

  private lazy val tableNamePrefix: String = s"h${getClass.getSimpleName.toLowerCase}_"

  protected def generateTableName: String = {
    s"$tableNamePrefix${tableId.incrementAndGet()}"
  }

  override protected def afterAll(): Unit = {
    if (sharedSessionEnabled) {
      // The context and warehouse outlive the suite; only undo this suite's Hadoop conf overrides.
      if (hadoopConfOverrides.nonEmpty) {
        val hadoopConf = HoodieSparkSqlTestBase.sharedBaseSession().sparkContext.hadoopConfiguration
        hadoopConfOverrides.reverse.foreach {
          case (key, null) => hadoopConf.unset(key)
          case (key, previous) => hadoopConf.set(key, previous)
        }
        hadoopConfOverrides = Seq.empty
      }
    } else {
      Utils.deleteRecursively(sparkWareHouse)
      spark.stop()
    }
  }

  protected def checkAnswer(sql: String)(expects: Seq[Any]*): Unit = {
    HoodieSparkSqlTestBase.checkAnswer(spark, sql)(expects: _*)
  }

  protected def checkAnswer(array: Array[Row])(expects: Seq[Any]*): Unit = {
    assertResult(expects.map(row => Row(row: _*)).toArray)(array)
  }

  /**
   * Analysis-time twin of [[checkNestedExceptionContains(runnable:Runnable)*]]: `spark.sql` is
   * lazy for queries, so this only catches failures raised while the statement is built.
   */
  protected def checkNestedExceptionContains(sql: String)(errorMsg: String): Unit = {
    checkNestedExceptionContains(() => spark.sql(sql))(errorMsg)
  }

  /**
   * The runnable twin of [[checkNestedExceptionContains(sql:String)*]], for failures that only
   * surface at EXECUTION time (e.g. a per-file read guard): `spark.sql` alone is lazy for
   * queries, so the caller collects inside the runnable. Walks the whole cause chain, unlike
   * [[checkExceptionContain(runnable:Runnable)*]], which checks only the top and the root cause.
   */
  protected def checkNestedExceptionContains(runnable: Runnable)(errorMsg: String): Unit = {
    var hasException = false
    try {
      runnable.run()
    } catch {
      case e: Throwable =>
        var t = e
        while (t != null) {
          if (t.getMessage != null && t.getMessage.trim.contains(errorMsg.trim)) {
            hasException = true
          }
          t = t.getCause
        }
        if (!hasException) {
          e.printStackTrace(System.err)
        }
    }
    assertResult(true)(hasException)
  }

  protected def checkExceptions(sql: String)(errorMsgs: Seq[String]): Unit = {
    var hasException = false
    try {
      spark.sql(sql)
    } catch {
      case e: Throwable =>
        assertResult(errorMsgs.contains(e.getMessage.split("\n")(0)))(true)
        hasException = true
    }
    assertResult(true)(hasException)
  }

  protected def checkException(sql: String)(errorMsg: String): Unit = {
    var hasException = false
    try {
      spark.sql(sql)
    } catch {
      case e: Throwable =>
        assertResult(errorMsg.trim)(e.getMessage.trim)
        hasException = true
    }
    assertResult(true)(hasException)
  }

  protected def checkException(runnable: Runnable)(errorMsg: String): Unit = {
    var hasException = false
    try {
      runnable.run()
    } catch {
      case e: Throwable =>
        assertResult(errorMsg.trim)(e.getMessage.trim)
        hasException = true
    }
    assertResult(true)(hasException)
  }

  protected def checkNestedException(sql: String)(errorMsg: String): Unit = {
    var hasException = false
    try {
      spark.sql(sql)
    } catch {
      case e: Throwable =>
        var t = e
        while (t != null) {
          if (errorMsg.trim.equals(t.getMessage.trim)) {
            hasException = true
          }
          t = t.getCause
        }
    }
    assertResult(true)(hasException)
  }

  protected def checkExceptionContain(runnable: Runnable)(errorMsg: String): Unit = {
    var hasException = false
    try {
      runnable.run()
    } catch {
      case e: Throwable if checkMessageContains(e, errorMsg) || checkMessageContains(HoodieTestUtils.getRootCause(e), errorMsg) =>
        hasException = true

      case f: Throwable =>
        fail("Exception should contain: " + errorMsg + ", error message: " + f.getMessage, f)
    }
    assertResult(true)(hasException)
  }

  protected def checkExceptionContain(sql: String)(errorMsg: String): Unit = {
    var hasException = false
    try {
      spark.sql(sql)
    } catch {
      case e: Throwable if checkMessageContains(e, errorMsg) || checkMessageContains(HoodieTestUtils.getRootCause(e), errorMsg) =>
        hasException = true

      case f: Throwable =>
        fail("Exception should contain: " + errorMsg + ", error message: " + f.getMessage, f)
    }
    assertResult(true)(hasException)
  }

  protected def checkExceptionMatch(sql: String)(errorMsgRegex: String): Unit = {
    var hasException = false
    try {
      spark.sql(sql)
    } catch {
      case e: Throwable if HoodieTestUtils.getRootCause(e).getMessage.matches(errorMsgRegex) =>
        hasException = true

      case f: Throwable =>
        fail("Exception should match pattern: " + errorMsgRegex + ", error message: " + HoodieTestUtils.getRootCause(f).getMessage, f)
    }
    assertResult(true)(hasException)
  }

  protected def getExpectedUnresolvedColumnExceptionMessage(columnName: String,
                                                            targetTableName: String): String = {
    val targetTableFields = spark.sql(s"select * from $targetTableName").schema.fields
      .map(e => (e.name, targetTableName, s"spark_catalog.default.$targetTableName.${e.name}"))
    getExpectedUnresolvedColumnExceptionMessage(columnName, targetTableFields)
  }

  protected def getExpectedUnresolvedColumnExceptionMessage(columnName: String,
                                                            fieldNameTuples: Seq[(String, String, String)]): String = {
    val fieldNames = fieldNameTuples.sortBy(e => (e._1, e._2))
      .map(e => e._3).mkString("[", ", ", "]")
    if (HoodieSparkUtils.gteqSpark4_0) {
      "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name " +
        s"$columnName cannot be resolved. Did you mean one of the following? $fieldNames."
    } else if (HoodieSparkUtils.gteqSpark3_5) {
      "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name " +
        s"$columnName cannot be resolved. Did you mean one of the following? $fieldNames."
    } else {
      s"cannot resolve $columnName in MERGE command given columns $fieldNames" +
        (if (HoodieSparkUtils.gteqSpark3_4) "." else "")
    }
  }

  protected def validateTableSchema(tableName: String,
                                    expectedStructFields: List[StructField]): Unit = {
    assertResult(expectedStructFields)(
      spark.sql(s"select * from $tableName").schema.fields
        .filter(e => !HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION.contains(e.name)))
  }

  def dropTypeLiteralPrefix(value: Any): Any = {
    value match {
      case s: String =>
        s.stripPrefix("DATE").stripPrefix("TIMESTAMP").stripPrefix("X")
      case _ => value
    }
  }

  protected def extractRawValue(value: Any): Any = {
    value match {
      case s: String =>
        // We need to strip out data-type prefixes like "DATE", "TIMESTAMP"
        dropTypeLiteralPrefix(s)
          .asInstanceOf[String]
          .stripPrefix("'")
          .stripSuffix("'")
      case _ => value
    }
  }

  protected def existsPath(filePath: String): Boolean = {
    val path = new Path(filePath)
    val fs = HadoopFSUtils.getFs(filePath, spark.sparkContext.hadoopConfiguration)
    fs.exists(path)
  }

  /**
   * Please use this method to set SQL conf in a block and restore them after the block.
   * WARN: Please don't set the SQL conf like `spark.sql("set xxx = yyy")`, replace it with this method.
   */
  protected def withSQLConf[T](pairs: (String, String)*)(f: => T): T = {
    val conf = spark.sessionState.conf
    val currentValues = pairs.unzip._1.map { k =>
      if (conf.contains(k)) {
        Some(conf.getConfString(k))
      } else None
    }
    pairs.foreach { case (k, v) => conf.setConfString(k, v) }
    try f finally {
      pairs.unzip._1.zip(currentValues).foreach {
        case (key, Some(value)) => conf.setConfString(key, value)
        case (key, None) => conf.unsetConf(key)
      }
    }
  }

  protected def withTable(tableName: String)(f: String => Unit): Unit = {
    try {
      f(tableName)
    } finally {
      if (sharedSessionEnabled) {
        val catalog = spark.sessionState.catalog
        val ident = spark.sessionState.sqlParser.parseTableIdentifier(tableName)
        if (catalog.tableExists(ident) && !catalog.isTempView(ident)) {
          Try(catalog.getTableMetadata(ident).location.getPath)
            .foreach(path => HoodieInMemoryHashIndex.clear(path))
        }
      }
      spark.sql(s"drop table if exists $tableName purge")
    }
  }

  protected def withSparkSqlSessionConfig(configNameValues: (String, String)*
                                         )(f: => Unit): Unit = {
    withSparkSqlSessionConfigWithCondition(configNameValues.map(e => (e, true)): _*)(f)
  }

  protected def withSparkSqlSessionConfigWithCondition(configNameValues: ((String, String), Boolean)*
                                                      )(f: => Unit): Unit = {
    try {
      configNameValues.foreach { case ((configName, configValue), condition) =>
        if (condition) {
          spark.sql(s"set $configName=$configValue")
        }
      }
      f
    } finally {
      configNameValues.foreach { case ((configName, configValue), condition) =>
        spark.sql(s"reset $configName")
      }
    }
  }

  protected def withRecordType(recordTypes: Seq[HoodieRecordType] = Seq(HoodieRecordType.AVRO, HoodieRecordType.SPARK),
                               recordConfig: Map[HoodieRecordType, Map[String, String]] = Map.empty)(f: => Unit) {
    // TODO HUDI-5264 Test parquet log with avro record in spark sql test
    recordTypes.foreach { recordType =>
      val (merger, format) = recordType match {
        case HoodieRecordType.SPARK => (classOf[DefaultSparkRecordMerger].getName, "parquet")
        case _ => (classOf[HoodieAvroRecordMerger].getName, "avro")
      }
      val config = Map(
        HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key -> merger,
        HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key -> format) ++ recordConfig.getOrElse(recordType, Map.empty)
      withSQLConf(config.toList: _*) {
        f
        // We need to clear indexed location in memory after each test.
        clearInMemoryIndex()
      }
    }
  }

  /**
   * Wraps test execution with RDD persistence validation.
   * This ensures that no new RDDs remain persisted after test execution.
   *
   * @param f The test code to execute
   */
  protected def withRDDPersistenceValidation(f: => Unit): Unit = {
    org.apache.hudi.testutils.SparkRDDValidationUtils.withRDDPersistenceValidation(spark, new org.apache.hudi.testutils.SparkRDDValidationUtils.ThrowingRunnable {
      override def run(): Unit = f
    })
  }
}

object HoodieSparkSqlTestBase {

  private val LOG = LoggerFactory.getLogger(classOf[HoodieSparkSqlTestBase])

  /**
   * System property forwarded by the scalatest plugin from the pom property of the same name
   * (`-Dhudi.spark.test.sharedSession=true`). When set, every suite in the JVM shares one
   * SparkContext and works in its own `newSession()` child; the context is never stopped by a suite.
   * Default off: each suite builds and stops its own session, as before.
   *
   * Only for runs whose suites all extend HoodieSparkSqlTestBase: a suite that builds its own
   * SparkContext (for example TestHoodieInternalRowUtils) fails with "Only one SparkContext
   * should be running in this JVM" and aborts the run, so CI enables the property only on
   * shards whose packages hold nothing else.
   */
  val SHARED_SESSION_PROPERTY = "hudi.spark.test.sharedSession"
  val sharedSessionEnabled: Boolean = java.lang.Boolean.getBoolean(SHARED_SESSION_PROPERTY)

  // Read side held by every suite for its whole run, write side by an ExclusiveSuite for its whole run.
  val suiteLock = new ReentrantReadWriteLock(true)

  private[common] lazy val sharedWarehouse: File = {
    val dir = Utils.createTempDir()
    Utils.deleteRecursively(dir)
    dir
  }

  @volatile private var sharedBase: SparkSession = _

  /** The JVM-wide base session; rebuilt if something stopped its context. Only its children are handed to suites. */
  private[common] def sharedBaseSession(): SparkSession = synchronized {
    if (sharedBase == null || sharedBase.sparkContext.isStopped) {
      LOG.warn("Shared session mode on ({}=true): one SparkContext for this JVM, a child session per suite",
        SHARED_SESSION_PROPERTY)
      sharedBase = sessionBuilder(sharedWarehouse, getSparkConfForTest("Hoodie SQL Test")).getOrCreate()
    }
    sharedBase
  }

  private[common] def sessionBuilder(warehouse: File, conf: SparkConf): SparkSession.Builder = {
    SparkSession.builder()
      .config("spark.sql.warehouse.dir", warehouse.getCanonicalPath)
      .config("spark.sql.session.timeZone", "UTC")
      .config("hoodie.insert.shuffle.parallelism", "4")
      .config("hoodie.upsert.shuffle.parallelism", "4")
      .config("hoodie.delete.shuffle.parallelism", "4")
      .config(conf)
  }

  // the naming format of 0.x version
  final val NAME_FORMAT_0_X: Pattern = Pattern.compile("^(\\d+)(\\.\\w+)(\\.\\D+)?$")

  def getLastCommitMetadata(spark: SparkSession, tablePath: String) = {
    val metaClient = createMetaClient(spark, tablePath)

    metaClient.getActiveTimeline.getLastCommitMetadataWithValidData.get.getRight
  }

  def getLastReplaceCommitMetadata(spark: SparkSession, tablePath: String): HoodieReplaceCommitMetadata = {
    val metaClient = createMetaClient(spark, tablePath)
    val lastInstant = metaClient.getActiveTimeline.getLastCommitMetadataWithValidData.get.getLeft
    metaClient.getActiveTimeline.readReplaceCommitMetadata(lastInstant)
  }

  def getLastCleanMetadata(spark: SparkSession, tablePath: String) = {
    val metaClient = createMetaClient(spark, tablePath)

    val cleanInstant = metaClient.reloadActiveTimeline().getCleanerTimeline.filterCompletedInstants().lastInstant().get()
    metaClient.getActiveTimeline.readCleanMetadata(cleanInstant)
  }

  def getMetaClientAndFileSystemView(basePath: String):
  (HoodieTableMetaClient, SyncableFileSystemView) = {
    val storageConf = HoodieTestUtils.getDefaultStorageConf
    val metaClient: HoodieTableMetaClient =
      HoodieTableMetaClient.builder.setConf(storageConf).setBasePath(basePath).build
    val metadataConfig = HoodieMetadataConfig.newBuilder.build
    val engineContext = new HoodieLocalEngineContext(storageConf)
    val viewManager: FileSystemViewManager = FileSystemViewManager.createViewManager(
      engineContext, metadataConfig, FileSystemViewStorageConfig.newBuilder.build,
      HoodieCommonConfig.newBuilder.build,
      (_: HoodieTableMetaClient) => {
        metaClient.getTableFormat.getMetadataFactory.create(
          engineContext, metaClient.getStorage, metadataConfig, metaClient.getBasePath.toString)
      }
    )
    val fsView: SyncableFileSystemView = viewManager.getFileSystemView(metaClient)
    (metaClient, fsView)
  }

  /**
   * Replaces the existing file with an empty file which is meant to be corrupted
   * in a Hudi table.
   *
   * @param storage  [[HoodieStorage]] instance
   * @param filePath file path
   */
  def replaceWithEmptyFile(storage: HoodieStorage,
                           filePath: StoragePath): Unit = {
    storage.deleteFile(filePath)
    storage.createNewFile(filePath)
  }

  def checkAnswer(spark: SparkSession, sql: String)(expects: Seq[Any]*): Unit = {
    assertResult(expects.map(row => Row(row: _*)).toArray.sortBy(_.toString()))(spark.sql(sql).collect().sortBy(_.toString()))
  }

  def validateDeleteLogBlockPrecombineNullOrZero(basePath: String): Unit = {
    val (metaClient, fsView) = getMetaClientAndFileSystemView(basePath)
    val fileSlice: Optional[FileSlice] = fsView.getAllFileSlices("").findFirst()
    assertTrue(fileSlice.isPresent)
    // Oldest first. A string sort of the paths orders by the Spark write token, which precedes the
    // instant in the file name and compares stage ids as text.
    val logFilePathList: java.util.List[String] = fileSlice.get.getLogFiles
      .sorted(HoodieLogFile.getLogFileComparator)
      .map[String](logFile => logFile.getPath.toString)
      .collect(Collectors.toList[String])
    var deleteLogBlockFound = false
    val schema = new TableSchemaResolver(metaClient).getTableSchema
    for (i <- 0 until logFilePathList.size()) {
      val logReader = HoodieLogFormat.newReader(
        metaClient, new HoodieLogFile(logFilePathList.get(i)), schema)
      try {
        assertTrue(logReader.hasNext)
        val logBlock = logReader.next()
        if (logBlock.isInstanceOf[HoodieDeleteBlock]) {
          val deleteLogBlock = logBlock.asInstanceOf[HoodieDeleteBlock]
          assertTrue(deleteLogBlock.getRecordsToDelete.forall(i => i.getOrderingValue() == null || i.getOrderingValue().equals(OrderingValues.getDefault)))
          deleteLogBlockFound = true
        }
      } finally {
        logReader.close()
      }
    }
    assertTrue(deleteLogBlockFound)
  }

  def validateTableConfig(storage: HoodieStorage,
                          basePath: String,
                          expectedConfigs: Map[String, String],
                          nonExistentConfigs: Seq[String]): Unit = {
    val tableConfig = HoodieTableConfig.loadFromHoodieProps(storage, basePath)
    expectedConfigs.foreach(e => {
      assertEquals(e._2, tableConfig.getString(e._1),
        s"Table config ${e._1} should be ${e._2} but is ${tableConfig.getString(e._1)}")
    })
    nonExistentConfigs.foreach(e => assertFalse(
      tableConfig.contains(e), s"$e should not be present in the table config"))
  }

  def enableComplexKeygenValidation(spark: SparkSession,
                                    tableName: String): Unit = {
    setComplexKeygenValidation(spark, tableName, value = true)
  }

  def disableComplexKeygenValidation(spark: SparkSession,
                                     tableName: String): Unit = {
    setComplexKeygenValidation(spark, tableName, value = false)
  }

  private def setComplexKeygenValidation(spark: SparkSession,
                                         tableName: String,
                                         value: Boolean): Unit = {
    spark.sql(
      s"""
         |ALTER TABLE $tableName
         |SET TBLPROPERTIES (hoodie.write.complex.keygen.validation.enable = '$value')
         |""".stripMargin)
  }

  private def checkMessageContains(e: Throwable, text: String): Boolean =
    e.getMessage.trim.contains(text.trim)
}
