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
import org.apache.hudi.common.model.{FileSlice, HoodieAvroRecordMerger, HoodieLogFile, HoodieRecord}
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.log.HoodieLogFileReader
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock
import org.apache.hudi.common.table.view.{FileSystemViewManager, FileSystemViewStorageConfig, SyncableFileSystemView}
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.common.util.OrderingValues
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.ExceptionUtil.getRootCause
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.index.inmemory.HoodieInMemoryHashIndex
import org.apache.hudi.storage.{HoodieStorage, StoragePath}
import org.apache.hudi.testutils.HoodieClientTestUtils.{createMetaClient, getSparkConfForTest}

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase.checkMessageContains
import org.apache.spark.sql.types.StructField
import org.apache.spark.util.Utils
import org.joda.time.DateTimeZone
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.scalactic.source
import org.scalatest.{BeforeAndAfterAll, FunSuite, Tag}
import org.scalatest.Assertions.assertResult
import org.slf4j.LoggerFactory

import java.io.File
import java.util.{Collections, Optional, TimeZone}
import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern

class HoodieSparkSqlTestBase extends FunSuite with BeforeAndAfterAll {
  org.apache.log4j.Logger.getRootLogger.setLevel(org.apache.log4j.Level.WARN)
  private val LOG = LoggerFactory.getLogger(getClass)

  private lazy val sparkWareHouse = {
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
  protected lazy val spark: SparkSession = SparkSession.builder()
    .config("spark.sql.warehouse.dir", sparkWareHouse.getCanonicalPath)
    .config("spark.sql.session.timeZone", "UTC")
    .config("hoodie.insert.shuffle.parallelism", "4")
    .config("hoodie.upsert.shuffle.parallelism", "4")
    .config("hoodie.delete.shuffle.parallelism", "4")
    .config(sparkConf())
    .getOrCreate()

  private var tableId = new AtomicInteger(0)

  private var extraConf = Map[String, String]()

  def sparkConf(): SparkConf = {
    val conf = getSparkConfForTest("Hoodie SQL Test")
    conf.setAll(extraConf)
    conf
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

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any /* Assertion */)(implicit pos: source.Position): Unit = {
    super.test(testName, testTags: _*)(
      try {
        testFun
      } finally {
        val catalog = spark.sessionState.catalog
        catalog.listDatabases().foreach { db =>
          catalog.listTables(db).foreach { table =>
            catalog.dropTable(table, true, true)
          }
        }
      }
    )
  }

  protected def generateTableName: String = {
    s"h${tableId.incrementAndGet()}"
  }

  override protected def afterAll(): Unit = {
    Utils.deleteRecursively(sparkWareHouse)
    spark.stop()
  }

  protected def checkAnswer(sql: String)(expects: Seq[Any]*): Unit = {
    HoodieSparkSqlTestBase.checkAnswer(spark, sql)(expects: _*)
  }

  protected def checkAnswer(array: Array[Row])(expects: Seq[Any]*): Unit = {
    assertResult(expects.map(row => Row(row: _*)).toArray)(array)
  }

  protected def checkNestedExceptionContains(sql: String)(errorMsg: String): Unit = {
    var hasException = false
    try {
      spark.sql(sql)
    } catch {
      case e: Throwable =>
        var t = e
        while (t != null) {
          if (t.getMessage.trim.contains(errorMsg.trim)) {
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
      case e: Throwable if checkMessageContains(e, errorMsg) || checkMessageContains(getRootCause(e), errorMsg) =>
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
      case e: Throwable if checkMessageContains(e, errorMsg) || checkMessageContains(getRootCause(e), errorMsg) =>
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
      case e: Throwable if getRootCause(e).getMessage.matches(errorMsgRegex) =>
        hasException = true

      case f: Throwable =>
        fail("Exception should match pattern: " + errorMsgRegex + ", error message: " + getRootCause(f).getMessage, f)
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
    if (HoodieSparkUtils.gteqSpark3_5) {
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
        HoodieInMemoryHashIndex.clear()
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

  // the naming format of 0.x version
  final val NAME_FORMAT_0_X: Pattern = Pattern.compile("^(\\d+)(\\.\\w+)(\\.\\D+)?$")

  def getLastCommitMetadata(spark: SparkSession, tablePath: String) = {
    val metaClient = createMetaClient(spark, tablePath)

    metaClient.getActiveTimeline.getLastCommitMetadataWithValidData.get.getRight
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
    val logFilePathList: java.util.List[String] = HoodieTestUtils.getLogFileListFromFileSlice(fileSlice.get)
    Collections.sort(logFilePathList)
    var deleteLogBlockFound = false
    val avroSchema = new TableSchemaResolver(metaClient).getTableAvroSchema
    for (i <- 0 until logFilePathList.size()) {
      val logReader = new HoodieLogFileReader(
        metaClient.getStorage, new HoodieLogFile(logFilePathList.get(i)),
        avroSchema, 1024 * 1024, false, false,
        "id", null)
      assertTrue(logReader.hasNext)
      val logBlock = logReader.next()
      if (logBlock.isInstanceOf[HoodieDeleteBlock]) {
        val deleteLogBlock = logBlock.asInstanceOf[HoodieDeleteBlock]
        assertTrue(deleteLogBlock.getRecordsToDelete.forall(i => i.getOrderingValue().equals(OrderingValues.getDefault) || i.getOrderingValue() == null))
        deleteLogBlockFound = true
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
