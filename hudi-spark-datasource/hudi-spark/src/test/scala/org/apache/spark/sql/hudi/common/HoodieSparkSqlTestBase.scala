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

import org.apache.hudi.{HoodieSparkRecordMerger, HoodieSparkUtils}
import org.apache.hudi.common.config.{HoodieCommonConfig, HoodieMetadataConfig, HoodieStorageConfig}
import org.apache.hudi.common.engine.HoodieLocalEngineContext
import org.apache.hudi.common.model.{HoodieAvroRecordMerger, HoodieRecord}
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.view.{FileSystemViewManager, FileSystemViewStorageConfig, SyncableFileSystemView}
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.ExceptionUtil.getRootCause
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.index.inmemory.HoodieInMemoryHashIndex
import org.apache.hudi.metadata.HoodieTableMetadata
import org.apache.hudi.storage.{HoodieStorage, StoragePath}
import org.apache.hudi.testutils.HoodieClientTestUtils.{createMetaClient, getSparkConfForTest}
import org.apache.hudi.util.JFunction

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase.checkMessageContains
import org.apache.spark.sql.types.StructField
import org.apache.spark.util.Utils
import org.joda.time.DateTimeZone
import org.scalactic.source
import org.scalatest.{BeforeAndAfterAll, FunSuite, Tag}

import java.io.File
import java.util.TimeZone

class HoodieSparkSqlTestBase extends FunSuite with BeforeAndAfterAll {
  org.apache.log4j.Logger.getRootLogger.setLevel(org.apache.log4j.Level.WARN)

  private lazy val sparkWareHouse = {
    val dir = Utils.createTempDir()
    Utils.deleteRecursively(dir)
    dir
  }

  protected def getTableStoragePath(tableName: String): String = {
    new File(sparkWareHouse, tableName).getCanonicalPath
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

  private var tableId = 0

  def sparkConf(): SparkConf = {
    getSparkConfForTest("Hoodie SQL Test")
  }

  protected def withTempDir(f: File => Unit): Unit = {
    val tempDir = Utils.createTempDir()
    try f(tempDir) finally {
      Utils.deleteRecursively(tempDir)
    }
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any /* Assertion */)(implicit pos: source.Position): Unit = {
    super.test(testName, testTags: _*)(
      try {
        testFun
      } finally {
        val catalog = spark.sessionState.catalog
        catalog.listDatabases().foreach{db =>
          catalog.listTables(db).foreach {table =>
            catalog.dropTable(table, true, true)
          }
        }
      }
    )
  }

  protected def generateTableName: String = {
    val name = s"h$tableId"
    tableId = tableId + 1
    name
  }

  override protected def afterAll(): Unit = {
    Utils.deleteRecursively(sparkWareHouse)
    spark.stop()
  }

  protected def checkAnswer(sql: String)(expects: Seq[Any]*): Unit = {
    assertResult(expects.map(row => Row(row: _*)).toArray.sortBy(_.toString()))(spark.sql(sql).collect().sortBy(_.toString()))
  }

  protected def checkAnswer(array: Array[Row])(expects: Seq[Any]*): Unit = {
    assertResult(expects.map(row => Row(row: _*)).toArray)(array)
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
    pairs.foreach { case(k, v) => conf.setConfString(k, v) }
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
      spark.sql(s"drop table if exists $tableName")
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
      if (HoodieSparkUtils.gteqSpark3_1) {
        // Spark 3.0.x does not support "RESET configuration_key"
        configNameValues.foreach { case ((configName, configValue), condition) =>
          spark.sql(s"reset $configName")
        }
      }
    }
  }

  protected def withRecordType(recordTypes: Seq[HoodieRecordType] = Seq(HoodieRecordType.AVRO, HoodieRecordType.SPARK),
                               recordConfig: Map[HoodieRecordType, Map[String, String]]=Map.empty)(f: => Unit) {
    // TODO HUDI-5264 Test parquet log with avro record in spark sql test
    recordTypes.foreach { recordType =>
      val (merger, format) = recordType match {
        case HoodieRecordType.SPARK => (classOf[HoodieSparkRecordMerger].getName, "parquet")
        case _ => (classOf[HoodieAvroRecordMerger].getName, "avro")
      }
      val config = Map(
        HoodieWriteConfig.RECORD_MERGER_IMPLS.key -> merger,
        HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key -> format) ++ recordConfig.getOrElse(recordType, Map.empty)
      withSQLConf(config.toList:_*) {
        f
        // We need to clear indexed location in memory after each test.
        HoodieInMemoryHashIndex.clear()
      }
    }
  }

  protected def getRecordType(): HoodieRecordType = {
    val merger = spark.sessionState.conf.getConfString(HoodieWriteConfig.RECORD_MERGER_IMPLS.key, HoodieWriteConfig.RECORD_MERGER_IMPLS.defaultValue())
    if (merger.equals(classOf[HoodieSparkRecordMerger].getName)) {
      HoodieRecordType.SPARK
    } else {
      HoodieRecordType.AVRO
    }
  }
}

object HoodieSparkSqlTestBase {

  def getLastCommitMetadata(spark: SparkSession, tablePath: String) = {
    val metaClient = createMetaClient(spark, tablePath)

    metaClient.getActiveTimeline.getLastCommitMetadataWithValidData.get.getRight
  }

  def getLastCleanMetadata(spark: SparkSession, tablePath: String) = {
    val metaClient = createMetaClient(spark, tablePath)

    val cleanInstant = metaClient.reloadActiveTimeline().getCleanerTimeline.filterCompletedInstants().lastInstant().get()
    TimelineMetadataUtils.deserializeHoodieCleanMetadata(metaClient
      .getActiveTimeline.getInstantDetails(cleanInstant).get)
  }

  def getMetaClientAndFileSystemView(basePath: String):
  (HoodieTableMetaClient, SyncableFileSystemView) = {
    val storageConf = HoodieTestUtils.getDefaultStorageConf
    val metaClient: HoodieTableMetaClient =
      HoodieTableMetaClient.builder.setConf(storageConf).setBasePath(basePath).build
    val metadataConfig = HoodieMetadataConfig.newBuilder.build
    val engineContext = new HoodieLocalEngineContext(storageConf)
    val viewManager: FileSystemViewManager = FileSystemViewManager.createViewManager(
      engineContext, FileSystemViewStorageConfig.newBuilder.build,
      HoodieCommonConfig.newBuilder.build,
      JFunction.toJavaSerializableFunctionUnchecked(
      (_: HoodieTableMetaClient) => {
        HoodieTableMetadata.create(
          engineContext, metaClient.getStorage, metadataConfig, metaClient.getBasePath.toString)
      })
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

  private def checkMessageContains(e: Throwable, text: String): Boolean =
    e.getMessage.trim.contains(text.trim)

}
