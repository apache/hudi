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

package org.apache.spark.sql.hudi

import org.apache.hadoop.fs.Path
import org.apache.hudi.{HoodieSparkRecordMerger, HoodieSparkUtils}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.config.HoodieStorageConfig
import org.apache.hudi.common.model.HoodieAvroRecordMerger
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.util.Utils
import org.joda.time.DateTimeZone
import org.scalactic.source
import org.scalatest.{BeforeAndAfterAll, FunSuite, Tag}

import java.io.File
import java.util.TimeZone

class HoodieSparkSqlTestBase extends FunSuite with BeforeAndAfterAll {
  org.apache.log4j.Logger.getRootLogger.setLevel(Level.WARN)

  private lazy val sparkWareHouse = {
    val dir = Utils.createTempDir()
    Utils.deleteRecursively(dir)
    dir
  }

  // NOTE: We have to fix the timezone to make sure all date-/timestamp-bound utilities output
  //       is consistent with the fixtures
  DateTimeZone.setDefault(DateTimeZone.UTC)
  TimeZone.setDefault(DateTimeUtils.getTimeZone("UTC"))
  protected lazy val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("hoodie sql test")
    .withExtensions(new HoodieSparkSessionExtension)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("hoodie.insert.shuffle.parallelism", "4")
    .config("hoodie.upsert.shuffle.parallelism", "4")
    .config("hoodie.delete.shuffle.parallelism", "4")
    .config("spark.sql.warehouse.dir", sparkWareHouse.getCanonicalPath)
    .config("spark.sql.session.timeZone", "UTC")
    .config(sparkConf())
    .getOrCreate()

  private var tableId = 0

  def sparkConf(): SparkConf = {
    val sparkConf = new SparkConf()
    if (HoodieSparkUtils.gteqSpark3_2) {
      sparkConf.set("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
    }
    sparkConf
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
      case e: Throwable if e.getMessage.trim.contains(errorMsg.trim) => hasException = true
      case f: Throwable => fail("Exception should contain: " + errorMsg + ", error message: " + f.getMessage, f)
    }
    assertResult(true)(hasException)
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
    val fs = FSUtils.getFs(filePath, spark.sparkContext.hadoopConfiguration)
    fs.exists(path)
  }

  protected def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
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

  protected def withRecordType(recordConfig: Map[HoodieRecordType, Map[String, String]]=Map.empty)(f: => Unit) {
    // TODO HUDI-5264 Test parquet log with avro record in spark sql test
    Seq(HoodieRecordType.AVRO, HoodieRecordType.SPARK).foreach { recordType =>
      val (merger, format) = recordType match {
        case HoodieRecordType.SPARK => (classOf[HoodieSparkRecordMerger].getName, "parquet")
        case _ => (classOf[HoodieAvroRecordMerger].getName, "avro")
      }
      val config = Map(
        HoodieWriteConfig.MERGER_IMPLS.key -> merger,
        HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key -> format) ++ recordConfig.getOrElse(recordType, Map.empty)
      withSQLConf(config.toList:_*) {
        f
      }
    }
  }

  protected def getRecordType(): HoodieRecordType = {
    val merger = spark.sessionState.conf.getConfString(HoodieWriteConfig.MERGER_IMPLS.key, HoodieWriteConfig.MERGER_IMPLS.defaultValue())
    if (merger.equals(classOf[HoodieSparkRecordMerger].getName)) {
      HoodieRecordType.SPARK
    } else {
      HoodieRecordType.AVRO
    }
  }
}
