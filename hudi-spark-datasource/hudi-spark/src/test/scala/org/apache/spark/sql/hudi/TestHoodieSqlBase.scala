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
import org.apache.hudi.common.fs.FSUtils
import org.apache.log4j.Level
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.util.Utils
import org.scalactic.source
import org.scalatest.{BeforeAndAfterAll, FunSuite, Tag}

import java.io.File
import java.util.TimeZone

class TestHoodieSqlBase extends FunSuite with BeforeAndAfterAll {
  org.apache.log4j.Logger.getRootLogger.setLevel(Level.WARN)

  private lazy val sparkWareHouse = {
    val dir = Utils.createTempDir()
    Utils.deleteRecursively(dir)
    dir
  }

  TimeZone.setDefault(DateTimeUtils.getTimeZone("CTT"))
  protected lazy val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("hoodie sql test")
    .withExtensions(new HoodieSparkSessionExtension)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("hoodie.insert.shuffle.parallelism", "4")
    .config("hoodie.upsert.shuffle.parallelism", "4")
    .config("hoodie.delete.shuffle.parallelism", "4")
    .config("spark.sql.warehouse.dir", sparkWareHouse.getCanonicalPath)
    .config("spark.sql.session.timeZone", "CTT")
    .getOrCreate()

  private var tableId = 0

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

  protected def checkException(sql: String)(errorMsg: String): Unit = {
    var hasException = false
    try {
      spark.sql(sql)
    } catch {
      case e: Throwable =>
        assertResult(errorMsg)(e.getMessage)
        hasException = true
    }
    assertResult(true)(hasException)
  }

  protected def checkExceptionContain(sql: String)(errorMsg: String): Unit = {
    var hasException = false
    try {
      spark.sql(sql)
    } catch {
      case e: Throwable =>
        assertResult(true)(e.getMessage.contains(errorMsg))
        hasException = true
    }
    assertResult(true)(hasException)
  }


  protected def removeQuotes(value: Any): Any = {
    value match {
      case s: String => s.stripPrefix("'").stripSuffix("'")
      case _=> value
    }
  }

  protected def existsPath(filePath: String): Boolean = {
    val path = new Path(filePath)
    val fs = FSUtils.getFs(filePath, spark.sparkContext.hadoopConfiguration)
    fs.exists(path)
  }
}
