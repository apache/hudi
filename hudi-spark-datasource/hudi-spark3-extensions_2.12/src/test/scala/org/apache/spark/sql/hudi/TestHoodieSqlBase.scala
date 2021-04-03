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

package org.apache.spark.sql.hudi

import java.io.File

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.util.Utils
import org.scalactic.source
import org.scalatest.{BeforeAndAfterAll, FunSuite, Tag}

class TestHoodieSqlBase extends FunSuite with BeforeAndAfterAll {
  protected lazy val spark = SparkSession.builder()
    .master("local[1]")
    .appName("hoodie sql test")
    .config("spark.sql.extensions", "io.hudi.sql.HudiSpark3SessionExtension")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.hive.convertMetastoreParquet", false)
    .config("hoodie.datasource.meta.sync.enable", "false")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.default.parallelism", "4")
    .config("set spark.hoodie.shuffle.parallelism", "4")
    .getOrCreate()

  private var tableId = 0

  protected def withTempDir(f: File => Unit): Unit = {
    val tempDir = Utils.createTempDir()
    try f(tempDir) finally {
      Utils.deleteRecursively(tempDir)
    }
  }

  override protected def afterAll(): Unit = {
    spark.stop()
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any /* Assertion */)(implicit pos: source.Position): Unit = {
    try super.test(testName, testTags: _*)(try testFun finally {
      val catalog = spark.sessionState.catalog
      catalog.listDatabases().foreach { db =>
        catalog.listTables(db).foreach { table =>
          catalog.dropTable(table, true, true)
        }
      }
    })
  }

  protected def checkAnswer(sql: String)(expects: Seq[Any]*): Unit = {
    val expectRow = expects.map(row => Row(row: _*)).toArray
    val realRow = spark.sql(sql).collect()
    assertResult(expectRow)(realRow)
  }
}

