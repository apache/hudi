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

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.hudi.HoodieSparkUtils

import org.apache.spark.SparkConf
import org.apache.spark.util.Utils

import java.io.File

class TestSpark3Catalog extends HoodieSparkSqlTestBase {

  val tempDir: File = Utils.createTempDir()
  val url = s"jdbc:h2:${tempDir.getCanonicalPath};user=testUser;password=testPass"

  override def sparkConf(): SparkConf = {
    val sparkConf = super.sparkConf()
    if (HoodieSparkUtils.gteqSpark3_1) {
      sparkConf
        .set("spark.sql.catalog.h2", "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog")
        .set("spark.sql.catalog.h2.url", url)
        .set("spark.sql.catalog.h2.driver", "org.h2.Driver")
    }
    sparkConf
  }

  private def withConnection[T](f: Connection => T): T = {
    val conn = DriverManager.getConnection(url, new Properties())
    try {
      f(conn)
    } finally {
      conn.close()
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    Utils.classForName("org.h2.Driver")
    withConnection { conn =>
      conn.prepareStatement("""CREATE SCHEMA "test"""").executeUpdate()
      conn.prepareStatement(
        """CREATE TABLE "test"."people" (id INTEGER NOT NULL, country TEXT(32) NOT NULL)""")
        .executeUpdate()
      conn.prepareStatement(
        """INSERT INTO "test"."people" VALUES (1, 'US')""")
        .executeUpdate()
    }
  }

  override def afterAll(): Unit = {
    Utils.deleteRecursively(tempDir)
    super.afterAll()
  }

  test("Test Read And Write Cross Multi Spark Catalog") {
    if (HoodieSparkUtils.gteqSpark3_1) {
      checkAnswer("SHOW TABLES IN h2.test")(Seq("test", "people", false))
      // Read table from other spark catalog
      checkAnswer("SELECT * FROM h2.test.people")(Seq(1, "US"))

      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  country string,
           |  ts long
           |) using hudi
           |tblproperties (
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           |)
       """.stripMargin)

      spark.sql(
        s"""
           |insert into $tableName values (2, "China", 1000), (3, "India", 1000)
           |""".stripMargin)

      // Write to table of other spark catalog using hudi from the current spark catalog
      spark.sql(
        s"""
           |insert into h2.test.people
           |select id, country
           |from spark_catalog.default.$tableName
           |""".stripMargin)

      checkAnswer("SELECT * FROM h2.test.people order by id")(
        Seq(1, "US"),
        Seq(2, "China"),
        Seq(3, "India")
      )
    }
  }
}
