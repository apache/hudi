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

package org.apache.spark.sql.hudi.procedure

import org.apache.spark.sql.Row

import java.io.File

class TestExportInstantsProcedure extends HoodieSparkProcedureTestBase {

  private def createCowTable(tableName: String, path: String): Unit = {
    spark.sql(
      s"""
         |create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  ts long
         |) using hudi
         | location '$path'
         | tblproperties (
         |  primaryKey = 'id',
         |  orderingFields = 'ts'
         | )
       """.stripMargin)
  }

  private def newExportDir(tmp: File, name: String): File = {
    val dir = new File(tmp, name)
    assert(dir.mkdirs(), s"Failed to create export dir $dir")
    dir
  }

  private def exportedCount(result: Array[Row]): Int = {
    assertResult(1)(result.length)
    val detail = result.head.getString(0)
    val matched = "Exported (\\d+) Instants".r.findFirstMatchIn(detail)
    assert(matched.isDefined, s"Unexpected export detail: $detail")
    matched.get.group(1).toInt
  }

  test("Test Call export_instants Procedure") {
    withTempDir { tmp =>
      val tableName = generateTableName
      createCowTable(tableName, s"${tmp.getCanonicalPath}/$tableName")

      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")

      val exportDir = newExportDir(tmp, "export_basic")
      val result = spark.sql(
        s"""call export_instants(table => '$tableName', local_folder => '${exportDir.getCanonicalPath}')""").collect()

      // A single insert produces one exportable commit instant that is written to disk.
      assertResult(1)(exportedCount(result))
      assertResult(1)(exportDir.listFiles().count(_.getName.endsWith(".commit")))
    }
  }

  test("Test Call export_instants Procedure with desc ordering") {
    withTempDir { tmp =>
      val tableName = generateTableName
      createCowTable(tableName, s"${tmp.getCanonicalPath}/$tableName")

      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 2000")
      spark.sql(s"insert into $tableName select 3, 'a3', 30, 3000")

      val exportDir = newExportDir(tmp, "export_desc")
      val result = spark.sql(
        s"""call export_instants(table => '$tableName',
           | local_folder => '${exportDir.getCanonicalPath}', desc => true)""".stripMargin).collect()

      // The desc branch reverses the active instants and exports all three commits to disk.
      assertResult(3)(exportedCount(result))
      assertResult(3)(exportDir.listFiles().count(_.getName.endsWith(".commit")))
    }
  }

  test("Test Call export_instants Procedure filters by action") {
    withTempDir { tmp =>
      val tableName = generateTableName
      createCowTable(tableName, s"${tmp.getCanonicalPath}/$tableName")

      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 2000")

      // Restricting to an action that is not present exports nothing.
      val cleanDir = newExportDir(tmp, "export_clean_only")
      val cleanResult = spark.sql(
        s"""call export_instants(table => '$tableName',
           | local_folder => '${cleanDir.getCanonicalPath}', actions => 'clean')""".stripMargin).collect()
      assertResult(0)(exportedCount(cleanResult))
      assertResult(0)(cleanDir.listFiles().count(_.getName.endsWith(".clean")))

      // Restricting to the commit action exports exactly the commit instants.
      val commitDir = newExportDir(tmp, "export_commit_only")
      val commitResult = spark.sql(
        s"""call export_instants(table => '$tableName',
           | local_folder => '${commitDir.getCanonicalPath}', actions => 'commit')""".stripMargin).collect()
      assertResult(2)(exportedCount(commitResult))
      assertResult(2)(commitDir.listFiles().count(_.getName.endsWith(".commit")))
    }
  }

  test("Test Call export_instants Procedure with an invalid local folder") {
    withTempDir { tmp =>
      val tableName = generateTableName
      createCowTable(tableName, s"${tmp.getCanonicalPath}/$tableName")
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")

      val notADir = new File(tmp, "does_not_exist").getCanonicalPath
      checkExceptionContain(
        s"""call export_instants(table => '$tableName', local_folder => '$notADir')""")(
        "is not a valid local directory")
    }
  }
}
