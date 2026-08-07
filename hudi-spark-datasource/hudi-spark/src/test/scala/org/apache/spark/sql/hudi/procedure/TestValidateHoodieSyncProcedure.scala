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

/**
 * Tests for [[org.apache.spark.sql.hudi.command.procedures.ValidateHoodieSyncProcedure]].
 *
 * The "complete" / "latestPartitions" modes require a live Hive/JDBC endpoint, which is not
 * available in the unit-test environment. Passing any other mode short-circuits the record
 * counting (record counts stay 0) while still exercising the timeline comparison, the
 * catch-up-commit computation and the result formatting, which is the bulk of the procedure.
 */
class TestValidateHoodieSyncProcedure extends HoodieSparkProcedureTestBase {

  private def createTable(tableName: String, path: String): Unit = {
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

  test("Test Call sync_validate when the target table is ahead (catch-up commits)") {
    withTempDir { tmp =>
      val srcTable = generateTableName
      val dstTable = generateTableName
      createTable(srcTable, s"${tmp.getCanonicalPath}/$srcTable")
      // The source table has a single, earlier commit.
      spark.sql(s"insert into $srcTable select 1, 'a1', 10, 1000")

      createTable(dstTable, s"${tmp.getCanonicalPath}/$dstTable")
      // The destination table receives later commits, so it is ahead of the source.
      spark.sql(s"insert into $dstTable select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $dstTable select 2, 'a2', 20, 2000")

      val result = spark.sql(
        s"""call sync_validate(src_table => '$srcTable', dst_table => '$dstTable',
           | mode => 'noop', hive_server_url => 'jdbc:hive2://unused', hive_pass => 'x')"""
          .stripMargin).collect()

      assertResult(1)(result.length)
      val message = result.head.getString(0)
      assert(message.startsWith("Count difference now is"),
        s"Unexpected message: $message")
      // The destination has commits after the source's latest, so a catch-up count is reported.
      assert(message.contains("Catach up count is"), s"Unexpected message: $message")
    }
  }

  test("Test Call sync_validate when both tables point at the same timeline (no catch-up)") {
    withTempDir { tmp =>
      val tableName = generateTableName
      createTable(tableName, s"${tmp.getCanonicalPath}/$tableName")
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")

      // Using the same table as both source and target means neither timeline is ahead,
      // so no catch-up commits are found and only the count difference is reported.
      val result = spark.sql(
        s"""call sync_validate(src_table => '$tableName', dst_table => '$tableName',
           | mode => 'noop', hive_server_url => 'jdbc:hive2://unused', hive_pass => 'x')"""
          .stripMargin).collect()

      assertResult(1)(result.length)
      val message = result.head.getString(0)
      assert(message.startsWith("Count difference now is"), s"Unexpected message: $message")
      assert(message.contains("== 0"), s"Unexpected message: $message")
      assert(!message.contains("Catach up count is"), s"Unexpected message: $message")
    }
  }

  test("Test Call sync_validate requires its mandatory arguments") {
    withTempDir { tmp =>
      val tableName = generateTableName
      createTable(tableName, s"${tmp.getCanonicalPath}/$tableName")
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")

      checkExceptionContain(
        s"call sync_validate(src_table => '$tableName', dst_table => '$tableName', mode => 'noop')")(
        "Argument: hive_server_url is required")
    }
  }
}
