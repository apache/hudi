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

import org.apache.hudi.HoodieSparkUtils

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

class TestCopyToTempViewProcedure extends HoodieSparkSqlTestBase {


  test("Test Call copy_to_temp_view Procedure with default params") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)

      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")
      spark.sql(s"insert into $tableName select 3, 'a3', 30, 2000")
      spark.sql(s"insert into $tableName select 4, 'a4', 40, 2500")

      // Check required fields
      checkExceptionContain(s"call copy_to_temp_view(table=>'$tableName')")(s"Argument: view_name is required")

      val viewName = generateTableName

      val row = spark.sql(s"""call copy_to_temp_view(table=>'$tableName',view_name=>'$viewName')""").collectAsList()
      assert(row.size() == 1 && row.get(0).get(0) == 0)
      val copyTableCount = spark.sql(s"""select count(1) from $viewName""").collectAsList()
      assert(copyTableCount.size() == 1 && copyTableCount.get(0).get(0) == 4)
    }
  }

  test("Test Call copy_to_temp_view Procedure with replace params") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)

      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")
      spark.sql(s"insert into $tableName select 3, 'a3', 30, 2000")
      spark.sql(s"insert into $tableName select 4, 'a4', 40, 2500")

      // Check required fields
      checkExceptionContain(s"call copy_to_temp_view(table=>'$tableName')")(s"Argument: view_name is required")

      // 1: copyToTempView
      val viewName = generateTableName
      val row = spark.sql(s"""call copy_to_temp_view(table=>'$tableName',view_name=>'$viewName')""").collectAsList()
      assert(row.size() == 1 && row.get(0).get(0) == 0)
      val copyTableCount = spark.sql(s"""select count(1) from $viewName""").collectAsList()
      assert(copyTableCount.size() == 1 && copyTableCount.get(0).get(0) == 4)

      // 2: add new record to hudi table
      spark.sql(s"insert into $tableName select 5, 'a5', 40, 2500")

      // 3: copyToTempView with replace=false
      val viewExistsErrorMsg = if (HoodieSparkUtils.gteqSpark3_4) {
        s"[TEMP_TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create the temporary view `$viewName` because it already exists."
      } else {
        s"Temporary view '$viewName' already exists"
      }
      checkExceptionContain(s"""call copy_to_temp_view(table=>'$tableName',view_name=>'$viewName',replace=>false)""")(viewExistsErrorMsg)
      // 4: copyToTempView with replace=true
      val row2 = spark.sql(s"""call copy_to_temp_view(table=>'$tableName',view_name=>'$viewName',replace=>true)""").collectAsList()
      assert(row2.size() == 1 && row2.get(0).get(0) == 0)
      // 5: query new replace view ,count=5
      val newViewCount = spark.sql(s"""select count(1) from $viewName""").collectAsList()
      assert(newViewCount.size() == 1 && newViewCount.get(0).get(0) == 5)
    }
  }

  test("Test Call copy_to_temp_view Procedure with global params") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)

      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")
      spark.sql(s"insert into $tableName select 3, 'a3', 30, 2000")
      spark.sql(s"insert into $tableName select 4, 'a4', 40, 2500")

      // Check required fields
      checkExceptionContain(s"call copy_to_temp_view(table=>'$tableName')")(s"Argument: view_name is required")

      // 1: copyToTempView with global=false
      val viewName = generateTableName
      val row = spark.sql(s"""call copy_to_temp_view(table=>'$tableName',view_name=>'$viewName',global=>false)""").collectAsList()
      assert(row.size() == 1 && row.get(0).get(0) == 0)
      val copyTableCount = spark.sql(s"""select count(1) from $viewName""").collectAsList()
      assert(copyTableCount.size() == 1 && copyTableCount.get(0).get(0) == 4)

      // 2: query view in other session
      var newSession = spark.newSession()
      var hasException = false
      val errorMsg = if (HoodieSparkUtils.gteqSpark3_4) {
        s"[TABLE_OR_VIEW_NOT_FOUND] The table or view `$viewName` cannot be found."
      } else {
        s"Table or view not found: $viewName"
      }

      try {
        newSession.sql(s"""select count(1) from $viewName""")
      } catch {
        case e: Throwable if e.getMessage.contains(errorMsg) => hasException = true
        case f: Throwable => fail("Exception should contain: " + errorMsg + ", error message: " + f.getMessage, f)
      }
      assertResult(true)(hasException)
      // 3: copyToTempView with global=true,
      val row2 = spark.sql(s"""call copy_to_temp_view(table=>'$tableName',view_name=>'$viewName',global=>true,replace=>true)""").collectAsList()
      assert(row2.size() == 1 && row2.get(0).get(0) == 0)

      newSession = spark.newSession()
      // 4: query view in other session
      val newViewCount = spark.sql(s"""select count(1) from $viewName""").collectAsList()
      assert(newViewCount.size() == 1 && newViewCount.get(0).get(0) == 4)

    }
  }
}
