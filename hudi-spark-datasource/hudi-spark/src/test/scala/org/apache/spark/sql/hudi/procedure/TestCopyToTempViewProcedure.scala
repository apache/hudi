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

      // 4: the global view is visible from another session through the global_temp database
      newSession = spark.newSession()
      val newViewCount = newSession.sql(s"""select count(1) from global_temp.$viewName""").collectAsList()
      assert(newViewCount.size() == 1 && newViewCount.get(0).get(0) == 4)

      // 5: copyToTempView with global=true and replace=false goes through createGlobalTempView
      val globalViewName = generateTableName
      val row3 = spark.sql(s"""call copy_to_temp_view(table=>'$tableName',view_name=>'$globalViewName',global=>true,replace=>false)""").collectAsList()
      assert(row3.size() == 1 && row3.get(0).get(0) == 0)
      val globalViewCount = spark.newSession().sql(s"""select count(1) from global_temp.$globalViewName""").collectAsList()
      assert(globalViewCount.size() == 1 && globalViewCount.get(0).get(0) == 4)

      // 6: re-creating the same global view without replace fails
      val globalViewExistsErrorMsg = if (HoodieSparkUtils.gteqSpark3_4) {
        s"[TEMP_TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create the temporary view `$globalViewName` because it already exists."
      } else {
        s"Temporary view '$globalViewName' already exists"
      }
      checkExceptionContain(s"""call copy_to_temp_view(table=>'$tableName',view_name=>'$globalViewName',global=>true,replace=>false)""")(globalViewExistsErrorMsg)
    }
  }

  test("Test Call copy_to_temp_view Procedure with incremental, read_optimized and as_of_instant") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // MOR so the read-optimized view diverges from the snapshot once an update lands in a log file.
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
           |  type = 'mor',
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)

      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 2000")

      val commits = spark.sql(s"select distinct _hoodie_commit_time from $tableName order by _hoodie_commit_time")
        .collect().map(_.getString(0))
      assert(commits.length == 2)

      // Incremental query type requires begin/end instant times.
      val incViewName = generateTableName
      checkExceptionContain(
        s"call copy_to_temp_view(table => '$tableName', view_name => '$incViewName', query_type => 'incremental')")(
        "begin_instance_time and end_instance_time can not be null")

      // Incremental reads filter on *completion* time, start exclusive and end inclusive
      // (RangeType.OPEN_CLOSED). begin => '000' sits below every completion time, so it drops
      // nothing; end_instance_time is the last commit's *requested* time, which is below that
      // commit's own completion time, so the last commit is excluded and only the first commit's
      // record (id = 1) survives.
      val incResult = spark.sql(
        s"""call copy_to_temp_view(table => '$tableName', view_name => '$incViewName',
           | query_type => 'incremental', begin_instance_time => '000', end_instance_time => '${commits.last}')"""
          .stripMargin).collectAsList()
      assert(incResult.size() == 1 && incResult.get(0).get(0) == 0)
      val incRows = spark.sql(s"select id from $incViewName").collectAsList()
      assert(incRows.size() == 1 && incRows.get(0).getInt(0) == 1)

      // The update goes to a log file, so the read-optimized view (base files only) still sees the
      // pre-update price where a snapshot view would see 99.
      spark.sql(s"update $tableName set price = 99 where id = 1")
      val roViewName = generateTableName
      val roResult = spark.sql(
        s"""call copy_to_temp_view(table => '$tableName', view_name => '$roViewName',
           | query_type => 'read_optimized')""".stripMargin).collectAsList()
      assert(roResult.size() == 1 && roResult.get(0).get(0) == 0)
      val roRows = spark.sql(s"select price from $roViewName where id = 1").collectAsList()
      assert(roRows.size() == 1 && roRows.get(0).getDouble(0) == 10.0)

      // The snapshot at the first commit contains only the first record.
      val asOfViewName = generateTableName
      val asOfResult = spark.sql(
        s"""call copy_to_temp_view(table => '$tableName', view_name => '$asOfViewName',
           | as_of_instant => '${commits.head}')""".stripMargin).collectAsList()
      assert(asOfResult.size() == 1 && asOfResult.get(0).get(0) == 0)
      val asOfRows = spark.sql(s"select id from $asOfViewName").collectAsList()
      assert(asOfRows.size() == 1 && asOfRows.get(0).getInt(0) == 1)
    }
  }
}
