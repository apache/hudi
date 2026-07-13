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

import org.apache.hudi.common.model.IOType
import org.apache.hudi.common.testutils.FileCreateUtilsLegacy

class TestCallProcedure extends HoodieSparkProcedureTestBase {

  test("Test Call show_commits Procedure") {
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

      // Check required fields
      checkExceptionContain(s"""call show_commits(limit => 10)""")(
        s"Argument: table is required")

      // collect commits for table
      val commits = spark.sql(s"""call show_commits(table => '$tableName', limit => 10)""").collect()
      assertResult(2) {
        commits.length
      }
    }
  }

  test("Test Call show_commits_metadata Procedure") {
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

      // Check required fields
      checkExceptionContain(s"""call show_commits_metadata(limit => 10)""")(
        s"Argument: table is required")

      // collect commits for table
      val commits = spark.sql(s"""call show_commits_metadata(table => '$tableName', limit => 10)""").collect()
      assertResult(1) {
        commits.length
      }
    }
  }

  test("Test Call rollback_to_instant Procedure") {
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

      // Check required fields
      checkExceptionContain(s"""call rollback_to_instant(table => '$tableName')""")(
        s"Argument: instant_time is required")

      // 3 commits are left before rollback
      var commits = spark.sql(s"""call show_commits(table => '$tableName', limit => 10)""").collect()
      assertResult(3){commits.length}

      // Call rollback_to_instant Procedure with Named Arguments
      var instant_time = commits(0).get(0).toString
      checkAnswer(s"""call rollback_to_instant(table => '$tableName', instant_time => '$instant_time')""")(Seq(true))
      // Call rollback_to_instant Procedure with Positional Arguments
      instant_time = commits(1).get(0).toString
      checkAnswer(s"""call rollback_to_instant('$tableName', '$instant_time')""")(Seq(true))

      // 1 commits are left after rollback
      commits = spark.sql(s"""call show_commits(table => '$tableName', limit => 10)""").collect()
      assertResult(1){commits.length}
    }
  }

  test("Test Call rollback_to_instant Procedure with refreshTable") {
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

      // 3 commits are left before rollback
      var commits = spark.sql(s"""call show_commits(table => '$tableName', limit => 10)""").collect()
      assertResult(3){commits.length}

      spark.table(s"$tableName").select("id").count
      spark.table(s"$tableName").select("id").cache()
      assertCached(spark.table(s"$tableName").select("id"), 1)

      // Call rollback_to_instant Procedure with Named Arguments
      var instant_time = commits(0).get(0).toString
      checkAnswer(s"""call rollback_to_instant(table => '$tableName', instant_time => '$instant_time')""")(Seq(true))
      // Check cache whether invalidate
      assertCached(spark.table(s"$tableName").select("id"), 0)
    }
  }

  test("Test Call delete_marker Procedure") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '$tablePath'
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)

      // Check required fields
      checkExceptionContain(s"""call delete_marker(table => '$tableName')""")(
        s"Argument: instant_time is required")

      val instantTime = "101"
      FileCreateUtilsLegacy.createMarkerFile(tablePath, "", instantTime, "f0", IOType.APPEND)
      assertResult(1) {
        FileCreateUtilsLegacy.getTotalMarkerFileCount(tablePath, "", instantTime, IOType.APPEND)
      }

      checkAnswer(s"""call delete_marker(table => '$tableName', instant_time => '$instantTime')""")(Seq(true))

      assertResult(0) {
        FileCreateUtilsLegacy.getTotalMarkerFileCount(tablePath, "", instantTime, IOType.APPEND)
      }
    }
  }

  test("Test Call delete_marker Procedure with batch mode") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '$tablePath'
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)

      // Check required fields
      checkExceptionContain(s"""call delete_marker(table => '$tableName')""")(
        s"Argument: instant_time is required")

      var instantTime = "101"
      FileCreateUtilsLegacy.createMarkerFile(tablePath, "", instantTime, "f0", IOType.APPEND)
      assertResult(1) {
        FileCreateUtilsLegacy.getTotalMarkerFileCount(tablePath, "", instantTime, IOType.APPEND)
      }
      instantTime = "102"
      FileCreateUtilsLegacy.createMarkerFile(tablePath, "", instantTime, "f0", IOType.APPEND)
      assertResult(1) {
        FileCreateUtilsLegacy.getTotalMarkerFileCount(tablePath, "", instantTime, IOType.APPEND)
      }

      instantTime = "101,102"
      checkAnswer(s"""call delete_marker(table => '$tableName', instant_time => '$instantTime')""")(Seq(true))

      assertResult(0) {
        FileCreateUtilsLegacy.getTotalMarkerFileCount(tablePath, "", instantTime, IOType.APPEND)
      }
    }
  }

  test("Test Call show_rollbacks Procedure") {
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

      // 3 commits are left before rollback
      var commits = spark.sql(s"""call show_commits(table => '$tableName', limit => 10)""").collect()
      assertResult(3){commits.length}

      // Call rollback_to_instant Procedure with Named Arguments
      var instant_time = commits(0).get(0).toString
      checkAnswer(s"""call rollback_to_instant(table => '$tableName', instant_time => '$instant_time')""")(Seq(true))
      // Call rollback_to_instant Procedure with Positional Arguments
      instant_time = commits(1).get(0).toString
      checkAnswer(s"""call rollback_to_instant('$tableName', '$instant_time')""")(Seq(true))

      // 1 commits are left after rollback
      commits = spark.sql(s"""call show_commits(table => '$tableName', limit => 10)""").collect()
      assertResult(1){commits.length}

      // collect rollbacks for table
      val rollbacks = spark.sql(s"""call show_rollbacks(table => '$tableName', limit => 10)""").collect()
      assertResult(2) {rollbacks.length}
    }
  }

  test("Test Call show_rollback_detail Procedure") {
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

      // 3 commits are left before rollback
      var commits = spark.sql(s"""call show_commits(table => '$tableName', limit => 10)""").collect()
      assertResult(3) {
        commits.length
      }

      // Call rollback_to_instant Procedure with Named Arguments
      var instant_time = commits(0).get(0).toString
      checkAnswer(s"""call rollback_to_instant(table => '$tableName', instant_time => '$instant_time')""")(Seq(true))
      // Call rollback_to_instant Procedure with Positional Arguments
      instant_time = commits(1).get(0).toString
      checkAnswer(s"""call rollback_to_instant('$tableName', '$instant_time')""")(Seq(true))

      // 1 commits are left after rollback
      commits = spark.sql(s"""call show_commits(table => '$tableName', limit => 10)""").collect()
      assertResult(1) {
        commits.length
      }

      // collect rollbacks for table
      val rollbacks = spark.sql(s"""call show_rollbacks(table => '$tableName', limit => 10)""").collect()
      assertResult(2) {
        rollbacks.length
      }

      // Check required fields
      checkExceptionContain(s"""call show_rollback_detail(table => '$tableName')""")(
        s"Argument: instant_time is required")

      // collect rollback's info for table
      instant_time = rollbacks(1).get(0).toString
      val rollback = spark.sql(s"""call show_rollback_detail(table => '$tableName', instant_time => '$instant_time')""").collect()
      assertResult(1) {
        rollback.length
      }
    }
  }
}
