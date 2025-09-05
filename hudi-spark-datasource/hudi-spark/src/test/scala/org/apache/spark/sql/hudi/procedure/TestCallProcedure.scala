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
        s"Table name or table path must be given one")

      // collect commits for table
      val commits = spark.sql(s"""call show_commits(table => '$tableName', limit => 10)""").collect()
      assertResult(2) {
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
      assertResult(3) {
        commits.length
      }

      spark.table(s"$tableName").select("id").cache()
      assertCached(spark.table(s"$tableName").select("id"), 1)

      // Call rollback_to_instant Procedure with Named Arguments
      var instant_time = commits(0).get(0).toString
      checkAnswer(s"""call rollback_to_instant(table => '$tableName', instant_time => '$instant_time')""")(Seq(true))
      // Check cache whether invalidate
      assertCached(spark.table(s"$tableName").select("id"), 0)
    }
  }

  test("Test Call show_rollbacks Procedure - validate schema and values") {
    withTempDir { tmp =>
      val tableName = generateTableName
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
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")
      spark.sql(s"insert into $tableName select 3, 'a3', 30, 2000")
      val commits = spark.sql(s"""call show_commits(table => '$tableName', limit => 10)""").collect()
      assertResult(3) {
        commits.length
      }
      val commitToRollbackTo = commits(0).getString(0)
      checkAnswer(s"""call rollback_to_instant(table => '$tableName', instant_time => '$commitToRollbackTo')""")(Seq(true))
      checkExceptionContain(s"""call show_rollbacks(limit => 10)""")(
        s"Table name or table path must be given one")
      val rollbacks = spark.sql(s"""call show_rollbacks(table => '$tableName', limit => 10)""")
      val rollbackData = rollbacks.collect()
      assert(rollbackData.length >= 1, "Should have at least one rollback operation")
      assert(rollbacks.schema.fields.length == 14, "show_rollbacks should have 14 fields")
      val schema = rollbacks.schema
      assert(schema.fieldNames.contains("rollback_time"))
      assert(schema.fieldNames.contains("state_transition_time"))
      assert(schema.fieldNames.contains("state"))
      assert(schema.fieldNames.contains("action"))
      assert(schema.fieldNames.contains("start_rollback_time"))
      assert(schema.fieldNames.contains("partition_path"))
      assert(schema.fieldNames.contains("rollback_instant"))
      assert(schema.fieldNames.contains("deleted_file"))
      assert(schema.fieldNames.contains("succeeded"))
      assert(schema.fieldNames.contains("total_files_deleted"))
      assert(schema.fieldNames.contains("time_taken_in_millis"))
      assert(schema.fieldNames.contains("total_partitions"))
      assert(schema.fieldNames.contains("version"))
      val completedRollbacks = rollbackData.filter(_.getString(2) == "COMPLETED")
      assert(completedRollbacks.length >= 1, "Should have at least one completed rollback")
      completedRollbacks.foreach { completedRollback =>
        assert(completedRollback.getString(0) != null)
        assert(completedRollback.getString(1) != null)
        assert(completedRollback.getString(2) == "COMPLETED")
        assert(completedRollback.getString(3) == "rollback")
        assert(completedRollback.getString(5) != null)
        assert(completedRollback.getInt(10) >= 0)
        assert(completedRollback.getLong(11) >= 0)
        assert(completedRollback.getInt(12) >= 0)
        assert(completedRollback.getInt(13) >= 1)
      }
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
    }
  }

  test("Test Call show_rollbacks Procedure - with total files deleted filter") {
    withTempDir { tmp =>
      val tableName = generateTableName
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

      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")
      spark.sql(s"insert into $tableName select 3, 'a3', 30, 2000")

      val commits = spark.sql(s"""call show_commits(table => '$tableName', limit => 10)""").collect()
      assertResult(3){commits.length}

      var instant_time = commits(0).get(0).toString
      spark.sql(s"""call rollback_to_instant(table => '$tableName', instant_time => '$instant_time')""")

      instant_time = commits(1).get(0).toString
      spark.sql(s"""call rollback_to_instant(table => '$tableName', instant_time => '$instant_time')""")

      val allRollbacks = spark.sql(s"""call show_rollbacks(table => '$tableName', limit => 10)""").collect()
      assertResult(2) {
        allRollbacks.length
      }

      val filteredRollbacksDf = spark.sql(
        s"""call show_rollbacks(
           |  table => '$tableName',
           |  limit => 10,
           |  filter => "total_files_deleted >= 0"
           |)""".stripMargin)
      filteredRollbacksDf.show(false)
      val filteredRollbacks = filteredRollbacksDf.collect()

      assert(filteredRollbacks.length >= 1, s"Should find at least 1 rollback with files deleted >= 0, got: ${filteredRollbacks.length}")
      filteredRollbacks.foreach { row =>
        assert(row.getInt(10) >= 0, s"total_files_deleted should be >= 0, got: ${row.getInt(10)}")
      }

      val specificDeleteFilter = spark.sql(
        s"""call show_rollbacks(
           |  table => '$tableName',
           |  limit => 10,
           |  filter => "total_files_deleted = 1"
           |)""".stripMargin).collect()

      specificDeleteFilter.foreach { row =>
        assert(row.getInt(10) == 1, s"total_files_deleted should be 1, got: ${row.getInt(10)}")
      }
    }
  }
}
