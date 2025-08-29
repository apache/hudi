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

class TestShowTimelineProcedure extends HoodieSparkSqlTestBase {

  test("Test show_timeline procedure") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tableLocation = tmp.getCanonicalPath
      if (HoodieSparkUtils.isSpark3_4) {
        spark.sql("set spark.sql.defaultColumn.enabled = false")
      }
      spark.sql(
        s"""
           |create table $tableName (
           | id int,
           | name string,
           | price double,
           | ts long
           |) using hudi
           | location '$tableLocation'
           | tblproperties (
           |   primaryKey = 'id',
           |   type = 'cow',
           |   preCombineField = 'ts'
           | )
           |""".stripMargin)

      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
      spark.sql(s"insert into $tableName values(2, 'a2', 20, 2000)")
      spark.sql(s"update $tableName set price = 15 where id = 1")

      val timelineResult = spark.sql(s"call show_timeline(table => '$tableName')").collect()

      assert(timelineResult.length == 3, "Should have 3 timeline entries (2 inserts + 1 update)")

      val firstRow = timelineResult.head
      assert(firstRow.length == 8, "Should have 8 columns in result")

      assert(firstRow.getString(0) != null, "instant_time should not be null")
      assert(firstRow.getString(1) != null, "action should not be null")
      assert(firstRow.getString(2) != null, "state should not be null")
      assert(firstRow.getString(6) != null, "timeline_type should not be null")

      timelineResult.foreach { row =>
        assert(row.getString(6) == "ACTIVE", s"Timeline type should be ACTIVE, got: ${row.getString(6)}")
      }

      val actions = timelineResult.map(_.getString(1)).distinct
      assert(actions.contains("commit"), "Should have commit actions in timeline")
    }
  }

  test("Test show_timeline procedure - MoR table with deltacommit") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tableLocation = tmp.getCanonicalPath
      if (HoodieSparkUtils.isSpark3_4) {
        spark.sql("set spark.sql.defaultColumn.enabled = false")
      }
      spark.sql(
        s"""
           |create table $tableName (
           | id int,
           | name string,
           | price double,
           | ts long
           |) using hudi
           | location '$tableLocation'
           | tblproperties (
           |   primaryKey = 'id',
           |   type = 'mor',
           |   preCombineField = 'ts'
           | )
           |""".stripMargin)

      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
      spark.sql(s"insert into $tableName values(2, 'a2', 20, 2000)")

      val timelineResult = spark.sql(s"call show_timeline(table => '$tableName')").collect()

      assert(timelineResult.length == 2, "Should have 2 timeline entries")

      val actions = timelineResult.map(_.getString(1)).distinct
      assert(actions.contains("deltacommit"), "Should have deltacommit actions in MoR table")
    }
  }

  test("Test show_timeline procedure - rollback operations") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tableLocation = tmp.getCanonicalPath
      if (HoodieSparkUtils.isSpark3_4) {
        spark.sql("set spark.sql.defaultColumn.enabled = false")
      }
      spark.sql(
        s"""
           |create table $tableName (
           | id int,
           | name string,
           | price double,
           | ts long
           |) using hudi
           | location '$tableLocation'
           | tblproperties (
           |   primaryKey = 'id',
           |   type = 'mor',
           |   preCombineField = 'ts'
           | )
           |""".stripMargin)

      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
      spark.sql(s"insert into $tableName values(2, 'a2', 20, 2000)")

      val timelineBeforeRollbackDf = spark.sql(s"call show_timeline(table => '$tableName')")
      timelineBeforeRollbackDf.show(false)
      val timelineBeforeRollback = timelineBeforeRollbackDf.collect()
      assert(timelineBeforeRollback.length == 2, "Should have at least 2 timeline entries")

      val firstCompletedInstant = timelineBeforeRollback.find(_.getString(2) == "COMPLETED")
      assert(firstCompletedInstant.isDefined, "Should have at least one completed instant")

      val instantTimeToRollback = firstCompletedInstant.get.getString(0)

      spark.sql(s"call rollback_to_instant(table => '$tableName', instant_time => '$instantTimeToRollback')")

      val timelineResultDf = spark.sql(s"call show_timeline(table => '$tableName', showArchived => true)")
      timelineResultDf.show(false)
      val timelineResult = timelineResultDf.collect()

      assert(timelineResult.length == 2, "Should have rollback and previous deltacommit instance")

      val actions = timelineResult.map(_.getString(1)).distinct
      assert(actions.contains("rollback"), "Should have rollback actions in timeline")

      val rollbackRows = timelineResult.filter(_.getString(1) == "rollback")
      rollbackRows.foreach { row =>
        assert(row.getString(7) != null, "rollback_info should not be null for rollback operations")
        assert(row.getString(7).contains("Rolled back"), "rollback_info should contain rollback information")
      }
    }
  }

  test("Test show_timeline procedure - pending commit with null completed time") {
    withSQLConf("hoodie.compact.inline" -> "false", "hoodie.parquet.max.file.size" -> "10000") {
      withTempDir { tmp =>
        val tableName = generateTableName
        val tableLocation = tmp.getCanonicalPath
        if (HoodieSparkUtils.isSpark3_4) {
          spark.sql("set spark.sql.defaultColumn.enabled = false")
        }
        spark.sql(
          s"""
             |create table $tableName (
             | id int,
             | name string,
             | price double,
             | ts long
             |) using hudi
             | location '$tableLocation'
             | tblproperties (
             |   primaryKey = 'id',
             |   type = 'mor',
             |   preCombineField = 'ts'
             | )
             |""".stripMargin)

        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"insert into $tableName values(2, 'a2', 20, 2000)")
        spark.sql(s"insert into $tableName values(3, 'a2', 20, 2000)")
        spark.sql(s"insert into $tableName values(4, 'a2', 20, 2000)")
        spark.sql(s"insert into $tableName values(5, 'a2', 20, 2000)")
        spark.sql(s"insert into $tableName values(6, 'a2', 20, 2000)")
        spark.sql(s"insert into $tableName values(7, 'a2', 20, 2000)")
        spark.sql(s"insert into $tableName values(8, 'a2', 20, 2000)")
        spark.sql(s"insert into $tableName values(9, 'a2', 20, 2000)")
        spark.sql(s"insert into $tableName values(10, 'a2', 20, 2000)")
        spark.sql(s"update $tableName set price = 15 where id = 1")
        spark.sql(s"update $tableName set price = 30 where id = 1")
        spark.sql(s"update $tableName set price = 30 where id = 2")
        spark.sql(s"update $tableName set price = 30 where id = 3")

        spark.sql(s"call run_compaction(table => '$tableName', op => 'schedule')")
        spark.sql(s"call show_compaction(table => '$tableName')").show(false)

        val timelineResultDf = spark.sql(s"call show_timeline(table => '$tableName')")
        timelineResultDf.show(false)
        val timelineResult = timelineResultDf.collect()

        assert(timelineResult.length == 15, "Should have timeline entries including scheduled pending compaction")

        val pendingRows = timelineResult.filter(_.getString(2) == "REQUESTED")
        assert(pendingRows.length == 1, "Should have 1 requested compaction operations")
        if (pendingRows.nonEmpty) {
          pendingRows.foreach { row =>
            assert(row.getString(5) == null, "completed_time should be null for REQUESTED state")
            assert(row.getString(4) == null, "inflight_time should be null for REQUESTED state")
            assert(row.getString(3) != null, "requested_time should not be null")
            assert(row.getString(1) == "compaction", "REQUESTED state should be for compaction action")
          }
        }
        val completedRows = timelineResult.filter(_.getString(2) == "COMPLETED")
        assert(completedRows.length == 14, "Should have 14 deltacommit completed operations")
        if (completedRows.nonEmpty) {
          completedRows.foreach { row =>
            assert(row.getString(5) != null, "completed_time should not be null for COMPLETED state")
            assert(row.getString(4) != null, "inflight_time should not be null for COMPLETED state")
          }
        }
      }
    }
  }
}
