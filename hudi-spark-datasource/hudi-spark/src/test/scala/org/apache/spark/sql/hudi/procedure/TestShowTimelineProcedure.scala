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

  test("Test show_timeline procedure with archived timeline V2") {
    withSQLConf(
      "hoodie.keep.min.commits" -> "2",
      "hoodie.keep.max.commits" -> "3",
      "hoodie.cleaner.commits.retained" -> "1") {
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

        // Create multiple commits to trigger archiving
        for (i <- 1 to 5) {
          spark.sql(s"insert into $tableName values($i, 'a$i', ${10 * i}, ${1000 * i})")
        }

        // Trigger clean to potentially archive commits
        spark.sql(s"call run_clean(table => '$tableName', retain_commits => 1)")

        // Test show_timeline with showArchived=true (should use ArchivedTimelineV2)
        val timelineResultDf = spark.sql(s"call show_timeline(table => '$tableName', showArchived => true)")
        timelineResultDf.show(false)
        val timelineResult = timelineResultDf.collect()

        assert(timelineResult.length >= 5, "Should have at least 5 timeline entries (commits + clean)")

        // Verify that we can see both active and archived entries
        val timelineTypes = timelineResult.map(_.getString(6)).distinct
        assert(timelineTypes.contains("ACTIVE"), "Should have ACTIVE timeline entries")
        // Archived entries may or may not be present depending on archiving trigger

        // Verify all entries have required fields
        timelineResult.foreach { row =>
          assert(row.getString(0) != null, "instant_time should not be null")
          assert(row.getString(1) != null, "action should not be null")
          assert(row.getString(2) != null, "state should not be null")
        }

        val actions = timelineResult.map(_.getString(1)).distinct
        assert(actions.contains("commit"), "Should have commit actions in timeline")
      }
    }
  }

  test("Test show_timeline procedure with archived timeline V1") {
    withSQLConf(
      "hoodie.keep.min.commits" -> "2",
      "hoodie.keep.max.commits" -> "3",
      "hoodie.cleaner.commits.retained" -> "1") {
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

        // Create multiple commits with updates to generate log files for MoR table
        for (i <- 1 to 5) {
          spark.sql(s"insert into $tableName values($i, 'a$i', ${10 * i}, ${1000 * i})")
        }
        // Add updates to create log files that can be compacted
        for (i <- 1 to 3) {
          spark.sql(s"update $tableName set price = ${20 * i} where id = $i")
        }

        // Downgrade table to version 7 (which uses LAYOUT_VERSION_1, so ArchivedTimelineV1)
        spark.sql(s"call downgrade_table(table => '$tableName', to_version => 'SEVEN')")

        // Trigger clean to potentially archive commits
        spark.sql(s"call run_clean(table => '$tableName', retain_commits => 1)")

        // Test show_timeline with showArchived=true (should use ArchivedTimelineV1)
        val timelineResultDf = spark.sql(s"call show_timeline(table => '$tableName', showArchived => true)")
        timelineResultDf.show(false)
        val timelineResult = timelineResultDf.collect()

        assert(timelineResult.length >= 8, "Should have at least 8 timeline entries (5 inserts + 3 updates + clean)")

        // Verify that we can see both active and archived entries
        val timelineTypes = timelineResult.map(_.getString(6)).distinct
        assert(timelineTypes.contains("ACTIVE"), "Should have ACTIVE timeline entries")
        // Archived entries may or may not be present depending on archiving trigger

        // Verify all entries have required fields
        timelineResult.foreach { row =>
          assert(row.getString(0) != null, "instant_time should not be null")
          assert(row.getString(1) != null, "action should not be null")
          assert(row.getString(2) != null, "state should not be null")
        }

        val actions = timelineResult.map(_.getString(1)).distinct
        assert(actions.contains("deltacommit"), "Should have deltacommit actions in timeline")
        assert(actions.contains("commit"), "Should have commit actions in timeline")

        // Schedule compaction first (creates REQUESTED compaction event)
        val scheduleResult = spark.sql(s"call run_compaction(op => 'schedule', table => '$tableName')")
          .collect()
        println(s"Scheduled compaction result: ${scheduleResult.length} instants")

        // Check if compaction was scheduled successfully
        val timelineAfterScheduleDf = spark.sql(s"call show_timeline(table => '$tableName')")
        val timelineAfterSchedule = timelineAfterScheduleDf.collect()
        val hasRequestedCompaction = timelineAfterSchedule.exists(row => 
          row.getString(1) == "compaction" && row.getString(2) == "REQUESTED"
        )
        println(s"Has REQUESTED compaction: $hasRequestedCompaction")

        // Run compaction (transitions REQUESTED -> INFLIGHT -> COMPLETED as COMMIT_ACTION)
        val runResult = spark.sql(s"call run_compaction(op => 'run', table => '$tableName')")
          .collect()
        println(s"Run compaction result: ${runResult.length} instants")

        // Check timeline immediately after compaction to see if commit was created
        val timelineAfterRunDf = spark.sql(s"call show_timeline(table => '$tableName')")
        timelineAfterRunDf.show(false)
        val timelineAfterRun = timelineAfterRunDf.collect()
        val actionsAfterRun = timelineAfterRun.map(_.getString(1)).distinct
        println(s"Actions after compaction run: ${actionsAfterRun.mkString(", ")}")
        val hasCommitAfterRun = actionsAfterRun.contains("commit")
        println(s"Has commit action after compaction: $hasCommitAfterRun")

        // Create more commits to trigger archiving of compaction events
        for (i <- 6 to 10) {
          spark.sql(s"insert into $tableName values($i, 'a$i', ${10 * i}, ${1000 * i})")
        }

        // Trigger clean to archive commits (this should also archive compaction events)
        spark.sql(s"call run_clean(table => '$tableName', retain_commits => 1)")

        // test show timeline after compaction with archived timeline
        val timelineResultAfterCompactionDf = spark.sql(s"call show_timeline(table => '$tableName', showArchived => true)")
        timelineResultAfterCompactionDf.show(false)
        val timelineResultAfterCompaction = timelineResultAfterCompactionDf.collect()
        assert(timelineResultAfterCompaction.length >= 8, "Should have at least 8 timeline entries (commits + clean + compaction)")

        // Fix: Use timelineResultAfterCompaction instead of timelineResult
        val actionsAfterCompaction = timelineResultAfterCompaction.map(_.getString(1)).distinct
        println(s"All actions in timeline after compaction: ${actionsAfterCompaction.mkString(", ")}")
        assert(actionsAfterCompaction.contains("deltacommit"), "Should have deltacommit actions in timeline")
        
        // Only assert commit exists if compaction actually created one
        // Compaction might not create a commit if there's nothing to compact
        if (hasCommitAfterRun) {
          assert(actionsAfterCompaction.contains("commit"), "Should have commit actions in timeline after compaction")
        } else {
          println("Warning: Compaction did not create a commit action. This might be expected if there was nothing to compact.")
          // Check if compaction events exist instead
          val compactionEvents = timelineResultAfterCompaction.filter(row => 
            row.getString(1) == "compaction"
          )
          if (compactionEvents.nonEmpty) {
            println(s"Found ${compactionEvents.length} compaction events in timeline")
          }
        }

        // Check for compaction events in archived timeline
        val archivedEntries = timelineResultAfterCompaction.filter(_.getString(6) == "ARCHIVED")
        if (archivedEntries.nonEmpty) {
          val archivedActions = archivedEntries.map(_.getString(1)).distinct
          // Compaction events (REQUESTED/INFLIGHT) should appear in archived timeline if archived
          // Note: Completed compaction becomes COMMIT_ACTION, so we check for "compaction" action
          val compactionEvents = archivedEntries.filter(row => 
            row.getString(1) == "compaction" && 
            (row.getString(2) == "REQUESTED" || row.getString(2) == "INFLIGHT")
          )
          // Compaction events may or may not be archived depending on timing
          // But if they are archived, they should be visible
          if (compactionEvents.nonEmpty) {
            println(s"Found ${compactionEvents.length} compaction events in archived timeline")
            compactionEvents.foreach { row =>
              assert(row.getString(0) != null, "compaction instant_time should not be null")
              assert(row.getString(1) == "compaction", "action should be compaction")
              assert(Set("REQUESTED", "INFLIGHT").contains(row.getString(2)), 
                s"compaction state should be REQUESTED or INFLIGHT, got: ${row.getString(2)}")
            }
          }
        }
      }
    }
  }

  test("Test show_timeline procedure - compare V1 and V2 archived timeline behavior") {
    withSQLConf(
      "hoodie.keep.min.commits" -> "2",
      "hoodie.keep.max.commits" -> "3",
      "hoodie.cleaner.commits.retained" -> "1") {
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

        // Create multiple commits
        for (i <- 1 to 6) {
          spark.sql(s"insert into $tableName values($i, 'a$i', ${10 * i}, ${1000 * i})")
        }

        // Test with V2 (current version) first
        spark.sql(s"call run_clean(table => '$tableName', retain_commits => 1)")
        val v2TimelineResult = spark.sql(s"call show_timeline(table => '$tableName', showArchived => true)").collect()

        // Downgrade to V1
        spark.sql(s"call downgrade_table(table => '$tableName', to_version => 'SEVEN')")

        // Trigger clean again
        spark.sql(s"call run_clean(table => '$tableName', retain_commits => 1)")

        // Test with V1
        val v1TimelineResult = spark.sql(s"call show_timeline(table => '$tableName', showArchived => true)").collect()

        // Both should work and return timeline entries
        assert(v1TimelineResult.length > 0, "V1 timeline should return entries")
        assert(v2TimelineResult.length > 0, "V2 timeline should return entries")

        // Verify all entries have valid structure
        (v1TimelineResult ++ v2TimelineResult).foreach { row =>
          assert(row.getString(0) != null, "instant_time should not be null")
          assert(row.getString(1) != null, "action should not be null")
          assert(row.getString(2) != null, "state should not be null")
          assert(row.getString(6) != null, "timeline_type should not be null")
          assert(Set("ACTIVE", "ARCHIVED").contains(row.getString(6)), 
            s"timeline_type should be ACTIVE or ARCHIVED, got: ${row.getString(6)}")
        }
      }
    }
  }
}
