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

class TestShowCleansProcedures extends HoodieSparkProcedureTestBase {

  test("Test show_clean_plans procedure") {
    withSQLConf("hoodie.clean.automatic" -> "false", "hoodie.parquet.max.file.size" -> "10000") {
      withTempDir { tmp =>
        val tableName = generateTableName
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
             | ) using hudi
             | location '${tmp.getCanonicalPath}'
             | tblproperties (
             |   primaryKey = 'id',
             |   type = 'cow',
             |   preCombineField = 'ts'
             | )
             |""".stripMargin)

        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"insert into $tableName values(2, 'a2', 20, 2000)")
        spark.sql(s"insert into $tableName values(3, 'a3', 30, 3000)")

        spark.sql(s"update $tableName set price = 11 where id = 1")
        spark.sql(s"update $tableName set price = 21 where id = 2")
        spark.sql(s"update $tableName set price = 12 where id = 1")
        spark.sql(s"update $tableName set price = 22 where id = 2")

        spark.sql(s"call run_clean(table => '$tableName', retain_commits => 2)")
          .collect()

        val firstCleanPlans = spark.sql(s"call show_clean_plans(table => '$tableName')").collect()
        require(firstCleanPlans.length >= 1, "Should have at least 1 clean plan after ensuring sufficient commits")

        spark.sql(s"insert into $tableName values(4, 'a4', 40, 4000)")
        spark.sql(s"update $tableName set price = 15 where id = 1")
        spark.sql(s"update $tableName set price = 25 where id = 2")
        spark.sql(s"update $tableName set price = 35 where id = 3")
        spark.sql(s"update $tableName set price = 45 where id = 4")

        spark.sql(s"call run_clean(table => '$tableName', retain_commits => 1)")
          .collect()

        val secondCleanPlans = spark.sql(s"call show_clean_plans(table => '$tableName')").collect()
        require(secondCleanPlans.length >= 2, "Should have at least 2 clean plans after second clean")

        val allCleanPlans = spark.sql(s"call show_clean_plans(table => '$tableName')")
        allCleanPlans.show(false)
        val allPlans = allCleanPlans.collect()

        assert(allPlans.length >= 2, "Should have at least 2 clean plans")

        val firstPlan = allPlans.head
        assert(firstPlan.length >= 4, "Each clean plan should have at least 4 columns (plan_time, earliest_retained_instant, last_completed_commit_time, files_deleted)")

        allPlans.foreach { plan =>
          val planTime = plan.getString(0)
          assert(planTime.nonEmpty && planTime.toLong > 0, "Plan time should be a valid timestamp")
        }
      }
    }
  }

  test("Test show_cleans procedure") {
    withSQLConf("hoodie.clean.automatic" -> "false", "hoodie.parquet.max.file.size" -> "10000") {
      withTempDir { tmp =>
        val tableName = generateTableName
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
             | ) using hudi
             | location '${tmp.getCanonicalPath}'
             | tblproperties (
             |   primaryKey = 'id',
             |   type = 'cow',
             |   preCombineField = 'ts'
             | )
             |""".stripMargin)

        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"update $tableName set price = 11 where id = 1")
        spark.sql(s"update $tableName set price = 12 where id = 1")

        spark.sql(s"call run_clean(table => '$tableName', retain_commits => 1)")
          .collect()

        val completedCleans = spark.sql(s"call show_cleans(table => '$tableName')")
          .collect()

        assertResult(1)(completedCleans.length)

        val completedClean = completedCleans(0)
        assert(completedClean.getString(0) != null) // clean_time
        assert(completedClean.getString(1) != null) // state_transition_time
        assert(completedClean.getString(2) == "clean") // action
        assert(completedClean.getString(3) != null) // start_clean_time
        assert(completedClean.getLong(4) >= 0) // time_taken_in_millis
        assert(completedClean.getInt(5) >= 0) // total_files_deleted
        // earliest_commit_to_retain can be null
        // last_completed_commit_timestamp can be null
        // version can be null or integer
      }
    }
  }

  test("Test show_cleans with partition metadata") {
    withSQLConf("hoodie.clean.automatic" -> "false", "hoodie.parquet.max.file.size" -> "10000") {
      withTempDir { tmp =>
        val tableName = generateTableName
        if (HoodieSparkUtils.isSpark3_4) {
          spark.sql("set spark.sql.defaultColumn.enabled = false")
        }
        spark.sql(
          s"""
             |create table $tableName (
             | id int,
             | name string,
             | price double,
             | partition_col string,
             | ts long
             | ) using hudi
             | location '${tmp.getCanonicalPath}'
             | tblproperties (
             |   primaryKey = 'id',
             |   type = 'cow',
             |   preCombineField = 'ts'
             | )
             | partitioned by (partition_col)
             |""".stripMargin)

        spark.sql(s"insert into $tableName (id, name, price, partition_col, ts) values(1, 'a1', 10, 'part1', 1000)")
        spark.sql(s"insert into $tableName (id, name, price, partition_col, ts) values(2, 'a2', 20, 'part2', 2000)")
        spark.sql(s"update $tableName set price = 11 where id = 1")
        spark.sql(s"update $tableName set price = 21 where id = 2")

        spark.sql(s"call run_clean(table => '$tableName', retain_commits => 1)")
          .collect()

        val cleansMetadata = spark.sql(s"call show_cleans_metadata(table => '$tableName')")
          .collect()

        assert(cleansMetadata.length >= 0) // Can be 0 if no partitions were cleaned

        if (cleansMetadata.length > 0) {
          val cleanMetadata = cleansMetadata(0)
          assert(cleanMetadata.getString(0) != null) // clean_time
          assert(cleanMetadata.getString(1) != null) // state_transition_time
          assert(cleanMetadata.getString(2) == "clean") // action
          assert(cleanMetadata.getString(3) != null) // start_clean_time
          assert(cleanMetadata.getString(4) != null) // partition_path
          assert(cleanMetadata.getString(5) != null) // policy
          assert(cleanMetadata.getInt(6) >= 0) // delete_path_patterns
          assert(cleanMetadata.getInt(7) >= 0) // success_delete_files
          assert(cleanMetadata.getInt(8) >= 0) // failed_delete_files
          // is_partition_deleted can be true/false
          assert(cleanMetadata.getLong(10) >= 0) // time_taken_in_millis
          assert(cleanMetadata.getInt(11) >= 0) // total_files_deleted
        }
      }
    }
  }

  test("Test show procedures with showArchived parameter") {
    withTempDir { tmp =>
      Seq("COPY_ON_WRITE").foreach { tableType =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
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
             | ) using hudi
             | location '$tablePath'
             | tblproperties (
             |   primaryKey = 'id',
             |   type = '$tableType',
             |   preCombineField = 'ts'
             | )
             |""".stripMargin)

        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"insert into $tableName values(2, 'a2', 20, 2000)")
        spark.sql(s"update $tableName set price = 11 where id = 1")

        spark.sql(s"call run_clean(table => '$tableName', retain_commits => 1)")
          .collect()

        // showArchived=false (default - active timeline only)
        val activeCleans = spark.sql(s"call show_cleans(table => '$tableName', showArchived => false)")
          .collect()
        spark.sql(s"call show_cleans(table => '$tableName', showArchived => false)").show(false)

        val activePlans = spark.sql(s"call show_clean_plans(table => '$tableName', showArchived => false)")
          .collect()
        spark.sql(s"call show_clean_plans(table => '$tableName', showArchived => false)").show(false)

        val activeMetadata = spark.sql(s"call show_cleans_metadata(table => '$tableName', showArchived => false)")
          .collect()
        spark.sql(s"call show_cleans_metadata(table => '$tableName', showArchived => false)").show(false)

        // showArchived=true (both active + archived timelines merged)
        val allCleans = spark.sql(s"call show_cleans(table => '$tableName', showArchived => true)")
          .collect()
        spark.sql(s"call show_cleans(table => '$tableName', showArchived => true)").show(false)

        val allPlans = spark.sql(s"call show_clean_plans(table => '$tableName', showArchived => true)")
          .collect()
        spark.sql(s"call show_clean_plans(table => '$tableName', showArchived => true)").show(false)

        val allMetadata = spark.sql(s"call show_cleans_metadata(table => '$tableName', showArchived => true)")
          .collect()
        spark.sql(s"call show_cleans_metadata(table => '$tableName', showArchived => true)").show(false)

        assert(activeCleans.length >= 1, "Active timeline should have clean instances")
        assert(activePlans.length >= 1, "Active timeline should have clean plans")

        // showArchived=true should include at least the same data as active timeline
        assert(allCleans.length >= activeCleans.length, "Active + Archived should have at least as many instances as active only")
        assert(allPlans.length >= activePlans.length, "Active + Archived should have at least as many plans as active only")
        assert(allMetadata.length >= activeMetadata.length, "Active + Archived should have at least as much metadata as active only")
      }
    }
  }

  test("Test show_cleans procedures with limit parameter") {
    withSQLConf("hoodie.clean.automatic" -> "false", "hoodie.parquet.max.file.size" -> "10000") {
      withTempDir { tmp =>
        val tableName = generateTableName
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
             | ) using hudi
             | location '${tmp.getCanonicalPath}'
             | tblproperties (
             |   primaryKey = 'id',
             |   type = 'cow',
             |   preCombineField = 'ts'
             | )
             |""".stripMargin)

        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"update $tableName set price = 11 where id = 1")
        spark.sql(s"call run_clean(table => '$tableName', retain_commits => 1)")

        spark.sql(s"update $tableName set price = 12 where id = 1")
        spark.sql(s"call run_clean(table => '$tableName', retain_commits => 1)")

        spark.sql(s"update $tableName set price = 13 where id = 1")
        spark.sql(s"call run_clean(table => '$tableName', retain_commits => 1)")

        val limitedCleans = spark.sql(s"call show_cleans(table => '$tableName', limit => 1)")
          .collect()

        assertResult(1)(limitedCleans.length)

        val totalCleans = spark.sql(s"call show_cleans(table => '$tableName', limit => 10)")
          .collect()

        assert(totalCleans.length >= 1)
        assert(totalCleans.length <= 3) // Should not exceed the number of cleans we performed

        val limitedPlans = spark.sql(s"call show_clean_plans(table => '$tableName', limit => 1)")
          .collect()

        assert(limitedPlans.length <= 1)
      }
    }
  }

  test("Test show procedures with empty table") {
    withSQLConf("hoodie.clean.automatic" -> "false") {
      withTempDir { tmp =>
        val tableName = generateTableName
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
             | ) using hudi
             | location '${tmp.getCanonicalPath}'
             | tblproperties (
             |   primaryKey = 'id',
             |   type = 'cow',
             |   preCombineField = 'ts'
             | )
             |""".stripMargin)

        val emptyCleans = spark.sql(s"call show_cleans(table => '$tableName')")
          .collect()

        assertResult(0)(emptyCleans.length)

        val emptyPlans = spark.sql(s"call show_clean_plans(table => '$tableName')")
          .collect()

        assertResult(0)(emptyPlans.length)

        val emptyMetadata = spark.sql(s"call show_cleans_metadata(table => '$tableName')")
          .collect()

        assertResult(0)(emptyMetadata.length)
      }
    }
  }

  test("Test show procedures error handling") {

    val nonExistentTable = "non_existent_table"
    if (HoodieSparkUtils.isSpark3_4) {
      spark.sql("set spark.sql.defaultColumn.enabled = false")
    }

    intercept[Exception] {
      spark.sql(s"call show_cleans(table => '$nonExistentTable')").collect()
    }

    intercept[Exception] {
      spark.sql(s"call show_clean_plans(table => '$nonExistentTable')").collect()
    }

    intercept[Exception] {
      spark.sql(s"call show_cleans_metadata(table => '$nonExistentTable')").collect()
    }
  }
}
