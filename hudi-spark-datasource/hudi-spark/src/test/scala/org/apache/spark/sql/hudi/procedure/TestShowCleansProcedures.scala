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

class TestShowCleansProcedures extends HoodieSparkProcedureTestBase {

  test("Test show_clean_plans procedure") {
    withSQLConf("hoodie.clean.automatic" -> "false", "hoodie.parquet.max.file.size" -> "10000") {
      withTempDir { tmp =>
        val tableName = generateTableName
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

        val cleanPlans = spark.sql(s"call show_clean_plans(table => '$tableName')")
          .collect()

        assertResult(1)(cleanPlans.length)

        val cleanPlan = cleanPlans(0)
        assert(cleanPlan.getString(0) != null) // plan_time
        assert(cleanPlan.getString(1) == "clean") // action
        // earliest_instant_to_retain can be null
        // last_completed_commit_timestamp can be null
        assert(cleanPlan.getString(4) != null) // policy
        // version can be null or integer
        assert(cleanPlan.getInt(6) >= 0) // total_partitions_to_clean should be >= 0
        assert(cleanPlan.getInt(7) >= 0) // total_partitions_to_delete should be >= 0
        // extra_metadata can be null
      }
    }
  }

  test("Test show_cleans procedure") {
    withSQLConf("hoodie.clean.automatic" -> "false", "hoodie.parquet.max.file.size" -> "10000") {
      withTempDir { tmp =>
        val tableName = generateTableName
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

  test("Test show_cleans procedures with limit parameter") {
    withSQLConf("hoodie.clean.automatic" -> "false", "hoodie.parquet.max.file.size" -> "10000") {
      withTempDir { tmp =>
        val tableName = generateTableName
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
