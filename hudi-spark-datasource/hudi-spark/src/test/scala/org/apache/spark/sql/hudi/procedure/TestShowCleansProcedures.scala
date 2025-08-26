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

        val firstCleanPlans = spark.sql(s"call show_cleans(table => '$tableName')").collect()
        require(firstCleanPlans.length >= 1, "Should have at least 1 clean operation after ensuring sufficient commits")

        spark.sql(s"insert into $tableName values(4, 'a4', 40, 4000)")
        spark.sql(s"update $tableName set price = 15 where id = 1")
        spark.sql(s"update $tableName set price = 25 where id = 2")
        spark.sql(s"update $tableName set price = 35 where id = 3")
        spark.sql(s"update $tableName set price = 45 where id = 4")

        spark.sql(s"call run_clean(table => '$tableName', retain_commits => 1)")
          .collect()

        val secondCleanPlans = spark.sql(s"call show_cleans(table => '$tableName')").collect()
        require(secondCleanPlans.length >= 2, "Should have at least 2 clean operations after second clean")

        val allCleanPlans = spark.sql(s"call show_cleans(table => '$tableName')")
        allCleanPlans.show(false)
        val allPlans = allCleanPlans.collect()

        assert(allPlans.length >= 2, "Should have at least 2 clean operations")

        val firstPlan = allPlans.head
        assert(firstPlan.length >= 19, "Each clean operation should have 19 columns")

        allPlans.foreach { plan =>
          val planTime = plan.getString(0)
          assert(planTime.nonEmpty && planTime.toLong > 0, "Plan time should be a valid timestamp")
        }
        val sortedPlans = secondCleanPlans.sortBy(_.getString(0))
        val actualFirstCleanTime = sortedPlans(0).getString(0)
        val startTimeStr = (actualFirstCleanTime.toLong + 1000).toString
        val afterStartFilter = spark.sql(s"""call show_cleans(table => '$tableName', filter => "clean_time > '$startTimeStr'")""")
        afterStartFilter.show(false)
        val afterStartRows = afterStartFilter.collect()
        assertResult(afterStartRows.length)(1)
      }
    }
  }

  test("Test show_cleans procedure - schema with partition metadata present") {
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
        spark.sql(s"update $tableName set price = 12 where id = 1")

        spark.sql(s"call run_clean(table => '$tableName', retain_commits => 1)")
          .collect()

        val allCleans = spark.sql(s"call show_cleans(table => '$tableName')")
        allCleans.show(false)
        val allCleansData = allCleans.collect()

        assert(allCleansData.length == 1, "Should have at least one clean operation")

        assert(allCleans.schema.fields.length == 19, "Unified schema should have 19 fields")

        val expectedFields = Seq(
          "clean_time", "state_transition_time", "state", "action", "start_clean_time",
          "partition_path", "policy", "delete_path_patterns", "success_delete_files", 
          "failed_delete_files", "is_partition_deleted", "time_taken_in_millis", 
          "total_files_deleted", "earliest_commit_to_retain", "last_completed_commit_timestamp",
          "version", "total_partitions_to_clean", "total_partitions_to_delete", "extra_metadata"
        )
        
        expectedFields.zipWithIndex.foreach { case (expectedField, index) =>
          assert(allCleans.schema.fields(index).name == expectedField, 
            s"Field at position $index should be '$expectedField', but was '${allCleans.schema.fields(index).name}'")
        }

        val completedCleans = allCleansData.filter(_.getString(2) == "COMPLETED")
        assert(completedCleans.length >= 1, "Should have at least one completed operation")

        completedCleans.foreach { completedClean =>
          assert(completedClean.getString(0) != null) // clean_time
          assert(completedClean.getString(1) != null) // state_transition_time  
          assert(completedClean.getString(2) == "COMPLETED") // state
          assert(completedClean.getString(3) == "clean") // action
          assert(completedClean.getString(4) != null) // start_clean_time
          assert(completedClean.getString(5) != null) // partition_path (should be available for completed)
          assert(completedClean.getString(6) != null) // policy
          assert(completedClean.get(7) != null) // delete_path_patterns
          assert(completedClean.get(8) != null) // success_delete_files
          assert(completedClean.get(9) != null) // failed_delete_files
          // is_partition_deleted can be true/false/null
          assert(completedClean.getLong(11) >= 0) // time_taken_in_millis
          assert(completedClean.getInt(12) >= 0) // total_files_deleted
          // Plan-specific fields should be null for completed operations
          assert(completedClean.get(16) == null) // total_partitions_to_clean
          assert(completedClean.get(17) == null) // total_partitions_to_delete
        }
      }
    }
  }

  test("Test show_cleans unified schema and graceful null handling") {
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

        val allCleans = spark.sql(s"call show_cleans(table => '$tableName')")
        allCleans.show(false)
        val allCleansData = allCleans.collect()

        assert(allCleansData.length >= 1, "Should have at least one clean operation")

        assert(allCleans.schema.fields.length == 19, "show_cleans should have 19 fields")

        val completedCleans = allCleansData.filter(_.getString(2) == "COMPLETED")
        assert(completedCleans.length >= 1, "Should have at least one completed operation")

        completedCleans.foreach { completedClean =>
          // Verify completed operations have the right data structure
          assert(completedClean.getString(0) != null) // clean_time
          assert(completedClean.getString(1) != null) // state_transition_time  
          assert(completedClean.getString(2) == "COMPLETED") // state
          assert(completedClean.getString(3) == "clean") // action
          assert(completedClean.getString(4) != null) // start_clean_time
          
          // For partitioned tables, partition metadata should be available
          if (completedClean.getString(5) != null) { // partition_path
            assert(completedClean.getString(6) != null) // policy
            assert(completedClean.get(7) != null) // delete_path_patterns
            assert(completedClean.get(8) != null) // success_delete_files  
            assert(completedClean.get(9) != null) // failed_delete_files
            // is_partition_deleted can be true/false/null
          }
          
          assert(completedClean.getLong(11) >= 0) // time_taken_in_millis
          assert(completedClean.getInt(12) >= 0) // total_files_deleted
          assert(completedClean.getString(13) != null) // earliest_commit_to_retain
          assert(completedClean.getString(14) != null) // last_completed_commit_timestamp
          assert(completedClean.getInt(15) >= 1) // version
          
          // Plan-specific fields should be null for completed operations
          assert(completedClean.get(16) == null) // total_partitions_to_clean
          assert(completedClean.get(17) == null) // total_partitions_to_delete
        }

        val completedOnly = spark.sql(
          s"""call show_cleans(
             |  table => '$tableName',
             |  filter => "state = 'COMPLETED'"
             |)""".stripMargin)
        
        val completedOnlyData = completedOnly.collect()
        assert(completedOnlyData.length >= 1, "Should have completed operations when filtered")
        assert(completedOnlyData.forall(_.getString(2) == "COMPLETED"), "All filtered results should be completed")

        val withPartitionData = spark.sql(
          s"""call show_cleans(
             |  table => '$tableName',
             |  filter => "partition_path IS NULL"
             |)""".stripMargin)
        
        val partitionData = withPartitionData.collect()
        assert(partitionData.length == 0, "No results should have partition data when filtered")
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

        // showArchived=true (both active + archived timelines merged)
        val allCleans = spark.sql(s"call show_cleans(table => '$tableName', showArchived => true)")
          .collect()
        spark.sql(s"call show_cleans(table => '$tableName', showArchived => true)").show(false)

        assert(activeCleans.length >= 1, "Active timeline should have clean instances")

        // showArchived=true should include at least the same data as active timeline
        assert(allCleans.length >= activeCleans.length, "Active + Archived should have at least as many instances as active only")
        
        // Verify that show_cleans includes both completed and pending operations
        val completedCleansActive = activeCleans.filter(_.getString(2) == "COMPLETED")
        val pendingCleansActive = activeCleans.filter(row => Set("REQUESTED", "INFLIGHT").contains(row.getString(2)))
        assert(completedCleansActive.length + pendingCleansActive.length == activeCleans.length, 
          "All clean operations should be either completed or pending")
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

        val limitedAllOperations = spark.sql(s"call show_cleans(table => '$tableName', limit => 1)")
          .collect()

        assert(limitedAllOperations.length <= 1)
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
  }

  test("Test cleaning with some complex filters") {
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
        spark.sql(s"update $tableName set price = 11 where id = 1")

        spark.sql(s"call run_clean(table => '$tableName', retain_commits => 1)").collect()

        spark.sql(s"update $tableName set price = 12 where id = 1")
        spark.sql(s"call run_clean(table => '$tableName', retain_commits => 1)").collect()

        spark.sql(s"update $tableName set price = 13 where id = 1")
        spark.sql(s"call run_clean(table => '$tableName', retain_commits => 1)").collect()

        val allCleans = spark.sql(s"call show_cleans(table => '$tableName')")
        allCleans.show(false)
        val allCleansDf = allCleans.collect()
        val firstCleanTime = if (allCleansDf.nonEmpty) allCleansDf.last.getAs[String]("clean_time") else "0"

        val firstCleanDF = spark.sql(
          s"""call show_cleans(table => '$tableName', filter => "clean_time = '$firstCleanTime' AND action = 'clean'")"""
        )
        firstCleanDF.show(false)
        val firstClean = firstCleanDF.collect()

        val laterCleansDF = spark.sql(
          s"""call show_cleans(table => '$tableName', filter => "clean_time > '$firstCleanTime' AND action = 'clean'")"""
        )
        laterCleansDF.show(false)
        val laterCleans = laterCleansDF.collect()

        val numericFilterDF = spark.sql(
          s"""call show_cleans(table => '$tableName', filter => "total_files_deleted > 0 AND LENGTH(action) > 3")"""
        )
        numericFilterDF.show(false)
        val numericFilter = numericFilterDF.collect()

        assert(firstClean.length == 1, "First clean filter should execute successfully")
        assert(laterCleans.length == allCleansDf.length - 1, "Later cleans filter should execute successfully")
        assert(numericFilter.length == allCleansDf.length, "Numeric filter should execute successfully")
      }
    }
  }

  test("Test filter expressions with various data types") {
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
             | active boolean,
             | ts long
             | ) using hudi
             | location '${tmp.getCanonicalPath}'
             | tblproperties (
             |   primaryKey = 'id',
             |   type = 'cow',
             |   preCombineField = 'ts'
             | )
             |""".stripMargin)

        spark.sql(s"insert into $tableName values(1, 'product1', 99.99, true, 1000)")
        spark.sql(s"insert into $tableName values(2, 'product2', 149.99, false, 2000)")

        spark.sql(s"update $tableName set price = 109.99 where id = 1")
        spark.sql(s"update $tableName set price = 119.99 where id = 1")
        spark.sql(s"update $tableName set price = 129.99 where id = 2")
        spark.sql(s"update $tableName set price = 139.99 where id = 2")

        spark.sql(s"insert into $tableName values(3, 'product3', 199.99, true, 3000)")
        spark.sql(s"update $tableName set price = 149.99 where id = 1")

        spark.sql(s"call run_clean(table => '$tableName', retain_commits => 2)").collect()

        val allCleansDF = spark.sql(s"call show_cleans(table => '$tableName', showArchived => true)")
        allCleansDF.show(false)

        val filterTests = Seq(
          ("action = 'clean'", "String equality"),
          ("action LIKE 'clean%'", "String LIKE pattern"),
          ("UPPER(action) = 'CLEAN'", "String function with equality"),
          ("LENGTH(clean_time) > 5", "String length function"),
          ("total_files_deleted >= 0", "Numeric comparison"),
          ("time_taken_in_millis BETWEEN 0 AND 999999", "Numeric BETWEEN"),
          ("clean_time IS NOT NULL", "NULL check"),
          ("action = 'clean' AND total_files_deleted >= 0", "AND logic"),
          ("total_files_deleted >= 0 OR time_taken_in_millis >= 0", "OR logic"),
          ("NOT (total_files_deleted < 0)", "NOT logic"),
          ("action IN ('clean', 'commit', 'rollback')", "IN operator")
        )

        filterTests.foreach { case (filterExpr, description) =>
          val filteredResult = spark.sql(
            s"""call show_cleans(table => '$tableName',
               |filter => "$filterExpr")""".stripMargin
          ).collect()
          assert(filteredResult.length > 0, s"Filter '$description' should execute successfully")
        }
      }
    }
  }
}
