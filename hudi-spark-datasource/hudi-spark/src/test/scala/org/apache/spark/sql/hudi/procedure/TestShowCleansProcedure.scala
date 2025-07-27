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

import org.apache.spark.sql.Row

class TestShowCleansProcedure extends HoodieSparkProcedureTestBase {

  test("Test Call show_cleans Procedure") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + "/" + tableName

      // Create table with cleaning configuration
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
           |  preCombineField = 'ts',
           |  'hoodie.clean.automatic' = 'true',
           |  'hoodie.clean.trigger.max.commits' = '1',
           |  'hoodie.clean.commits.retained' = '1'
           | )
       """.stripMargin)

      // Insert initial data
      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)").show
      spark.sql(s"insert into $tableName values(2, 'a2', 20, 2000)").show

      // Update data to trigger cleaning
      spark.sql(s"update $tableName set price = 15 where id = 1").show
      spark.sql(s"update $tableName set price = 25 where id = 2").show

      // Another update to ensure cleaning happens
      spark.sql(s"update $tableName set name = 'updated1' where id = 1").show

      // Test basic show_cleans procedure
      spark.sql(s"""call show_cleans(table => '$tableName', limit => 10)""").show

      val cleanDF = spark.sql(s"""call show_cleans(table => '$tableName', limit => 10)""")
      cleanDF.select("clean_time", "action", "total_files_deleted", "time_taken_in_millis").show

      val basicResult = spark.sql(s"""call show_cleans(table => '$tableName', limit => 10)""").collect()

      // Should have at least one clean operation
      assertResult(true)(basicResult.length > 0)

      // Verify the schema of basic result
      val basicColumns = basicResult.head.schema.fieldNames
      assertResult(true)(basicColumns.contains("clean_time"))
      assertResult(true)(basicColumns.contains("action"))
      assertResult(true)(basicColumns.contains("total_files_deleted"))
      assertResult(true)(basicColumns.contains("start_clean_time"))
      assertResult(true)(basicColumns.contains("time_taken_in_millis"))

      // Verify that action is CLEAN
      basicResult.foreach { row =>
        assertResult("clean")(row.getAs[String]("action"))
        assertResult(true)(row.getAs[Int]("total_files_deleted") >= 0)
        assertResult(true)(row.getAs[Long]("time_taken_in_millis") >= 0)
      }
    }
  }

  test("Test Call show_cleans_metadata Procedure") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + "/" + tableName

      // Create table with cleaning configuration
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  partition_key string
           |) using hudi
           | location '$tablePath'
           | partitioned by (partition_key)
           | tblproperties (
           |  primaryKey = 'id',
           |  preCombineField = 'ts',
           |  'hoodie.clean.automatic' = 'true',
           |  'hoodie.clean.trigger.max.commits' = '1',
           |  'hoodie.clean.commits.retained' = '1'
           | )
       """.stripMargin)

      // Insert initial data with different partitions
      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000, 'part1')")
      spark.sql(s"insert into $tableName values(2, 'a2', 20, 2000, 'part2')")
      spark.sql(s"insert into $tableName values(3, 'a3', 30, 3000, 'part1')")

      // Update data to create more file versions
      spark.sql(s"update $tableName set price = 15 where id = 1")
      spark.sql(s"update $tableName set price = 25 where id = 2")

      // More updates to ensure cleaning
      spark.sql(s"update $tableName set name = 'updated1' where id = 1")
      spark.sql(s"update $tableName set name = 'updated2' where id = 2")

      // Test detailed show_cleans_metadata procedure
      val detailedResult = spark.sql(s"""call show_cleans_metadata(table => '$tableName', limit => 20)""").collect()

      // Should have at least one clean operation with partition details
      assertResult(true)(detailedResult.length > 0)

      // Verify the schema of detailed result
      val detailedColumns = detailedResult.head.schema.fieldNames
      assertResult(true)(detailedColumns.contains("clean_time"))
      assertResult(true)(detailedColumns.contains("action"))
      assertResult(true)(detailedColumns.contains("partition_path"))
      assertResult(true)(detailedColumns.contains("policy"))
      assertResult(true)(detailedColumns.contains("success_delete_files"))
      assertResult(true)(detailedColumns.contains("failed_delete_files"))
      assertResult(true)(detailedColumns.contains("delete_path_patterns"))

      // Verify that action is CLEAN and we have partition-level details
      detailedResult.foreach { row =>
        assertResult("clean")(row.getAs[String]("action"))
        // Partition path should be present
        val partitionPath = row.getAs[String]("partition_path")
        assertResult(true)(partitionPath != null)
        // Should have some metrics
        assertResult(true)(row.getAs[Int]("success_delete_files") >= 0)
        assertResult(true)(row.getAs[Int]("failed_delete_files") >= 0)
      }
    }
  }

  test("Test Call show_cleans with No Clean Operations") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + "/" + tableName

      // Create table with cleaning disabled
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
           |  preCombineField = 'ts',
           |  'hoodie.clean.automatic' = 'false'
           | )
       """.stripMargin)

      // Insert some data
      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
      spark.sql(s"insert into $tableName values(2, 'a2', 20, 2000)")

      // Test show_cleans procedure - should return empty result
      val result = spark.sql(s"""call show_cleans(table => '$tableName', limit => 10)""").collect()

      // Should have no clean operations
      assertResult(0)(result.length)
    }
  }

  test("Test Call show_cleans with Limit Parameter") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + "/" + tableName

      // Create table with aggressive cleaning to generate multiple clean operations
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
           |  preCombineField = 'ts',
           |  'hoodie.clean.automatic' = 'true',
           |  'hoodie.clean.trigger.max.commits' = '1',
           |  'hoodie.clean.commits.retained' = '1'
           | )
       """.stripMargin)

      // Generate multiple operations to create multiple clean operations
      for (i <- 1 to 10) {
        spark.sql(s"insert into $tableName values($i, 'name$i', ${i * 10}, ${i * 1000})")
        if (i > 1) {
          spark.sql(s"update $tableName set price = ${i * 15} where id = $i")
        }
      }

      // Test with limit parameter
      val limitedResult = spark.sql(s"""call show_cleans(table => '$tableName', limit => 3)""").collect()
      val unlimitedResult = spark.sql(s"""call show_cleans(table => '$tableName', limit => 100)""").collect()

      // Limited result should have at most 3 entries
      assertResult(true)(limitedResult.length <= 3)

      // If we have clean operations, unlimited should have same or more entries
      if (unlimitedResult.length > 0) {
        assertResult(true)(unlimitedResult.length >= limitedResult.length)
      }
    }
  }
}
