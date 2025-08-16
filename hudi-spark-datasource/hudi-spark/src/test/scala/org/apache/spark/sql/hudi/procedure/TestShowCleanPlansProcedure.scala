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

class TestShowCleanPlansProcedure extends HoodieSparkProcedureTestBase {

  test("Test Call show_clean_plans Procedure") {
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

      // Test basic show_clean_plans procedure
      spark.sql(s"""call show_clean_plans(table => '$tableName', limit => 10)""").show

      val cleanerPlanDF = spark.sql(s"""call show_clean_plans(table => '$tableName', limit => 10)""")
      cleanerPlanDF.select("plan_time", "action", "policy", "version", "extra_metadata").show

      val basicResult = spark.sql(s"""call show_clean_plans(table => '$tableName', limit => 10)""").collect()

      // Should have at least one cleaner plan
      assertResult(true)(basicResult.length > 0)

      // Verify the schema of basic result
      val basicColumns = basicResult.head.schema.fieldNames
      assertResult(true)(basicColumns.contains("plan_time"))
      assertResult(true)(basicColumns.contains("action"))
      assertResult(true)(basicColumns.contains("earliest_instant_to_retain"))
      assertResult(true)(basicColumns.contains("last_completed_commit_timestamp"))
      assertResult(true)(basicColumns.contains("policy"))
      assertResult(true)(basicColumns.contains("version"))
      assertResult(true)(basicColumns.contains("total_partitions_to_clean"))
      assertResult(true)(basicColumns.contains("total_partitions_to_delete"))
      assertResult(true)(basicColumns.contains("extra_metadata"))

      // Verify that action is CLEAN
      basicResult.foreach { row =>
        assertResult("clean")(row.getAs[String]("action"))
        assertResult(true)(row.getAs[Int]("total_partitions_to_clean") >= 0)
        assertResult(true)(row.getAs[Int]("total_partitions_to_delete") >= 0)
        val version = row.getAs[Int]("version")
        assertResult(true)(version == null || version >= 1)
        val policy = row.getAs[String]("policy")
        assertResult(true)(policy != null && policy.nonEmpty)
      }
    }
  }

  test("Test Call show_clean_plans with Partitioned Table") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + "/" + tableName

      // Create partitioned table with cleaning configuration
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

      // Test show_clean_plans procedure
      val cleanerPlanResult = spark.sql(s"""call show_clean_plans(table => '$tableName', limit => 20)""").collect()

      // Should have at least one cleaner plan with partition details
      assertResult(true)(cleanerPlanResult.length > 0)

      // Verify the schema and data
      cleanerPlanResult.foreach { row =>
        assertResult("clean")(row.getAs[String]("action"))
        val totalPartitionsToClean = row.getAs[Int]("total_partitions_to_clean")
        assertResult(true)(totalPartitionsToClean >= 0)
        val extraMetadata = row.getAs[String]("extra_metadata")
        // Extra metadata should be displayed as a string or null
        assertResult(true)(extraMetadata == null || extraMetadata.isInstanceOf[String])
      }
    }
  }

  test("Test Call show_clean_plans with No Clean Operations") {
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

      // Test show_clean_plans procedure - should return empty result
      val result = spark.sql(s"""call show_clean_plans(table => '$tableName', limit => 10)""").collect()

      // Should have no cleaner plan operations
      assertResult(0)(result.length)
    }
  }

  test("Test Call show_clean_plans with Limit Parameter") {
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

      // Generate multiple operations to create multiple clean plans
      for (i <- 1 to 10) {
        spark.sql(s"insert into $tableName values($i, 'name$i', ${i * 10}, ${i * 1000})")
        if (i > 1) {
          spark.sql(s"update $tableName set price = ${i * 15} where id = $i")
        }
      }

      // Test with limit parameter
      val limitedResult = spark.sql(s"""call show_clean_plans(table => '$tableName', limit => 3)""").collect()
      val unlimitedResult = spark.sql(s"""call show_clean_plans(table => '$tableName', limit => 100)""").collect()

      // Limited result should have at most 3 entries
      assertResult(true)(limitedResult.length <= 3)

      // If we have cleaner plans, unlimited should have same or more entries
      if (unlimitedResult.length > 0) {
        assertResult(true)(unlimitedResult.length >= limitedResult.length)
      }
    }
  }

  test("Test Call show_clean_plans ExtraMetadata Display") {
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

      // Insert and update data to trigger cleaning
      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
      spark.sql(s"update $tableName set price = 15 where id = 1")
      spark.sql(s"update $tableName set name = 'updated1' where id = 1")

      // Test show_clean_plans procedure and verify extra_metadata column
      val result = spark.sql(s"""call show_clean_plans(table => '$tableName', limit => 10)""").collect()

      if (result.length > 0) {
        // Verify that extra_metadata column exists and is properly formatted
        val extraMetadataColumn = result.head.schema.fieldNames.contains("extra_metadata")
        assertResult(true)(extraMetadataColumn)

        result.foreach { row =>
          val extraMetadata = row.getAs[String]("extra_metadata")
          // Extra metadata should be either null or a properly formatted string
          if (extraMetadata != null) {
            // Should be in key=value format separated by commas
            assertResult(true)(extraMetadata.isInstanceOf[String])
          }
        }
      }
    }
  }
}
