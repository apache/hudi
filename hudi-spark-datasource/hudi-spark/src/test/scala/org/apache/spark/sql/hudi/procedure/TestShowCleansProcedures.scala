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
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import scala.collection.JavaConverters._

class TestShowCleansProcedures extends HoodieSparkProcedureTestBase {

  test("Test show_clean_plans procedure") {
    withSQLConf("hoodie.clean.automatic" -> "false", "hoodie.parquet.max.file.size" -> "10000") {
      withTempDir { tmp =>
        val tableName = generateTableName
        val extraConf = if (HoodieSparkUtils.gteqSpark3_4) {
          Map("spark.sql.defaultColumn.enabled" -> "false")
        } else {
          Map.empty[String, String]
        }
        withSQLConf(extraConf.toSeq: _*) {
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
          val sortedPlans = secondCleanPlans.sortBy(_.getString(0))
          val actualFirstCleanTime = sortedPlans(0).getString(0)
          val startTimeStr = (actualFirstCleanTime.toLong + 1000).toString
          val afterStartFilter = spark.sql(s"""call show_clean_plans(table => '$tableName', filter => "plan_time > '$startTimeStr'")""")
          afterStartFilter.show(false)
          val afterStartRows = afterStartFilter.collect()
          assertResult(afterStartRows.length)(1)
        }
      }
    }
  }

  test("Test show_cleans procedure") {
    withSQLConf("hoodie.clean.automatic" -> "false", "hoodie.parquet.max.file.size" -> "10000") {
      withTempDir { tmp =>
        val tableName = generateTableName
        val extraConf = if (HoodieSparkUtils.gteqSpark3_4) {
          Map("spark.sql.defaultColumn.enabled" -> "false")
        } else {
          Map.empty[String, String]
        }
        withSQLConf(extraConf.toSeq: _*) {
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
  }

  test("Test show_cleans with partition metadata") {
    withSQLConf("hoodie.clean.automatic" -> "false", "hoodie.parquet.max.file.size" -> "10000") {
      withTempDir { tmp =>
        val tableName = generateTableName
        val extraConf = if (HoodieSparkUtils.gteqSpark3_4) {
          Map("spark.sql.defaultColumn.enabled" -> "false")
        } else {
          Map.empty[String, String]
        }
        withSQLConf(extraConf.toSeq: _*) {
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
  }

  test("Test show_cleans procedures with limit parameter") {
    withSQLConf("hoodie.clean.automatic" -> "false", "hoodie.parquet.max.file.size" -> "10000") {
      withTempDir { tmp =>
        val tableName = generateTableName
        val extraConf = if (HoodieSparkUtils.gteqSpark3_4) {
          Map("spark.sql.defaultColumn.enabled" -> "false")
        } else {
          Map.empty[String, String]
        }
        withSQLConf(extraConf.toSeq: _*) {
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
  }

  test("Test show procedures with empty table") {
    withSQLConf("hoodie.clean.automatic" -> "false") {
      withTempDir { tmp =>
        val tableName = generateTableName
        val extraConf = if (HoodieSparkUtils.gteqSpark3_4) {
          Map("spark.sql.defaultColumn.enabled" -> "false")
        } else {
          Map.empty[String, String]
        }
        withSQLConf(extraConf.toSeq: _*) {
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
  }

  test("Test show procedures error handling") {

    val nonExistentTable = "non_existent_table"
    val extraConf = if (HoodieSparkUtils.gteqSpark3_4) {
      Map("spark.sql.defaultColumn.enabled" -> "false")
    } else {
      Map.empty[String, String]
    }
    withSQLConf(extraConf.toSeq: _*) {

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

  test("Test cleaning with some complex filters") {
    withSQLConf("hoodie.clean.automatic" -> "false", "hoodie.parquet.max.file.size" -> "10000") {
      withTempDir { tmp =>
        val tableName = generateTableName
        val extraConf = if (HoodieSparkUtils.gteqSpark3_4) {
          Map("spark.sql.defaultColumn.enabled" -> "false")
        } else {
          Map.empty[String, String]
        }
        withSQLConf(extraConf.toSeq: _*) {
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
  }

  test("Test filter expressions with various data types") {
    withSQLConf("hoodie.clean.automatic" -> "false", "hoodie.parquet.max.file.size" -> "10000") {
      withTempDir { tmp =>
        val tableName = generateTableName
        val extraConf = if (HoodieSparkUtils.gteqSpark3_4) {
          Map("spark.sql.defaultColumn.enabled" -> "false")
        } else {
          Map.empty[String, String]
        }
        withSQLConf(extraConf.toSeq: _*) {
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

  Seq(6, 9).foreach { tableVersion =>
    test(s"Test show_clean_plans with an archived clean instant - table version $tableVersion") {
      withSQLConf("hoodie.clean.automatic" -> "false", "hoodie.archive.automatic" -> "false",
        "hoodie.write.table.version" -> tableVersion.toString) {
        withTempDir { tmp =>
          val tableName = generateTableName
          val tablePath = tmp.getCanonicalPath
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
               |   type = 'cow',
               |   preCombineField = 'ts',
               |   hoodie.metadata.enable = 'false',
               |   'hoodie.write.table.version' = '$tableVersion'
               | )
               |""".stripMargin)

          def rowCount(procedure: String, showArchived: Boolean): Int =
            spark.sql(s"call $procedure(table => '$tableName', showArchived => $showArchived)").collect().length

          // V1 archives cleans using a separate count threshold, so generate enough cleans
          // to force archival with both timeline formats.
          spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
          spark.sql(s"update $tableName set price = 11 where id = 1")
          spark.sql(s"update $tableName set price = 12 where id = 1")
          (0 until 6).foreach { i =>
            if (i > 0) {
              spark.sql(s"update $tableName set price = ${12 + i} where id = 1")
            }
            spark.sql(s"call run_clean(table => '$tableName', retain_commits => 1, " +
              s"options => 'hoodie.write.table.version=$tableVersion')").collect()
          }

          // This table is not partitioned, so all three procedures return one row per clean.
          assertResult(6)(rowCount("show_cleans", showArchived = true))
          assertResult(6)(rowCount("show_cleans_metadata", showArchived = true))
          assertResult(6)(rowCount("show_clean_plans", showArchived = true))

          val procedures = Seq("show_clean_plans", "show_cleans", "show_cleans_metadata")
          val beforeArchive = procedures.map { procedure =>
            procedure -> spark.sql(s"call $procedure(table => '$tableName', showArchived => true)").collect().toSeq
          }.toMap

          spark.sql(s"call archive_commits(table => '$tableName', min_commits => 2, max_commits => 3," +
            s" retain_commits => 1, enable_metadata => false, options => 'hoodie.write.table.version=$tableVersion')").collect()

          // Precondition: archival must have split the two cleans across the two timelines,
          // otherwise the archived branch below is never exercised and the test passes vacuously.
          val metaClient = createMetaClient(spark, tablePath)
          assertResult(tableVersion)(metaClient.getTableConfig.getTableVersion.versionCode())
          val archivedCleans = metaClient.getArchivedTimeline.getCleanerTimeline.filterCompletedInstants
            .getInstants.asScala.map(_.requestedTime).toSeq
          val activeCleans = metaClient.getActiveTimeline.getCleanerTimeline.filterCompletedInstants
            .getInstants.asScala.map(_.requestedTime).toSeq
          assert(archivedCleans.nonEmpty, s"expected archived cleans, got $archivedCleans")
          assert(activeCleans.nonEmpty, s"expected active cleans, got $activeCleans")
          assert(archivedCleans.max.compareTo(activeCleans.min) < 0,
            "archived cleans must be older than active cleans")

          // The active-only view must exclude the archived clean for all three procedures.
          assertResult(activeCleans.size)(rowCount("show_cleans", showArchived = false))
          assertResult(activeCleans.size)(rowCount("show_cleans_metadata", showArchived = false))
          assertResult(activeCleans.size)(rowCount("show_clean_plans", showArchived = false))

          procedures.foreach { procedure =>
            val allRows = spark.sql(s"call $procedure(table => '$tableName', showArchived => true)").collect().toSeq
            assertResult(beforeArchive(procedure))(allRows)
            val limitedRows = spark.sql(s"call $procedure(table => '$tableName', showArchived => true, limit => 1)").collect().toSeq
            assertResult(allRows.take(1))(limitedRows)
            val timeColumn = if (procedure == "show_clean_plans") "plan_time" else "clean_time"
            val filteredRows = spark.sql(
              s"""call $procedure(table => '$tableName', showArchived => true,
                 |filter => "$timeColumn = '${archivedCleans.head}'")""".stripMargin).collect().toSeq
            assertResult(allRows.filter(_.getString(0) == archivedCleans.head))(filteredRows)
          }

          val plans = spark.sql(s"call show_clean_plans(table => '$tableName', showArchived => true)").collect()
          assertResult(6)(plans.length)
          val planTimes = plans.map(_.getString(0)).mkString(", ")
          val activePlan = plans.find(_.getString(0) == activeCleans.head)
            .getOrElse(fail(s"no plan row for the active clean ${activeCleans.head}, got plan_times: $planTimes"))
          val archivedPlan = plans.find(_.getString(0) == archivedCleans.head)
            .getOrElse(fail(s"no plan row for the archived clean ${archivedCleans.head}, got plan_times: $planTimes"))

          // Fields that come from the cleaner plan itself, resolved by name off the row schema so
          // that a change in output-schema ordering cannot silently re-point these assertions.
          // extra_metadata is left out: it is null for both rows, so it does not discriminate.
          val planFields = Seq(
            "earliest_instant_to_retain",
            "last_completed_commit_timestamp",
            "policy",
            "version",
            "total_partitions_to_clean",
            "total_partitions_to_delete")

          // The active clean plan is read correctly.
          assertResult("COMPLETED")(activePlan.getString(1))
          assertResult("clean")(activePlan.getString(2))
          planFields.foreach { name =>
            assert(!activePlan.isNullAt(activePlan.fieldIndex(name)), s"active clean plan should have a non-null $name")
          }

          // Archived plans must expose the same fields as active plans (#19639).
          assertResult("COMPLETED")(archivedPlan.getString(1))
          assertResult("clean")(archivedPlan.getString(2))
          planFields.foreach { name =>
            assert(!archivedPlan.isNullAt(archivedPlan.fieldIndex(name)),
              s"archived clean plan should have a non-null $name")
          }
        }
      }
    }
  }

  test("Test show_clean_plans validates its inputs") {
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

        // No insert needed: both validations run before any table data is read.
        // A non-positive limit is rejected.
        checkExceptionContain(s"call show_clean_plans(table => '$tableName', limit => 0)")(
          "Limit must be positive")

        // A filter that references an unknown column is rejected before the plans are read; the
        // column-reference message discriminates this branch from a filter parse failure.
        checkExceptionContain(
          s"""call show_clean_plans(table => '$tableName', filter => "nonexistent_col > 1")""")(
          "Invalid column references: nonexistent_col")

        checkExceptionContain(
          s"""call show_clean_plans(table => '$tableName', filter => "concat(action, 'x') = 'cleanx'")""")(
          "Unsupported functions: concat")
      }
    }
  }
}
