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

package org.apache.spark.sql.hudi.analysis

import org.apache.hudi.HoodieConversionUtils.toJavaOption
import org.apache.hudi.ScalaAssertionSupport
import org.apache.hudi.testutils.HoodieClientTestBase
import org.apache.hudi.util.JFunction

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, Expression, GreaterThan, IsNotNull, LessThan, Literal}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.types.StringType
import org.junit.jupiter.api.{Assertions, BeforeEach}
import org.junit.jupiter.api.Assertions.{assertEquals, fail}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

import java.util.function.Consumer

import scala.collection.mutable.ArrayBuffer

class TestHoodiePruneFileSourcePartitions extends HoodieClientTestBase with ScalaAssertionSupport {

  private var spark: SparkSession = _

  @BeforeEach
  override def setUp() {
    setTableName("hoodie_test")
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
  }

  override def getSparkSessionExtensionsInjector: org.apache.hudi.common.util.Option[Consumer[SparkSessionExtensions]] =
    toJavaOption(
      Some(
        JFunction.toJavaConsumer((receiver: SparkSessionExtensions) => new HoodieSparkSessionExtension().apply(receiver)))
    )

  @ParameterizedTest
  @CsvSource(value = Array(
    "cow,true", "cow,false",
    "mor,true", "mor,false"
  ))
  def testPartitionFiltersPushDown(tableType: String, partitioned: Boolean): Unit = {
    spark.sql(
      s"""
         |CREATE TABLE $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  ts long,
         |  partition string
         |) USING hudi
         |${if (partitioned) "PARTITIONED BY (partition)" else ""}
         |TBLPROPERTIES (
         |  type = '$tableType',
         |  primaryKey = 'id',
         |  orderingFields = 'ts'
         |)
         |LOCATION '$basePath/$tableName'
         """.stripMargin)

    spark.sql(
      s"""
         |INSERT INTO $tableName VALUES
         |  (1, 'a1', 10, 1000, "2021-01-05"),
         |  (2, 'a2', 20, 2000, "2021-01-06"),
         |  (3, 'a3', 30, 3000, "2021-01-07")
         """.stripMargin)

    Seq("eager", "lazy").foreach { listingModeOverride =>
      // We need to refresh the table to make sure Spark is re-processing the query every time
      // instead of serving already cached value
      spark.sessionState.catalog.invalidateAllCachedTables()

      spark.sql(s"SET hoodie.datasource.read.file.index.listing.mode=$listingModeOverride")

      val df = spark.sql(s"SELECT * FROM $tableName WHERE partition = '2021-01-05'")
      val optimizedPlan = df.queryExecution.optimizedPlan

      optimizedPlan match {
        case f @ Filter(And(IsNotNull(_), EqualTo(attr: AttributeReference, Literal(value, StringType))), lr: LogicalRelation)
          if attr.name == "partition" && value.toString.equals("2021-01-05") =>

          listingModeOverride match {
            // Case #1: Eager listing (fallback mode).
            //          Whole table will be _listed_ before partition-pruning would be applied. This is default
            //          Spark behavior that naturally occurs, since predicate push-down for tables w/o appropriate catalog
            //          support (for partition-pruning) will only occur during execution phase, while file-listing
            //          actually happens during analysis stage
            case "eager" =>
              // NOTE: In case of partitioned table 3 files will be created, while in case of non-partitioned just 1
              if (partitioned) {
                assertEquals(1275, f.stats.sizeInBytes.longValue / 1024)
                assertEquals(1275, lr.stats.sizeInBytes.longValue / 1024)
              } else {
                // NOTE: We're adding 512 to make sure we always round to the next integer value
                assertEquals(425, (f.stats.sizeInBytes.longValue + 512) / 1024)
                assertEquals(425, (lr.stats.sizeInBytes.longValue + 512) / 1024)
              }

            // Case #2: Lazy listing (default mode).
            //          In case of lazy listing mode, Hudi will only list partitions matching partition-predicates that are
            //          eagerly pushed down (w/ help of [[HoodiePruneFileSourcePartitions]]) avoiding the necessity to
            //          list the whole table
            case "lazy" =>
              // NOTE: We're adding 512 to make sure we always round to the next integer value
              assertEquals(425, (f.stats.sizeInBytes.longValue + 512) / 1024)
              assertEquals(425, (lr.stats.sizeInBytes.longValue + 512) / 1024)

            case _ => throw new UnsupportedOperationException()
          }

          if (partitioned) {
            val executionPlan = df.queryExecution.executedPlan
            val expectedPhysicalPlanPartitionFiltersClause = s"PartitionFilters: [isnotnull($attr), ($attr = 2021-01-05)]"

            Assertions.assertTrue(executionPlan.toString().contains(expectedPhysicalPlanPartitionFiltersClause))
          }

        case _ =>
          val failureHint =
            s"""Expected to see plan like below:
               |```
               |== Optimized Logical Plan ==
               |Filter (isnotnull(partition#74) AND (partition#74 = 2021-01-05)), Statistics(sizeInBytes=...)
               |+- Relation default.hoodie_test[_hoodie_commit_time#65,_hoodie_commit_seqno#66,_hoodie_record_key#67,_hoodie_partition_path#68,_hoodie_file_name#69,id#70,name#71,price#72,ts#73L,partition#74] parquet, Statistics(sizeInBytes=...)
               |```
               |
               |Instead got
               |```
               |$optimizedPlan
               |```
               |""".stripMargin.trim

          fail(failureHint)
      }
    }
  }

  @ParameterizedTest
  @CsvSource(value = Array("cow", "mor"))
  def testEmptyPartitionFiltersPushDown(tableType: String): Unit = {
    spark.sql(
      s"""
         |CREATE TABLE $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  ts long,
         |  partition string
         |) USING hudi
         |PARTITIONED BY (partition)
         |TBLPROPERTIES (
         |  type = '$tableType',
         |  primaryKey = 'id',
         |  orderingFields = 'ts'
         |)
         |LOCATION '$basePath/$tableName'
         """.stripMargin)

    spark.sql(
      s"""
         |INSERT INTO $tableName VALUES
         |  (1, 'a1', 10, 1000, "2021-01-05"),
         |  (2, 'a2', 20, 2000, "2021-01-06"),
         |  (3, 'a3', 30, 3000, "2021-01-07")
         """.stripMargin)

    Seq("eager", "lazy").foreach { listingModeOverride =>
      // We need to refresh the table to make sure Spark is re-processing the query every time
      // instead of serving already cached value
      spark.sessionState.catalog.invalidateAllCachedTables()

      spark.sql(s"SET hoodie.datasource.read.file.index.listing.mode=$listingModeOverride")

      val df = spark.sql(s"SELECT * FROM $tableName")
      val optimizedPlan = df.queryExecution.optimizedPlan

      optimizedPlan match {
        case lr: LogicalRelation  =>

          // When there are no partition pruning predicates pushed down in both cases of lazy/eager listing the whole
          // table have to be listed
          listingModeOverride match {
            case "eager" | "lazy" =>
              assertEquals(1275, lr.stats.sizeInBytes.longValue / 1024)

            case _ => throw new UnsupportedOperationException()
          }

          val executionPlan = df.queryExecution.executedPlan
          val expectedPhysicalPlanPartitionFiltersClause = tableType match {
            case "cow" => s"PartitionFilters: []"
            case "mor" => s"PushedFilters: []"
          }

          Assertions.assertTrue(executionPlan.toString().contains(expectedPhysicalPlanPartitionFiltersClause))

        case _ =>
          val failureHint =
            s"""Expected to see plan like below:
               |```
               |== Optimized Logical Plan ==
               |Filter (isnotnull(partition#74) AND (partition#74 = 2021-01-05)), Statistics(sizeInBytes=...)
               |+- Relation default.hoodie_test[_hoodie_commit_time#65,_hoodie_commit_seqno#66,_hoodie_record_key#67,_hoodie_partition_path#68,_hoodie_file_name#69,id#70,name#71,price#72,ts#73L,partition#74] parquet, Statistics(sizeInBytes=...)
               |```
               |
               |Instead got
               |```
               |$optimizedPlan
               |```
               |""".stripMargin.trim

          fail(failureHint)
      }
    }
  }

  @ParameterizedTest
  @CsvSource(value = Array(
    "cow,6", "cow,9",
    "mor,6", "mor,9"
  ))
  def testMultiplePartitionFiltersPushDown(tableType: String, tableVersion: String): Unit = {
    spark.sql(
      s"""
         |CREATE TABLE $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  ts long,
         |  datestr string
         |) USING hudi
         |PARTITIONED BY (datestr)
         |TBLPROPERTIES (
         |  type = '$tableType',
         |  primaryKey = 'id',
         |  orderingFields = 'ts',
         |  hoodie.write.table.version = '$tableVersion'
         |)
         |LOCATION '$basePath/$tableName'
         """.stripMargin)

    spark.sql(
      s"""
         |INSERT INTO $tableName VALUES
         |  (1, 'a1', 10, 1000, "2016-01-01"),
         |  (2, 'a2', 20, 2000, "2016-06-15"),
         |  (3, 'a3', 30, 3000, "2016-12-31"),
         |  (4, 'a4', 40, 4000, "2017-01-01")
         """.stripMargin)

    Seq("lazy").foreach { listingModeOverride =>
      // We need to refresh the table to make sure Spark is re-processing the query every time
      // instead of serving already cached value
      spark.sessionState.catalog.invalidateAllCachedTables()

      spark.sql(s"SET hoodie.datasource.read.file.index.listing.mode=$listingModeOverride")

      // Test with range query using multiple filters on the same partition column
      val df = spark.sql(s"SELECT * FROM $tableName WHERE datestr > '2016-01-01' AND datestr < '2016-12-31'")
      val optimizedPlan = df.queryExecution.optimizedPlan

      // Extract partition filters from the execution plan
      val executionPlan = df.queryExecution.executedPlan
      val executionPlanStr = executionPlan.toString()

      // Method 1: Programmatically extract partition filters from the physical plan
      val partitionFilters = extractPartitionFilters(executionPlan)
      if (partitionFilters.nonEmpty) {
        // Verify we have at least 3 partition filters (isnotnull, >, <)
        Assertions.assertTrue(partitionFilters.size >= 3,
          s"Expected at least 3 partition filters but found ${partitionFilters.size}: ${partitionFilters.mkString(", ")}")

        // Verify the filters contain the expected types
        val hasIsNotNull = partitionFilters.exists(_.isInstanceOf[IsNotNull])
        val hasGreaterThan = partitionFilters.exists(_.isInstanceOf[GreaterThan])
        val hasLessThan = partitionFilters.exists(_.isInstanceOf[LessThan])

        Assertions.assertTrue(hasIsNotNull, s"Expected IsNotNull filter in: ${partitionFilters.mkString(", ")}")
        Assertions.assertTrue(hasGreaterThan, s"Expected GreaterThan filter in: ${partitionFilters.mkString(", ")}")
        Assertions.assertTrue(hasLessThan, s"Expected LessThan filter in: ${partitionFilters.mkString(", ")}")
      }

      // Method 2: Extract partition filters by parsing the plan string (fallback if Method 1 doesn't work)
      // Look for "PartitionFilters: [...]" in the execution plan
      val partitionFiltersPattern = """PartitionFilters: \[(.*?)\]""".r
      val partitionFiltersMatch = partitionFiltersPattern.findFirstMatchIn(executionPlanStr)

      partitionFiltersMatch match {
        case Some(m) =>
          val filtersStr = m.group(1)
          // Verify all three expected partition filters are present:
          // 1. isnotnull(datestr)
          // 2. (datestr > 2016-01-01)
          // 3. (datestr < 2016-12-31)
          Assertions.assertTrue(filtersStr.contains("isnotnull(datestr"),
            s"Expected isnotnull filter on datestr in partition filters: $filtersStr")
          Assertions.assertTrue(filtersStr.contains("datestr") && filtersStr.contains("> 2016-01-01"),
            s"Expected greater than filter on datestr in partition filters: $filtersStr")
          Assertions.assertTrue(filtersStr.contains("datestr") && filtersStr.contains("< 2016-12-31"),
            s"Expected less than filter on datestr in partition filters: $filtersStr")

          // Count the filters - should have at least 3 (isnotnull, >, <)
          val filterCount = filtersStr.split(",").length
          Assertions.assertTrue(filterCount >= 3,
            s"Expected at least 3 partition filters but found $filterCount: $filtersStr")

        case None =>
          // If we couldn't extract via string parsing and also couldn't extract programmatically, fail
          if (partitionFilters.isEmpty) {
            fail(s"Could not find PartitionFilters in execution plan: $executionPlanStr")
          }
      }

      // Execute the query and verify only the expected partitions are scanned
      val result = df.collect()

      // Should only return the row with datestr = "2016-06-15" (between 2016-01-01 and 2016-12-31)
      assertEquals(1, result.length, s"Expected 1 row but got ${result.length}")
      assertEquals(2, result(0).getAs[Int]("id"))
      assertEquals("a2", result(0).getAs[String]("name"))
      assertEquals("2016-06-15", result(0).getAs[String]("datestr"))
    }
  }

  /**
   * Helper method to extract partition filters from the physical execution plan.
   * Recursively traverses the plan tree to find scan nodes with partition filters.
   */
  private def extractPartitionFilters(plan: org.apache.spark.sql.execution.SparkPlan): Seq[Expression] = {
    val filters = ArrayBuffer[Expression]()

    // Recursively collect partition filters from the plan tree
    plan.foreach {
      case scan if scan.getClass.getSimpleName.contains("Scan") =>
        // Try to access partitionFilters using reflection since the field name varies across Spark versions
        try {
          val partitionFiltersField = scan.getClass.getMethods.find(_.getName == "partitionFilters")
          partitionFiltersField.foreach { method =>
            val pf = method.invoke(scan).asInstanceOf[Seq[Expression]]
            filters ++= pf
          }
        } catch {
          case _: Exception => // Ignore if we can't access partition filters
        }
      case _ =>
    }
    filters.toSeq
  }
}
