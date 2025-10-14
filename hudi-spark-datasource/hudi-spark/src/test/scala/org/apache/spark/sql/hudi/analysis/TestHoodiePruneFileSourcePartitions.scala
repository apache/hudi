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
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, IsNotNull, Literal}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.types.StringType
import org.junit.jupiter.api.{Assertions, BeforeEach}
import org.junit.jupiter.api.Assertions.{assertEquals, fail}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

import java.util.function.Consumer

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
                assertEquals(1275, Math.ceil(f.stats.sizeInBytes.longValue * 1.0 / 1024))
                assertEquals(1275, Math.ceil(lr.stats.sizeInBytes.longValue * 1.0 / 1024))
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
              assertEquals(1275, Math.ceil(lr.stats.sizeInBytes.longValue * 1.0 / 1024))

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

}
