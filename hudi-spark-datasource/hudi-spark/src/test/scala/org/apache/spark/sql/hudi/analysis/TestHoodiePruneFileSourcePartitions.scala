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
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.junit.jupiter.api.{Assertions, BeforeEach}
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
  @CsvSource(value = Array("cow", "mor"))
  def testPartitionPredicatesPushDown(tableType: String): Unit = {
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
         |  preCombineField = 'ts'
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

      spark.sql(s"SET hoodie.datasource.read.file.index.listing.mode.override=$listingModeOverride")

      val explainCostPlanString = spark.sql(s"EXPLAIN COST SELECT * FROM $tableName WHERE partition = '2021-01-05'")
        .collectAsList()
        .get(0)
        .getString(0)

      listingModeOverride match {
        // Case #1: Eager listing (fallback mode).
        //          Whole table will be _listed_ before partition-pruning would be applied. This is default
        //          Spark behavior that naturally occurs, since predicate push-down for tables w/o appropriate catalog
        //          support (for partition-pruning) will only occur during execution phase, while file-listing
        //          actually happens during analysis stage
        case "eager" =>
          val expectedOptimizedLogicalPlan =
            """== Optimized Logical Plan ==
              |Filter (isnotnull(partition#74) AND (partition#74 = 2021-01-05)), Statistics(sizeInBytes=1275.3 KiB)
              |+- Relation default.hoodie_test[_hoodie_commit_time#65,_hoodie_commit_seqno#66,_hoodie_record_key#67,_hoodie_partition_path#68,_hoodie_file_name#69,id#70,name#71,price#72,ts#73L,partition#74] parquet, Statistics(sizeInBytes=1275.3 KiB)
              |""".stripMargin.trim

          Assertions.assertTrue(explainCostPlanString.contains(expectedOptimizedLogicalPlan))

        // Case #2: Lazy listing (default mode).
        //          In case of lazy listing mode, Hudi will only list partitions matching partition-predicates that are
        //          eagerly pushed down (w/ help of [[HoodiePruneFileSourcePartitions]]) avoiding the necessity to
        //          list the whole table
        case "lazy" =>
          val expectedOptimizedLogicalPlan =
            """== Optimized Logical Plan ==
              |Filter (isnotnull(partition#64) AND (partition#64 = 2021-01-05)), Statistics(sizeInBytes=425.1 KiB)
              |+- Relation default.hoodie_test[_hoodie_commit_time#55,_hoodie_commit_seqno#56,_hoodie_record_key#57,_hoodie_partition_path#58,_hoodie_file_name#59,id#60,name#61,price#62,ts#63L,partition#64] parquet, Statistics(sizeInBytes=425.1 KiB)
              |""".stripMargin.trim

          Assertions.assertTrue(explainCostPlanString.contains(expectedOptimizedLogicalPlan))

        case _ => throw new UnsupportedOperationException()
      }

      val expectedPhysicalPlanPartitionFiltersClause = listingModeOverride match {
        case "eager" => "PartitionFilters: [isnotnull(partition#74), (partition#74 = 2021-01-05)]"
        case "lazy" => "PartitionFilters: [isnotnull(partition#64), (partition#64 = 2021-01-05)]"
      }

      Assertions.assertTrue(explainCostPlanString.contains(expectedPhysicalPlanPartitionFiltersClause))
    }
  }

}
