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

package org.apache.spark.sql.hudi.feature.v2

import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness.getSparkSqlConf

import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}

/**
 * Functional tests for limit pushdown, statistics reporting, and aggregate pushdown via DSv2.
 */
@Tag("functional")
class TestDSv2Pushdowns extends SparkClientFunctionalTestHarness {

  override def conf: SparkConf = conf(getSparkSqlConf)

  private def writeTestData(path: String, tableName: String, numRecords: Int = 10): Unit = {
    val _spark = spark
    import _spark.implicits._
    (1 to numRecords).map(i => (i, s"name_$i", i * 100.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)
  }

  private def writePartitionedTestData(path: String, tableName: String): Unit = {
    val _spark = spark
    import _spark.implicits._
    Seq(
      (1, "Alice", 100.0, "US"),
      (2, "Bob", 200.0, "UK"),
      (3, "Charlie", 300.0, "US"),
      (4, "Diana", 150.0, "FR"),
      (5, "Eve", 250.0, "UK"),
      (6, "Frank", 350.0, "US"),
      (7, "Grace", 450.0, "FR")
    ).toDF("id", "name", "amount", "country")
      .write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .option("hoodie.datasource.write.partitionpath.field", "country")
      .mode(SaveMode.Overwrite)
      .save(path)
  }

  // ==========================================================================
  // Limit pushdown tests
  // ==========================================================================

  @Test
  def testLimitReducesRowCount(): Unit = {
    val path = basePath() + "/limit_basic"
    writeTestData(path, "limit_basic")

    val df = spark.read.format("hudi_v2").load(path).limit(3)
    assertEquals(3, df.count())
  }

  @Test
  def testLimitGreaterThanTableSize(): Unit = {
    val path = basePath() + "/limit_greater"
    writeTestData(path, "limit_greater", numRecords = 5)

    val df = spark.read.format("hudi_v2").load(path).limit(100)
    assertEquals(5, df.count())
  }

  @Test
  def testLimitWithFilter(): Unit = {
    val path = basePath() + "/limit_filter"
    writePartitionedTestData(path, "limit_filter")

    val df = spark.read.format("hudi_v2").load(path)
      .filter("country = 'US'")
      .limit(1)
    assertEquals(1, df.count())

    val country = df.select("country").collect().head.getString(0)
    assertEquals("US", country)
  }

  @Test
  def testLimitDsv1VsDsv2(): Unit = {
    val path = basePath() + "/limit_v1_v2"
    writeTestData(path, "limit_v1_v2", numRecords = 10)

    val v1Count = spark.read.format("hudi").load(path).limit(5).count()
    val v2Count = spark.read.format("hudi_v2").load(path).limit(5).count()
    assertEquals(v1Count, v2Count)
    assertEquals(5, v2Count)
  }

  @Test
  def testLimitOne(): Unit = {
    val path = basePath() + "/limit_one"
    writeTestData(path, "limit_one", numRecords = 10)

    val df = spark.read.format("hudi_v2").load(path).limit(1)
    assertEquals(1, df.count())
  }

  @Test
  def testExplainShowsLimitInfo(): Unit = {
    val path = basePath() + "/limit_explain"
    writeTestData(path, "limit_explain", numRecords = 5)

    val df = spark.read.format("hudi_v2").load(path).limit(3)
    val plan = df.queryExecution.executedPlan.toString()
    assertTrue(plan.contains("BatchScan"), s"Expected BatchScan in plan:\n$plan")
    assertTrue(plan.contains("PushedLimit"), s"Expected PushedLimit in plan:\n$plan")
  }

  // ==========================================================================
  // Statistics reporting tests
  // ==========================================================================

  @Test
  def testStatisticsReportsSizeInBytes(): Unit = {
    val path = basePath() + "/stats_size"
    writeTestData(path, "stats_size", numRecords = 10)

    val df = spark.read.format("hudi_v2").load(path)
    val plan = df.queryExecution.optimizedPlan
    val stats = plan.stats

    assertTrue(stats.sizeInBytes > 0, s"Expected positive sizeInBytes, got: ${stats.sizeInBytes}")
  }

  @Test
  def testStatisticsWithNoMatchFilter(): Unit = {
    val path = basePath() + "/stats_no_match"
    val _spark = spark
    import _spark.implicits._

    // Write one row so the table has metadata and a base file
    Seq((1, "Alice", 100.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "stats_no_match")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    // Apply a filter that matches nothing; the table still has physical files
    val df = spark.read.format("hudi_v2").load(path).filter("id < 0")
    val count = df.count()
    assertEquals(0L, count, "Expected zero rows for no-match filter")

    // Statistics should still reflect the underlying file sizes
    val plan = df.queryExecution.optimizedPlan
    val stats = plan.stats
    assertTrue(stats.sizeInBytes >= 0,
      s"Expected non-negative sizeInBytes, got: ${stats.sizeInBytes}")
  }

  // ==========================================================================
  // Aggregate pushdown tests (conditional on column stats availability)
  // ==========================================================================

  @Test
  def testCountStarWithoutColumnStats(): Unit = {
    // Without column stats enabled, COUNT(*) should still return correct result
    // (falls back to scanning all files)
    val path = basePath() + "/count_no_stats"
    writeTestData(path, "count_no_stats", numRecords = 5)

    val count = spark.read.format("hudi_v2").load(path)
      .selectExpr("count(*)").collect().head.getLong(0)
    assertEquals(5, count)
  }

  @Test
  def testCountStarDsv1VsDsv2(): Unit = {
    val path = basePath() + "/count_v1_v2"
    writeTestData(path, "count_v1_v2", numRecords = 8)

    val v1Count = spark.read.format("hudi").load(path)
      .selectExpr("count(*)").collect().head.getLong(0)
    val v2Count = spark.read.format("hudi_v2").load(path)
      .selectExpr("count(*)").collect().head.getLong(0)
    assertEquals(v1Count, v2Count)
  }

  @Test
  def testMinMaxWithoutColumnStats(): Unit = {
    val path = basePath() + "/minmax_no_stats"
    writeTestData(path, "minmax_no_stats", numRecords = 5)

    val row = spark.read.format("hudi_v2").load(path)
      .selectExpr("min(amount)", "max(amount)").collect().head

    assertEquals(100.0, row.getDouble(0))
    assertEquals(500.0, row.getDouble(1))
  }

  @Test
  def testAggregateWithGroupByNotPushed(): Unit = {
    val path = basePath() + "/agg_groupby"
    writePartitionedTestData(path, "agg_groupby")

    // GROUP BY should prevent aggregate pushdown, but still return correct results
    val rows = spark.read.format("hudi_v2").load(path)
      .groupBy("country").count()
      .collect().map(r => (r.getString(0), r.getLong(1))).sortBy(_._1)

    assertEquals("FR", rows(0)._1)
    assertEquals(2, rows(0)._2)
    assertEquals("UK", rows(1)._1)
    assertEquals(2, rows(1)._2)
    assertEquals("US", rows(2)._1)
    assertEquals(3, rows(2)._2)
  }

  @Test
  def testCountWithPartitionFilter(): Unit = {
    val path = basePath() + "/count_part_filter"
    writePartitionedTestData(path, "count_part_filter")

    val count = spark.read.format("hudi_v2").load(path)
      .filter("country = 'US'")
      .selectExpr("count(*)").collect().head.getLong(0)
    assertEquals(3, count)
  }
}
