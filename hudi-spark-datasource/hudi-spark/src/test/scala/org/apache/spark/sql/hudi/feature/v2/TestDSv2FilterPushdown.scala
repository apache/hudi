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

import org.apache.hudi.DataSourceReadOptions
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness.getSparkSqlConf
import org.apache.hudi.testutils.SparkClientFunctionalTestHarnessScala

import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}

/**
 * Functional tests for filter pushdown (partition pruning + data filters) via the DSv2 path.
 */
@Tag("functional")
class TestDSv2FilterPushdown extends SparkClientFunctionalTestHarnessScala with DSv2PlanAssertions {

  override def conf: SparkConf = conf(getSparkSqlConf)

  private def writePartitionedData(path: String, tableName: String): Unit = {
    val _spark = spark
    import _spark.implicits._
    Seq(
      (1, "Alice", 100.0, "US"),
      (2, "Bob", 200.0, "UK"),
      (3, "Charlie", 300.0, "US"),
      (4, "Diana", 150.0, "FR"),
      (5, "Eve", 250.0, "UK")
    ).toDF("id", "name", "amount", "country")
      .write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .option("hoodie.datasource.write.partitionpath.field", "country")
      .mode(SaveMode.Overwrite)
      .save(path)
  }

  @Test
  def testPartitionPruning(): Unit = {
    val path = basePath() + "/filter_part_prune"
    writePartitionedData(path, "filter_part_prune")

    val df = spark.read.format("hudi_v2").load(path).filter("country = 'US'")
    val rows = df.select("id", "name", "country").collect()
      .map(r => (r.getInt(0), r.getString(1), r.getString(2))).sortBy(_._1)

    assertEquals(2, rows.length)
    assertEquals(Seq((1, "Alice", "US"), (3, "Charlie", "US")), rows.toSeq)

    assertBatchScan(df)
  }

  @Test
  def testPartitionPruningViaSql(): Unit = {
    val tableName = "filter_sql_prune"
    val tablePath = basePath() + "/" + tableName
    val confKey = DataSourceReadOptions.USE_V2_READ.key
    try {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      spark.sql(
        s"""CREATE TABLE $tableName (
           |  id INT,
           |  name STRING,
           |  amount DOUBLE,
           |  country STRING
           |) USING hudi
           |TBLPROPERTIES (
           |  type = 'cow',
           |  primaryKey = 'id',
           |  orderingFields = 'amount'
           |)
           |PARTITIONED BY (country)
           |LOCATION '$tablePath'
           """.stripMargin)

      spark.sql(
        s"""INSERT INTO $tableName VALUES
           |(1, 'Alice', 100.0, 'US'),
           |(2, 'Bob', 200.0, 'UK'),
           |(3, 'Charlie', 300.0, 'US')
           """.stripMargin)

      withSQLConf(confKey -> "true") {
        val df = spark.sql(s"SELECT * FROM $tableName WHERE country = 'US'")
        assertEquals(2, df.count())

        val names = df.select("name").collect().map(_.getString(0)).sorted
        assertEquals(Seq("Alice", "Charlie"), names.toSeq)

        assertBatchScan(df)
      }
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  @Test
  def testDataFilterPushdown(): Unit = {
    val path = basePath() + "/filter_data_push"
    val _spark = spark
    import _spark.implicits._

    Seq(
      (1, "Alice", 100.0),
      (2, "Bob", 200.0),
      (3, "Charlie", 300.0),
      (4, "Diana", 400.0),
      (5, "Eve", 500.0)
    ).toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "filter_data_push")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    val df = spark.read.format("hudi_v2").load(path).filter("id > 3")
    val rows = df.select("id", "name").collect()
      .map(r => (r.getInt(0), r.getString(1))).sortBy(_._1)

    assertEquals(2, rows.length)
    assertEquals(Seq((4, "Diana"), (5, "Eve")), rows.toSeq)

    assertBatchScan(df)
  }

  @Test
  def testMixedPartitionAndDataFilters(): Unit = {
    val path = basePath() + "/filter_mixed"
    writePartitionedData(path, "filter_mixed")

    val df = spark.read.format("hudi_v2").load(path)
      .filter("country = 'US' AND name = 'Alice'")
    val rows = df.select("id", "name", "country").collect()
      .map(r => (r.getInt(0), r.getString(1), r.getString(2)))

    assertEquals(1, rows.length)
    assertEquals((1, "Alice", "US"), rows.head)

    assertBatchScan(df)
  }

  @Test
  def testExplainShowsPushedFilters(): Unit = {
    val path = basePath() + "/filter_explain"
    writePartitionedData(path, "filter_explain")

    val df = spark.read.format("hudi_v2").load(path).filter("country = 'US'")
    assertBatchScan(df)
    val plan = df.queryExecution.executedPlan.toString()
    assertTrue(plan.contains("PushedFilters"), s"Expected PushedFilters in plan:\n$plan")
    assertTrue(plan.contains("country"), s"Expected 'country' in pushed filters:\n$plan")
  }

  @Test
  def testMixedReferenceOrFilterNotForwardedToParquet(): Unit = {
    val path = basePath() + "/filter_mixed_or"
    writePartitionedData(path, "filter_mixed_or")

    // country='US' OR id=2 references both a partition column and a data column.
    // Forwarding it to Parquet would make the reader see country as null in base files
    // and could prune row-groups that contain the id=2 row (which lives in the UK
    // partition). Spark must re-apply the full predicate row-wise.
    val filter = "country = 'US' OR id = 2"
    val v2Df = spark.read.format("hudi_v2").load(path).filter(filter)
      .select("id", "name", "country")
    val v2Rows = v2Df.collect()
      .map(r => (r.getInt(0), r.getString(1), r.getString(2))).sortBy(_._1).toSeq

    val v1Df = spark.read.format("hudi").load(path).filter(filter)
      .select("id", "name", "country")
    val v1Rows = v1Df.collect()
      .map(r => (r.getInt(0), r.getString(1), r.getString(2))).sortBy(_._1).toSeq

    assertEquals(v1Rows, v2Rows)
    assertEquals(3, v2Rows.length)
    assertTrue(v2Rows.exists { case (id, _, country) => id == 2 && country == "UK" },
      s"Expected id=2 (UK) row; got $v2Rows")

    assertBatchScan(v2Df)
  }

  @Test
  def testDsv1VsDsv2FilterResults(): Unit = {
    val path = basePath() + "/filter_v1_v2"
    writePartitionedData(path, "filter_v1_v2")

    val filter = "country = 'UK'"
    val v1Df = spark.read.format("hudi").load(path).filter(filter)
      .select("id", "name", "amount", "country")
    val v1Rows = v1Df.collect()
      .map(r => (r.getInt(0), r.getString(1), r.getDouble(2), r.getString(3))).sortBy(_._1)

    val v2Df = spark.read.format("hudi_v2").load(path).filter(filter)
      .select("id", "name", "amount", "country")
    val v2Rows = v2Df.collect()
      .map(r => (r.getInt(0), r.getString(1), r.getDouble(2), r.getString(3))).sortBy(_._1)

    assertEquals(v1Rows.toSeq, v2Rows.toSeq)
    assertEquals(2, v2Rows.length)

    assertFileScan(v1Df)
    assertBatchScan(v2Df)
  }

  @Test
  def testNoFilterScansAllPartitions(): Unit = {
    val path = basePath() + "/filter_no_filter"
    writePartitionedData(path, "filter_no_filter")

    val df = spark.read.format("hudi_v2").load(path)
    assertEquals(5, df.count())

    val countries = df.select("country").distinct().collect().map(_.getString(0)).sorted
    assertEquals(Seq("FR", "UK", "US"), countries.toSeq)

    assertBatchScan(df)
  }

  @Test
  def testInFilterOnPartitionColumn(): Unit = {
    val path = basePath() + "/filter_in"
    writePartitionedData(path, "filter_in")

    val df = spark.read.format("hudi_v2").load(path).filter("country IN ('US', 'UK')")
    assertEquals(4, df.count())

    val countries = df.select("country").distinct().collect().map(_.getString(0)).sorted
    assertEquals(Seq("UK", "US"), countries.toSeq)

    assertBatchScan(df)
  }

  @Test
  def testIsNullFilter(): Unit = {
    val path = basePath() + "/filter_null"
    val _spark = spark
    import _spark.implicits._

    Seq(
      (1, "Alice", "US"),
      (2, "Bob", null: String),
      (3, "Charlie", "UK"),
      (4, "Diana", null: String)
    ).toDF("id", "name", "country")
      .write.format("hudi")
      .option("hoodie.table.name", "filter_null")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "id")
      .mode(SaveMode.Overwrite)
      .save(path)

    val df = spark.read.format("hudi_v2").load(path).filter("country IS NULL")
    assertEquals(2, df.count())

    val ids = df.select("id").collect().map(_.getInt(0)).sorted
    assertEquals(Seq(2, 4), ids.toSeq)

    assertBatchScan(df)
  }
}
