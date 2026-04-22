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
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness.getSparkSqlConf

import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}

/**
 * Functional tests for DSv2 supportability gate: out-of-scope scenarios must fall back
 * to DSv1 (SQL/catalog path) or throw (DataFrame API `format("hudi_v2")` path).
 */
@Tag("functional")
class TestDSv2Fallback extends SparkClientFunctionalTestHarness {

  override def conf: SparkConf = conf(getSparkSqlConf)

  private def containsBatchScan(plan: String): Boolean = plan.contains("BatchScan")

  private def containsFileScan(plan: String): Boolean = plan.contains("FileScan")

  private def explainPlan(sql: String): String =
    spark.sql(s"EXPLAIN $sql").collect().map(_.getString(0)).mkString("\n")

  @Test
  def testMorSnapshotFallsBackToDsv1UnderSqlWithV2Enabled(): Unit = {
    val tableName = "mor_snapshot_fallback"
    val tablePath = basePath() + "/" + tableName
    val useV2Key = DataSourceReadOptions.USE_V2_READ.key
    try {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      spark.sql(
        s"""CREATE TABLE $tableName (
           |  id INT,
           |  name STRING,
           |  amount DOUBLE,
           |  ts LONG
           |) USING hudi
           |TBLPROPERTIES (
           |  type = 'mor',
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           |)
           |LOCATION '$tablePath'
           """.stripMargin)

      spark.sql(s"INSERT INTO $tableName VALUES (1, 'Alice', 100.0, 1), (2, 'Bob', 200.0, 1)")
      // UPDATE on MOR writes to log files; a DSv2 path that dropped log files would see stale values.
      spark.sql(s"UPDATE $tableName SET name = 'Alice2', amount = 150.0, ts = 2 WHERE id = 1")
      spark.sql(s"INSERT INTO $tableName VALUES (3, 'Charlie', 300.0, 2)")

      spark.sessionState.conf.setConfString(useV2Key, "true")
      val plan = explainPlan(s"SELECT * FROM $tableName")
      assertTrue(containsFileScan(plan),
        s"MOR snapshot should fall back to DSv1 FileScan, got:\n$plan")

      val rows = spark.sql(s"SELECT id, name, amount FROM $tableName ORDER BY id").collect()
      assertEquals(3, rows.length)
      assertEquals("Alice2", rows(0).getString(1))
      assertEquals(150.0, rows(0).getDouble(2))
      assertEquals("Bob", rows(1).getString(1))
      assertEquals("Charlie", rows(2).getString(1))
    } finally {
      spark.sessionState.conf.unsetConf(useV2Key)
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  @Test
  def testMorSnapshotRejectsHudiV2DataFrameApi(): Unit = {
    val path = basePath() + "/mor_df_reject"
    val _spark = spark
    import _spark.implicits._

    Seq((1, "Alice", 100.0, 1L), (2, "Bob", 200.0, 1L))
      .toDF("id", "name", "amount", "ts")
      .write.format("hudi")
      .option("hoodie.table.name", "mor_df_reject")
      .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .mode(SaveMode.Overwrite)
      .save(path)

    assertThrows(classOf[HoodieException],
      () => spark.read.format("hudi_v2").load(path).collect())
  }

  @Test
  def testMorReadOptimizedGoesThroughDsv2(): Unit = {
    val path = basePath() + "/mor_read_opt"
    val _spark = spark
    import _spark.implicits._

    Seq((1, "Alice", 100.0, 1L), (2, "Bob", 200.0, 1L), (3, "Charlie", 300.0, 1L))
      .toDF("id", "name", "amount", "ts")
      .write.format("hudi")
      .option("hoodie.table.name", "mor_read_opt")
      .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .mode(SaveMode.Overwrite)
      .save(path)

    val v2Df = spark.read.format("hudi_v2")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
      .load(path)
    val v2Plan = v2Df.queryExecution.executedPlan.toString()
    assertTrue(containsBatchScan(v2Plan),
      s"MOR read_optimized via hudi_v2 should use BatchScan, got:\n$v2Plan")
    assertEquals(3, v2Df.count())

    val v1Names = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
      .load(path)
      .select("name").collect().map(_.getString(0)).sorted
    val v2Names = v2Df.select("name").collect().map(_.getString(0)).sorted
    assertEquals(v1Names.toSeq, v2Names.toSeq)
  }

  @Test
  def testIncrementalQueryRejectsHudiV2DataFrameApi(): Unit = {
    val path = basePath() + "/cow_incremental_reject"
    val _spark = spark
    import _spark.implicits._

    Seq((1, "Alice", 100.0))
      .toDF("id", "name", "amount")
      .write.format("hudi")
      .option("hoodie.table.name", "cow_incremental_reject")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)

    assertThrows(classOf[HoodieException],
      () => spark.read.format("hudi_v2")
        .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
        .load(path).collect())
  }

  @Test
  def testCowSnapshotHappyPathUsesDsv2(): Unit = {
    val tableName = "cow_happy_path"
    val tablePath = basePath() + "/" + tableName
    val useV2Key = DataSourceReadOptions.USE_V2_READ.key
    try {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      spark.sql(
        s"""CREATE TABLE $tableName (
           |  id INT,
           |  name STRING,
           |  amount DOUBLE,
           |  ts LONG
           |) USING hudi
           |TBLPROPERTIES (
           |  type = 'cow',
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           |)
           |LOCATION '$tablePath'
           """.stripMargin)

      spark.sql(s"INSERT INTO $tableName VALUES (1, 'Alice', 100.0, 1), (2, 'Bob', 200.0, 2)")

      spark.sessionState.conf.setConfString(useV2Key, "true")
      val plan = explainPlan(s"SELECT * FROM $tableName")
      assertTrue(containsBatchScan(plan),
        s"COW snapshot with use.v2=true should use BatchScan, got:\n$plan")
      assertEquals(2, spark.sql(s"SELECT * FROM $tableName").count())
    } finally {
      spark.sessionState.conf.unsetConf(useV2Key)
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }
}
