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
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness.getSparkSqlConf

import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}

/**
 * Functional tests proving DSv2 and DSv1 coexistence.
 *
 * DataFrame API tests use `format("hudi_v2")` to activate the DSv2 path.
 * SQL/Catalog tests use `hoodie.datasource.read.use.v2=true` to route through
 * [[org.apache.spark.sql.hudi.catalog.HoodieCatalog.loadTable]] -> [[HoodieSparkV2Table]].
 */
@Tag("functional")
class TestDSv2CoexistenceWithDSv1 extends SparkClientFunctionalTestHarness {

  override def conf: SparkConf = conf(getSparkSqlConf)

  private def writeTestData(path: String): Unit = {
    val _spark = spark
    import _spark.implicits._
    val df = Seq(
      (1, "Alice", 100.0),
      (2, "Bob", 200.0),
      (3, "Charlie", 300.0)
    ).toDF("id", "name", "amount")

    df.write.format("hudi")
      .option("hoodie.table.name", "test_table")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Overwrite)
      .save(path)
  }

  private def containsBatchScan(plan: String): Boolean = plan.contains("BatchScan")

  private def containsFileScan(plan: String): Boolean = plan.contains("FileScan")

  // ---- DataFrame API write tests ----

  @Test
  def testDSv2WriteViaDataFrameAPI(): Unit = {
    val path = basePath() + "/v2_write_df_api"
    val _spark = spark
    import _spark.implicits._
    val df = Seq(
      (1, "Alice", 100.0),
      (2, "Bob", 200.0),
      (3, "Charlie", 300.0)
    ).toDF("id", "name", "amount")

    df.write.format("hudi_v2")
      .option("hoodie.table.name", "test_v2_write")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "amount")
      .mode(SaveMode.Append)
      .save(path)

    // Read back via DSv1 to verify data was written correctly
    val result = spark.read.format("hudi").load(path)
    assertEquals(3, result.count())

    val names = result.select("name").collect().map(_.getString(0)).sorted
    assertEquals(Array("Alice", "Bob", "Charlie").toSeq, names.toSeq)
  }

  // ---- DataFrame API read tests ----

  @Test
  def testBaselineDSv1WriteAndRead(): Unit = {
    val path = basePath() + "/baseline_v1"
    writeTestData(path)

    val df = spark.read.format("hudi").load(path)
    assertEquals(3, df.count())

    val plan = df.queryExecution.executedPlan.toString()
    assertTrue(containsFileScan(plan),
      s"DSv1 read should use FileScan, but plan was:\n$plan")
  }

  @Test
  def testDSv1WriteAndDSv2Read(): Unit = {
    val path = basePath() + "/v1_write_v2_read"
    writeTestData(path)

    val df = spark.read.format("hudi_v2").load(path)
    assertEquals(3, df.count())

    val names = df.select("name").collect().map(_.getString(0)).sorted
    assertEquals(Seq("Alice", "Bob", "Charlie"), names.toSeq)

    val plan = df.queryExecution.executedPlan.toString()
    assertTrue(containsBatchScan(plan),
      s"DSv2 read should use BatchScan, but plan was:\n$plan")
  }

  @Test
  def testDSv1WriteAndDSv1ReadAfterDSv2Read(): Unit = {
    val path = basePath() + "/v1_write_both_read"
    writeTestData(path)

    // DSv2 read returns real data
    val v2Df = spark.read.format("hudi_v2").load(path)
    assertEquals(3, v2Df.count())

    // DSv1 read still works after DSv2 read
    val df = spark.read.format("hudi").load(path)
    assertEquals(3, df.count())

    val plan = df.queryExecution.executedPlan.toString()
    assertTrue(containsFileScan(plan),
      s"DSv1 read should use FileScan, but plan was:\n$plan")
  }

  @Test
  def testDSv2ReadSchemaAndPlan(): Unit = {
    val path = basePath() + "/v2_read_schema"
    writeTestData(path)

    val df = spark.read.format("hudi_v2").load(path)
    assertEquals(3, df.count())

    // Verify schema is correct
    val fieldNames = df.schema.fieldNames
    assertTrue(fieldNames.contains("id"), s"Schema should contain 'id', got: ${fieldNames.mkString(", ")}")
    assertTrue(fieldNames.contains("name"), s"Schema should contain 'name', got: ${fieldNames.mkString(", ")}")
    assertTrue(fieldNames.contains("amount"), s"Schema should contain 'amount', got: ${fieldNames.mkString(", ")}")

    // Verify data values
    val names = df.select("name").collect().map(_.getString(0)).sorted
    assertEquals(Seq("Alice", "Bob", "Charlie"), names.toSeq)

    val plan = df.queryExecution.executedPlan.toString()
    assertTrue(containsBatchScan(plan),
      s"DSv2 read should use BatchScan, but plan was:\n$plan")
  }

  // ---- SQL/Catalog tests ----

  @Test
  def testSqlConfigFalseUsesDSv1(): Unit = {
    val tableName = "sql_v1_test"
    val tablePath = basePath() + "/" + tableName
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
         |  orderingFields = 'ts'
         |)
         |LOCATION '$tablePath'
         """.stripMargin)

    spark.sql(s"INSERT INTO $tableName VALUES (1, 'Alice', 100.0, 1), (2, 'Bob', 200.0, 2), (3, 'Charlie', 300.0, 3)")

    spark.sessionState.conf.setConfString(DataSourceReadOptions.USE_V2_READ.key, "false")
    try {
      val df = spark.sql(s"SELECT * FROM $tableName")
      assertEquals(3, df.count())

      val plan = df.queryExecution.executedPlan.toString()
      assertTrue(containsFileScan(plan),
        s"With use.v2=false, should use FileScan, but plan was:\n$plan")
    } finally {
      spark.sessionState.conf.unsetConf(DataSourceReadOptions.USE_V2_READ.key)
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  @Test
  def testSqlConfigTrueUsesDSv2(): Unit = {
    val tableName = "sql_v2_test"
    val tablePath = basePath() + "/" + tableName
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
         |  orderingFields = 'ts'
         |)
         |LOCATION '$tablePath'
         """.stripMargin)

    spark.sql(s"INSERT INTO $tableName VALUES (1, 'Alice', 100.0, 1), (2, 'Bob', 200.0, 2), (3, 'Charlie', 300.0, 3)")

    spark.sessionState.conf.setConfString(DataSourceReadOptions.USE_V2_READ.key, "true")
    try {
      val df = spark.sql(s"SELECT * FROM $tableName")
      assertEquals(3, df.count())

      val names = df.select("name").collect().map(_.getString(0)).sorted
      assertEquals(Seq("Alice", "Bob", "Charlie"), names.toSeq)

      val plan = df.queryExecution.executedPlan.toString()
      assertTrue(containsBatchScan(plan),
        s"With use.v2=true, should use BatchScan, but plan was:\n$plan")
    } finally {
      spark.sessionState.conf.unsetConf(DataSourceReadOptions.USE_V2_READ.key)
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  @Test
  def testSqlSwitchBetweenV1AndV2Reads(): Unit = {
    val tableName = "sql_switch_read_test"
    val tablePath = basePath() + "/" + tableName
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
         |  orderingFields = 'ts'
         |)
         |LOCATION '$tablePath'
         """.stripMargin)

    // Write with v1 (default)
    spark.sql(s"INSERT INTO $tableName VALUES (1, 'Alice', 100.0, 1), (2, 'Bob', 200.0, 2), (3, 'Charlie', 300.0, 3)")

    // Read with v2 — returns real data
    spark.sessionState.conf.setConfString(DataSourceReadOptions.USE_V2_READ.key, "true")
    try {
      val v2Df = spark.sql(s"SELECT * FROM $tableName")
      assertEquals(3, v2Df.count())

      val v2Plan = v2Df.queryExecution.executedPlan.toString()
      assertTrue(containsBatchScan(v2Plan),
        s"V2 read should use BatchScan, but plan was:\n$v2Plan")
    } finally {
      spark.sessionState.conf.unsetConf(DataSourceReadOptions.USE_V2_READ.key)
    }

    // Switch back to v1 — should also see real data
    spark.sessionState.conf.setConfString(DataSourceReadOptions.USE_V2_READ.key, "false")
    try {
      val v1Df = spark.sql(s"SELECT * FROM $tableName")
      assertEquals(3, v1Df.count())

      val v1Plan = v1Df.queryExecution.executedPlan.toString()
      assertTrue(containsFileScan(v1Plan),
        s"V1 read should use FileScan, but plan was:\n$v1Plan")
    } finally {
      spark.sessionState.conf.unsetConf(DataSourceReadOptions.USE_V2_READ.key)
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  @Test
  def testSqlInsertWithV2ReadEnabled(): Unit = {
    val tableName = "sql_v2_insert_test"
    val tablePath = basePath() + "/" + tableName
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
         |  orderingFields = 'ts'
         |)
         |LOCATION '$tablePath'
         """.stripMargin)

    // Enable V2 read BEFORE inserting — write should still work via V1 fallback
    spark.sessionState.conf.setConfString(DataSourceReadOptions.USE_V2_READ.key, "true")
    try {
      spark.sql(s"INSERT INTO $tableName VALUES (1, 'Alice', 100.0, 1), (2, 'Bob', 200.0, 2)")

      // Verify data was written by reading back via V1
      spark.sessionState.conf.setConfString(DataSourceReadOptions.USE_V2_READ.key, "false")
      val df = spark.sql(s"SELECT * FROM $tableName")
      assertEquals(2, df.count())
    } finally {
      spark.sessionState.conf.unsetConf(DataSourceReadOptions.USE_V2_READ.key)
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  @Test
  def testDdlOperationsWithV2ReadEnabled(): Unit = {
    val tableName = "sql_v2_ddl_test"
    val tablePath = basePath() + "/" + tableName
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
         |  orderingFields = 'ts'
         |)
         |LOCATION '$tablePath'
         """.stripMargin)

    spark.sql(s"INSERT INTO $tableName VALUES (1, 'Alice', 100.0, 1)")

    spark.sessionState.conf.setConfString(DataSourceReadOptions.USE_V2_READ.key, "true")
    try {
      // ALTER TABLE should work with V2 read enabled
      spark.sql(s"ALTER TABLE $tableName ADD COLUMNS (extra STRING)")

      // Verify the column was added by checking schema
      val df = spark.sql(s"DESCRIBE $tableName")
      val colNames = df.select("col_name").collect().map(_.getString(0))
      assertTrue(colNames.contains("extra"),
        s"ALTER TABLE ADD COLUMNS should work with v2 read enabled, columns: ${colNames.mkString(", ")}")

      // DROP TABLE should work with V2 read enabled
      spark.sql(s"DROP TABLE $tableName")

      // Verify table was dropped
      val tables = spark.sql("SHOW TABLES").collect().map(_.getString(1))
      assertTrue(!tables.contains(tableName),
        s"DROP TABLE should work with v2 read enabled, tables: ${tables.mkString(", ")}")
    } finally {
      spark.sessionState.conf.unsetConf(DataSourceReadOptions.USE_V2_READ.key)
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  @Test
  def testSchemaEvolutionPathUnaffectedByV2Config(): Unit = {
    val tableName = "sql_schema_evol_test"
    val tablePath = basePath() + "/" + tableName
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
         |  orderingFields = 'ts'
         |)
         |LOCATION '$tablePath'
         """.stripMargin)

    spark.sql(s"INSERT INTO $tableName VALUES (1, 'Alice', 100.0, 1)")

    // With schema evolution enabled and v2 read disabled, the loadTable()
    // should return HoodieInternalV2Table (schema evolution path), not V1Table.
    // Verify via EXPLAIN that the plan uses BatchScan (V2 path), not FileScan (V1 path).
    spark.sessionState.conf.setConfString("hoodie.schema.on.read.enable", "true")
    spark.sessionState.conf.setConfString(DataSourceReadOptions.USE_V2_READ.key, "false")
    try {
      val explainRows = spark.sql(s"EXPLAIN SELECT * FROM $tableName").collect()
      val plan = explainRows.map(_.getString(0)).mkString("\n")
      assertTrue(containsBatchScan(plan),
        s"Schema evolution path should use BatchScan (V2), but got:\n$plan")

      val df = spark.sql(s"SELECT * FROM $tableName")
      assertEquals(1, df.count())
    } finally {
      spark.sessionState.conf.unsetConf("hoodie.schema.on.read.enable")
      spark.sessionState.conf.unsetConf(DataSourceReadOptions.USE_V2_READ.key)
    }

    // With both schema evolution and v2 read enabled, the loadTable() should
    // return HoodieSparkV2Table (DSv2 path takes precedence over schema evolution).
    spark.sessionState.conf.setConfString("hoodie.schema.on.read.enable", "true")
    spark.sessionState.conf.setConfString(DataSourceReadOptions.USE_V2_READ.key, "true")
    try {
      val explainRows = spark.sql(s"EXPLAIN SELECT * FROM $tableName").collect()
      val plan = explainRows.map(_.getString(0)).mkString("\n")
      assertTrue(containsBatchScan(plan),
        s"V2 read with schema evolution should use BatchScan (V2), but got:\n$plan")

      val df = spark.sql(s"SELECT * FROM $tableName")
      assertEquals(1, df.count())
    } finally {
      spark.sessionState.conf.unsetConf("hoodie.schema.on.read.enable")
      spark.sessionState.conf.unsetConf(DataSourceReadOptions.USE_V2_READ.key)
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  @Test
  def testExplainShowsDSv2(): Unit = {
    val tableName = "sql_explain_test"
    val tablePath = basePath() + "/" + tableName
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
         |  orderingFields = 'ts'
         |)
         |LOCATION '$tablePath'
         """.stripMargin)

    spark.sql(s"INSERT INTO $tableName VALUES (1, 'Alice', 100.0, 1)")

    // Verify V1 EXPLAIN shows FileScan
    spark.sessionState.conf.setConfString(DataSourceReadOptions.USE_V2_READ.key, "false")
    try {
      val v1ExplainRows = spark.sql(s"EXPLAIN SELECT * FROM $tableName").collect()
      val v1Plan = v1ExplainRows.map(_.getString(0)).mkString("\n")
      assertTrue(containsFileScan(v1Plan),
        s"V1 EXPLAIN should contain FileScan, but got:\n$v1Plan")
    } finally {
      spark.sessionState.conf.unsetConf(DataSourceReadOptions.USE_V2_READ.key)
    }

    // Verify V2 EXPLAIN shows BatchScan
    spark.sessionState.conf.setConfString(DataSourceReadOptions.USE_V2_READ.key, "true")
    try {
      val v2ExplainRows = spark.sql(s"EXPLAIN SELECT * FROM $tableName").collect()
      val v2Plan = v2ExplainRows.map(_.getString(0)).mkString("\n")
      assertTrue(containsBatchScan(v2Plan),
        s"V2 EXPLAIN should contain BatchScan, but got:\n$v2Plan")
    } finally {
      spark.sessionState.conf.unsetConf(DataSourceReadOptions.USE_V2_READ.key)
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }
}
