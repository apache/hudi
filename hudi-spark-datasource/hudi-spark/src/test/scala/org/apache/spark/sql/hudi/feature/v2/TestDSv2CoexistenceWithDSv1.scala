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
class TestDSv2CoexistenceWithDSv1 extends SparkClientFunctionalTestHarness with DSv2PlanAssertions {

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

  @Test
  def testDSv2WriteToExistingMorTableFallsBackToV1(): Unit = {
    val path = basePath() + "/v2_write_existing_mor"
    val _spark = spark
    import _spark.implicits._

    // Create an existing MOR table first.
    Seq((1, "Alice", 100.0, 1L)).toDF("id", "name", "amount", "ts")
      .write.format("hudi")
      .option("hoodie.table.name", "v2_write_existing_mor")
      .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .mode(SaveMode.Overwrite)
      .save(path)

    // A subsequent write via hudi_v2 must fall through to V1 createRelation instead of
    // throwing at getTable — MOR is not DSv2-readable, but the V1 writer handles it.
    Seq((2, "Bob", 200.0, 2L)).toDF("id", "name", "amount", "ts")
      .write.format("hudi_v2")
      .option("hoodie.table.name", "v2_write_existing_mor")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .mode(SaveMode.Append)
      .save(path)

    // Read back via DSv1 (MOR snapshot is not DSv2-readable).
    assertEquals(2, spark.read.format("hudi").load(path).count())
  }

  // ---- DataFrame API read tests ----

  @Test
  def testBaselineDSv1WriteAndRead(): Unit = {
    val path = basePath() + "/baseline_v1"
    writeTestData(path)

    val df = spark.read.format("hudi").load(path)
    assertEquals(3, df.count())

    assertFileScan(df)
  }

  @Test
  def testDSv1WriteAndDSv2Read(): Unit = {
    val path = basePath() + "/v1_write_v2_read"
    writeTestData(path)

    val df = spark.read.format("hudi_v2").load(path)
    assertEquals(3, df.count())

    val names = df.select("name").collect().map(_.getString(0)).sorted
    assertEquals(Seq("Alice", "Bob", "Charlie"), names.toSeq)

    assertBatchScan(df)
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

    assertFileScan(df)
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

    assertBatchScan(df)
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

      assertFileScan(df)
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

      assertBatchScan(df)
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

      assertBatchScan(v2Df)
    } finally {
      spark.sessionState.conf.unsetConf(DataSourceReadOptions.USE_V2_READ.key)
    }

    // Switch back to v1 — should also see real data
    spark.sessionState.conf.setConfString(DataSourceReadOptions.USE_V2_READ.key, "false")
    try {
      val v1Df = spark.sql(s"SELECT * FROM $tableName")
      assertEquals(3, v1Df.count())

      assertFileScan(v1Df)
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

    // With schema evolution enabled and v2 read disabled, loadTable() returns
    // HoodieInternalV2Table. It does not implement SupportsRead, so reads fall back
    // to V1 via V2TableWithV1Fallback and use HoodieFileGroupReaderBasedFileFormat —
    // a Hudi-aware V1 FileScan, NOT DSv2 BatchScan.
    spark.sessionState.conf.setConfString("hoodie.schema.on.read.enable", "true")
    spark.sessionState.conf.setConfString(DataSourceReadOptions.USE_V2_READ.key, "false")
    try {
      val explainRows = spark.sql(s"EXPLAIN SELECT * FROM $tableName").collect()
      val plan = explainRows.map(_.getString(0)).mkString("\n")
      assertTrue(containsFileScan(plan),
        s"HoodieInternalV2Table should use V1 FileScan, but got:\n$plan")
      assertTrue(plan.contains("HoodieFileGroupReaderBasedFileFormat") || plan.contains("HudiFileGroup"),
        s"Plan should use Hudi-aware V1 file format, proving HoodieInternalV2Table was used:\n$plan")
      assertTrue(!containsBatchScan(plan),
        s"HoodieInternalV2Table should not produce a DSv2 BatchScan, but got:\n$plan")

      val df = spark.sql(s"SELECT * FROM $tableName")
      assertEquals(1, df.count())
    } finally {
      spark.sessionState.conf.unsetConf("hoodie.schema.on.read.enable")
      spark.sessionState.conf.unsetConf(DataSourceReadOptions.USE_V2_READ.key)
    }

    // With both schema evolution and v2 read enabled, schema evolution takes precedence:
    // loadTable() returns HoodieInternalV2Table so the Spark3xResolveHudiAlterTableCommand
    // rewrite rules keep matching DROP COLUMN / RENAME COLUMN, and reads fall back to the
    // V1 schema-evolution-aware FileScan via V2TableWithV1Fallback.
    spark.sessionState.conf.setConfString("hoodie.schema.on.read.enable", "true")
    spark.sessionState.conf.setConfString(DataSourceReadOptions.USE_V2_READ.key, "true")
    try {
      val explainRows = spark.sql(s"EXPLAIN SELECT * FROM $tableName").collect()
      val plan = explainRows.map(_.getString(0)).mkString("\n")
      assertTrue(containsFileScan(plan),
        s"Schema evolution should force V1 FileScan even with use.v2=true, got:\n$plan")
      assertTrue(!containsBatchScan(plan),
        s"Schema evolution should not produce a DSv2 BatchScan, got:\n$plan")

      val df = spark.sql(s"SELECT * FROM $tableName")
      assertEquals(1, df.count())
    } finally {
      spark.sessionState.conf.unsetConf("hoodie.schema.on.read.enable")
      spark.sessionState.conf.unsetConf(DataSourceReadOptions.USE_V2_READ.key)
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  @Test
  def testSchemaEvolutionDdlWithV2ReadEnabled(): Unit = {
    val tableName = "schema_evol_v2_ddl"
    val tablePath = basePath() + "/" + tableName

    // Schema-on-read must be enabled before CREATE TABLE so the internal-schema
    // bookkeeping is set up from the start. DROP COLUMN also needs
    // auto.evolution.column.drop so Hudi's write-time schema validation accepts the
    // removal.
    spark.sessionState.conf.setConfString("hoodie.schema.on.read.enable", "true")
    spark.sessionState.conf.setConfString(
      "hoodie.datasource.write.schema.allow.auto.evolution.column.drop", "true")
    spark.sessionState.conf.setConfString(DataSourceReadOptions.USE_V2_READ.key, "true")
    try {
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

      spark.sql(s"INSERT INTO $tableName VALUES (1, 'Alice', 100.0, 1)")

      // Both DROP COLUMN and RENAME COLUMN must still be rewritten to Hudi commands
      // by Spark3xResolveHudiAlterTableCommand — which only matches HoodieInternalV2Table.
      spark.sql(s"ALTER TABLE $tableName DROP COLUMN amount")
      val afterDrop = spark.sql(s"DESCRIBE $tableName")
        .select("col_name").collect().map(_.getString(0))
      assertTrue(!afterDrop.contains("amount"),
        s"DROP COLUMN should have removed 'amount', got: ${afterDrop.mkString(", ")}")

      spark.sql(s"ALTER TABLE $tableName RENAME COLUMN name TO full_name")
      val afterRename = spark.sql(s"DESCRIBE $tableName")
        .select("col_name").collect().map(_.getString(0))
      assertTrue(afterRename.contains("full_name"),
        s"RENAME COLUMN should have renamed 'name' to 'full_name', got: ${afterRename.mkString(", ")}")
    } finally {
      spark.sessionState.conf.unsetConf("hoodie.schema.on.read.enable")
      spark.sessionState.conf.unsetConf(
        "hoodie.datasource.write.schema.allow.auto.evolution.column.drop")
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
