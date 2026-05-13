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
import org.apache.hudi.common.config.HoodieCommonConfig
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness.getSparkSqlConf
import org.apache.hudi.testutils.SparkClientFunctionalTestHarnessScala

import org.apache.spark.SparkConf
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNull, assertTrue}

/**
 * Functional tests verifying schema-evolved COW reads return correct values.
 *
 * The DataFrame-API path (`spark.read.format("hudi_v2")`) exercises DSv2 end-to-end:
 * the internal schema is fetched from the commit timeline and threaded into the columnar
 * file reader (see [[HoodieScanBuilder]] and [[HoodiePartitionReader]]).
 *
 * The catalog SQL path with `hoodie.schema.on.read.enable=true` routes through
 * [[org.apache.spark.sql.hudi.catalog.HoodieInternalV2Table]] and falls back to a V1
 * FileScan via `V2TableWithV1Fallback` — preserving the schema-evolution DDL rewrite
 * rules — even when `hoodie.datasource.read.use.v2=true` is also set. Reads still return
 * correct values; they just don't use DSv2 BatchScan.
 */
@Tag("functional")
class TestDSv2SchemaEvolution extends SparkClientFunctionalTestHarnessScala with DSv2PlanAssertions {

  override def conf: SparkConf = conf(getSparkSqlConf)

  private val schemaEvolKey = HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key

  private def explainPlan(sql: String): String =
    spark.sql(s"EXPLAIN $sql").collect().map(_.getString(0)).mkString("\n")

  @Test
  def testSchemaEvolvedColumnAddReadUnderCatalogSQL(): Unit = {
    val tableName = "cow_schema_evol_add_col"
    val tablePath = basePath() + "/" + tableName
    val useV2Key = DataSourceReadOptions.USE_V2_READ.key
    try {
      withSQLConf(schemaEvolKey -> "true") {
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

        spark.sql(s"INSERT INTO $tableName VALUES (1, 'Alice', 100.0, 1), (2, 'Bob', 200.0, 1)")
        spark.sql(s"ALTER TABLE $tableName ADD COLUMNS (category STRING)")
        spark.sql(s"INSERT INTO $tableName VALUES (3, 'Charlie', 300.0, 2, 'gold')")

        withSQLConf(useV2Key -> "true") {
          val plan = explainPlan(s"SELECT * FROM $tableName")
          assertTrue(containsFileScan(plan),
            s"Schema evolution should force V1 FileScan even with use.v2=true, got:\n$plan")
          assertFalse(containsBatchScan(plan),
            s"Schema evolution should not produce a DSv2 BatchScan, got:\n$plan")

          val rows = spark.sql(s"SELECT id, name, category FROM $tableName ORDER BY id").collect()
          assertEquals(3, rows.length)
          assertEquals(1, rows(0).getInt(0))
          assertNull(rows(0).get(2), "pre-evolution row should have null for added column")
          assertEquals(2, rows(1).getInt(0))
          assertNull(rows(1).get(2), "pre-evolution row should have null for added column")
          assertEquals(3, rows(2).getInt(0))
          assertEquals("gold", rows(2).getString(2), "post-evolution row should have the added-column value")
        }
      }
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  @Test
  def testSchemaEvolvedTypePromotionReadUnderCatalogSQL(): Unit = {
    val tableName = "cow_schema_evol_type_promo"
    val tablePath = basePath() + "/" + tableName
    val useV2Key = DataSourceReadOptions.USE_V2_READ.key
    try {
      withSQLConf(
        schemaEvolKey -> "true",
        "spark.sql.storeAssignmentPolicy" -> "legacy"
      ) {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
        spark.sql(
          s"""CREATE TABLE $tableName (
             |  id INT,
             |  name STRING,
             |  amount INT,
             |  ts LONG
             |) USING hudi
             |TBLPROPERTIES (
             |  type = 'cow',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             |)
             |LOCATION '$tablePath'
           """.stripMargin)

        spark.sql(s"INSERT INTO $tableName VALUES (1, 'Alice', 100, 1), (2, 'Bob', 200, 1)")
        spark.sql(s"ALTER TABLE $tableName ALTER COLUMN amount TYPE BIGINT")
        spark.sql(s"INSERT INTO $tableName VALUES (3, 'Charlie', 3000000000, 2)")

        withSQLConf(useV2Key -> "true") {
          val plan = explainPlan(s"SELECT * FROM $tableName")
          assertTrue(containsFileScan(plan),
            s"Schema evolution should force V1 FileScan even with use.v2=true, got:\n$plan")
          assertFalse(containsBatchScan(plan),
            s"Schema evolution should not produce a DSv2 BatchScan, got:\n$plan")

          val rows = spark.sql(s"SELECT id, amount FROM $tableName ORDER BY id").collect()
          assertEquals(3, rows.length)
          assertEquals(100L, rows(0).getLong(1))
          assertEquals(200L, rows(1).getLong(1))
          assertEquals(3000000000L, rows(2).getLong(1))

          // Cross-check with use.v2=false — both paths route through the same V1 FileScan.
          withSQLConf(useV2Key -> "false") {
            val v1Rows = spark.sql(s"SELECT id, amount FROM $tableName ORDER BY id").collect()
            assertEquals(rows(0).getLong(1), v1Rows(0).getLong(1))
            assertEquals(rows(1).getLong(1), v1Rows(1).getLong(1))
            assertEquals(rows(2).getLong(1), v1Rows(2).getLong(1))
          }
        }
      }
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  @Test
  def testSchemaEvolvedReadViaHudiV2DataFrameApi(): Unit = {
    val tableName = "cow_schema_evol_df_api"
    val tablePath = basePath() + "/" + tableName
    try {
      withSQLConf(schemaEvolKey -> "true") {
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

        spark.sql(s"INSERT INTO $tableName VALUES (1, 'Alice', 100.0, 1), (2, 'Bob', 200.0, 1)")
        spark.sql(s"ALTER TABLE $tableName ADD COLUMNS (region STRING)")
        spark.sql(s"INSERT INTO $tableName VALUES (3, 'Charlie', 300.0, 2, 'us-west')")

        val v2Df = spark.read.format("hudi_v2")
          .option(schemaEvolKey, "true")
          .load(tablePath)
        assertBatchScan(v2Df)

        val rows = v2Df.orderBy("id").select("id", "region").collect()
        assertEquals(3, rows.length)
        assertNull(rows(0).get(1))
        assertNull(rows(1).get(1))
        assertEquals("us-west", rows(2).getString(1))
      }
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  @Test
  def testTimeTravelReadsPreEvolutionSchema(): Unit = {
    // Time-travel queries on a schema-evolved table must load the internal schema as of
    // the target instant, not the latest. The DSv2 read path (format("hudi_v2") since the
    // catalog SQL path falls back to V1 for schema-evolved tables) previously always
    // fetched the latest internal schema, causing column ID/type mismatches on pre-
    // evolution snapshots.
    val tableName = "cow_schema_evol_time_travel"
    val tablePath = basePath() + "/" + tableName
    try {
      withSQLConf(schemaEvolKey -> "true") {
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

        spark.sql(s"INSERT INTO $tableName VALUES (1, 'Alice', 100.0, 1), (2, 'Bob', 200.0, 1)")

        val metaClient = HoodieTableMetaClient.builder()
          .setBasePath(tablePath)
          .setConf(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf))
          .build()
        val firstInstant = metaClient.getActiveTimeline.filterCompletedInstants()
          .getInstants.get(0).getCompletionTime

        spark.sql(s"ALTER TABLE $tableName ADD COLUMNS (region STRING)")
        spark.sql(s"INSERT INTO $tableName VALUES (3, 'Charlie', 300.0, 2, 'us-west')")

        val v2Df = spark.read.format("hudi_v2")
          .option(schemaEvolKey, "true")
          .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key, firstInstant)
          .load(tablePath)
        assertBatchScan(v2Df)

        // The DataFrame schema must reflect the table at the requested instant, not the
        // latest. Before the fix, columns added after the instant (here `region`) leaked
        // into the schema and let Spark project them against pre-evolution Parquet files.
        val v2DfFields = v2Df.schema.fieldNames.toSet
        assertTrue(v2DfFields.contains("id") && v2DfFields.contains("name")
          && v2DfFields.contains("amount") && v2DfFields.contains("ts"),
          s"Time-travel schema missing original columns: ${v2Df.schema.fieldNames.mkString(",")}")
        assertFalse(v2DfFields.contains("region"),
          s"Time-travel schema must not expose post-evolution column `region`: " +
            v2Df.schema.fieldNames.mkString(","))

        // Pre-evolution snapshot: only the two original rows from the first commit must
        // surface, projected with their original values. Before the fix, the reader used
        // the latest internal schema against pre-evolution Parquet files, which could
        // raise column-ID mismatch errors on schemas that require ID-aware projection.
        val rows = v2Df.orderBy("id").select("id", "name", "amount").collect()
        assertEquals(2, rows.length)
        assertEquals(1, rows(0).getInt(0))
        assertEquals("Alice", rows(0).getString(1))
        assertEquals(100.0, rows(0).getDouble(2))
        assertEquals(2, rows(1).getInt(0))
        assertEquals("Bob", rows(1).getString(1))
        assertEquals(200.0, rows(1).getDouble(2))
      }
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }
}
