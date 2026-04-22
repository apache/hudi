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
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNull, assertTrue}

/**
 * Functional tests verifying DSv2 reads return correct values on schema-evolved COW
 * tables. The internal schema is fetched from the commit timeline and threaded into
 * the columnar file reader (see [[HoodieScanBuilder]] and [[HoodiePartitionReader]]).
 */
@Tag("functional")
class TestDSv2SchemaEvolution extends SparkClientFunctionalTestHarness {

  override def conf: SparkConf = conf(getSparkSqlConf)

  private def containsBatchScan(plan: String): Boolean = plan.contains("BatchScan")

  private def explainPlan(sql: String): String =
    spark.sql(s"EXPLAIN $sql").collect().map(_.getString(0)).mkString("\n")

  @Test
  def testSchemaEvolvedColumnAddReadViaDSv2(): Unit = {
    val tableName = "cow_schema_evol_add_col"
    val tablePath = basePath() + "/" + tableName
    val useV2Key = DataSourceReadOptions.USE_V2_READ.key
    val schemaEvolKey = "hoodie.schema.on.read.enable"
    try {
      spark.sessionState.conf.setConfString(schemaEvolKey, "true")
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

      spark.sessionState.conf.setConfString(useV2Key, "true")
      val plan = explainPlan(s"SELECT * FROM $tableName")
      assertTrue(containsBatchScan(plan),
        s"Schema-evolved COW read via DSv2 should use BatchScan, got:\n$plan")

      val rows = spark.sql(s"SELECT id, name, category FROM $tableName ORDER BY id").collect()
      assertEquals(3, rows.length)
      assertEquals(1, rows(0).getInt(0))
      assertNull(rows(0).get(2), "pre-evolution row should have null for added column")
      assertEquals(2, rows(1).getInt(0))
      assertNull(rows(1).get(2), "pre-evolution row should have null for added column")
      assertEquals(3, rows(2).getInt(0))
      assertEquals("gold", rows(2).getString(2), "post-evolution row should have the added-column value")
    } finally {
      spark.sessionState.conf.unsetConf(useV2Key)
      spark.sessionState.conf.unsetConf(schemaEvolKey)
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  @Test
  def testSchemaEvolvedTypePromotionReadViaDSv2(): Unit = {
    val tableName = "cow_schema_evol_type_promo"
    val tablePath = basePath() + "/" + tableName
    val useV2Key = DataSourceReadOptions.USE_V2_READ.key
    val schemaEvolKey = "hoodie.schema.on.read.enable"
    try {
      spark.sessionState.conf.setConfString(schemaEvolKey, "true")
      spark.sessionState.conf.setConfString("spark.sql.storeAssignmentPolicy", "legacy")
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

      spark.sessionState.conf.setConfString(useV2Key, "true")
      val plan = explainPlan(s"SELECT * FROM $tableName")
      assertTrue(containsBatchScan(plan),
        s"Schema-evolved COW read via DSv2 should use BatchScan, got:\n$plan")

      val rows = spark.sql(s"SELECT id, amount FROM $tableName ORDER BY id").collect()
      assertEquals(3, rows.length)
      assertEquals(100L, rows(0).getLong(1))
      assertEquals(200L, rows(1).getLong(1))
      assertEquals(3000000000L, rows(2).getLong(1))

      // Cross-check with DSv1
      spark.sessionState.conf.setConfString(useV2Key, "false")
      val v1Rows = spark.sql(s"SELECT id, amount FROM $tableName ORDER BY id").collect()
      assertEquals(rows(0).getLong(1), v1Rows(0).getLong(1))
      assertEquals(rows(1).getLong(1), v1Rows(1).getLong(1))
      assertEquals(rows(2).getLong(1), v1Rows(2).getLong(1))
    } finally {
      spark.sessionState.conf.unsetConf(useV2Key)
      spark.sessionState.conf.unsetConf(schemaEvolKey)
      spark.sessionState.conf.unsetConf("spark.sql.storeAssignmentPolicy")
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  @Test
  def testSchemaEvolvedReadViaHudiV2DataFrameApi(): Unit = {
    val tableName = "cow_schema_evol_df_api"
    val tablePath = basePath() + "/" + tableName
    val schemaEvolKey = "hoodie.schema.on.read.enable"
    try {
      spark.sessionState.conf.setConfString(schemaEvolKey, "true")
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
      val v2Plan = v2Df.queryExecution.executedPlan.toString()
      assertTrue(containsBatchScan(v2Plan),
        s"DataFrame API schema-evolved read via hudi_v2 should use BatchScan, got:\n$v2Plan")

      val rows = v2Df.orderBy("id").select("id", "region").collect()
      assertEquals(3, rows.length)
      assertNull(rows(0).get(1))
      assertNull(rows(1).get(1))
      assertEquals("us-west", rows(2).getString(1))
    } finally {
      spark.sessionState.conf.unsetConf(schemaEvolKey)
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }
}
