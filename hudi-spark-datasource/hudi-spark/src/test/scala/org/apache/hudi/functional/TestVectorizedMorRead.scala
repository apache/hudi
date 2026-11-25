/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.functional

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness.getSparkSqlConf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.functions.{col, expr, lit}
import org.junit.jupiter.api.{BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

/**
 * Merge-on-read snapshot reads that merge log records into vectorized base file batches return exactly the rows of
 * the row-based merge.
 */
class TestVectorizedMorRead extends SparkClientFunctionalTestHarness {

  private val vectorizedKey = "hoodie.read.mor.vectorized.enabled"

  // The harness shares one base path across the tests of a class, so each test writes its own table under it
  private var tablePath: String = _

  override def conf: SparkConf = conf(getSparkSqlConf)

  @BeforeEach
  def newTablePath(): Unit = {
    tablePath = s"$basePath/table_${System.nanoTime()}"
  }

  private def writeOptions(mergeMode: Option[String]): Map[String, String] = Map(
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "part",
    DataSourceWriteOptions.TABLE_TYPE.key -> DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL,
    "hoodie.table.ordering.fields" -> "ts",
    "hoodie.compact.inline" -> "false",
    HoodieWriteConfig.TBL_NAME.key -> "vectorized_mor",
    "hoodie.insert.shuffle.parallelism" -> "2",
    "hoodie.upsert.shuffle.parallelism" -> "2",
    "hoodie.delete.shuffle.parallelism" -> "1") ++ mergeMode.map("hoodie.write.record.merge.mode" -> _)

  private def rows(n: Int, ts: Long, tag: String): DataFrame = spark.range(n).select(
    expr("concat('k', id)").as("id"),
    expr("concat('p', id % 3)").as("part"),
    lit(ts).as("ts"),
    col("id").cast("int").as("v"),
    expr(s"cast(id * 1.25 as decimal(10, 2))").as("d"),
    expr(s"concat('$tag', id)").as("s"))

  /** Base file, then updates (some with a lower ordering value), deletes and inserts in log files. */
  private def writeTable(mergeMode: String): Unit = {
    val opts = writeOptions(Some(mergeMode))
    rows(100, 10L, "base").write.format("hudi").options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite).save(tablePath)
    // Updates to keys 0..19: even keys with a higher ordering value, odd keys with a lower one
    rows(20, 0L, "upd").withColumn("ts", expr("if(v % 2 = 0, 20L, 5L)"))
      .write.format("hudi").options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append).save(tablePath)
    // Updates to keys 20..24 with the same ordering value as the base rows
    rows(25, 10L, "tie").filter("v >= 20")
      .write.format("hudi").options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append).save(tablePath)
    // Deletes of keys 30..34
    rows(35, 30L, "del").filter("v >= 30")
      .write.format("hudi").options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append).save(tablePath)
    // Inserts of new keys 100..104 and a second update of keys 0..4
    rows(105, 40L, "new").filter("v >= 100 or v < 5")
      .write.format("hudi").options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append).save(tablePath)
  }

  private def read(vectorized: Boolean, select: Seq[String], filter: Option[String]): (Array[Row], Boolean) = {
    spark.conf.set(vectorizedKey, vectorized.toString)
    try {
      val table = spark.read.format("hudi").load(tablePath)
      val df = filter.map(f => table.where(f)).getOrElse(table).selectExpr(select: _*)
      val scan = df.queryExecution.executedPlan.collectFirst { case s: FileSourceScanExec => s }
      val sorted = df.collect().sortBy(_.toString)
      (sorted, scan.exists(_.supportsColumnar))
    } finally {
      spark.conf.unset(vectorizedKey)
    }
  }

  private def assertSameRows(select: Seq[String], expectBatches: Boolean, filter: Option[String] = None): Unit = {
    val (rowPath, rowScanBatches) = read(vectorized = false, select, filter)
    val (vectorizedPath, scanBatches) = read(vectorized = true, select, filter)
    assertFalse(rowScanBatches, "merge-on-read scan returned batches with the vectorized read turned off")
    assertEquals(expectBatches, scanBatches)
    assertEquals(rowPath.toSeq, vectorizedPath.toSeq)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("COMMIT_TIME_ORDERING", "EVENT_TIME_ORDERING"))
  def testVectorizedReadMatchesRowRead(mergeMode: String): Unit = {
    writeTable(mergeMode)
    // Log records matched by record position, which the log blocks carry by default
    spark.conf.set("hoodie.merge.use.record.positions", "true")
    try {
      assertAllShapes()
    } finally {
      spark.conf.unset("hoodie.merge.use.record.positions")
    }
    // Log records matched by record key
    spark.conf.set("hoodie.merge.use.record.positions", "false")
    try {
      assertAllShapes()
    } finally {
      spark.conf.unset("hoodie.merge.use.record.positions")
    }
  }

  private def assertAllShapes(): Unit = {
    // All columns, including meta columns
    assertSameRows(Seq("*"), expectBatches = true)
    // A projection that leaves out the record key and the ordering field
    assertSameRows(Seq("v", "d", "s"), expectBatches = true)
    // Only a partition column
    assertSameRows(Seq("part"), expectBatches = true)
    // A filter on a data column, which an update moves rows into and out of
    assertSameRows(Seq("id", "v", "s"), expectBatches = true, filter = Some("ts >= 10 and v < 50"))
    spark.conf.set(vectorizedKey, "true")
    try {
      // 100 inserted, 5 deleted, 5 new keys
      assertEquals(100L, spark.read.format("hudi").load(tablePath).count())
    } finally {
      spark.conf.unset(vectorizedKey)
    }
  }

  @Test
  def testEventTimeKeepsBaseRowWithHigherOrdering(): Unit = {
    writeTable("EVENT_TIME_ORDERING")
    spark.conf.set(vectorizedKey, "true")
    try {
      val byKey = spark.read.format("hudi").load(tablePath).select("id", "ts", "s").collect()
        .map(r => r.getString(0) -> (r.getLong(1), r.getString(2))).toMap
      // Odd keys 5..19 got an update with a lower ordering value, so the base row stays
      assertEquals((10L, "base7"), byKey("k7"))
      // Even keys 6..18 got a higher ordering value, so the update wins
      assertEquals((20L, "upd8"), byKey("k8"))
      // Keys 0..4 were updated twice; the last update has the highest ordering value
      assertEquals((40L, "new3"), byKey("k3"))
      assertFalse(byKey.contains("k31"))
      assertTrue(byKey.contains("k102"))
    } finally {
      spark.conf.unset(vectorizedKey)
    }
  }

  @Test
  def testCustomPayloadStaysRowBased(): Unit = {
    // A payload that merges column by column needs the full row, so the scan keeps the row-based reader
    val opts = writeOptions(None) +
      ("hoodie.datasource.write.payload.class" -> "org.apache.hudi.common.model.PartialUpdateAvroPayload")
    val nullableS = expr("if(v >= 0, s, cast(null as string))").as("s")
    rows(20, 10L, "base").select(col("id"), col("part"), col("ts"), col("v"), col("d"), nullableS)
      .write.format("hudi").options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite).save(tablePath)
    // Keys 0..4 get a higher ordering value and a null s, which the payload fills from the base row
    rows(5, 20L, "upd").select(col("id"), col("part"), col("ts"), col("v"), col("d"),
      lit(null).cast("string").as("s"))
      .write.format("hudi").options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append).save(tablePath)
    assertSameRows(Seq("id", "ts", "s"), expectBatches = false)
    spark.conf.set(vectorizedKey, "true")
    try {
      val byKey = spark.read.format("hudi").load(tablePath).select("id", "ts", "s").collect()
        .map(r => r.getString(0) -> (r.getLong(1), r.getString(2))).toMap
      assertEquals(20, byKey.size)
      assertEquals((20L, "base3"), byKey("k3"))
      assertEquals((10L, "base7"), byKey("k7"))
    } finally {
      spark.conf.unset(vectorizedKey)
    }
  }

  @Test
  def testNestedColumnStaysRowBased(): Unit = {
    val opts = writeOptions(Some("EVENT_TIME_ORDERING"))
    def nested(n: Int, ts: Long, tag: String): DataFrame = rows(n, ts, tag)
      .withColumn("st", expr("named_struct('a', v, 'b', s)"))
    nested(20, 10L, "base").write.format("hudi").options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite).save(tablePath)
    nested(5, 20L, "upd").write.format("hudi").options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append).save(tablePath)
    // A struct column in the projection keeps the row-based reader; without it the scan returns batches
    assertSameRows(Seq("id", "st", "s"), expectBatches = false)
    assertSameRows(Seq("id", "ts", "d", "s"), expectBatches = true)
  }

  @Test
  def testAddedColumnInLogFiles(): Unit = {
    val opts = writeOptions(Some("EVENT_TIME_ORDERING"))
    rows(20, 10L, "base").write.format("hudi").options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite).save(tablePath)
    // The update adds a nullable column that the base file does not have
    rows(5, 20L, "upd").withColumn("extra", expr("cast(v * 10 as bigint)"))
      .write.format("hudi").options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append).save(tablePath)
    assertSameRows(Seq("*"), expectBatches = true)
    assertSameRows(Seq("id", "extra"), expectBatches = true)
  }
}
