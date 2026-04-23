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
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness.getSparkSqlConf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.col
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
  def testCowIncrementalViaSqlConfFallsBackToDsv1(): Unit = {
    val tableName = "cow_incr_sql_conf"
    val tablePath = basePath() + "/" + tableName
    val useV2Key = DataSourceReadOptions.USE_V2_READ.key
    val queryTypeKey = DataSourceReadOptions.QUERY_TYPE.key
    val startCommitKey = DataSourceReadOptions.START_COMMIT.key
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
           |  preCombineField = 'ts',
           |  'hoodie.parquet.small.file.limit' = '0'
           |)
           |LOCATION '$tablePath'
           """.stripMargin)

      spark.sql(s"INSERT INTO $tableName VALUES (1, 'Alice', 100.0, 1)")
      val metaClient = HoodieTableMetaClient.builder()
        .setBasePath(tablePath)
        .setConf(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf()))
        .build()
      val firstCommitCompletionTime = metaClient.getActiveTimeline
        .filterCompletedInstants().lastInstant().get().getCompletionTime
      spark.sql(s"INSERT INTO $tableName VALUES (2, 'Bob', 200.0, 2)")

      spark.sessionState.conf.setConfString(useV2Key, "true")
      spark.sessionState.conf.setConfString(queryTypeKey,
        DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      spark.sessionState.conf.setConfString(startCommitKey, firstCommitCompletionTime)
      spark.catalog.refreshTable(tableName)

      val plan = explainPlan(s"SELECT * FROM $tableName")
      assertTrue(!containsBatchScan(plan),
        s"COW incremental driven by SQL conf must fall back to DSv1 (not BatchScan), got:\n$plan")

      val rows = spark.sql(s"SELECT id, name FROM $tableName").collect()
      assertEquals(1, rows.length,
        "Incremental read must only return rows from the second commit")
      assertEquals(2, rows(0).getInt(0))
      assertEquals("Bob", rows(0).getString(1))
    } finally {
      spark.sessionState.conf.unsetConf(useV2Key)
      spark.sessionState.conf.unsetConf(queryTypeKey)
      spark.sessionState.conf.unsetConf(startCommitKey)
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  @Test
  def testMorReadOptimizedViaSqlConfUsesDsv2(): Unit = {
    val tableName = "mor_ro_sql_conf"
    val tablePath = basePath() + "/" + tableName
    val useV2Key = DataSourceReadOptions.USE_V2_READ.key
    val queryTypeKey = DataSourceReadOptions.QUERY_TYPE.key
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
      // UPDATE writes log files on MOR; read_optimized must skip them.
      spark.sql(s"UPDATE $tableName SET name = 'Alice2', amount = 150.0, ts = 2 WHERE id = 1")

      spark.sessionState.conf.setConfString(useV2Key, "true")
      spark.sessionState.conf.setConfString(queryTypeKey,
        DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
      spark.catalog.refreshTable(tableName)

      val plan = explainPlan(s"SELECT * FROM $tableName")
      assertTrue(containsBatchScan(plan),
        s"MOR read_optimized driven by SQL conf should use DSv2 BatchScan, got:\n$plan")

      val rows = spark.sql(s"SELECT id, name, amount FROM $tableName ORDER BY id").collect()
      assertEquals(2, rows.length)
      assertEquals("Alice", rows(0).getString(1))
      assertEquals(100.0, rows(0).getDouble(2))
      assertEquals("Bob", rows(1).getString(1))
    } finally {
      spark.sessionState.conf.unsetConf(useV2Key)
      spark.sessionState.conf.unsetConf(queryTypeKey)
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  @Test
  def testStorageOptionsReadOptimizedRoutesToDsv2(): Unit = {
    // Exercises the gate's view of CatalogTable.storage.properties — a hoodie read option
    // persisted there (as CREATE TABLE ... OPTIONS(...) can on non-Hudi catalogs) must
    // drive the V1/V2 decision in loadTable, matching HoodieSparkV2Table.properties()
    // which merges storage.properties ++ properties.
    val tableName = "mor_storage_options_ro"
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
      // UPDATE writes log files on MOR; read_optimized must skip them.
      spark.sql(s"UPDATE $tableName SET name = 'Alice2', ts = 2 WHERE id = 1")

      val sessionCatalog = spark.sessionState.catalog
      val tableIdt = TableIdentifier(tableName)
      val catalogTable = sessionCatalog.getTableMetadata(tableIdt)
      val newStorage = catalogTable.storage.copy(
        properties = catalogTable.storage.properties +
          (DataSourceReadOptions.QUERY_TYPE.key ->
            DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL))
      sessionCatalog.alterTable(catalogTable.copy(storage = newStorage))
      spark.catalog.refreshTable(tableName)

      spark.sessionState.conf.setConfString(useV2Key, "true")
      val plan = explainPlan(s"SELECT * FROM $tableName")
      assertTrue(containsBatchScan(plan),
        s"read_optimized in storage.properties should route to DSv2 BatchScan, got:\n$plan")

      val rows = spark.sql(s"SELECT id, name FROM $tableName ORDER BY id").collect()
      assertEquals(2, rows.length)
      assertEquals("Alice", rows(0).getString(1))
      assertEquals("Bob", rows(1).getString(1))
    } finally {
      spark.sessionState.conf.unsetConf(useV2Key)
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  @Test
  def testStorageOptionsIncrementalFallsBackToDsv1(): Unit = {
    // Mirror of the above: an incremental query type placed in storage.properties must
    // be honored by the gate and fall back to V1 instead of reaching newScanBuilder and
    // throwing.
    val tableName = "cow_storage_options_incr"
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
      spark.sql(s"INSERT INTO $tableName VALUES (1, 'Alice', 100.0, 1)")
      val metaClient = HoodieTableMetaClient.builder()
        .setBasePath(tablePath)
        .setConf(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf()))
        .build()
      val firstCommitCompletionTime = metaClient.getActiveTimeline
        .filterCompletedInstants().lastInstant().get().getCompletionTime
      spark.sql(s"INSERT INTO $tableName VALUES (2, 'Bob', 200.0, 2)")

      val sessionCatalog = spark.sessionState.catalog
      val tableIdt = TableIdentifier(tableName)
      val catalogTable = sessionCatalog.getTableMetadata(tableIdt)
      val newStorage = catalogTable.storage.copy(
        properties = catalogTable.storage.properties +
          (DataSourceReadOptions.QUERY_TYPE.key ->
            DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL) +
          (DataSourceReadOptions.START_COMMIT.key -> firstCommitCompletionTime))
      sessionCatalog.alterTable(catalogTable.copy(storage = newStorage))
      spark.catalog.refreshTable(tableName)

      spark.sessionState.conf.setConfString(useV2Key, "true")
      val plan = explainPlan(s"SELECT * FROM $tableName")
      assertTrue(!containsBatchScan(plan),
        s"incremental in storage.properties must fall back to DSv1 (not BatchScan), got:\n$plan")
    } finally {
      spark.sessionState.conf.unsetConf(useV2Key)
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  @Test
  def testWriteToOverwriteByFilterRejected(): Unit = {
    // OVERWRITE_BY_FILTER is not advertised because HoodieV1WriteBuilder can't honor a
    // filter expression — it only knows full-table or full-partition overwrite. Spark
    // must therefore reject df.writeTo(tbl).overwrite(expr) at analysis time rather than
    // silently rewriting extra rows.
    val tableName = "cow_overwrite_filter_reject"
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
      spark.sql(s"INSERT INTO $tableName VALUES (1, 'Alice', 100.0, 1), (2, 'Bob', 200.0, 1)")

      spark.sessionState.conf.setConfString(useV2Key, "true")
      spark.catalog.refreshTable(tableName)

      val _spark = spark
      import _spark.implicits._
      val replacement = Seq((1, "Alice2", 150.0, 2L))
        .toDF("id", "name", "amount", "ts")

      assertThrows(classOf[AnalysisException],
        () => replacement.writeTo(tableName).overwrite(col("id") === 1))
    } finally {
      spark.sessionState.conf.unsetConf(useV2Key)
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  @Test
  def testInsertOverwritePartitionFallsBackToV1(): Unit = {
    // INSERT OVERWRITE ... PARTITION (...) targets a HoodieSparkV2Table when use.v2=true;
    // HoodieSparkV2Table does not advertise OVERWRITE_BY_FILTER, so a V2 writer would
    // reject the statement. The Spark{33,34,35,40}DataSourceV2ToV1Fallback rule converts
    // the InsertIntoStatement to a V1 relation so the existing V1 partition-overwrite path
    // (InsertIntoHoodieTableCommand) handles it.
    val tableName = "cow_insert_overwrite_partition_fallback"
    val tablePath = basePath() + "/" + tableName
    val useV2Key = DataSourceReadOptions.USE_V2_READ.key
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
        s"""INSERT INTO $tableName PARTITION (country='US') VALUES
           |(1, 'Alice', 100.0),
           |(2, 'Bob', 200.0)
           """.stripMargin)
      spark.sql(s"INSERT INTO $tableName PARTITION (country='UK') VALUES (3, 'Charlie', 300.0)")

      spark.sessionState.conf.setConfString(useV2Key, "true")
      spark.catalog.refreshTable(tableName)

      // Overwrite only the US partition with a new row; UK must be preserved.
      spark.sql(
        s"INSERT OVERWRITE TABLE $tableName PARTITION (country='US') VALUES (4, 'Dana', 400.0)")

      val rows = spark.sql(s"SELECT id, name, country FROM $tableName ORDER BY id")
        .collect()
        .map(r => (r.getInt(0), r.getString(1), r.getString(2)))
      assertEquals(2, rows.length)
      assertEquals((3, "Charlie", "UK"), rows(0))
      assertEquals((4, "Dana", "US"), rows(1))
    } finally {
      spark.sessionState.conf.unsetConf(useV2Key)
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  @Test
  def testCowPartitionedReadValuesMatchDsv1(): Unit = {
    // Default DSv1 behavior (extract.partition.values.from.path=false, drop.partition.columns=false)
    // reads partition column values from the base files. The DSv2 path must match: it cannot
    // unconditionally derive values from the parsed partition path, since for STRING partition
    // values containing characters that get URL-encoded in the path (e.g. spaces, multi-byte
    // chars) the path-derived form differs from what was actually written. Parity with DSv1 is
    // the safest cross-check.
    val tableName = "cow_part_values_default"
    val tablePath = basePath() + "/" + tableName
    val useV2Key = DataSourceReadOptions.USE_V2_READ.key
    try {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      spark.sql(
        s"""CREATE TABLE $tableName (
           |  id INT,
           |  name STRING,
           |  amount DOUBLE,
           |  region STRING
           |) USING hudi
           |TBLPROPERTIES (
           |  type = 'cow',
           |  primaryKey = 'id',
           |  preCombineField = 'amount'
           |)
           |PARTITIONED BY (region)
           |LOCATION '$tablePath'
           """.stripMargin)
      // Mix of normal and URL-sensitive partition values: a space-containing string surfaces
      // the encode/decode round-trip discrepancy between path values and stored values.
      spark.sql(
        s"""INSERT INTO $tableName VALUES
           |(1, 'Alice',   100.0, 'EU West'),
           |(2, 'Bob',     200.0, 'US'),
           |(3, 'Charlie', 300.0, 'EU West'),
           |(4, 'Dana',    400.0, 'AP/SE')
           """.stripMargin)

      // Read via V1 first to get the source-of-truth values.
      spark.sessionState.conf.unsetConf(useV2Key)
      spark.catalog.refreshTable(tableName)
      val v1Rows = spark.sql(s"SELECT id, region FROM $tableName ORDER BY id")
        .collect().map(r => (r.getInt(0), r.getString(1)))

      spark.sessionState.conf.setConfString(useV2Key, "true")
      spark.catalog.refreshTable(tableName)
      val plan = explainPlan(s"SELECT id, region FROM $tableName")
      assertTrue(containsBatchScan(plan),
        s"Partitioned COW snapshot with use.v2=true should use BatchScan, got:\n$plan")
      val v2Rows = spark.sql(s"SELECT id, region FROM $tableName ORDER BY id")
        .collect().map(r => (r.getInt(0), r.getString(1)))

      assertEquals(v1Rows.toSeq, v2Rows.toSeq,
        "DSv2 partition column values must match DSv1 values exactly")
    } finally {
      spark.sessionState.conf.unsetConf(useV2Key)
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  @Test
  def testCowPartitionedDropPartitionColumnsExtractsFromPath(): Unit = {
    // When the table was written with hoodie.datasource.write.drop.partition.columns=true,
    // partition columns are NOT in the base files; values must come from the parsed path.
    // Verify DSv2 takes that branch and returns correct values.
    val tableName = "cow_part_values_drop_cols"
    val tablePath = basePath() + "/" + tableName
    val useV2Key = DataSourceReadOptions.USE_V2_READ.key
    val _spark = spark
    import _spark.implicits._
    try {
      Seq(
        (1, "Alice",   100.0, "US"),
        (2, "Bob",     200.0, "UK"),
        (3, "Charlie", 300.0, "US"))
        .toDF("id", "name", "amount", "region")
        .write.format("hudi")
        .option("hoodie.table.name", tableName)
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.datasource.write.precombine.field", "amount")
        .option("hoodie.datasource.write.partitionpath.field", "region")
        .option("hoodie.datasource.write.drop.partition.columns", "true")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      spark.sql(
        s"""CREATE TABLE $tableName USING hudi LOCATION '$tablePath'""")

      spark.sessionState.conf.setConfString(useV2Key, "true")
      spark.catalog.refreshTable(tableName)

      val rows = spark.sql(s"SELECT id, region FROM $tableName ORDER BY id")
        .collect().map(r => (r.getInt(0), r.getString(1)))
      assertEquals(Seq((1, "US"), (2, "UK"), (3, "US")), rows.toSeq)
    } finally {
      spark.sessionState.conf.unsetConf(useV2Key)
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  @Test
  def testCowSelectMetaFieldsViaDsv2(): Unit = {
    // With populate.meta.fields=true (default), Hudi writes _hoodie_commit_time and
    // _hoodie_record_key into every base file. The DSv2 schema must surface them so
    // queries that reference them analyze and execute, matching DSv1 behavior.
    val tableName = "cow_meta_fields_visible"
    val tablePath = basePath() + "/" + tableName
    val useV2Key = DataSourceReadOptions.USE_V2_READ.key
    try {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      spark.sql(
        s"""CREATE TABLE $tableName (
           |  id INT,
           |  name STRING,
           |  ts LONG
           |) USING hudi
           |TBLPROPERTIES (
           |  type = 'cow',
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           |)
           |LOCATION '$tablePath'
           """.stripMargin)
      spark.sql(s"INSERT INTO $tableName VALUES (1, 'Alice', 1), (2, 'Bob', 2)")

      spark.sessionState.conf.setConfString(useV2Key, "true")
      spark.catalog.refreshTable(tableName)
      val plan = explainPlan(s"SELECT _hoodie_commit_time, _hoodie_record_key, id FROM $tableName")
      assertTrue(containsBatchScan(plan),
        s"SELECT meta-fields with use.v2=true should still use BatchScan, got:\n$plan")

      val rows = spark.sql(
        s"SELECT _hoodie_commit_time, _hoodie_record_key, id FROM $tableName ORDER BY id")
        .collect()
      assertEquals(2, rows.length)
      assertTrue(rows(0).getString(0) != null && rows(0).getString(0).nonEmpty,
        "_hoodie_commit_time must be populated")
      // Exact record-key encoding depends on the key generator in effect; we only need
      // to confirm the column was actually read from the base file.
      val recordKey0 = rows(0).getString(1)
      assertTrue(recordKey0 != null && recordKey0.contains("1"),
        s"_hoodie_record_key for id=1 must reference 1, got: $recordKey0")
      assertEquals(1, rows(0).getInt(2))
      val recordKey1 = rows(1).getString(1)
      assertTrue(recordKey1 != null && recordKey1.contains("2"),
        s"_hoodie_record_key for id=2 must reference 2, got: $recordKey1")
    } finally {
      spark.sessionState.conf.unsetConf(useV2Key)
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  @Test
  def testCowInsertIntoStillWorksWithMetaFieldsExposed(): Unit = {
    // Regression guard: exposing meta-fields in HoodieSparkV2Table.schema must not break
    // SQL INSERT INTO. The V1 fallback rule converts InsertIntoStatement → LogicalRelation
    // and Hudi's InsertIntoHoodieTableCommand realigns user data to the table schema; the
    // ACCEPT_ANY_SCHEMA capability prevents Spark from arity-checking the user query
    // against the meta-field-augmented output.
    val tableName = "cow_insert_with_meta_visible"
    val tablePath = basePath() + "/" + tableName
    val useV2Key = DataSourceReadOptions.USE_V2_READ.key
    try {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      spark.sql(
        s"""CREATE TABLE $tableName (
           |  id INT,
           |  name STRING,
           |  ts LONG
           |) USING hudi
           |TBLPROPERTIES (
           |  type = 'cow',
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           |)
           |LOCATION '$tablePath'
           """.stripMargin)

      spark.sessionState.conf.setConfString(useV2Key, "true")
      spark.catalog.refreshTable(tableName)
      spark.sql(s"INSERT INTO $tableName VALUES (1, 'Alice', 1), (2, 'Bob', 2)")
      spark.sql(s"INSERT INTO $tableName VALUES (3, 'Charlie', 3)")

      val rows = spark.sql(s"SELECT id, name FROM $tableName ORDER BY id")
        .collect().map(r => (r.getInt(0), r.getString(1)))
      assertEquals(Seq((1, "Alice"), (2, "Bob"), (3, "Charlie")), rows.toSeq)
    } finally {
      spark.sessionState.conf.unsetConf(useV2Key)
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  @Test
  def testCowVirtualKeysDoesNotExposeMetaFields(): Unit = {
    // When populate.meta.fields=false, base files have no meta columns. The DSv2 schema
    // must reflect that and not surface them, otherwise SELECT * would fabricate columns
    // that don't exist on disk.
    val tableName = "cow_virtual_keys_no_meta"
    val tablePath = basePath() + "/" + tableName
    val useV2Key = DataSourceReadOptions.USE_V2_READ.key
    val _spark = spark
    import _spark.implicits._
    try {
      Seq((1, "Alice", 100.0), (2, "Bob", 200.0))
        .toDF("id", "name", "amount")
        .write.format("hudi")
        .option("hoodie.table.name", tableName)
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.datasource.write.precombine.field", "amount")
        .option("hoodie.populate.meta.fields", "false")
        .option("hoodie.datasource.write.keygenerator.class",
          "org.apache.hudi.keygen.NonpartitionedKeyGenerator")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      spark.sql(s"CREATE TABLE $tableName USING hudi LOCATION '$tablePath'")

      spark.sessionState.conf.setConfString(useV2Key, "true")
      spark.catalog.refreshTable(tableName)
      val cols = spark.sql(s"SELECT * FROM $tableName").columns.toSet
      assertTrue(!cols.contains("_hoodie_commit_time"),
        s"Virtual-keys table must not expose _hoodie_commit_time; got columns=$cols")
      assertTrue(!cols.contains("_hoodie_record_key"),
        s"Virtual-keys table must not expose _hoodie_record_key; got columns=$cols")

      val rows = spark.sql(s"SELECT id, name FROM $tableName ORDER BY id")
        .collect().map(r => (r.getInt(0), r.getString(1)))
      assertEquals(Seq((1, "Alice"), (2, "Bob")), rows.toSeq)
    } finally {
      spark.sessionState.conf.unsetConf(useV2Key)
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
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
