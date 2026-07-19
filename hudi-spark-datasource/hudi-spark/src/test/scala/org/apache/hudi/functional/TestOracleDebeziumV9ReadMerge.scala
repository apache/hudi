/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.functional

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions.{OPERATION, ORDERING_FIELDS, RECORDKEY_FIELD, TABLE_TYPE}
import org.apache.hudi.common.model.debezium.OracleDebeziumAvroPayload
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness

import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNull, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

/**
 * End-to-end validation that an Oracle Debezium CDC table created on table version 9 merges via the
 * built-in EVENT_TIME_ORDERING + FILL_UNCHANGED partial-update strategy (driven by the
 * _changed_columns list) rather than the payload, and that unchanged columns are preserved. Covers
 * MOR snapshot read (asserting log files are actually created), MOR inline compaction, COW write-time
 * merge, deletes, nested metadata, the disjoint-update union, and the guard that rejects incompatible
 * partial-update (schema-partial) writes against a FILL_UNCHANGED table. _event_ordering is the ordering field.
 */
class TestOracleDebeziumV9ReadMerge extends SparkClientFunctionalTestHarness {

  private val cols = Seq("id", "name", "amount", "_changed_columns", "_hoodie_is_deleted", "_event_ordering")

  private def row(id: Int, name: String, amount: Long, changed: String, ordering: String): DataFrame =
    spark.createDataFrame(Seq((id, name, amount, changed, false, ordering))).toDF(cols: _*)

  /** Create the v9 table with the Oracle payload (which the v9 config inference maps to
   * EVENT_TIME_ORDERING + FILL_UNCHANGED). Defaults to MOR; pass COW to exercise the write-time merge. */
  private def createV9Table(frame: DataFrame, table: String,
                            tableType: String = DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL): Unit =
    baseWriter(frame, table, tableType)
      .option(OPERATION.key(), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "9")
      .option(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key(), classOf[OracleDebeziumAvroPayload].getName)
      .mode(SaveMode.Overwrite)
      .save(basePath)

  private def upsert(frame: DataFrame, table: String,
                     tableType: String = DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL): Unit =
    baseWriter(frame, table, tableType)
      .option(OPERATION.key(), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

  private def baseWriter(frame: DataFrame, table: String,
                         tableType: String = DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL) =
    frame.write.format("hudi")
      .option(RECORDKEY_FIELD.key(), "id")
      .option(ORDERING_FIELDS.key(), "_event_ordering")
      .option(TABLE_TYPE.key(), tableType)
      .option(DataSourceWriteOptions.TABLE_NAME.key(), table)
      .option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false")

  private def readRow(id: Int): Row =
    spark.read.format("hudi").load(basePath).select("id", "name", "amount").where(s"id = $id").collect()(0)

  private def ord(n: Int): String = "00000000000000000000." + "%020d".format(n)

  /** Count MOR delta-commit log files under the (non-partitioned) table base path. Used to prove a
   * test actually exercised the merge-on-read path rather than passing vacuously on a base-only read. */
  private def logFileCount(): Int = {
    val storage = org.apache.hudi.common.table.HoodieTableMetaClient.builder()
      .setConf(org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf)
      .setBasePath(basePath).build().getStorage
    import scala.collection.JavaConverters._
    storage.listDirectEntries(new org.apache.hudi.storage.StoragePath(basePath)).asScala
      .count(_.getPath.getName.contains(".log."))
  }

  @Test
  def v9OracleChangedColumnsPreservesUnchangedColumns(): Unit = {
    createV9Table(row(1, "alice", 100L, null, ord(100)), "oracle_v9_test")
    // Only `name` changed; amount carries a zero-value placeholder that must NOT win.
    upsert(row(1, "bob", 0L, "name", ord(200)), "oracle_v9_test")

    // The upsert must land as a log file so this exercises the merge-on-read path (not a base-only read).
    assertTrue(logFileCount() >= 1, "MOR upsert must create at least one log file")
    val out = readRow(1)
    assertEquals("bob", out.getAs[String]("name"), "name is in _changed_columns -> takes the update")
    assertEquals(100L, out.getAs[Long]("amount"),
      "amount is NOT in _changed_columns -> preserves the prior value, not the placeholder 0")
  }

  @Test
  def v9OracleChangedColumnsPreservesUnchangedColumnsCow(): Unit = {
    // COW applies the partial merge at WRITE time (no log files); verify FILL_UNCHANGED still preserves
    // the unchanged column when the merge runs on the write path rather than the read path.
    val cow = DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL
    createV9Table(row(1, "alice", 100L, null, ord(100)), "oracle_v9_cow", cow)
    upsert(row(1, "bob", 0L, "name", ord(200)), "oracle_v9_cow", cow)

    assertEquals(0, logFileCount(), "COW must not create log files (merge happens on the write path)")
    val out = readRow(1)
    assertEquals("bob", out.getAs[String]("name"), "name is in _changed_columns -> takes the update (COW)")
    assertEquals(100L, out.getAs[Long]("amount"),
      "amount is NOT in _changed_columns -> preserves the prior value through the COW write-time merge")
  }

  @Test
  def v9OracleUnchangedNullableColumnStaysNull(): Unit = {
    createV9Table(row(1, null, 100L, null, ord(100)), "oracle_v9_test2")
    // Update changes only amount; name is unchanged (not listed) and was stored null -> stays null.
    upsert(row(1, "placeholder", 200L, "amount", ord(200)), "oracle_v9_test2")

    val out = readRow(1)
    assertEquals(200L, out.getAs[Long]("amount"), "amount is in _changed_columns -> takes the update")
    assertNull(out.getAs[String]("name"),
      "name is NOT in _changed_columns and was stored null -> stays null (no fallback to placeholder)")
  }

  @Test
  def v9OracleDisjointUpdatesUnionChangedColumns(): Unit = {
    // Two uncompacted log updates change different columns. The reader combines them log-vs-log
    // (deltaMerge) into one buffered record before merging against the base, so the buffered record's
    // _changed_columns must be the UNION (name,amount) or the base merge drops the column changed only
    // by the older update. Discriminating test for the union fix.
    createV9Table(row(1, "alice", 100L, null, ord(100)), "oracle_v9_test3")
    upsert(row(1, "bob", 0L, "name", ord(200)), "oracle_v9_test3") // only name changed
    upsert(row(1, "placeholder", 200L, "amount", ord(300)), "oracle_v9_test3") // only amount changed

    // Two uncompacted log files are required for the log-vs-log deltaMerge this test targets; without
    // them (e.g. if the 2nd upsert compacted) the union path would never be exercised.
    assertTrue(logFileCount() >= 2, "the disjoint-update test needs >=2 uncompacted log files")
    val out = readRow(1)
    assertEquals("bob", out.getAs[String]("name"),
      "name was changed only by update1 -> union of changed-columns preserves it through the base merge")
    assertEquals(200L, out.getAs[Long]("amount"), "amount was changed by update2")
  }

  @Test
  def v9OraclePartialUpdateWriteRejectedOnFillUnchangedTable(): Unit = {
    // Guard: a FILL_UNCHANGED table requires full-schema writes. A partial-update write
    // (WRITE_PARTIAL_UPDATE_SCHEMA, as Spark MERGE INTO with a partial UPDATE SET would emit) writes a
    // schema-partial IS_PARTIAL log block, which would flip the reader to KEEP_VALUES and silently drop
    // the changed-columns merge. The write must be rejected up front instead.
    createV9Table(row(1, "alice", 100L, null, ord(100)), "oracle_v9_guard")
    val partialSchema =
      "{\"type\":\"record\",\"name\":\"p\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}," +
        "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null}]}"
    val ex = assertThrows(classOf[Throwable], () =>
      baseWriter(row(1, "bob", 0L, "name", ord(200)), "oracle_v9_guard")
        .option(OPERATION.key(), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
        .option(HoodieWriteConfig.WRITE_PARTIAL_UPDATE_SCHEMA.key(), partialSchema)
        .mode(SaveMode.Append)
        .save(basePath))
    val flattened = Iterator.iterate[Throwable](ex)(_.getCause).takeWhile(_ != null).map(_.getMessage).mkString(" | ")
    assertTrue(flattened != null && flattened.contains("FILL_UNCHANGED"),
      s"expected a FILL_UNCHANGED-incompatibility error, got: $flattened")
  }

  @Test
  def v9OracleSchemaEvolutionAddColumnPreservesUnchanged(): Unit = {
    // Schema evolution x FILL_UNCHANGED: the base has no `email` column; an evolved update adds it and
    // changes only email. The pre-existing unchanged columns (name, amount) must still be preserved from
    // the base (not overwritten by the update's placeholders), and the newly added changed column must
    // take the incoming value. reconcileChangedColumns iterates the incoming schema and reads unchanged
    // cols from the (older) base schema, so this exercises a base != incoming schema merge.
    val schemaOnRead = "hoodie.schema.on.read.enable"
    baseWriter(row(1, "alice", 100L, null, ord(100)), "oracle_v9_evolve")
      .option(OPERATION.key(), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "9")
      .option(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key(), classOf[OracleDebeziumAvroPayload].getName)
      .option(schemaOnRead, "true")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val evolvedCols =
      Seq("id", "name", "amount", "_changed_columns", "_hoodie_is_deleted", "_event_ordering", "email")
    val evolved = spark.createDataFrame(Seq((1, "placeholder", 0L, "email", false, ord(200), "new@x")))
      .toDF(evolvedCols: _*)
    baseWriter(evolved, "oracle_v9_evolve")
      .option(OPERATION.key(), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option(schemaOnRead, "true")
      .mode(SaveMode.Append)
      .save(basePath)

    val out = spark.read.format("hudi").load(basePath)
      .select("id", "name", "amount", "email").where("id = 1").collect()(0)
    assertEquals("alice", out.getAs[String]("name"),
      "unchanged name preserved across an add-column schema evolution (not the update's placeholder)")
    assertEquals(100L, out.getAs[Long]("amount"),
      "unchanged amount preserved across an add-column schema evolution")
    assertEquals("new@x", out.getAs[String]("email"),
      "the newly added column, listed in _changed_columns, takes the incoming value")
  }

  // --- delete: exercises the configured DELETE_KEY (_change_operation_type) / DELETE_MARKER ("d")
  //     reader path (distinct from the update merge). ---
  private val delCols =
    Seq("id", "name", "amount", "_change_operation_type", "_changed_columns", "_hoodie_is_deleted", "_event_ordering")

  private def delRow(id: Int, name: String, amount: Long, op: String, deleted: Boolean, ordering: String): DataFrame =
    spark.createDataFrame(Seq((id, name, amount, op, null.asInstanceOf[String], deleted, ordering))).toDF(delCols: _*)

  @Test
  def v9OracleDeleteRemovesRow(): Unit = {
    createV9Table(delRow(1, "alice", 100L, "c", false, ord(100)), "oracle_v9_del")
    // A newer op='d' delete event must drop the row on read.
    upsert(delRow(1, "alice", 100L, "d", true, ord(200)), "oracle_v9_del")

    val count = spark.read.format("hudi").load(basePath).where("id = 1").count()
    assertEquals(0L, count, "a newer op='d' delete event removes the row on a v9 MOR read")
  }

  @Test
  def v9OracleUnchangedColumnPreservedAfterCompaction(): Unit = {
    createV9Table(row(1, "alice", 100L, null, ord(100)), "oracle_v9_compact")
    // Force inline compaction so the log update is merged into the base; the FILL_UNCHANGED merge must
    // preserve the unchanged column in the compacted base, not only at snapshot-read time.
    upsertCompacting(row(1, "bob", 0L, "name", ord(200)), "oracle_v9_compact")

    val out = readRow(1)
    assertEquals("bob", out.getAs[String]("name"), "changed name applied through compaction")
    assertEquals(100L, out.getAs[Long]("amount"), "unchanged amount preserved in the compacted base")
  }

  private def upsertCompacting(frame: DataFrame, table: String): Unit =
    baseWriter(frame, table)
      .option(OPERATION.key(), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option(HoodieCompactionConfig.INLINE_COMPACT.key(), "true")
      .option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "1")
      .mode(SaveMode.Append)
      .save(basePath)

  // --- nested metadata: scn/commit_scn grouped under a _debezium_metadata struct (a retain field);
  //     the partial merge must still work with the struct column present. ---
  private val nestedCols =
    Seq("id", "name", "amount", "_changed_columns", "_hoodie_is_deleted", "_event_ordering", "_debezium_metadata")

  private def nestedRow(id: Int, name: String, amount: Long, changed: String, ordering: String,
                        scn: Long, commitScn: Long): DataFrame =
    spark.createDataFrame(Seq((id, name, amount, changed, false, ordering, (scn, commitScn)))).toDF(nestedCols: _*)

  @Test
  def v9OracleMergeWorksWithNestedMetadataStruct(): Unit = {
    createV9Table(nestedRow(1, "alice", 100L, null, ord(100), 100L, 200L), "oracle_v9_nested")
    upsert(nestedRow(1, "bob", 0L, "name", ord(200), 101L, 201L), "oracle_v9_nested")

    val out = readRow(1)
    assertEquals("bob", out.getAs[String]("name"), "changed name applied with a nested metadata struct present")
    assertEquals(100L, out.getAs[Long]("amount"),
      "unchanged amount preserved (the _debezium_metadata struct retain field doesn't break the merge)")
  }

  // ---------------------------------------------------------------------------------------------
  // Property / differential harness: generate random Oracle-CDC event sequences (an insert followed
  // by updates that change random, often disjoint, column subsets), run each through three merge
  // paths -- MOR snapshot (log-vs-log deltaMerge), MOR with inline compaction, and COW write-time
  // merge -- and diff all three against a tiny reference merge model. The reference applies
  // FILL_UNCHANGED semantics directly: changed columns take the incoming value, unchanged columns
  // keep the prior value, in _event_ordering order. Any path disagreeing with the reference (or with
  // each other) is a real merge bug. This would have caught the union bug (a column changed only by
  // an earlier update dropped by the base merge) and any COW/MOR path divergence by construction.
  // ---------------------------------------------------------------------------------------------
  private val propCols = Seq("id", "v1", "v2", "v3", "_changed_columns", "_hoodie_is_deleted", "_event_ordering")
  private val propDataCols = Seq("v1", "v2", "v3")
  private val propPlaceholder = -999L // an unchanged column's incoming value; must never win over the prior value

  private def propRow(v1: Long, v2: Long, v3: Long, changed: String, ordering: String): DataFrame =
    spark.createDataFrame(Seq((1, v1, v2, v3, changed, false, ordering))).toDF(propCols: _*)

  private def propBaseWriter(frame: DataFrame, tableType: String) =
    frame.write.format("hudi")
      .option(RECORDKEY_FIELD.key(), "id")
      .option(ORDERING_FIELDS.key(), "_event_ordering")
      .option(TABLE_TYPE.key(), tableType)
      .option(DataSourceWriteOptions.TABLE_NAME.key(), "prop")

  private def writeSequence(path: String, tableType: String, compact: Boolean,
                            base: Map[String, Long], updates: Seq[(Set[String], Map[String, Long], String)]): Unit = {
    propBaseWriter(propRow(base("v1"), base("v2"), base("v3"), null, ord(100)), tableType)
      .option(OPERATION.key(), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "9")
      .option(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key(), classOf[OracleDebeziumAvroPayload].getName)
      .option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false")
      .mode(SaveMode.Overwrite)
      .save(path)
    updates.foreach { case (subset, newVals, ordering) =>
      def cell(c: String): Long = if (subset.contains(c)) newVals(c) else propPlaceholder
      var w = propBaseWriter(propRow(cell("v1"), cell("v2"), cell("v3"), subset.toSeq.sorted.mkString(","), ordering), tableType)
        .option(OPERATION.key(), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
        .option(HoodieCompactionConfig.INLINE_COMPACT.key(), String.valueOf(compact))
      if (compact) {
        w = w.option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "1")
      }
      w.mode(SaveMode.Append).save(path)
    }
  }

  @Test
  def v9OracleDifferentialRandomSequencesAcrossMergePaths(): Unit = {
    val rnd = new scala.util.Random(20260719L) // fixed seed -> reproducible
    val numSeqs = 8
    for (i <- 0 until numSeqs) {
      val base = propDataCols.map(c => c -> (1L + rnd.nextInt(1000))).toMap
      val k = 2 + rnd.nextInt(3) // 2..4 updates
      var ordN = 100
      val updates = (0 until k).map { _ =>
        var subset = propDataCols.filter(_ => rnd.nextBoolean()).toSet
        if (subset.isEmpty) subset = Set(propDataCols(rnd.nextInt(propDataCols.size)))
        val newVals = subset.map(c => c -> (2000L + rnd.nextInt(1000))).toMap
        ordN += 100
        (subset, newVals, ord(ordN))
      }
      // reference model: apply changed columns in ordering order, keep prior otherwise.
      var expected = base
      updates.foreach { case (subset, nv, _) =>
        expected = expected.map { case (c, v) => if (subset.contains(c)) (c, nv(c)) else (c, v) }
      }
      val desc = s"seq$i base=$base updates=${updates.map(u => (u._1.toSeq.sorted.mkString("+"), u._2)).mkString("; ")}"
      Seq(("mor", DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL, false),
        ("morCompacted", DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL, true),
        ("cow", DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL, false)).foreach { case (tag, tableType, compact) =>
        val path = s"$basePath/prop_${i}_$tag"
        writeSequence(path, tableType, compact, base, updates)
        val out = spark.read.format("hudi").load(path).select("v1", "v2", "v3").where("id = 1").collect()(0)
        propDataCols.foreach { c =>
          assertEquals(expected(c), out.getAs[Long](c), s"[$tag] column $c mismatch vs reference model; $desc")
        }
      }
    }
  }
}
