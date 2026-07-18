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
import org.junit.jupiter.api.Assertions.{assertEquals, assertNull}
import org.junit.jupiter.api.Test

/**
 * End-to-end validation that an Oracle Debezium CDC table created on table version 9 merges via the
 * built-in EVENT_TIME_ORDERING + FILL_UNCHANGED partial-update strategy (driven by the
 * _changed_columns list) rather than the payload, and that unchanged columns are preserved through a
 * MOR snapshot read. _event_ordering is the ordering field.
 */
class TestOracleDebeziumV9ReadMerge extends SparkClientFunctionalTestHarness {

  private val cols = Seq("id", "name", "amount", "_changed_columns", "_hoodie_is_deleted", "_event_ordering")

  private def row(id: Int, name: String, amount: Long, changed: String, ordering: String): DataFrame =
    spark.createDataFrame(Seq((id, name, amount, changed, false, ordering))).toDF(cols: _*)

  /** Create the v9 MOR table with the Oracle payload (which the v9 config inference maps to
   * EVENT_TIME_ORDERING + FILL_UNCHANGED). */
  private def createV9Table(frame: DataFrame, table: String): Unit =
    baseWriter(frame, table)
      .option(OPERATION.key(), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "9")
      .option(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key(), classOf[OracleDebeziumAvroPayload].getName)
      .mode(SaveMode.Overwrite)
      .save(basePath)

  private def upsert(frame: DataFrame, table: String): Unit =
    baseWriter(frame, table)
      .option(OPERATION.key(), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

  private def baseWriter(frame: DataFrame, table: String) =
    frame.write.format("hudi")
      .option(RECORDKEY_FIELD.key(), "id")
      .option(ORDERING_FIELDS.key(), "_event_ordering")
      .option(TABLE_TYPE.key(), DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_NAME.key(), table)
      .option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false")

  private def readRow(id: Int): Row =
    spark.read.format("hudi").load(basePath).select("id", "name", "amount").where(s"id = $id").collect()(0)

  private def ord(n: Int): String = "00000000000000000000." + "%020d".format(n)

  @Test
  def v9OracleChangedColumnsPreservesUnchangedColumns(): Unit = {
    createV9Table(row(1, "alice", 100L, null, ord(100)), "oracle_v9_test")
    // Only `name` changed; amount carries a zero-value placeholder that must NOT win.
    upsert(row(1, "bob", 0L, "name", ord(200)), "oracle_v9_test")

    val out = readRow(1)
    assertEquals("bob", out.getAs[String]("name"), "name is in _changed_columns -> takes the update")
    assertEquals(100L, out.getAs[Long]("amount"),
      "amount is NOT in _changed_columns -> preserves the prior value, not the placeholder 0")
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

    val out = readRow(1)
    assertEquals("bob", out.getAs[String]("name"),
      "name was changed only by update1 -> union of changed-columns preserves it through the base merge")
    assertEquals(200L, out.getAs[Long]("amount"), "amount was changed by update2")
  }
}
