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

package org.apache.hudi.utilities.transform.debezium;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.debezium.DebeziumConstants;
import org.apache.hudi.utilities.config.DebeziumTransformerConfig;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the Postgres-specific behavior of {@link PostgresDebeziumTransformer}: the surfaced source
 * metadata columns, the nested-by-default layout, and the snapshot LSN defaulting.
 */
class TestPostgresDebeziumTransformer extends DebeziumTransformerTestBase {

  // A Postgres source struct carries txId/lsn/xmin/schema in addition to name/ts_ms.
  // before is populated (same shape as after) so Spark infers it as a struct; these tests exercise
  // inserts/snapshots (op c/r), which take `after` regardless of before's contents.
  private static String pgEvent(String op, long id, long lsn, boolean nullLsn) {
    String row = "{\"id\":" + id + ",\"name\":\"row" + id + "\",\"email\":\"e" + id + "@x.com\"}";
    return "{\"op\":\"" + op + "\",\"ts_ms\":1700000000500,"
        + "\"before\":" + row + ","
        + "\"after\":" + row + ","
        + "\"source\":{\"name\":\"pgdb\",\"ts_ms\":1700000000000,\"schema\":\"public\","
        + "\"txId\":" + (500 + id) + ",\"lsn\":" + (nullLsn ? "null" : lsn) + ",\"xmin\":" + (9000 + id) + "}}";
  }

  @Test
  void testPostgresMetadataColumnsAreSurfaced() {
    TypedProperties props = new TypedProperties();
    // force flat so all metadata columns sit at root and are easy to assert
    props.setProperty(DebeziumTransformerConfig.ENABLE_NESTED_FIELDS.key(), "false");

    Dataset<Row> result = new PostgresDebeziumTransformer()
        .apply(jsc, spark, jsonToDataset(pgEvent("c", 1, 1001, false)), props);

    List<String> columns = Arrays.asList(result.columns());
    assertTrue(columns.contains(DebeziumConstants.FLATTENED_TX_ID_COL_NAME), "_event_tx_id present");
    assertTrue(columns.contains(DebeziumConstants.FLATTENED_LSN_COL_NAME), "_event_lsn present");
    assertTrue(columns.contains(DebeziumConstants.FLATTENED_XMIN_COL_NAME), "_event_xmin present");

    Row row = result.first();
    long lsn = row.<Long>getAs(DebeziumConstants.FLATTENED_LSN_COL_NAME);
    long txId = row.<Long>getAs(DebeziumConstants.FLATTENED_TX_ID_COL_NAME);
    assertEquals(1001L, lsn);
    assertEquals(501L, txId);
  }

  @Test
  void testPostgresDefaultsToFlatMetadata() {
    // No nested property set -> Postgres defaults to flat (consistent with MySQL), so every metadata
    // column sits at the root and there is no _debezium_metadata struct.
    Dataset<Row> result = new PostgresDebeziumTransformer()
        .apply(jsc, spark, jsonToDataset(pgEvent("c", 1, 1001, false)), new TypedProperties());

    List<String> columns = Arrays.asList(result.columns());
    assertFalse(columns.contains(DebeziumConstants.DEBEZIUM_METADATA_FIELD), "flat by default for Postgres");
    assertTrue(columns.contains(DebeziumConstants.FLATTENED_OP_COL_NAME), "op at root");
    assertTrue(columns.contains(DebeziumConstants.FLATTENED_LSN_COL_NAME), "LSN at root (payload ordering field)");
    assertTrue(columns.contains(DebeziumConstants.FLATTENED_TX_ID_COL_NAME), "tx id at root when flat");
  }

  @Test
  void testNullLsnDefaultedToZeroForSnapshotRowsWhenNested() {
    // Snapshot LSN defaulting must also hold under the nested layout: the LSN stays at the root level
    // even when nested, so the "null snapshot LSN -> 0" post-processing still applies.
    Dataset<Row> input = jsonToDataset(
        pgEvent("r", 1, 0, true),    // snapshot, null lsn -> expect 0
        pgEvent("r", 2, 2002, false) // snapshot, real lsn -> unchanged
    );
    TypedProperties props = new TypedProperties();
    props.setProperty(DebeziumTransformerConfig.ENABLE_NESTED_FIELDS.key(), "true");

    Dataset<Row> result = new PostgresDebeziumTransformer().apply(jsc, spark, input, props);
    assertTrue(Arrays.asList(result.columns()).contains(DebeziumConstants.DEBEZIUM_METADATA_FIELD), "nested layout");
    List<Row> rows = result.orderBy("id").collectAsList();

    assertEquals(0L, rows.get(0).<Long>getAs(DebeziumConstants.FLATTENED_LSN_COL_NAME), "null snapshot LSN defaulted to 0 (nested)");
    assertEquals(2002L, rows.get(1).<Long>getAs(DebeziumConstants.FLATTENED_LSN_COL_NAME), "real LSN preserved (nested)");
  }

  @Test
  void testNullLsnDefaultedToZeroForSnapshotRows() {
    // Snapshot rows (op = r) from incremental snapshots can have a null LSN; it must become 0
    // so the payload's "higher LSN wins" ordering does not choke on null.
    Dataset<Row> input = jsonToDataset(
        pgEvent("r", 1, 0, true),    // snapshot, null lsn -> expect 0
        pgEvent("r", 2, 2002, false) // snapshot, real lsn -> unchanged
    );
    TypedProperties props = new TypedProperties();
    props.setProperty(DebeziumTransformerConfig.ENABLE_NESTED_FIELDS.key(), "false");

    Dataset<Row> result = new PostgresDebeziumTransformer().apply(jsc, spark, input, props);
    List<Row> rows = result.orderBy("id").collectAsList();

    long defaultedLsn = rows.get(0).<Long>getAs(DebeziumConstants.FLATTENED_LSN_COL_NAME);
    long preservedLsn = rows.get(1).<Long>getAs(DebeziumConstants.FLATTENED_LSN_COL_NAME);
    assertEquals(0L, defaultedLsn, "null snapshot LSN defaulted to 0");
    assertEquals(2002L, preservedLsn, "real LSN preserved");
  }

  @Test
  void testNullLsnNotDefaultedForNonSnapshotRows() {
    // For a real change event (op != r) a null LSN is left as null (only snapshots are defaulted).
    Dataset<Row> input = jsonToDataset(pgEvent("c", 1, 0, true));
    TypedProperties props = new TypedProperties();
    props.setProperty(DebeziumTransformerConfig.ENABLE_NESTED_FIELDS.key(), "false");

    Dataset<Row> result = new PostgresDebeziumTransformer().apply(jsc, spark, input, props);
    assertNull(result.first().getAs(DebeziumConstants.FLATTENED_LSN_COL_NAME));
  }
}
