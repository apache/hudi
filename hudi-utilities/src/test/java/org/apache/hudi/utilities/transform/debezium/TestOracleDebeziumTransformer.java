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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.debezium.DebeziumConstants;
import org.apache.hudi.utilities.config.DebeziumTransformerConfig;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload.DEBEZIUM_TOASTED_VALUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies {@link OracleDebeziumTransformer} flattens the Oracle Debezium envelope, surfaces the
 * SCN metadata + composite {@code _event_ordering}, computes {@code _changed_columns} for updates
 * (excluding toasted values), sets {@code _hoodie_is_deleted}, drops unsupported operations, and
 * honors flat vs nested layouts.
 */
class TestOracleDebeziumTransformer extends DebeziumTransformerTestBase {

  /** before/after each carry {id, name, amount}; source carries scn/commit_scn. */
  private static String oracleEvent(String op, String beforeName, Long beforeAmount,
                                    String afterName, Long afterAmount, Long scn, Long commitScn) {
    return "{\"op\":\"" + op + "\",\"ts_ms\":1700000000500,"
        + "\"before\":" + row(beforeName, beforeAmount) + ","
        + "\"after\":" + row(afterName, afterAmount) + ","
        + "\"source\":{\"name\":\"oradb\",\"ts_ms\":1700000000000,"
        + "\"scn\":" + (scn == null ? "null" : scn) + ",\"commit_scn\":" + (commitScn == null ? "null" : commitScn) + "}}";
  }

  private static String row(String name, Long amount) {
    return "{\"id\":1,\"name\":" + (name == null ? "null" : "\"" + name + "\"")
        + ",\"amount\":" + (amount == null ? "null" : amount) + "}";
  }

  private Dataset<Row> transform(TypedProperties props, String... events) {
    return new OracleDebeziumTransformer().apply(jsc, spark, jsonToDataset(events), props);
  }

  private static TypedProperties flat() {
    TypedProperties props = new TypedProperties();
    props.setProperty(DebeziumTransformerConfig.ENABLE_NESTED_FIELDS.key(), "false");
    return props;
  }

  @Test
  void testInsertSurfacesMetadataAndNoChangedColumns() {
    Row r = transform(flat(), oracleEvent("c", null, null, "alice", 100L, 1700L, 1800L)).first();
    List<String> cols = Arrays.asList(r.schema().fieldNames());
    assertTrue(cols.contains(DebeziumConstants.FLATTENED_SCN_COL_NAME));
    assertTrue(cols.contains(DebeziumConstants.FLATTENED_COMMIT_SCN_COL_NAME));
    assertTrue(cols.contains(DebeziumConstants.FLATTENED_ORDERING_COL_NAME));
    assertEquals("alice", r.getAs("name"));
    assertNull(r.getAs(DebeziumConstants.CHANGED_COLUMNS_FIELD), "insert has no _changed_columns");
    assertFalse((Boolean) r.getAs(HoodieRecord.HOODIE_IS_DELETED_FIELD));
    assertEquals("1700", r.getAs(DebeziumConstants.FLATTENED_SCN_COL_NAME));
    // _event_ordering = zero-padded commit_scn "." zero-padded scn
    assertEquals(pad(1800L) + "." + pad(1700L), r.getAs(DebeziumConstants.FLATTENED_ORDERING_COL_NAME));
  }

  @Test
  void testUpdateComputesChangedColumns() {
    Row r = transform(flat(), oracleEvent("u", "alice", 100L, "bob", 200L, 1700L, 1800L)).first();
    String changed = r.getAs(DebeziumConstants.CHANGED_COLUMNS_FIELD);
    assertTrue(changedSet(changed).contains("name"), "name changed");
    assertTrue(changedSet(changed).contains("amount"), "amount changed");
    assertFalse(changedSet(changed).contains("id"), "id unchanged");
  }

  @Test
  void testUpdateWithExplicitNullIsChanged() {
    Row r = transform(flat(), oracleEvent("u", "alice", 100L, null, 100L, 1700L, 1800L)).first();
    String changed = r.getAs(DebeziumConstants.CHANGED_COLUMNS_FIELD);
    assertTrue(changedSet(changed).contains("name"), "name -> null is a change");
    assertFalse(changedSet(changed).contains("amount"), "amount unchanged");
    assertNull(r.getAs("name"));
  }

  @Test
  void testUpdateWithToastedValueExcludedFromChangedColumns() {
    Row r = transform(flat(),
        oracleEvent("u", "alice", 100L, DEBEZIUM_TOASTED_VALUE, 200L, 1700L, 1800L)).first();
    String changed = r.getAs(DebeziumConstants.CHANGED_COLUMNS_FIELD);
    assertFalse(changedSet(changed).contains("name"), "toasted name excluded");
    assertTrue(changedSet(changed).contains("amount"), "amount still changed");
  }

  @Test
  void testUpdateNoColumnsChangedYieldsNull() {
    Row r = transform(flat(), oracleEvent("u", "alice", 100L, "alice", 100L, 1700L, 1800L)).first();
    assertNull(r.getAs(DebeziumConstants.CHANGED_COLUMNS_FIELD), "no diff -> null _changed_columns");
  }

  @Test
  void testDeleteSetsHoodieIsDeletedAndNullChangedColumns() {
    Row r = transform(flat(), oracleEvent("d", "alice", 100L, null, null, 1700L, 1800L)).first();
    assertTrue((Boolean) r.getAs(HoodieRecord.HOODIE_IS_DELETED_FIELD));
    assertNull(r.getAs(DebeziumConstants.CHANGED_COLUMNS_FIELD));
    assertEquals("d", r.getAs(DebeziumConstants.FLATTENED_OP_COL_NAME));
    // delete surfaces the before image
    assertEquals("alice", r.getAs("name"));
  }

  @Test
  void testSnapshotHasNoChangedColumnsAndNotDeleted() {
    Row r = transform(flat(), oracleEvent("r", null, null, "alice", 100L, 1700L, 1800L)).first();
    assertEquals("r", r.getAs(DebeziumConstants.FLATTENED_OP_COL_NAME));
    assertNull(r.getAs(DebeziumConstants.CHANGED_COLUMNS_FIELD));
    assertFalse((Boolean) r.getAs(HoodieRecord.HOODIE_IS_DELETED_FIELD));
  }

  @Test
  void testUnsupportedOperationIsDropped() {
    Dataset<Row> result = transform(flat(),
        oracleEvent("c", null, null, "alice", 100L, 1700L, 1800L),
        oracleEvent("t", null, null, null, null, 1900L, 2000L)); // truncate -> dropped
    assertEquals(1, result.count(), "only the insert survives, truncate dropped");
  }

  @Test
  void testOrderingFallsBackToZeroWhenScnNull() {
    Row r = transform(flat(), oracleEvent("c", null, null, "alice", 100L, null, null)).first();
    assertEquals(pad(0L) + "." + pad(0L), r.getAs(DebeziumConstants.FLATTENED_ORDERING_COL_NAME));
  }

  @Test
  void testMixedBatchFlagsPerRow() {
    Dataset<Row> result = transform(flat(),
        oracleEvent("c", null, null, "a", 1L, 1L, 1L),
        oracleEvent("u", "a", 1L, "b", 1L, 2L, 2L),
        oracleEvent("d", "b", 1L, null, null, 3L, 3L));
    Map<String, Row> byOp = new HashMap<>();
    for (Row r : result.collectAsList()) {
      byOp.put(r.getAs(DebeziumConstants.FLATTENED_OP_COL_NAME), r);
    }
    assertFalse((Boolean) byOp.get("c").getAs(HoodieRecord.HOODIE_IS_DELETED_FIELD));
    assertNull(byOp.get("c").getAs(DebeziumConstants.CHANGED_COLUMNS_FIELD));
    assertTrue(changedSet(byOp.get("u").getAs(DebeziumConstants.CHANGED_COLUMNS_FIELD)).contains("name"));
    assertTrue((Boolean) byOp.get("d").getAs(HoodieRecord.HOODIE_IS_DELETED_FIELD));
  }

  @Test
  void testNestedModeKeepsOrderingAndOpAtRoot() {
    TypedProperties props = new TypedProperties();
    props.setProperty(DebeziumTransformerConfig.ENABLE_NESTED_FIELDS.key(), "true");
    Row r = transform(props, oracleEvent("u", "alice", 100L, "bob", 100L, 1700L, 1800L)).first();
    List<String> cols = Arrays.asList(r.schema().fieldNames());
    assertTrue(cols.contains(DebeziumConstants.DEBEZIUM_METADATA_FIELD), "metadata nested");
    assertTrue(cols.contains(DebeziumConstants.FLATTENED_ORDERING_COL_NAME), "_event_ordering at root");
    assertTrue(cols.contains(DebeziumConstants.FLATTENED_OP_COL_NAME), "op at root");
    assertTrue(cols.contains(DebeziumConstants.CHANGED_COLUMNS_FIELD), "_changed_columns at root");
    assertFalse(cols.contains(DebeziumConstants.FLATTENED_SCN_COL_NAME), "scn is nested, not root");
    // ordering is still computed correctly from the nested scn/commit_scn
    assertEquals(pad(1800L) + "." + pad(1700L), r.getAs(DebeziumConstants.FLATTENED_ORDERING_COL_NAME));
  }

  private static String pad(long v) {
    StringBuilder sb = new StringBuilder(Long.toString(v));
    while (sb.length() < 20) {
      sb.insert(0, '0');
    }
    return sb.toString();
  }

  private static java.util.Set<String> changedSet(String csv) {
    java.util.Set<String> s = new java.util.HashSet<>();
    if (csv != null && !csv.isEmpty()) {
      s.addAll(Arrays.asList(csv.split(",")));
    }
    return s;
  }
}
