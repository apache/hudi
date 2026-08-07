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

package org.apache.hudi.common.model.debezium;

import org.apache.hudi.common.model.HoodiePayloadProps;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload.DEBEZIUM_TOASTED_VALUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link OracleDebeziumAvroPayload}.
 */
class TestOracleDebeziumAvroPayload {

  private Schema schema;
  private Properties properties;

  // Schema with: id (PK), name (string), amount (long), _changed_columns, _hoodie_is_deleted, _event_ordering
  private static final String JSON_SCHEMA = "{\n"
      + "  \"type\": \"record\",\n"
      + "  \"name\": \"oracleDebeziumRecord\", \"namespace\":\"org.apache.hudi\",\n"
      + "  \"fields\": [\n"
      + "    {\"name\": \"id\", \"type\": \"string\"},\n"
      + "    {\"name\": \"name\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
      + "    {\"name\": \"amount\", \"type\": [\"null\", \"long\"], \"default\": null},\n"
      + "    {\"name\": \"_changed_columns\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
      + "    {\"name\": \"_hoodie_is_deleted\", \"type\": \"boolean\", \"default\": false},\n"
      + "    {\"name\": \"_event_ordering\", \"type\": [\"null\", \"string\"], \"default\": null}\n"
      + "  ]\n"
      + "}";

  @BeforeEach
  void setUp() {
    schema = new Schema.Parser().parse(JSON_SCHEMA);
    properties = new Properties();
    properties.put(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY, "_event_ordering");
  }

  @Test
  void testInsert() throws IOException {
    GenericRecord record = createRecord("1", "alice", 100L, null, false, "00000000000100.00000000000050");
    OracleDebeziumAvroPayload payload = new OracleDebeziumAvroPayload(record, (Comparable) record.get("_event_ordering"));

    Option<IndexedRecord> result = payload.getInsertValue(schema);
    assertTrue(result.isPresent());
    GenericRecord out = (GenericRecord) result.get();
    assertEquals("alice", out.get("name").toString());
    assertEquals(100L, out.get("amount"));
    assertNull(out.get("_changed_columns"));
  }

  @Test
  void testIncomingDeleteNewerThanHealthyExistingReturnsEmpty() throws IOException {
    // Scenario A3 (incoming delete newer): the newer-resolved record is a delete. In
    // combineAndGetUpdateValue this signals deletion via empty-return — Hudi's reader /
    // compaction path drops the row from the output. (preCombine preserves the delete marker,
    // so the tombstone still lands in log files ahead of this merge.)
    GenericRecord deleteRecord = createRecord("1", "alice", 100L, null, true, "00000000000200.00000000000060");
    OracleDebeziumAvroPayload payload = new OracleDebeziumAvroPayload(deleteRecord, (Comparable) deleteRecord.get("_event_ordering"));

    GenericRecord existing = createRecord("1", "alice", 100L, null, false, "00000000000100.00000000000050");
    Option<IndexedRecord> result = payload.combineAndGetUpdateValue(existing, schema, properties);
    assertFalse(result.isPresent(),
        "newer-resolved delete must signal deletion via empty-return so the reader drops the row");
  }

  @Test
  void testIncomingDeleteOlderThanHealthyExistingEmitsHealthy() throws IOException {
    // Scenario A3 (incoming delete older): existing healthy record has higher ordering and
    // should win. The older delete is overruled — emit the merged healthy record.
    GenericRecord deleteRecord = createRecord("1", "alice", 100L, null, true, "00000000000100.00000000000050");
    OracleDebeziumAvroPayload payload = new OracleDebeziumAvroPayload(deleteRecord, (Comparable) deleteRecord.get("_event_ordering"));

    GenericRecord existing = createRecord("1", "alice", 100L, null, false, "00000000000200.00000000000060");
    Option<IndexedRecord> result = payload.combineAndGetUpdateValue(existing, schema, properties);
    assertTrue(result.isPresent());
    GenericRecord out = (GenericRecord) result.get();
    assertEquals("alice", out.get("name").toString(), "newer healthy record wins");
    assertFalse((Boolean) out.get("_hoodie_is_deleted"),
        "older delete is overruled by newer healthy record");
  }

  @Test
  void testHealthyAgainstStoredTombstoneIncomingNewerSkipsTombstoneFields() throws IOException {
    // Scenario A4: stored is a delete tombstone with placeholder non-PK fields (simulating
    // Oracle PK-only supplemental logging where the delete event's `before` image carries
    // null/placeholder values for non-PK columns). Incoming healthy update has higher ordering
    // and _changed_columns covers only some fields.
    //
    // Expected: incoming healthy wins; non-PK fields outside _changed_columns must NOT
    // inherit values from the tombstone — fall back to newer's value (or null) instead.
    GenericRecord storedTombstone = createRecord("1", null, 0L, null, true, "00000000000100.00000000000050");
    GenericRecord incomingHealthy = createRecord("1", "bob", 0L, "name", false, "00000000000200.00000000000060");

    OracleDebeziumAvroPayload payload = new OracleDebeziumAvroPayload(incomingHealthy, (Comparable) incomingHealthy.get("_event_ordering"));
    Option<IndexedRecord> result = payload.combineAndGetUpdateValue(storedTombstone, schema, properties);
    assertTrue(result.isPresent());
    GenericRecord out = (GenericRecord) result.get();
    assertEquals("1", out.get("id").toString());
    assertEquals("bob", out.get("name").toString(), "name is in _changed_columns -> use incoming");
    assertEquals(0L, out.get("amount"),
        "amount not in _changed_columns -> tombstone garbage skipped, falls back to incoming");
    assertFalse((Boolean) out.get("_hoodie_is_deleted"),
        "newer healthy record overrides stored _hoodie_is_deleted=true");
  }

  @Test
  void testHealthyAgainstStoredTombstoneStoredNewerReturnsEmpty() throws IOException {
    // Scenario A5: stored delete tombstone has higher ordering than incoming healthy update.
    // The tombstone wins and combineAndGetUpdateValue returns empty to signal deletion, so
    // the row is dropped from the read output. (This is the same standard Hudi delete signal
    // that Postgres/MySQL payloads use.)
    GenericRecord storedTombstone = createRecord("1", null, 0L, null, true, "00000000000300.00000000000070");
    GenericRecord incomingHealthy = createRecord("1", "bob", 200L, "name,amount", false, "00000000000100.00000000000050");

    OracleDebeziumAvroPayload payload = new OracleDebeziumAvroPayload(incomingHealthy, (Comparable) incomingHealthy.get("_event_ordering"));
    Option<IndexedRecord> result = payload.combineAndGetUpdateValue(storedTombstone, schema, properties);
    assertFalse(result.isPresent(),
        "stored-newer tombstone must signal deletion via empty-return");
  }

  @Test
  void testBothDeleteReturnsEmpty() throws IOException {
    // Scenario A6: both incoming and stored are delete tombstones. Newer-resolved record is
    // a delete regardless of which side wins ordering, so combineAndGetUpdateValue returns
    // empty — the row is dropped.
    GenericRecord storedTombstone = createRecord("1", "alice", 100L, null, true, "00000000000100.00000000000050");
    GenericRecord incomingDelete = createRecord("1", null, 0L, null, true, "00000000000200.00000000000060");

    OracleDebeziumAvroPayload payload = new OracleDebeziumAvroPayload(incomingDelete, (Comparable) incomingDelete.get("_event_ordering"));
    Option<IndexedRecord> result = payload.combineAndGetUpdateValue(storedTombstone, schema, properties);
    assertFalse(result.isPresent(), "delete-vs-delete must signal deletion via empty-return");
  }

  @Test
  void testTombstoneOlderDoesNotPolluteUnchangedColumns() throws IOException {
    // Targeted test: a stored tombstone with PLACEHOLDER values for non-PK fields
    // (e.g., name="" from Oracle PK-only supplemental logging) must not leak those
    // placeholders into the merged output for columns absent from _changed_columns.
    // Without the tombstone-aware fix, name="" would be preserved as the "older value"
    // since name is not in _changed_columns.
    GenericRecord storedTombstone = createRecord("1", "" /* placeholder */, 0L /* placeholder */, null, true,
        "00000000000100.00000000000050");
    GenericRecord incomingHealthy = createRecord("1", null /* unchanged */, 500L, "amount", false,
        "00000000000200.00000000000060");

    OracleDebeziumAvroPayload payload = new OracleDebeziumAvroPayload(incomingHealthy, (Comparable) incomingHealthy.get("_event_ordering"));
    Option<IndexedRecord> result = payload.combineAndGetUpdateValue(storedTombstone, schema, properties);
    assertTrue(result.isPresent());
    GenericRecord out = (GenericRecord) result.get();
    // Without the tombstone-aware fix, name would have been "" (the tombstone placeholder).
    // With the fix, tombstone is treated as having no usable field values, so name falls
    // back to incoming's null.
    assertNull(out.get("name"),
        "tombstone's placeholder value for unchanged column must NOT leak into merged output");
    assertEquals(500L, out.get("amount"), "changed column from incoming");
  }

  @Test
  void testUpdateWithAllColumnsChanged() throws IOException {
    // Full update: both name and amount changed
    GenericRecord existing = createRecord("1", "alice", 100L, null, false, "00000000000100.00000000000050");
    GenericRecord incoming = createRecord("1", "bob", 200L, "name,amount", false, "00000000000200.00000000000060");

    OracleDebeziumAvroPayload payload = new OracleDebeziumAvroPayload(incoming, (Comparable) incoming.get("_event_ordering"));
    GenericRecord out = (GenericRecord) payload.combineAndGetUpdateValue(existing, schema, properties).get();

    assertEquals("bob", out.get("name").toString());
    assertEquals(200L, out.get("amount"));
  }

  @Test
  void testUpdateWithUnchangedColumnPreservesExisting() throws IOException {
    // Only amount changed; name is not in _changed_columns and is null -> preserve existing "alice"
    GenericRecord existing = createRecord("1", "alice", 100L, null, false, "00000000000100.00000000000050");
    GenericRecord incoming = createRecord("1", null, 200L, "amount", false, "00000000000200.00000000000060");

    OracleDebeziumAvroPayload payload = new OracleDebeziumAvroPayload(incoming, (Comparable) incoming.get("_event_ordering"));
    GenericRecord out = (GenericRecord) payload.combineAndGetUpdateValue(existing, schema, properties).get();

    assertEquals("alice", out.get("name").toString(),
        "name should be preserved from existing record since it's not in _changed_columns");
    assertEquals(200L, out.get("amount"));
  }

  @Test
  void testUpdateWithExplicitNullInChangedColumns() throws IOException {
    // name changed to NULL (is in _changed_columns) -> keep null
    GenericRecord existing = createRecord("1", "alice", 100L, null, false, "00000000000100.00000000000050");
    GenericRecord incoming = createRecord("1", null, 200L, "name,amount", false, "00000000000200.00000000000060");

    OracleDebeziumAvroPayload payload = new OracleDebeziumAvroPayload(incoming, (Comparable) incoming.get("_event_ordering"));
    GenericRecord out = (GenericRecord) payload.combineAndGetUpdateValue(existing, schema, properties).get();

    assertNull(out.get("name"),
        "name should be null since it's in _changed_columns (intentional SET NULL)");
    assertEquals(200L, out.get("amount"));
  }

  @Test
  void testUpdateWithZeroValueFallbackPreservesExisting() throws IOException {
    // Simulates PK-only supplemental logging with NOT NULL columns:
    // amount has zero-value fallback (0) but is NOT in _changed_columns -> preserve existing 100
    GenericRecord existing = createRecord("1", "alice", 100L, null, false, "00000000000100.00000000000050");
    GenericRecord incoming = createRecord("1", null, 0L, "name", false, "00000000000200.00000000000060");

    OracleDebeziumAvroPayload payload = new OracleDebeziumAvroPayload(incoming, (Comparable) incoming.get("_event_ordering"));
    GenericRecord out = (GenericRecord) payload.combineAndGetUpdateValue(existing, schema, properties).get();

    assertNull(out.get("name"), "name is in _changed_columns -> use incoming null");
    assertEquals(100L, out.get("amount"),
        "amount is NOT in _changed_columns -> preserve existing value, not the zero-value fallback");
  }

  @Test
  void testUpdateWithEmptyStringFallbackPreservesExisting() throws IOException {
    // Simulates PK-only supplemental logging with NOT NULL VARCHAR2 column:
    // name has empty-string fallback ("") but is NOT in _changed_columns -> preserve existing "alice"
    GenericRecord existing = createRecord("1", "alice", 100L, null, false, "00000000000100.00000000000050");
    GenericRecord incoming = createRecord("1", "", 200L, "amount", false, "00000000000200.00000000000060");

    OracleDebeziumAvroPayload payload = new OracleDebeziumAvroPayload(incoming, (Comparable) incoming.get("_event_ordering"));
    GenericRecord out = (GenericRecord) payload.combineAndGetUpdateValue(existing, schema, properties).get();

    assertEquals("alice", out.get("name").toString(),
        "name is NOT in _changed_columns -> preserve existing value, not the empty-string fallback");
    assertEquals(200L, out.get("amount"));
  }

  @Test
  void testUpdateWithMultipleChangedColumns() throws IOException {
    // Both name and amount changed to NULL
    GenericRecord existing = createRecord("1", "alice", 100L, null, false, "00000000000100.00000000000050");
    GenericRecord incoming = createRecord("1", null, null, "name,amount", false, "00000000000200.00000000000060");

    OracleDebeziumAvroPayload payload = new OracleDebeziumAvroPayload(incoming, (Comparable) incoming.get("_event_ordering"));
    GenericRecord out = (GenericRecord) payload.combineAndGetUpdateValue(existing, schema, properties).get();

    assertNull(out.get("name"), "name should be null (in _changed_columns)");
    assertNull(out.get("amount"), "amount should be null (in _changed_columns)");
  }

  @Test
  void testUpdateWithToastedValue() throws IOException {
    // name has __debezium_unavailable_value -> preserve existing value
    GenericRecord existing = createRecord("1", "alice", 100L, null, false, "00000000000100.00000000000050");
    GenericRecord incoming = createRecord("1", DEBEZIUM_TOASTED_VALUE, 200L, "amount", false,
        "00000000000200.00000000000060");

    OracleDebeziumAvroPayload payload = new OracleDebeziumAvroPayload(incoming, (Comparable) incoming.get("_event_ordering"));
    GenericRecord out = (GenericRecord) payload.combineAndGetUpdateValue(existing, schema, properties).get();

    assertEquals("alice", out.get("name").toString(),
        "toasted value should be replaced with existing stored value");
    assertEquals(200L, out.get("amount"));
  }

  @Test
  void testUpdateWithNullExistingAndNullIncoming() throws IOException {
    // Both existing and incoming are null, not in _changed_columns -> stays null (from existing)
    GenericRecord existing = createRecord("1", null, 100L, null, false, "00000000000100.00000000000050");
    GenericRecord incoming = createRecord("1", null, 200L, "amount", false, "00000000000200.00000000000060");

    OracleDebeziumAvroPayload payload = new OracleDebeziumAvroPayload(incoming, (Comparable) incoming.get("_event_ordering"));
    GenericRecord out = (GenericRecord) payload.combineAndGetUpdateValue(existing, schema, properties).get();

    assertNull(out.get("name"), "null + null (not in changed) -> null from existing");
    assertEquals(200L, out.get("amount"));
  }

  @Test
  void testOrderingHigherWins() throws IOException {
    GenericRecord existing = createRecord("1", "alice", 100L, null, false, "00000000000300.00000000000070");
    GenericRecord incoming = createRecord("1", "bob", 200L, "name,amount", false, "00000000000100.00000000000050");

    // incoming has lower ordering -> existing should win for non-null fields
    OracleDebeziumAvroPayload payload = new OracleDebeziumAvroPayload(incoming, (Comparable) incoming.get("_event_ordering"));
    GenericRecord out = (GenericRecord) payload.combineAndGetUpdateValue(existing, schema, properties).get();

    assertEquals("alice", out.get("name").toString(), "existing has higher ordering, should win");
    assertEquals(100L, out.get("amount"), "existing has higher ordering, should win");
    // Metadata fields should always come from the newer (existing) record
    assertEquals("00000000000300.00000000000070", out.get("_event_ordering").toString(),
        "_event_ordering should come from the newer record (existing)");
  }

  @Test
  void testStoredNewerWithNullRespectsNull() throws IOException {
    // Existing has higher ordering and name=null (legitimate null — not a tombstone, no toasted
    // marker). Older incoming says name="bob" but it has lower ordering. The newer record is the
    // source of truth, so the merged name must be null, NOT the older "bob".
    //
    // This is the null-is-real semantic: we don't fall back to older when newer's value is a
    // legitimate null.
    GenericRecord existing = createRecord("1", null, 100L, null, false, "00000000000300.00000000000070");
    GenericRecord incoming = createRecord("1", "bob", 200L, "name,amount", false, "00000000000100.00000000000050");

    OracleDebeziumAvroPayload payload = new OracleDebeziumAvroPayload(incoming, (Comparable) incoming.get("_event_ordering"));
    GenericRecord out = (GenericRecord) payload.combineAndGetUpdateValue(existing, schema, properties).get();

    assertNull(out.get("name"),
        "stored newer record's null is the truth — do not resurrect older 'bob'");
    assertEquals(100L, out.get("amount"), "stored newer's amount wins");
  }

  @Test
  void testUnchangedNullableColumnStoredAsNullStaysNull() throws IOException {
    // Incoming is an UPDATE that changes only `amount`; `name` is not in _changed_columns.
    // Stored has name=null (legitimate — the column is nullable and was never set). The
    // merged output must keep name=null, not overwrite it with incoming's zero-value
    // placeholder or any other non-null value.
    GenericRecord existing = createRecord("1", null, 100L, null, false, "00000000000100.00000000000050");
    GenericRecord incoming = createRecord("1", "", 500L, "amount", false, "00000000000200.00000000000060");

    OracleDebeziumAvroPayload payload = new OracleDebeziumAvroPayload(incoming, (Comparable) incoming.get("_event_ordering"));
    GenericRecord out = (GenericRecord) payload.combineAndGetUpdateValue(existing, schema, properties).get();

    assertNull(out.get("name"),
        "unchanged column with legitimate null in storage must stay null — "
            + "do not fall back to incoming's placeholder \"\"");
    assertEquals(500L, out.get("amount"));
  }

  @Test
  void testInsertWithExplicitNullStaysNull() throws IOException {
    // Incoming is an INSERT (no _changed_columns) with name explicitly null. There is a
    // stored record for the same PK with name="alice" (unusual but possible via upsert or
    // out-of-order replay). Under the null-is-real semantic, the insert's null wins — we
    // do NOT resurrect the stored "alice".
    GenericRecord existing = createRecord("1", "alice", 100L, null, false, "00000000000100.00000000000050");
    GenericRecord incoming = createRecord("1", null, 500L, null, false, "00000000000200.00000000000060");

    OracleDebeziumAvroPayload payload = new OracleDebeziumAvroPayload(incoming, (Comparable) incoming.get("_event_ordering"));
    GenericRecord out = (GenericRecord) payload.combineAndGetUpdateValue(existing, schema, properties).get();

    assertNull(out.get("name"),
        "insert/snapshot with explicit null must stay null — no fallback to older's value");
    assertEquals(500L, out.get("amount"));
  }

  @Test
  void testPreCombinePicksHigherOrdering() throws IOException {
    GenericRecord record1 = createRecord("1", "alice", 100L, null, false, "00000000000100.00000000000050");
    GenericRecord record2 = createRecord("1", null, 200L, "amount", false, "00000000000200.00000000000060");

    OracleDebeziumAvroPayload payload1 = new OracleDebeziumAvroPayload(record1, (Comparable) record1.get("_event_ordering"));
    OracleDebeziumAvroPayload payload2 = new OracleDebeziumAvroPayload(record2, (Comparable) record2.get("_event_ordering"));

    // payload2 has higher ordering -> should win, but preserve name from payload1 since payload2.name is null
    OracleDebeziumAvroPayload merged = payload2.preCombine(payload1, schema, properties);
    GenericRecord out = (GenericRecord) merged.getInsertValue(schema).get();

    assertEquals("alice", out.get("name").toString(), "name preserved from record1");
    assertEquals(200L, out.get("amount"), "amount from record2 (higher ordering)");
  }

  @Test
  void testPreCombineDeleteWithHigherOrdering() throws IOException {
    // Delete record has higher ordering -> should win in preCombine
    GenericRecord insertRecord = createRecord("1", "alice", 100L, null, false, "00000000000100.00000000000050");
    GenericRecord deleteRecord = createRecord("1", "alice", 100L, null, true, "00000000000200.00000000000060");

    OracleDebeziumAvroPayload insertPayload = new OracleDebeziumAvroPayload(insertRecord, (Comparable) insertRecord.get("_event_ordering"));
    OracleDebeziumAvroPayload deletePayload = new OracleDebeziumAvroPayload(deleteRecord, (Comparable) deleteRecord.get("_event_ordering"));

    // delete has higher ordering -> delete wins
    OracleDebeziumAvroPayload merged = insertPayload.preCombine(deletePayload, schema, properties);
    assertTrue(merged.isDeleted(schema, properties), "Delete with higher ordering should win in preCombine");
  }

  @Test
  void testPreCombineDeleteWithLowerOrdering() throws IOException {
    // Delete record has lower ordering -> insert should win
    GenericRecord insertRecord = createRecord("1", "alice", 100L, null, false, "00000000000200.00000000000060");
    GenericRecord deleteRecord = createRecord("1", "alice", 100L, null, true, "00000000000100.00000000000050");

    OracleDebeziumAvroPayload insertPayload = new OracleDebeziumAvroPayload(insertRecord, (Comparable) insertRecord.get("_event_ordering"));
    OracleDebeziumAvroPayload deletePayload = new OracleDebeziumAvroPayload(deleteRecord, (Comparable) deleteRecord.get("_event_ordering"));

    // insert has higher ordering -> insert wins
    OracleDebeziumAvroPayload merged = insertPayload.preCombine(deletePayload, schema, properties);
    assertFalse(merged.isDeleted(schema, properties), "Insert with higher ordering should win over delete in preCombine");
    GenericRecord out = (GenericRecord) merged.getInsertValue(schema).get();
    assertEquals("alice", out.get("name").toString());
  }

  @Test
  void testCombinedScenario() throws IOException {
    // Simulate a realistic sequence:
    // 1. Insert: id=1, name=alice, amount=100
    // 2. Update: name toasted (unchanged LOB), amount=200
    // 3. Update: name explicitly set to NULL, amount=300

    GenericRecord insert = createRecord("1", "alice", 100L, null, false, "00000000000100.00000000000050");
    GenericRecord update1 = createRecord("1", DEBEZIUM_TOASTED_VALUE, 200L, "amount", false,
        "00000000000200.00000000000060");
    GenericRecord update2 = createRecord("1", null, 300L, "name,amount", false, "00000000000300.00000000000070");

    // Step 1 -> 2: toasted name preserved
    OracleDebeziumAvroPayload payload1 = new OracleDebeziumAvroPayload(update1, (Comparable) update1.get("_event_ordering"));
    GenericRecord after1 = (GenericRecord) payload1.combineAndGetUpdateValue(insert, schema, properties).get();
    assertEquals("alice", after1.get("name").toString());
    assertEquals(200L, after1.get("amount"));

    // Step 2 -> 3: name explicitly set to NULL
    OracleDebeziumAvroPayload payload2 = new OracleDebeziumAvroPayload(update2, (Comparable) update2.get("_event_ordering"));
    GenericRecord after2 = (GenericRecord) payload2.combineAndGetUpdateValue(after1, schema, properties).get();
    assertNull(after2.get("name"), "name was explicitly set to NULL via _changed_columns");
    assertEquals(300L, after2.get("amount"));
  }

  @Test
  void testTwoUpdatesPreCombinedThenMergedWithInsertPreservesUnchangedColumns() throws IOException {
    // Regression test for the production bug where two CDC updates landing in the same
    // deltastreamer sync cycle (preCombined together) caused the merged result to drop
    // `_changed_columns` via the trim in mergeChangedColumnsSets. Downstream merge with the
    // stored INSERT then fell into the insert/snapshot branch and overwrote unchanged NOT
    // NULL columns with zero-value placeholders from the updates' PK-only supplemental
    // logging payloads.
    //
    // Fix: mergeChangedColumnsSets no longer trims. The union is preserved so subsequent
    // merges see newerChangedCols populated and take the older (stored) value for unchanged
    // columns.

    // Stored INSERT (bootstrap or prior INSERT already persisted in the base file).
    GenericRecord stored = createRecord("1", "stored_name", 999L, null, false,
        "00000000000000000100.00000000000000000050");

    // Two UPDATEs. Each only changed `amount`. `name` in each is a placeholder (empty /
    // null) simulating Oracle PK-only supplemental logging where unchanged columns arrive
    // as zero-value placeholders.
    GenericRecord update1 = createRecord("1", "", 100L, "amount", false,
        "00000000000000000200.00000000000000000060");
    GenericRecord update2 = createRecord("1", "", 200L, "amount", false,
        "00000000000000000300.00000000000000000070");

    OracleDebeziumAvroPayload p1 = new OracleDebeziumAvroPayload(
        update1, (Comparable) update1.get("_event_ordering"));
    OracleDebeziumAvroPayload p2 = new OracleDebeziumAvroPayload(
        update2, (Comparable) update2.get("_event_ordering"));

    // Step 1: preCombine the two updates (what deltastreamer does when both are in the
    // same sync cycle).
    OracleDebeziumAvroPayload precombined = p2.preCombine(p1, schema, properties);
    GenericRecord precombinedRecord =
        (GenericRecord) precombined.getInsertValue(schema).get();

    // `_changed_columns` must still contain "amount" after preCombine — the old buggy
    // trim would have removed it because update2.amount=200 was a real non-null value.
    assertEquals("amount", precombinedRecord.get("_changed_columns").toString(),
        "_changed_columns must survive preCombine so downstream merges know `amount` is real");

    // Step 2: merge the preCombined payload against the stored INSERT.
    GenericRecord finalRec =
        (GenericRecord) precombined.combineAndGetUpdateValue(stored, schema, properties).get();

    // The pre-existing values must be preserved. Before the fix, `name` would get the
    // placeholder "" and `_changed_columns`-less merge would treat everything like an insert.
    assertEquals("stored_name", finalRec.get("name").toString(),
        "name was not changed by either update — must remain 'stored_name'");
    assertEquals(200L, finalRec.get("amount"),
        "amount is in _changed_columns — must be the newer update's value (200)");
  }

  @Test
  void testDisjointUpdatesPreCombinedUnionsChangedColumns() throws IOException {
    // Mirrors production validation ID=501 on the new image: two updates in the same sync
    // cycle that touch DIFFERENT columns. preCombine must produce the union of changed_cols
    // and carry each column's value from the update that actually changed it. Downstream
    // merge with the stored INSERT must then preserve every column whose value has a
    // trustworthy source.
    GenericRecord stored = createRecord("1", "stored_name", 999L, null, false,
        "00000000000000000100.00000000000000000050");

    // U1 only changed `name`. `amount` in U1 is a placeholder 0 (PK-only supplemental logging).
    GenericRecord update1 = createRecord("1", "u1_name", 0L, "name", false,
        "00000000000000000200.00000000000000000060");
    // U2 only changed `amount`. `name` in U2 is a placeholder "" (different placeholder — still
    // unreliable). U2 is newer.
    GenericRecord update2 = createRecord("1", "", 200L, "amount", false,
        "00000000000000000300.00000000000000000070");

    OracleDebeziumAvroPayload p1 = new OracleDebeziumAvroPayload(
        update1, (Comparable) update1.get("_event_ordering"));
    OracleDebeziumAvroPayload p2 = new OracleDebeziumAvroPayload(
        update2, (Comparable) update2.get("_event_ordering"));

    OracleDebeziumAvroPayload precombined = p2.preCombine(p1, schema, properties);
    GenericRecord precombinedRecord =
        (GenericRecord) precombined.getInsertValue(schema).get();

    Set<String> changed = new HashSet<>(Arrays.asList(
        precombinedRecord.get("_changed_columns").toString().split(",")));
    assertEquals(new HashSet<>(Arrays.asList("name", "amount")), changed,
        "_changed_columns must be the union {name, amount}");

    GenericRecord finalRec =
        (GenericRecord) precombined.combineAndGetUpdateValue(stored, schema, properties).get();
    assertEquals("u1_name", finalRec.get("name").toString(),
        "name came from U1 (changed there); not the stored 'stored_name'");
    assertEquals(200L, finalRec.get("amount"),
        "amount came from U2 (changed there); not the stored 999 nor U1's placeholder 0");
  }

  @Test
  void testPreCombineNullThenRestoreLandsOnRestoredValue() throws IOException {
    // Mirrors production validation ID=502 on the new image: U1 sets a nullable column to
    // NULL (explicit delete-value), U2 restores it to a real value. Both in the same sync
    // cycle. The newer event (U2) must win, the restored value must land, and the column must
    // stay tracked in _changed_columns.
    GenericRecord stored = createRecord("1", "NOT_NULL_INIT", 100L, null, false,
        "00000000000000000100.00000000000000000050");

    // U1 sets name=null (explicit SET NULL; name is in changed_cols).
    GenericRecord update1 = createRecord("1", null, 0L, "name", false,
        "00000000000000000200.00000000000000000060");
    // U2 sets name="RESTORED" (same column, real value).
    GenericRecord update2 = createRecord("1", "RESTORED", 0L, "name", false,
        "00000000000000000300.00000000000000000070");

    OracleDebeziumAvroPayload p1 = new OracleDebeziumAvroPayload(
        update1, (Comparable) update1.get("_event_ordering"));
    OracleDebeziumAvroPayload p2 = new OracleDebeziumAvroPayload(
        update2, (Comparable) update2.get("_event_ordering"));

    OracleDebeziumAvroPayload precombined = p2.preCombine(p1, schema, properties);
    GenericRecord precombinedRecord =
        (GenericRecord) precombined.getInsertValue(schema).get();

    assertEquals("name", precombinedRecord.get("_changed_columns").toString(),
        "_changed_columns must still contain `name` after preCombine");
    assertEquals("RESTORED", precombinedRecord.get("name").toString(),
        "preCombine picks the newer update's value; restore wins over null");

    GenericRecord finalRec =
        (GenericRecord) precombined.combineAndGetUpdateValue(stored, schema, properties).get();
    assertEquals("RESTORED", finalRec.get("name").toString(),
        "final merge must propagate the restored value");
    assertEquals(100L, finalRec.get("amount"),
        "amount was never in any changed_cols; must be preserved from stored");
  }

  @Test
  void testThreeUpdatesPreCombinedMergedWithInsertKeepsAllChangedSourcesIntact() throws IOException {
    // Chain of three updates in the same sync cycle:
    //   U1 (oldest) changes `name` to "from_u1", `amount` placeholder.
    //   U2 changes `amount` to 200, `name` placeholder.
    //   U3 (newest) changes `name` to "from_u3", `amount` placeholder.
    // Expected: merged _changed_columns = {name, amount}. Merged name = "from_u3" (newest
    // event that touched name). Merged amount = 200 (only U2 touched it). Stored INSERT's
    // `amount=999` must be overridden by U2's 200.
    GenericRecord stored = createRecord("1", "stored_name", 999L, null, false,
        "00000000000000000100.00000000000000000050");

    GenericRecord u1 = createRecord("1", "from_u1", 0L, "name", false,
        "00000000000000000200.00000000000000000060");
    GenericRecord u2 = createRecord("1", "", 200L, "amount", false,
        "00000000000000000300.00000000000000000070");
    GenericRecord u3 = createRecord("1", "from_u3", 0L, "name", false,
        "00000000000000000400.00000000000000000080");

    OracleDebeziumAvroPayload p1 = new OracleDebeziumAvroPayload(u1, (Comparable) u1.get("_event_ordering"));
    OracleDebeziumAvroPayload p2 = new OracleDebeziumAvroPayload(u2, (Comparable) u2.get("_event_ordering"));
    OracleDebeziumAvroPayload p3 = new OracleDebeziumAvroPayload(u3, (Comparable) u3.get("_event_ordering"));

    // Chain: preCombine(U2, U1) -> result; preCombine(U3, result)
    OracleDebeziumAvroPayload after12 = p2.preCombine(p1, schema, properties);
    OracleDebeziumAvroPayload after123 = p3.preCombine(after12, schema, properties);

    GenericRecord chained = (GenericRecord) after123.getInsertValue(schema).get();
    Set<String> changed = new HashSet<>(Arrays.asList(
        chained.get("_changed_columns").toString().split(",")));
    assertEquals(new HashSet<>(Arrays.asList("name", "amount")), changed,
        "after 3-update chain, _changed_columns must be union {name, amount}");
    assertEquals("from_u3", chained.get("name").toString(),
        "name must be U3's value (newest event that touched it)");
    assertEquals(200L, chained.get("amount"),
        "amount must be U2's value (only U2 touched it; U1/U3 carried placeholders)");

    GenericRecord finalRec =
        (GenericRecord) after123.combineAndGetUpdateValue(stored, schema, properties).get();
    assertEquals("from_u3", finalRec.get("name").toString());
    assertEquals(200L, finalRec.get("amount"),
        "stored's 999 must be overridden by U2's tracked 200");
  }

  /**
   * Schema that intentionally OMITS {@code _hoodie_is_deleted} but includes
   * {@code _change_operation_type}. Reproduces the customer's production read schema:
   * a Hudi target schema where the meta-delete field is absent. Used to verify that
   * {@link OracleDebeziumAvroPayload#isDeleteRecord} detects deletes via the Debezium
   * op field as a fallback.
   */
  private static final String JSON_SCHEMA_NO_HOODIE_DELETE = "{\n"
      + "  \"type\": \"record\",\n"
      + "  \"name\": \"oracleDebeziumRecordNoHoodieDelete\", \"namespace\":\"org.apache.hudi\",\n"
      + "  \"fields\": [\n"
      + "    {\"name\": \"id\", \"type\": \"string\"},\n"
      + "    {\"name\": \"_change_operation_type\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
      + "    {\"name\": \"_event_ordering\", \"type\": [\"null\", \"string\"], \"default\": null}\n"
      + "  ]\n"
      + "}";

  @Test
  void testDeleteDetectedViaOpFieldWhenHoodieIsDeletedAbsent() throws IOException {
    // Reproduces the customer's production scenario: the read schema does not include
    // _hoodie_is_deleted (the schemaprovider's target schema strips it). Without the
    // op-field check in isDeleteRecord, this record would fail to be recognized as a
    // delete and would surface as a NULL-padded "zombie" row in MOR snapshot reads
    // until compaction merges it away.
    //
    // The op-field check restores delete-detection robustness in the same shape as
    // AbstractDebeziumAvroPayload (Postgres/MySQL), which does not have this zombie
    // problem because their payload checks the Debezium op field.
    Schema schemaNoHoodieDelete = new Schema.Parser().parse(JSON_SCHEMA_NO_HOODIE_DELETE);
    GenericRecord deleteRecord = new GenericData.Record(schemaNoHoodieDelete);
    deleteRecord.put("id", "id_1");
    deleteRecord.put("_change_operation_type", "d");
    deleteRecord.put("_event_ordering", "00000000000100.00000000000050");

    OracleDebeziumAvroPayload payload = new OracleDebeziumAvroPayload(
        deleteRecord, (Comparable) deleteRecord.get("_event_ordering"));

    Properties propsNoHoodieDelete = new Properties();
    propsNoHoodieDelete.put(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY, "_event_ordering");

    assertTrue(payload.isDeleted(schemaNoHoodieDelete, propsNoHoodieDelete),
        "Record with op='d' must be recognized as a delete even when _hoodie_is_deleted "
            + "is absent from the schema (covers the MOR snapshot-read zombie scenario).");
  }

  @Test
  void testNonDeleteOpNotRecognizedAsDelete() throws IOException {
    // Negative case: op='u' (update) without _hoodie_is_deleted must NOT be flagged as
    // a delete. Guards against the override over-firing.
    Schema schemaNoHoodieDelete = new Schema.Parser().parse(JSON_SCHEMA_NO_HOODIE_DELETE);
    GenericRecord updateRecord = new GenericData.Record(schemaNoHoodieDelete);
    updateRecord.put("id", "id_1");
    updateRecord.put("_change_operation_type", "u");
    updateRecord.put("_event_ordering", "00000000000100.00000000000050");

    OracleDebeziumAvroPayload payload = new OracleDebeziumAvroPayload(
        updateRecord, (Comparable) updateRecord.get("_event_ordering"));

    Properties propsNoHoodieDelete = new Properties();
    propsNoHoodieDelete.put(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY, "_event_ordering");

    assertFalse(payload.isDeleted(schemaNoHoodieDelete, propsNoHoodieDelete),
        "Record with op='u' must not be recognized as a delete.");
  }

  private GenericRecord createRecord(String id, String name, Long amount, String changedColumns,
                                     boolean isDeleted, String eventOrdering) {
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", id);
    record.put("name", name);
    record.put("amount", amount);
    record.put("_changed_columns", changedColumns);
    record.put("_hoodie_is_deleted", isDeleted);
    record.put("_event_ordering", eventOrdering);
    return record;
  }

  // ----------------------------------------------------------------------------------------
  // Edge-case coverage: secondary constructors, single-arg overload, defensive branches.
  // ----------------------------------------------------------------------------------------

  @Test
  void emptyOptionConstructorMarksPayloadEmpty() throws IOException {
    OracleDebeziumAvroPayload empty = new OracleDebeziumAvroPayload(Option.empty());
    // Empty payload should round-trip as an absent insert value via the standard overload.
    assertFalse(empty.getInsertValue(schema).isPresent());
  }

  @Test
  void singleArgCombineAndGetUpdateValueTreatsIncomingAsNewer() throws IOException {
    // Covers the (IndexedRecord, Schema) overload that lacks Properties — incoming is
    // always treated as newer, so a changed column on incoming should win over stored.
    GenericRecord stored = createRecord("1", "alice", 100L, null, false, "00000000000100.00000000000050");
    GenericRecord incoming = createRecord("1", "bob", null, "name", false, "00000000000200.00000000000060");
    OracleDebeziumAvroPayload payload = new OracleDebeziumAvroPayload(
        incoming, (Comparable) incoming.get("_event_ordering"));

    Option<IndexedRecord> result = payload.combineAndGetUpdateValue(stored, schema);

    assertTrue(result.isPresent());
    GenericRecord out = (GenericRecord) result.get();
    assertEquals("bob", out.get("name").toString());
    // Stored amount preserved because incoming did not list it in _changed_columns.
    assertEquals(100L, out.get("amount"));
  }

  @Test
  void getInsertValueWithPropertiesReturnsEmptyForEmptyPayload() throws IOException {
    // Bandaid override: covers the bytes.length==0 branch returning Option.empty().
    OracleDebeziumAvroPayload empty = new OracleDebeziumAvroPayload(Option.empty());
    assertFalse(empty.getInsertValue(schema, properties).isPresent());
  }

  @Test
  void getInsertValueWithPropertiesReturnsRecordEvenIfMarkedDeleted() throws IOException {
    // Bandaid override: covers the non-empty branch and verifies that, unlike the single-arg
    // overload, this one returns the record bytes even when the payload is flagged delete.
    GenericRecord deleteRecord = createRecord("1", "alice", 100L, null, true, "00000000000200.00000000000060");
    OracleDebeziumAvroPayload payload = new OracleDebeziumAvroPayload(
        deleteRecord, (Comparable) deleteRecord.get("_event_ordering"));

    // Single-arg overload filters delete → empty.
    assertFalse(payload.getInsertValue(schema).isPresent());
    // Two-arg override returns the record bytes even for delete.
    Option<IndexedRecord> twoArg = payload.getInsertValue(schema, properties);
    assertTrue(twoArg.isPresent());
    assertEquals("alice", ((GenericRecord) twoArg.get()).get("name").toString());
  }

  @Test
  void preCombineShortCircuitsWhenIncomingIsEmpty() {
    // Covers the isEmptyRecord() early-return branch in preCombine.
    GenericRecord stored = createRecord("1", "alice", 100L, null, false, "00000000000100.00000000000050");
    OracleDebeziumAvroPayload incoming = new OracleDebeziumAvroPayload(Option.empty());
    OracleDebeziumAvroPayload storedPayload = new OracleDebeziumAvroPayload(
        stored, (Comparable) stored.get("_event_ordering"));

    OracleDebeziumAvroPayload result = incoming.preCombine(storedPayload, schema, properties);
    assertTrue(result == incoming, "Empty incoming must short-circuit and return itself");
  }

  @Test
  void extractChangedColumnsHandlesEmptyString() throws IOException {
    // Covers the str.isEmpty() branch in extractChangedColumns. Behavior contract: an
    // empty _changed_columns string is equivalent to no _changed_columns at all — the merge
    // treats the record as an insert/snapshot and uses the incoming values verbatim. This
    // exercises both the empty-string branch in extractChangedColumns and the empty-set
    // fall-through branch in mergeOldRecordWithModifiedColumns.
    GenericRecord stored = createRecord("1", "alice", 100L, null, false, "00000000000100.00000000000050");
    GenericRecord incoming = createRecord("1", "bob", 200L, "", false, "00000000000200.00000000000060");

    OracleDebeziumAvroPayload payload = new OracleDebeziumAvroPayload(
        incoming, (Comparable) incoming.get("_event_ordering"));

    Option<IndexedRecord> result = payload.combineAndGetUpdateValue(stored, schema, properties);
    assertTrue(result.isPresent());
    GenericRecord out = (GenericRecord) result.get();
    // Empty _changed_columns ⇒ insert/snapshot semantics ⇒ incoming wins for non-metadata fields.
    assertEquals("bob", out.get("name").toString());
    assertEquals(200L, out.get("amount"));
  }

  @Test
  void bytesToastedValueFallsBackToStored() throws IOException {
    // Covers isBytesToasted (bytes-typed columns with the toasted sentinel). Existing tests
    // cover string-typed toasted; this exercises the bytes branch.
    Schema bytesSchema = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"oracleDebeziumBytesRecord\",\"namespace\":\"org.apache.hudi\","
            + "\"fields\":["
            + "{\"name\":\"id\",\"type\":\"string\"},"
            + "{\"name\":\"blob_col\",\"type\":[\"null\",\"bytes\"],\"default\":null},"
            + "{\"name\":\"_changed_columns\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"_hoodie_is_deleted\",\"type\":\"boolean\",\"default\":false},"
            + "{\"name\":\"_event_ordering\",\"type\":[\"null\",\"string\"],\"default\":null}"
            + "]}");

    java.nio.ByteBuffer realBytes = java.nio.ByteBuffer.wrap(new byte[] {1, 2, 3, 4});
    java.nio.ByteBuffer toastedBytes = java.nio.ByteBuffer.wrap(
        DEBEZIUM_TOASTED_VALUE.getBytes(java.nio.charset.StandardCharsets.UTF_8));

    GenericRecord stored = new GenericData.Record(bytesSchema);
    stored.put("id", "1");
    stored.put("blob_col", realBytes);
    stored.put("_changed_columns", null);
    stored.put("_hoodie_is_deleted", false);
    stored.put("_event_ordering", "00000000000100.00000000000050");

    GenericRecord incoming = new GenericData.Record(bytesSchema);
    incoming.put("id", "1");
    incoming.put("blob_col", toastedBytes);
    incoming.put("_changed_columns", "blob_col");
    incoming.put("_hoodie_is_deleted", false);
    incoming.put("_event_ordering", "00000000000200.00000000000060");

    OracleDebeziumAvroPayload payload = new OracleDebeziumAvroPayload(
        incoming, (Comparable) incoming.get("_event_ordering"));

    Option<IndexedRecord> result = payload.combineAndGetUpdateValue(stored, bytesSchema, properties);
    assertTrue(result.isPresent());
    GenericRecord out = (GenericRecord) result.get();
    // Incoming had a toasted bytes value → stored bytes must be preserved.
    assertEquals(realBytes, out.get("blob_col"));
  }

  // Schema matching the production nested-metadata layout
  // (onehousedataplane.debezium.enable.nested.fields=true): scn/commit_scn live inside the
  // _debezium_metadata struct instead of flat root columns.
  private static final String JSON_SCHEMA_NESTED_METADATA = "{\n"
      + "  \"type\": \"record\",\n"
      + "  \"name\": \"oracleDebeziumNestedRecord\", \"namespace\":\"org.apache.hudi\",\n"
      + "  \"fields\": [\n"
      + "    {\"name\": \"_debezium_metadata\", \"type\": [\"null\", {\n"
      + "      \"type\": \"record\", \"name\": \"debeziumMetadata\", \"fields\": [\n"
      + "        {\"name\": \"_event_scn\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
      + "        {\"name\": \"_event_commit_scn\", \"type\": [\"null\", \"string\"], \"default\": null}\n"
      + "      ]}], \"default\": null},\n"
      + "    {\"name\": \"id\", \"type\": \"string\"},\n"
      + "    {\"name\": \"name\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
      + "    {\"name\": \"amount\", \"type\": [\"null\", \"long\"], \"default\": null},\n"
      + "    {\"name\": \"_changed_columns\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
      + "    {\"name\": \"_hoodie_is_deleted\", \"type\": \"boolean\", \"default\": false},\n"
      + "    {\"name\": \"_event_ordering\", \"type\": [\"null\", \"string\"], \"default\": null}\n"
      + "  ]\n"
      + "}";

  @Test
  void nestedDebeziumMetadataTakesNewerValueOnUpdateMerge() throws IOException {
    // Regression: _debezium_metadata is in METADATA_FIELDS, so an update merge must carry the
    // NEWER event's metadata struct. Without it, the struct falls into the
    // preserve-older-value branch (it is never listed in _changed_columns) and the merged row
    // reports the previous event's scn/commit_scn alongside the new _event_ordering.
    Schema nestedSchema = new Schema.Parser().parse(JSON_SCHEMA_NESTED_METADATA);
    Schema metadataSchema = nestedSchema.getField("_debezium_metadata").schema().getTypes().get(1);

    GenericRecord stored = createNestedRecord(nestedSchema, metadataSchema,
        "100", "alice", 100L, null, "00000000000100.00000000000050");
    GenericRecord incoming = createNestedRecord(nestedSchema, metadataSchema,
        "200", "bob", 100L, "name", "00000000000200.00000000000060");

    OracleDebeziumAvroPayload payload = new OracleDebeziumAvroPayload(
        incoming, (Comparable) incoming.get("_event_ordering"));
    Option<IndexedRecord> result = payload.combineAndGetUpdateValue(stored, nestedSchema, properties);

    assertTrue(result.isPresent());
    GenericRecord out = (GenericRecord) result.get();
    assertEquals("bob", out.get("name").toString(), "changed column takes incoming value");
    assertEquals(100L, out.get("amount"), "unchanged column preserved");
    GenericRecord outMetadata = (GenericRecord) out.get("_debezium_metadata");
    assertEquals("200", outMetadata.get("_event_scn").toString(),
        "_debezium_metadata must reflect the newer event, consistent with _event_ordering");
    assertEquals("00000000000200.00000000000060", out.get("_event_ordering").toString());
  }

  @Test
  void nestedDebeziumMetadataKeptFromStoredWhenIncomingIsOlder() throws IOException {
    // Out-of-order arrival: stored has higher ordering, so the merged row must keep the
    // stored (newer) event's metadata struct, not the late-arriving older one's.
    Schema nestedSchema = new Schema.Parser().parse(JSON_SCHEMA_NESTED_METADATA);
    Schema metadataSchema = nestedSchema.getField("_debezium_metadata").schema().getTypes().get(1);

    GenericRecord stored = createNestedRecord(nestedSchema, metadataSchema,
        "300", "carol", 100L, "name", "00000000000300.00000000000070");
    GenericRecord lateIncoming = createNestedRecord(nestedSchema, metadataSchema,
        "100", "alice", 50L, "amount", "00000000000100.00000000000050");

    OracleDebeziumAvroPayload payload = new OracleDebeziumAvroPayload(
        lateIncoming, (Comparable) lateIncoming.get("_event_ordering"));
    Option<IndexedRecord> result = payload.combineAndGetUpdateValue(stored, nestedSchema, properties);

    assertTrue(result.isPresent());
    GenericRecord out = (GenericRecord) result.get();
    GenericRecord outMetadata = (GenericRecord) out.get("_debezium_metadata");
    assertEquals("300", outMetadata.get("_event_scn").toString(),
        "stored record is newer — its metadata struct must win");
    assertEquals("carol", out.get("name").toString(), "newer stored change preserved");
    assertEquals(50L, out.get("amount"), "older event's changed column still applied");
  }

  private GenericRecord createNestedRecord(Schema nestedSchema, Schema metadataSchema, String scn,
                                           String name, Long amount, String changedColumns, String eventOrdering) {
    GenericRecord metadata = new GenericData.Record(metadataSchema);
    metadata.put("_event_scn", scn);
    metadata.put("_event_commit_scn", scn);
    GenericRecord record = new GenericData.Record(nestedSchema);
    record.put("_debezium_metadata", metadata);
    record.put("id", "1");
    record.put("name", name);
    record.put("amount", amount);
    record.put("_changed_columns", changedColumns);
    record.put("_hoodie_is_deleted", false);
    record.put("_event_ordering", eventOrdering);
    return record;
  }
}
