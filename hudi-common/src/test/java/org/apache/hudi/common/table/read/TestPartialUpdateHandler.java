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

package org.apache.hudi.common.table.read;

import org.apache.hudi.avro.AvroRecordContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.PartialUpdateMode;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.hudi.common.table.HoodieTableConfig.DEBEZIUM_UNAVAILABLE_VALUE;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTIAL_UPDATE_UNAVAILABLE_VALUE;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_PROPERTY_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestPartialUpdateHandler {
  @Test
  void testEmptyProperties() {
    TypedProperties props = new TypedProperties();
    Map<String, String> result = PartialUpdateHandler.parseMergeProperties(props);
    assertTrue(result.isEmpty());
  }

  @Test
  void testNonEmptyProperties() {
    TypedProperties props = new TypedProperties();
    props.put(RECORD_MERGE_PROPERTY_PREFIX + PARTIAL_UPDATE_UNAVAILABLE_VALUE, DEBEZIUM_UNAVAILABLE_VALUE);
    Map<String, String> result = PartialUpdateHandler.parseMergeProperties(props);
    assertTrue(result.containsKey(PARTIAL_UPDATE_UNAVAILABLE_VALUE));
    assertEquals(DEBEZIUM_UNAVAILABLE_VALUE, result.get(PARTIAL_UPDATE_UNAVAILABLE_VALUE));
  }

  @Test
  void testDirectMatch() {
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    assertTrue(PartialUpdateHandler.hasTargetType(stringSchema, HoodieSchemaType.STRING));
  }

  @Test
  void testUnionWithTargetType() {
    HoodieSchema unionSchema = HoodieSchema.createUnion(
        HoodieSchema.create(HoodieSchemaType.NULL),
        HoodieSchema.create(HoodieSchemaType.BOOLEAN),
        HoodieSchema.create(HoodieSchemaType.STRING)
    );
    assertTrue(PartialUpdateHandler.hasTargetType(unionSchema, HoodieSchemaType.STRING));
  }

  @Test
  void testUnionWithoutTargetType() {
    HoodieSchema unionSchema = HoodieSchema.createUnion(
        HoodieSchema.create(HoodieSchemaType.NULL),
        HoodieSchema.create(HoodieSchemaType.BOOLEAN),
        HoodieSchema.create(HoodieSchemaType.INT)
    );
    assertFalse(PartialUpdateHandler.hasTargetType(unionSchema, HoodieSchemaType.STRING));
  }

  @Test
  void testNonUnionNonTargetType() {
    HoodieSchema intSchema = HoodieSchema.create(HoodieSchemaType.INT);
    assertFalse(PartialUpdateHandler.hasTargetType(intSchema, HoodieSchemaType.STRING));
  }

  // ---------------------------------------------------------------------------
  // FILL_UNCHANGED (reconcileChangedColumns) — direct unit tests over an Avro RecordContext.
  // Records are built from an Avro schema; the handler is driven with the HoodieSchema wrapper.
  // Fields: name (nullable string), amount (nullable long), _changed_columns (the changed list),
  // _event_ordering (retained metadata).
  // ---------------------------------------------------------------------------

  private static final String CHANGED_COL = "_changed_columns";
  private static final String ORDERING_COL = "_event_ordering";
  private static final Schema AVRO = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},"
          + "{\"name\":\"amount\",\"type\":[\"null\",\"long\"],\"default\":null},"
          + "{\"name\":\"" + CHANGED_COL + "\",\"type\":[\"null\",\"string\"],\"default\":null},"
          + "{\"name\":\"" + ORDERING_COL + "\",\"type\":[\"null\",\"string\"],\"default\":null}]}");
  private static final HoodieSchema FU_SCHEMA = HoodieSchema.fromAvroSchema(AVRO);

  private static PartialUpdateHandler<IndexedRecord> fillUnchangedHandler(boolean withChangedFieldConfig) {
    TypedProperties props = new TypedProperties();
    if (withChangedFieldConfig) {
      props.put(RECORD_MERGE_PROPERTY_PREFIX + "hoodie.write.partial.update.changed.fields", CHANGED_COL);
    }
    props.put(RECORD_MERGE_PROPERTY_PREFIX + "hoodie.write.partial.update.retain.fields", ORDERING_COL);
    props.put(RECORD_MERGE_PROPERTY_PREFIX + PARTIAL_UPDATE_UNAVAILABLE_VALUE, DEBEZIUM_UNAVAILABLE_VALUE);
    return new PartialUpdateHandler<>(new AvroRecordContext(), PartialUpdateMode.FILL_UNCHANGED, props);
  }

  private static BufferedRecord<IndexedRecord> rec(String name, Long amount, String changed, String ordering,
                                                   HoodieOperation op) {
    GenericRecord g = new GenericData.Record(AVRO);
    g.put("name", name);
    g.put("amount", amount);
    g.put(CHANGED_COL, changed);
    g.put(ORDERING_COL, ordering);
    return new BufferedRecord<>("k", ordering, g, 0, op);
  }

  private BufferedRecord<IndexedRecord> merge(BufferedRecord<IndexedRecord> high, BufferedRecord<IndexedRecord> low) {
    return merge(high, low, true);
  }

  private BufferedRecord<IndexedRecord> merge(BufferedRecord<IndexedRecord> high, BufferedRecord<IndexedRecord> low,
                                              boolean withChangedFieldConfig) {
    return fillUnchangedHandler(withChangedFieldConfig).partialMerge(high, low, FU_SCHEMA, FU_SCHEMA, FU_SCHEMA);
  }

  private static String str(Object v) {
    return v == null ? null : v.toString();
  }

  @Test
  void changedColumnTakesNewerUnchangedTakesPrior() {
    IndexedRecord out = merge(rec("bob", 999L, "name", "200", null), rec("alice", 100L, null, "100", null)).getRecord();
    assertEquals("bob", str(((GenericRecord) out).get("name")));
    assertEquals(100L, ((GenericRecord) out).get("amount"), "unchanged amount preserved from prior");
  }

  @Test
  void unchangedNonStringColumnPreservedRegardlessOfPlaceholder() {
    // The differentiator vs FILL_UNAVAILABLE: a numeric unchanged column keeps its prior value even
    // though its incoming value is a non-sentinel placeholder (0).
    IndexedRecord out = merge(rec("bob", 0L, "name", "200", null), rec("alice", 100L, null, "100", null)).getRecord();
    assertEquals(100L, ((GenericRecord) out).get("amount"), "numeric placeholder must not overwrite prior");
  }

  @Test
  void explicitNullInChangedSetStaysNull() {
    IndexedRecord out = merge(rec(null, 100L, "name", "200", null), rec("alice", 100L, null, "100", null)).getRecord();
    assertNull(((GenericRecord) out).get("name"), "name is in changed set -> explicit null wins");
  }

  @Test
  void changedColumnsUnionedAcrossBothRecords() {
    // Disjoint: higher changed "amount", lower changed "name". Result carries name (from lower) AND
    // amount (from higher), and _changed_columns is the union.
    IndexedRecord out = merge(rec("placeholder", 200L, "amount", "300", null),
        rec("bob", 100L, "name", "200", null)).getRecord();
    assertEquals(200L, ((GenericRecord) out).get("amount"));
    assertEquals("bob", str(((GenericRecord) out).get("name")), "name changed only by lower -> preserved");
    Set<String> union = new HashSet<>(Arrays.asList(str(((GenericRecord) out).get(CHANGED_COL)).split(",")));
    assertTrue(union.contains("name") && union.contains("amount"), "changed set is the union");
  }

  @Test
  void toastedValueInChangedColumnFallsBackToPrior() {
    IndexedRecord out = merge(rec(DEBEZIUM_UNAVAILABLE_VALUE, 100L, "name", "200", null),
        rec("alice", 100L, null, "100", null)).getRecord();
    assertEquals("alice", str(((GenericRecord) out).get("name")), "toasted sentinel -> prior value");
  }

  @Test
  void retainFieldAlwaysTakesNewer() {
    IndexedRecord out = merge(rec("bob", 100L, "name", "300", null), rec("alice", 100L, null, "100", null)).getRecord();
    assertEquals("300", str(((GenericRecord) out).get(ORDERING_COL)), "ordering (retain) always from newer");
  }

  @Test
  void emptyChangedSetReturnsHigherRecordAsIs() {
    BufferedRecord<IndexedRecord> high = rec("bob", 200L, null, "300", null);
    assertSame(high, merge(high, rec("alice", 100L, "name", "100", null)), "no changed-columns -> return higher record");
  }

  @Test
  void missingChangedFieldConfigReturnsHigherRecord() {
    BufferedRecord<IndexedRecord> high = rec("bob", 200L, "name", "300", null);
    assertSame(high, merge(high, rec("alice", 100L, null, "100", null), false), "no changed-fields config -> higher record");
  }

  @Test
  void deleteRecordShortCircuitsPartialMerge() {
    BufferedRecord<IndexedRecord> high = rec("bob", 200L, "name", "300", null);
    assertSame(high, merge(high, rec("alice", 100L, null, "100", HoodieOperation.DELETE)),
        "a delete on either side skips partial merge (guard in partialMerge)");
  }

  @Test
  void changedColumnsListTrimsWhitespaceAroundNames() {
    IndexedRecord out = merge(rec("bob", 200L, "name , amount ,", "300", null),
        rec("alice", 100L, null, "100", null)).getRecord();
    assertEquals("bob", str(((GenericRecord) out).get("name")));
    assertEquals(200L, ((GenericRecord) out).get("amount"), "whitespace-padded name still matches -> newer value");
  }

  @Test
  void unchangedColumnAbsentFromLowerSchemaResolvesToNull() {
    // amount is unchanged so it is taken from the lower record, whose (older) schema has no amount
    // field -> must resolve to null, not NPE.
    Schema avroNoAmount = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"rOld\",\"fields\":["
            + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"" + CHANGED_COL + "\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"" + ORDERING_COL + "\",\"type\":[\"null\",\"string\"],\"default\":null}]}");
    GenericRecord lowRec = new GenericData.Record(avroNoAmount);
    lowRec.put("name", "alice");
    lowRec.put(CHANGED_COL, null);
    lowRec.put(ORDERING_COL, "100");
    BufferedRecord<IndexedRecord> low = new BufferedRecord<>("k", "100", lowRec, 0, null);
    BufferedRecord<IndexedRecord> high = rec("bob", 200L, "name", "300", null);

    IndexedRecord out = fillUnchangedHandler(true)
        .partialMerge(high, low, FU_SCHEMA, HoodieSchema.fromAvroSchema(avroNoAmount), FU_SCHEMA).getRecord();
    assertEquals("bob", str(((GenericRecord) out).get("name")), "changed name -> newer value");
    assertNull(((GenericRecord) out).get("amount"), "amount absent from lower schema resolves to null (no NPE)");
  }
}
