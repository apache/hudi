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

package org.apache.hudi.common.schema.evolution;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaIdAssigner;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.schema.evolution.legacy.InternalSchema;
import org.apache.hudi.common.schema.evolution.legacy.utils.SerDeHelper;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 3 façade tests. The on-disk JSON format is a hard backward-compatibility
 * boundary — old tables must remain readable, and tables written by the new code
 * must be readable by the legacy SerDe. These tests pin both directions:
 *
 * <ul>
 *   <li>Round-trip through {@link HoodieSchemaSerDe} preserves field ids and structure.</li>
 *   <li>JSON produced by the new façade parses cleanly via the legacy {@link SerDeHelper}.</li>
 *   <li>JSON produced by the legacy SerDeHelper parses cleanly via the new façade.</li>
 *   <li>The {@code LATEST_SCHEMA} / {@code SCHEMAS} constants alias the legacy keys exactly.</li>
 * </ul>
 */
public class TestHoodieSchemaSerDe {

  private static HoodieSchema sampleSchema(long schemaId) {
    HoodieSchema schema = HoodieSchema.createRecord(
        "Record", null, "ns", false,
        Arrays.asList(
            HoodieSchemaField.of("a", HoodieSchema.create(HoodieSchemaType.INT)),
            HoodieSchemaField.of("b", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("c", HoodieSchema.create(HoodieSchemaType.LONG))));
    HoodieSchemaIdAssigner.assign(schema, 0);
    schema.setSchemaId(schemaId);
    schema.invalidateIdIndex();
    return schema;
  }

  @Test
  public void constantsAliasLegacyKeys() {
    // Commit-metadata payloads written by the legacy code use these exact keys —
    // any drift breaks backward compatibility.
    assertEquals(SerDeHelper.LATEST_SCHEMA, HoodieSchemaSerDe.LATEST_SCHEMA);
    assertEquals(SerDeHelper.SCHEMAS, HoodieSchemaSerDe.SCHEMAS);
  }

  @Test
  public void roundTripsSingleSchemaPreservingIds() {
    HoodieSchema schema = sampleSchema(7L);
    String json = HoodieSchemaSerDe.toJson(schema);

    Option<HoodieSchema> parsed = HoodieSchemaSerDe.fromJson(json);
    assertTrue(parsed.isPresent());
    HoodieSchema reparsed = parsed.get();

    assertEquals(0, reparsed.getField("a").get().fieldId());
    assertEquals(1, reparsed.getField("b").get().fieldId());
    assertEquals(2, reparsed.getField("c").get().fieldId());
    assertEquals(7L, reparsed.schemaId());
  }

  @Test
  public void newFacadeJsonIsReadableByLegacySerDe() {
    HoodieSchema schema = sampleSchema(11L);
    String json = HoodieSchemaSerDe.toJson(schema);

    // The legacy parser must accept the bytes — any byte-format drift here would
    // mean tables written by the new code are unreadable to old readers.
    Option<InternalSchema> legacyParsed = SerDeHelper.fromJson(json);
    assertTrue(legacyParsed.isPresent());
    assertEquals(11L, legacyParsed.get().schemaId());
    // Field id of "a" survives the new-write -> legacy-read path.
    assertNotNull(legacyParsed.get().findField("a"));
    assertEquals(0, legacyParsed.get().findField("a").fieldId());
  }

  @Test
  public void legacySerDeJsonIsReadableByNewFacade() {
    // Reverse direction: a blob the legacy code wrote must still parse through
    // the new façade. This is the path tables-on-disk-from-old-Hudi take after
    // upgrade.
    HoodieSchema schema = sampleSchema(13L);
    InternalSchema legacy = HoodieSchemaInternalSchemaBridge.toInternalSchema(schema);
    String legacyJson = SerDeHelper.toJson(legacy);

    Option<HoodieSchema> parsed = HoodieSchemaSerDe.fromJson(legacyJson);
    assertTrue(parsed.isPresent());
    assertEquals(13L, parsed.get().schemaId());
    assertEquals(0, parsed.get().getField("a").get().fieldId());
    assertEquals(2, parsed.get().getField("c").get().fieldId());
  }

  @Test
  public void parseHistorySchemasReturnsMapByVersionId() {
    HoodieSchema v1 = sampleSchema(100L);
    HoodieSchema v2 = sampleSchema(200L);

    String historyJson = HoodieSchemaSerDe.toJsonHistory(Arrays.asList(v1, v2));
    TreeMap<Long, HoodieSchema> parsed = HoodieSchemaSerDe.parseHistorySchemas(historyJson);

    assertEquals(2, parsed.size());
    assertTrue(parsed.containsKey(100L));
    assertTrue(parsed.containsKey(200L));
    // Each entry's field ids are preserved.
    assertEquals(0, parsed.get(100L).getField("a").get().fieldId());
    assertEquals(2, parsed.get(200L).getField("c").get().fieldId());
  }

  @Test
  public void inheritHistoryAppendsNewSchemaToExistingBlob() {
    HoodieSchema v1 = sampleSchema(100L);
    String firstHistory = HoodieSchemaSerDe.toJsonHistory(Arrays.asList(v1));

    HoodieSchema v2 = sampleSchema(200L);
    String mergedHistory = HoodieSchemaSerDe.inheritHistory(v2, firstHistory);

    TreeMap<Long, HoodieSchema> parsed = HoodieSchemaSerDe.parseHistorySchemas(mergedHistory);
    assertEquals(2, parsed.size());
    assertTrue(parsed.containsKey(100L));
    assertTrue(parsed.containsKey(200L));
  }

  @Test
  public void emptyOrNullJsonReturnsEmptyOption() {
    assertFalse(HoodieSchemaSerDe.fromJson(null).isPresent());
    assertFalse(HoodieSchemaSerDe.fromJson("").isPresent());
  }

  // ---------------------------------------------------------------------------
  // Per-primitive byte-format coverage. These exist so the upcoming pure-
  // HoodieSchema rewrite of HoodieSchemaSerDe can't silently drift on any of
  // the variants the legacy SerDeHelper handles. For each fixture we assert:
  //   1. round-trip through the new façade preserves structure + ids
  //   2. JSON written by the new façade parses cleanly via the legacy SerDe
  //      (forwards compat: tables written today readable by old readers)
  //   3. JSON written by the legacy SerDe parses cleanly via the new façade
  //      (backwards compat: tables already on disk readable by new code)
  // ---------------------------------------------------------------------------

  /**
   * Wraps a single subschema as the only field of a record so we can exercise
   * SerDe on each primitive in turn without hand-rolling boilerplate per case.
   */
  private static HoodieSchema oneFieldRecord(long schemaId, String fieldName, HoodieSchema fieldSchema) {
    HoodieSchema schema = HoodieSchema.createRecord(
        "OneField", null, "ns", false,
        Collections.singletonList(HoodieSchemaField.of(fieldName, fieldSchema)));
    HoodieSchemaIdAssigner.assign(schema, 0);
    schema.setSchemaId(schemaId);
    schema.invalidateIdIndex();
    return schema;
  }

  /**
   * Returns the primitive variants the legacy SerDeHelper can serialize.
   * Excludes HoodieSchema's VECTOR / BLOB / VARIANT — those have no
   * representation in the legacy InternalSchema type system and are out
   * of scope for the byte-compat boundary.
   */
  private static Stream<Arguments> primitiveSubSchemas() {
    return Stream.of(
        Arguments.of("int",                    HoodieSchema.create(HoodieSchemaType.INT)),
        Arguments.of("long",                   HoodieSchema.create(HoodieSchemaType.LONG)),
        Arguments.of("float",                  HoodieSchema.create(HoodieSchemaType.FLOAT)),
        Arguments.of("double",                 HoodieSchema.create(HoodieSchemaType.DOUBLE)),
        Arguments.of("boolean",                HoodieSchema.create(HoodieSchemaType.BOOLEAN)),
        Arguments.of("string",                 HoodieSchema.create(HoodieSchemaType.STRING)),
        Arguments.of("binary",                 HoodieSchema.create(HoodieSchemaType.BYTES)),
        Arguments.of("date",                   HoodieSchema.createDate()),
        Arguments.of("uuid",                   HoodieSchema.createUUID()),
        Arguments.of("time-millis",            HoodieSchema.createTimeMillis()),
        Arguments.of("time-micros",            HoodieSchema.createTimeMicros()),
        Arguments.of("timestamp-millis",       HoodieSchema.createTimestampMillis()),
        Arguments.of("timestamp-micros",       HoodieSchema.createTimestampMicros()),
        Arguments.of("local-timestamp-millis", HoodieSchema.createLocalTimestampMillis()),
        Arguments.of("local-timestamp-micros", HoodieSchema.createLocalTimestampMicros()),
        Arguments.of("fixed[16]",              HoodieSchema.createFixed("md5", null, null, 16)),
        Arguments.of("decimal-bytes(10,2)",    HoodieSchema.createDecimal(10, 2)),
        Arguments.of("decimal-fixed(15,4)",    HoodieSchema.createDecimalFixed(15, 4)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("primitiveSubSchemas")
  public void primitiveRoundTripsThroughNewFacade(String label, HoodieSchema sub) {
    HoodieSchema schema = oneFieldRecord(42L, "v", sub);
    String json = HoodieSchemaSerDe.toJson(schema);

    Option<HoodieSchema> parsed = HoodieSchemaSerDe.fromJson(json);
    assertTrue(parsed.isPresent(), label);
    assertEquals(42L, parsed.get().schemaId(), label);
    HoodieSchemaField vField = parsed.get().getField("v").get();
    assertEquals(0, vField.fieldId(), label);
    // Structural equivalence at the leaf, modulo the parts the wire format
    // doesn't carry: FIXED and decimal-fixed types lose their {@code name} /
    // {@code namespace} on the wire (the format only emits "fixed[N]" /
    // "decimal_fixed(p,s)[N]"), so the round-tripped schema gets a default name.
    // Comparing the canonicalized InternalSchema form normalizes that away.
    HoodieSchema rtField = vField.schema().getNonNullType();
    assertEquals(canonicalize(sub), canonicalize(rtField), label);
  }

  /** Round-trip a primitive HoodieSchema through the bridge to canonicalize the Avro
   *  name/namespace assignments that the wire format doesn't preserve. */
  private static String canonicalize(HoodieSchema primitive) {
    HoodieSchema wrapped = HoodieSchema.createRecord(
        "Wrap", null, "ns", false,
        Collections.singletonList(HoodieSchemaField.of("v", primitive)));
    return HoodieSchemaInternalSchemaBridge.toInternalSchema(wrapped).getRecord().toString();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("primitiveSubSchemas")
  public void primitiveJsonIsReadableByLegacySerDe(String label, HoodieSchema sub) {
    HoodieSchema schema = oneFieldRecord(42L, "v", sub);
    String json = HoodieSchemaSerDe.toJson(schema);

    Option<InternalSchema> legacyParsed = SerDeHelper.fromJson(json);
    assertTrue(legacyParsed.isPresent(), label);
    assertEquals(42L, legacyParsed.get().schemaId(), label);
    assertNotNull(legacyParsed.get().findField("v"), label);
    assertEquals(0, legacyParsed.get().findField("v").fieldId(), label);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("primitiveSubSchemas")
  public void primitiveLegacyJsonIsReadableByNewFacade(String label, HoodieSchema sub) {
    HoodieSchema schema = oneFieldRecord(42L, "v", sub);
    InternalSchema legacy = HoodieSchemaInternalSchemaBridge.toInternalSchema(schema);
    String legacyJson = SerDeHelper.toJson(legacy);

    Option<HoodieSchema> parsed = HoodieSchemaSerDe.fromJson(legacyJson);
    assertTrue(parsed.isPresent(), label);
    assertEquals(42L, parsed.get().schemaId(), label);
    assertEquals(0, parsed.get().getField("v").get().fieldId(), label);
  }

  // ---------------------------------------------------------------------------
  // Nesting coverage: array of primitive, map<string, primitive>, map<string,
  // record>, nested record, plus an "everything everywhere" combined fixture
  // that mirrors the schema shape used by TestSpark3DDL.alterColumnComplexSchema.
  // Each asserts bidirectional SerDe compatibility and id preservation.
  // ---------------------------------------------------------------------------

  @Test
  public void arrayOfPrimitiveSurvivesBothDirections() {
    HoodieSchema schema = oneFieldRecord(7L, "arr",
        HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.INT)));

    String newJson = HoodieSchemaSerDe.toJson(schema);
    String legacyJson = SerDeHelper.toJson(HoodieSchemaInternalSchemaBridge.toInternalSchema(schema));

    // Both directions parse + preserve the array element id.
    HoodieSchema rtNew = HoodieSchemaSerDe.fromJson(newJson).get();
    HoodieSchema rtLegacy = HoodieSchemaSerDe.fromJson(legacyJson).get();
    InternalSchema legacyOfNew = SerDeHelper.fromJson(newJson).get();

    assertEquals(0, rtNew.getField("arr").get().fieldId());
    assertEquals(0, rtLegacy.getField("arr").get().fieldId());
    assertEquals(0, legacyOfNew.findField("arr").fieldId());
    // Element id (next sequential id assigned by HoodieSchemaIdAssigner) survives —
    // looked up by the dotted full name "arr.element".
    int origElemId = schema.findIdByName("arr.element");
    assertTrue(origElemId >= 0);
    assertEquals(origElemId, rtNew.findIdByName("arr.element"));
    assertEquals(origElemId, rtLegacy.findIdByName("arr.element"));
    assertEquals(origElemId, legacyOfNew.findIdByName("arr.element"));
  }

  @Test
  public void mapStringPrimitiveSurvivesBothDirections() {
    HoodieSchema schema = oneFieldRecord(7L, "m",
        HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.LONG)));

    String newJson = HoodieSchemaSerDe.toJson(schema);
    HoodieSchema rt = HoodieSchemaSerDe.fromJson(newJson).get();

    // Map key/value ids are independent integers, looked up via the dotted
    // full names "m.key" / "m.value".
    int origKeyId = schema.findIdByName("m.key");
    int origValueId = schema.findIdByName("m.value");
    assertTrue(origKeyId >= 0);
    assertTrue(origValueId >= 0);

    assertEquals(origKeyId, rt.findIdByName("m.key"));
    assertEquals(origValueId, rt.findIdByName("m.value"));

    // Cross-format: legacy SerDe parses the new JSON and produces a map with
    // the same keyId / valueId — these are surfaced as findIdByName lookups.
    InternalSchema legacy = SerDeHelper.fromJson(newJson).get();
    assertEquals(origKeyId, legacy.findIdByName("m.key"));
    assertEquals(origValueId, legacy.findIdByName("m.value"));
  }

  @Test
  public void mapStringRecordSurvivesBothDirections() {
    HoodieSchema valueRec = HoodieSchema.createRecord(
        "Inner", null, "ns", false,
        Arrays.asList(
            HoodieSchemaField.of("a", HoodieSchema.create(HoodieSchemaType.INT)),
            HoodieSchemaField.of("n", HoodieSchema.create(HoodieSchemaType.STRING))));
    HoodieSchema schema = oneFieldRecord(7L, "members", HoodieSchema.createMap(valueRec));

    String newJson = HoodieSchemaSerDe.toJson(schema);

    // Inner record fields are addressable at members.value.a / members.value.n.
    int origAId = schema.findIdByName("members.value.a");
    int origNId = schema.findIdByName("members.value.n");
    assertTrue(origAId >= 0);
    assertTrue(origNId >= 0);

    HoodieSchema rt = HoodieSchemaSerDe.fromJson(newJson).get();
    assertEquals(origAId, rt.findIdByName("members.value.a"));
    assertEquals(origNId, rt.findIdByName("members.value.n"));

    InternalSchema legacy = SerDeHelper.fromJson(newJson).get();
    assertEquals(origAId, legacy.findIdByName("members.value.a"));
    assertEquals(origNId, legacy.findIdByName("members.value.n"));
  }

  @Test
  public void nestedRecordSurvivesBothDirections() {
    HoodieSchema inner = HoodieSchema.createRecord(
        "User", null, "ns", false,
        Arrays.asList(
            HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("age", HoodieSchema.create(HoodieSchemaType.INT))));
    HoodieSchema schema = oneFieldRecord(7L, "user", inner);

    String newJson = HoodieSchemaSerDe.toJson(schema);

    int origNameId = schema.findIdByName("user.name");
    int origAgeId = schema.findIdByName("user.age");

    HoodieSchema rt = HoodieSchemaSerDe.fromJson(newJson).get();
    assertEquals(origNameId, rt.findIdByName("user.name"));
    assertEquals(origAgeId, rt.findIdByName("user.age"));

    InternalSchema legacy = SerDeHelper.fromJson(newJson).get();
    assertEquals(origNameId, legacy.findIdByName("user.name"));
    assertEquals(origAgeId, legacy.findIdByName("user.age"));
  }

  @Test
  public void docFieldEmittedOnlyWhenNonNull() {
    // Field with explicit doc -> JSON contains "doc" key.
    HoodieSchema withDoc = HoodieSchema.createRecord(
        "R", null, "ns", false,
        Collections.singletonList(HoodieSchemaField.of(
            "v", HoodieSchema.create(HoodieSchemaType.INT), "added back", null)));
    HoodieSchemaIdAssigner.assign(withDoc, 0);
    withDoc.setSchemaId(99L);
    withDoc.invalidateIdIndex();

    String json = HoodieSchemaSerDe.toJson(withDoc);
    assertTrue(json.contains("\"doc\":\"added back\""), () -> "expected doc key in: " + json);

    // Round-trip preserves the doc string.
    HoodieSchema rt = HoodieSchemaSerDe.fromJson(json).get();
    assertEquals("added back", rt.getField("v").get().doc().orElse(null));

    // Field without doc -> no "doc" key in JSON. The legacy parser tolerates
    // its absence (jsonNode.has(DOC) ? ... : null), so emitting it would still
    // parse but would drift from byte-for-byte equivalence with the legacy.
    HoodieSchema noDoc = oneFieldRecord(99L, "v", HoodieSchema.create(HoodieSchemaType.INT));
    String noDocJson = HoodieSchemaSerDe.toJson(noDoc);
    assertFalse(noDocJson.contains("\"doc\":"),
        () -> "expected no doc key in: " + noDocJson);
    HoodieSchema rt2 = HoodieSchemaSerDe.fromJson(noDocJson).get();
    assertNull(rt2.getField("v").get().doc().orElse(null));
  }

  @Test
  public void newFacadeJsonByteEqualsLegacyForCommonShape() {
    // Strongest check: for a schema with shape used by TestSpark3DDL
    // (members map, userx struct, top-level primitives), the new façade's
    // JSON must be byte-for-byte identical to what the legacy emits given
    // the same input, because that's what tables on disk were written with.
    HoodieSchema valueRec = HoodieSchema.createRecord(
        "members", "ns.hoodieSchema", null, false,
        Arrays.asList(
            HoodieSchemaField.of("n", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("a", HoodieSchema.create(HoodieSchemaType.INT))));
    HoodieSchema userRec = HoodieSchema.createRecord(
        "userx", "ns.hoodieSchema", null, false,
        Arrays.asList(
            HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("age", HoodieSchema.create(HoodieSchemaType.LONG)),
            HoodieSchemaField.of("score", HoodieSchema.create(HoodieSchemaType.INT))));
    HoodieSchema schema = HoodieSchema.createRecord(
        "Record", "ns", null, false,
        Arrays.asList(
            HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
            HoodieSchemaField.of("members", HoodieSchema.createMap(valueRec)),
            HoodieSchemaField.of("userx", userRec),
            HoodieSchemaField.of("ts", HoodieSchema.create(HoodieSchemaType.LONG))));
    HoodieSchemaIdAssigner.assign(schema, 0);
    schema.setSchemaId(123L);
    schema.invalidateIdIndex();

    String newJson = HoodieSchemaSerDe.toJson(schema);
    String legacyJson = SerDeHelper.toJson(HoodieSchemaInternalSchemaBridge.toInternalSchema(schema));

    assertEquals(legacyJson, newJson);
  }
}
