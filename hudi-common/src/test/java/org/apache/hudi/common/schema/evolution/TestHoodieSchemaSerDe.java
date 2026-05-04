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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HoodieSchemaSerDe}: round-trip stability of structure and
 * field ids across {@code toJson} / {@code fromJson} for primitives, arrays,
 * maps, and nested records.
 *
 * <p>Earlier revisions of this file also asserted byte-for-byte equality
 * against the legacy {@code SerDeHelper} output (so tables written by either
 * code path remained mutually readable). The legacy SerDe is being deleted;
 * the byte-format-compat regression net needs to be re-established as
 * golden-file tests against captured legacy output before the legacy package
 * actually goes. See `project_phase5_serde_rewrite.md` in auto-memory.
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
    assertEquals("latest_schema", HoodieSchemaSerDe.LATEST_SCHEMA);
    assertEquals("schemas", HoodieSchemaSerDe.SCHEMAS);
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
  // Per-primitive round-trip coverage. Each fixture is wrapped in a one-field
  // record so the wrapping schemaId / field-id behavior is exercised too.
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
   * Returns the primitive variants HoodieSchemaSerDe handles. Excludes
   * HoodieSchema's VECTOR / BLOB / VARIANT — those have no representation in
   * the wire format and are out of scope for this byte-compat boundary.
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

    // Re-serializing the parsed schema must produce the same JSON the original
    // emitted: that's the strongest stability check we can run without legacy
    // — it confirms the wire format is a stable fixed point of SerDe regardless
    // of the surface details (FIXED / decimal-fixed name/namespace, default-null
    // emission, etc.) that don't survive the round-trip exactly.
    String reJson = HoodieSchemaSerDe.toJson(parsed.get());
    assertEquals(json, reJson, label);
  }

  // ---------------------------------------------------------------------------
  // Nesting coverage: array of primitive, map<string, primitive>, map<string,
  // record>, nested record, doc-field emission. Each verifies HoodieSchemaSerDe
  // round-trip preserves ids at every addressable position.
  // ---------------------------------------------------------------------------

  @Test
  public void arrayOfPrimitiveSurvivesRoundTrip() {
    HoodieSchema schema = oneFieldRecord(7L, "arr",
        HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.INT)));

    String json = HoodieSchemaSerDe.toJson(schema);
    HoodieSchema rt = HoodieSchemaSerDe.fromJson(json).get();

    assertEquals(0, rt.getField("arr").get().fieldId());
    // Element id (next sequential id assigned by HoodieSchemaIdAssigner) survives —
    // looked up by the dotted full name "arr.element".
    int origElemId = schema.findIdByName("arr.element");
    assertTrue(origElemId >= 0);
    assertEquals(origElemId, rt.findIdByName("arr.element"));
  }

  @Test
  public void mapStringPrimitiveSurvivesRoundTrip() {
    HoodieSchema schema = oneFieldRecord(7L, "m",
        HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.LONG)));

    String json = HoodieSchemaSerDe.toJson(schema);
    HoodieSchema rt = HoodieSchemaSerDe.fromJson(json).get();

    // Map key/value ids are independent integers, looked up via the dotted
    // full names "m.key" / "m.value".
    int origKeyId = schema.findIdByName("m.key");
    int origValueId = schema.findIdByName("m.value");
    assertTrue(origKeyId >= 0);
    assertTrue(origValueId >= 0);

    assertEquals(origKeyId, rt.findIdByName("m.key"));
    assertEquals(origValueId, rt.findIdByName("m.value"));
  }

  @Test
  public void mapStringRecordSurvivesRoundTrip() {
    HoodieSchema valueRec = HoodieSchema.createRecord(
        "Inner", null, "ns", false,
        Arrays.asList(
            HoodieSchemaField.of("a", HoodieSchema.create(HoodieSchemaType.INT)),
            HoodieSchemaField.of("n", HoodieSchema.create(HoodieSchemaType.STRING))));
    HoodieSchema schema = oneFieldRecord(7L, "members", HoodieSchema.createMap(valueRec));

    String json = HoodieSchemaSerDe.toJson(schema);

    // Inner record fields are addressable at members.value.a / members.value.n.
    int origAId = schema.findIdByName("members.value.a");
    int origNId = schema.findIdByName("members.value.n");
    assertTrue(origAId >= 0);
    assertTrue(origNId >= 0);

    HoodieSchema rt = HoodieSchemaSerDe.fromJson(json).get();
    assertEquals(origAId, rt.findIdByName("members.value.a"));
    assertEquals(origNId, rt.findIdByName("members.value.n"));
  }

  @Test
  public void nestedRecordSurvivesRoundTrip() {
    HoodieSchema inner = HoodieSchema.createRecord(
        "User", null, "ns", false,
        Arrays.asList(
            HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("age", HoodieSchema.create(HoodieSchemaType.INT))));
    HoodieSchema schema = oneFieldRecord(7L, "user", inner);

    String json = HoodieSchemaSerDe.toJson(schema);

    int origNameId = schema.findIdByName("user.name");
    int origAgeId = schema.findIdByName("user.age");

    HoodieSchema rt = HoodieSchemaSerDe.fromJson(json).get();
    assertEquals(origNameId, rt.findIdByName("user.name"));
    assertEquals(origAgeId, rt.findIdByName("user.age"));
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

    // Field without doc -> no "doc" key in JSON. Emitting it would still parse
    // but would drift from the byte format we've shipped since the feature
    // landed.
    HoodieSchema noDoc = oneFieldRecord(99L, "v", HoodieSchema.create(HoodieSchemaType.INT));
    String noDocJson = HoodieSchemaSerDe.toJson(noDoc);
    assertFalse(noDocJson.contains("\"doc\":"),
        () -> "expected no doc key in: " + noDocJson);
    HoodieSchema rt2 = HoodieSchemaSerDe.fromJson(noDocJson).get();
    assertNull(rt2.getField("v").get().doc().orElse(null));
  }
}
