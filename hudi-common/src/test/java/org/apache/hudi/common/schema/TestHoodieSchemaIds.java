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

package org.apache.hudi.common.schema;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the Phase-0 HoodieSchema id surface: {@link HoodieSchemaIdAssigner},
 * {@link HoodieSchemaIndex}, and the new accessors on {@link HoodieSchema} /
 * {@link HoodieSchemaField}.
 *
 * <p>Verifies the three behaviors the InternalSchema-subsumption depends on:
 * (1) sequential id assignment matches the InternalSchema traversal order so on-disk
 *     compatibility is preserved; (2) id assignment is idempotent so reapplying it
 *     after a partial migration doesn't shift ids; (3) ids survive
 *     {@code Schema#toString} → {@code Schema.Parser#parse} round trips end-to-end via
 *     the HoodieSchema accessors.</p>
 */
public class TestHoodieSchemaIds {

  private static HoodieSchema buildFlatRecord() {
    return HoodieSchema.createRecord(
        "Flat", null, "ns", false,
        Arrays.asList(
            HoodieSchemaField.of("a", HoodieSchema.create(HoodieSchemaType.INT)),
            HoodieSchemaField.of("b", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("c", HoodieSchema.create(HoodieSchemaType.LONG))));
  }

  private static HoodieSchema buildNestedRecord() {
    HoodieSchema inner = HoodieSchema.createRecord(
        "Inner", null, "ns", false,
        Arrays.asList(
            HoodieSchemaField.of("x", HoodieSchema.create(HoodieSchemaType.INT)),
            HoodieSchemaField.of("y", HoodieSchema.create(HoodieSchemaType.STRING))));
    return HoodieSchema.createRecord(
        "Outer", null, "ns", false,
        Arrays.asList(
            HoodieSchemaField.of("a", HoodieSchema.create(HoodieSchemaType.INT)),
            HoodieSchemaField.of("nested", inner)));
  }

  @Test
  public void assignsSequentialIdsToFlatRecord() {
    HoodieSchema schema = buildFlatRecord();
    int max = HoodieSchemaIdAssigner.assign(schema, 0);

    schema.invalidateIdIndex();
    assertEquals(0, schema.getField("a").get().fieldId());
    assertEquals(1, schema.getField("b").get().fieldId());
    assertEquals(2, schema.getField("c").get().fieldId());
    assertEquals(2, max);
    assertEquals(2, schema.maxColumnId());
  }

  @Test
  public void assignsIdsBreadthFirstThenRecursesForNested() {
    // Mirrors InternalSchemaConverter#visitSchemaToBuildType: outer fields first, then
    // recurse into inner. For Outer{a, nested:Inner{x, y}}: a=0, nested=1, x=2, y=3.
    HoodieSchema schema = buildNestedRecord();
    HoodieSchemaIdAssigner.assign(schema, 0);

    schema.invalidateIdIndex();
    assertEquals(0, schema.getField("a").get().fieldId());
    assertEquals(1, schema.getField("nested").get().fieldId());
    HoodieSchema inner = schema.getField("nested").get().schema();
    assertEquals(2, inner.getField("x").get().fieldId());
    assertEquals(3, inner.getField("y").get().fieldId());
  }

  @Test
  public void assignmentIsIdempotent() {
    HoodieSchema schema = buildNestedRecord();
    int firstMax = HoodieSchemaIdAssigner.assign(schema, 0);
    int secondMax = HoodieSchemaIdAssigner.assign(schema, 0);
    assertEquals(firstMax, secondMax);

    schema.invalidateIdIndex();
    assertEquals(0, schema.getField("a").get().fieldId());
    assertEquals(1, schema.getField("nested").get().fieldId());
    HoodieSchema inner = schema.getField("nested").get().schema();
    assertEquals(2, inner.getField("x").get().fieldId());
    assertEquals(3, inner.getField("y").get().fieldId());
  }

  @Test
  public void indexResolvesIdsAndPositions() {
    HoodieSchema schema = buildNestedRecord();
    HoodieSchemaIdAssigner.assign(schema, 0);
    schema.invalidateIdIndex();

    assertEquals(0, schema.findIdByName("a"));
    assertEquals(1, schema.findIdByName("nested"));
    assertEquals(2, schema.findIdByName("nested.x"));
    assertEquals(3, schema.findIdByName("nested.y"));

    assertEquals("a", schema.findFullName(0));
    assertEquals("nested", schema.findFullName(1));
    assertEquals("nested.x", schema.findFullName(2));
    assertEquals("nested.y", schema.findFullName(3));

    Set<Integer> ids = schema.getAllIds();
    assertTrue(ids.containsAll(Arrays.asList(0, 1, 2, 3)));
    assertEquals(4, ids.size());
  }

  @Test
  public void fieldIdsRoundTripThroughAvroJson() {
    // field-id Avro properties survive Schema#toString -> Schema.Parser#parse.
    // Schema-level metadata (schemaId / maxColumnId) does NOT — those live on
    // direct fields of HoodieSchema and are persisted via SerDeHelper, not Avro JSON.
    HoodieSchema schema = buildFlatRecord();
    HoodieSchemaIdAssigner.assign(schema, 0);

    String json = schema.getAvroSchema().toString();
    HoodieSchema reparsed = HoodieSchema.parse(json);

    assertEquals(0, reparsed.findIdByName("a"));
    assertEquals(1, reparsed.findIdByName("b"));
    assertEquals(2, reparsed.findIdByName("c"));
  }

  @Test
  public void schemaIdAndMaxColumnIdAreMutableFields() {
    // setSchemaId / setMaxColumnId can be called multiple times — they're plain
    // fields on HoodieSchema, not Avro custom properties, so the Avro set-once
    // restriction does not apply.
    HoodieSchema schema = buildFlatRecord();
    schema.setSchemaId(99L);
    assertEquals(99L, schema.schemaId());
    schema.setSchemaId(100L);
    assertEquals(100L, schema.schemaId());

    schema.setMaxColumnId(50);
    assertEquals(50, schema.maxColumnId());
    schema.setMaxColumnId(60);
    assertEquals(60, schema.maxColumnId());
  }

  @Test
  public void respectsExistingIdsAndStartsAfterMax() {
    // Pre-stamp one field with an id so the assigner has to skip past it.
    HoodieSchema schema = HoodieSchema.createRecord(
        "Pre", null, "ns", false,
        Arrays.asList(
            HoodieSchemaField.of("a", HoodieSchema.create(HoodieSchemaType.INT)),
            HoodieSchemaField.of("b", HoodieSchema.create(HoodieSchemaType.INT))));
    schema.getField("a").get().getAvroField().addProp(HoodieSchema.FIELD_ID_PROP, 100);

    int max = HoodieSchemaIdAssigner.assign(schema, 0);
    schema.invalidateIdIndex();

    assertEquals(100, schema.getField("a").get().fieldId());
    assertEquals(101, schema.getField("b").get().fieldId());
    assertEquals(101, max);
  }

  @Test
  public void unassignedFieldReturnsNegativeOne() {
    HoodieSchema schema = buildFlatRecord();
    assertEquals(-1, schema.getField("a").get().fieldId());
    assertEquals(-1, schema.findIdByName("a"));
    assertEquals(-1, schema.findIdByName(""));
    assertEquals(-1, schema.findIdByName(null));
    assertEquals("", schema.findFullName(42));
    assertEquals(-1, schema.schemaId());
    // maxColumnId falls back to the index, which sees no ids -> -1.
    assertEquals(-1, schema.maxColumnId());
  }

  @Test
  public void emptyRecordHasEmptyIdIndex() {
    HoodieSchema schema = HoodieSchema.createRecord("Empty", null, "ns", false, Collections.emptyList());
    HoodieSchemaIdAssigner.assign(schema, 0);
    assertTrue(schema.getAllIds().isEmpty());
    assertTrue(schema.getNameToPosition().isEmpty());
  }
}
