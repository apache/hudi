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

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Validates that integer "field-id" custom properties survive a full Avro
 * {@link Schema#toString()} -> {@link Schema.Parser#parse(String)} round trip on
 * every container surface that the InternalSchema -> HoodieSchema migration relies on:
 *
 * <ul>
 *   <li>Each {@link Schema.Field} (the carrier for {@code field-id} on flat and nested fields)</li>
 *   <li>Array element schema (the carrier for {@code element-id})</li>
 *   <li>Map schema (the carrier for {@code key-id} and {@code value-id})</li>
 * </ul>
 *
 * <p>The whole subsume-InternalSchema-into-HoodieSchema design depends on Avro preserving
 * these properties through serialization. If this test ever fails, the design needs a
 * different ID-carrier (e.g. a sidecar JSON), so this is intentionally a pinned test of
 * the underlying Avro guarantee rather than of Hudi code.</p>
 *
 * <p>Schema-level metadata (schemaId / maxColumnId) intentionally does NOT travel through
 * Avro JSON — it lives on direct fields of {@link org.apache.hudi.common.schema.HoodieSchema}
 * and is serialized via SerDeHelper instead, sidestepping Avro's set-once property
 * restriction.</p>
 */
public class TestHoodieSchemaFieldIdRoundTrip {

  private static final String FIELD_ID = "field-id";
  private static final String ELEMENT_ID = "element-id";
  private static final String KEY_ID = "key-id";
  private static final String VALUE_ID = "value-id";

  @Test
  public void fieldIdSurvivesOnFlatRecord() {
    List<Schema.Field> fields = new ArrayList<>();
    Schema.Field a = new Schema.Field("a", Schema.create(Schema.Type.INT), null, null);
    a.addProp(FIELD_ID, 1);
    Schema.Field b = new Schema.Field("b", Schema.create(Schema.Type.STRING), null, null);
    b.addProp(FIELD_ID, 2);
    fields.add(a);
    fields.add(b);

    Schema record = Schema.createRecord("R", null, "ns", false, fields);

    Schema parsed = new Schema.Parser().parse(record.toString());

    assertEquals(1, ((Number) parsed.getField("a").getObjectProp(FIELD_ID)).intValue());
    assertEquals(2, ((Number) parsed.getField("b").getObjectProp(FIELD_ID)).intValue());
  }

  @Test
  public void fieldIdSurvivesOnNestedRecord() {
    Schema.Field inner = new Schema.Field("x", Schema.create(Schema.Type.LONG), null, null);
    inner.addProp(FIELD_ID, 11);
    Schema innerRecord = Schema.createRecord("Inner", null, "ns", false, listOf(inner));

    Schema.Field outer = new Schema.Field("nested", innerRecord, null, null);
    outer.addProp(FIELD_ID, 10);
    Schema outerRecord = Schema.createRecord("Outer", null, "ns", false, listOf(outer));

    Schema parsed = new Schema.Parser().parse(outerRecord.toString());

    Schema.Field parsedOuter = parsed.getField("nested");
    assertEquals(10, ((Number) parsedOuter.getObjectProp(FIELD_ID)).intValue());
    Schema.Field parsedInner = parsedOuter.schema().getField("x");
    assertEquals(11, ((Number) parsedInner.getObjectProp(FIELD_ID)).intValue());
  }

  @Test
  public void fieldIdSurvivesOnArrayElement() {
    Schema element = Schema.create(Schema.Type.STRING);
    Schema arr = Schema.createArray(element);
    // Avro disallows props on the unwrapped primitive but allows them on the array container.
    arr.addProp(ELEMENT_ID, 7);

    Schema.Field arrField = new Schema.Field("xs", arr, null, null);
    arrField.addProp(FIELD_ID, 5);
    Schema record = Schema.createRecord("R", null, "ns", false, listOf(arrField));

    Schema parsed = new Schema.Parser().parse(record.toString());

    Schema.Field parsedField = parsed.getField("xs");
    assertEquals(5, ((Number) parsedField.getObjectProp(FIELD_ID)).intValue());
    assertEquals(7, ((Number) parsedField.schema().getObjectProp(ELEMENT_ID)).intValue());
  }

  @Test
  public void fieldIdSurvivesOnMapKeyAndValue() {
    Schema map = Schema.createMap(Schema.create(Schema.Type.LONG));
    map.addProp(KEY_ID, 21);
    map.addProp(VALUE_ID, 22);

    Schema.Field mapField = new Schema.Field("m", map, null, null);
    mapField.addProp(FIELD_ID, 20);
    Schema record = Schema.createRecord("R", null, "ns", false, listOf(mapField));

    Schema parsed = new Schema.Parser().parse(record.toString());

    Schema.Field parsedField = parsed.getField("m");
    assertEquals(20, ((Number) parsedField.getObjectProp(FIELD_ID)).intValue());
    assertEquals(21, ((Number) parsedField.schema().getObjectProp(KEY_ID)).intValue());
    assertEquals(22, ((Number) parsedField.schema().getObjectProp(VALUE_ID)).intValue());
  }

  @Test
  public void fieldIdSurvivesInsideUnionMembers() {
    // A nullable record field: union(null, inner-record). The inner record's field IDs
    // must survive even though it's wrapped in a union.
    Schema.Field innerField = new Schema.Field("x", Schema.create(Schema.Type.INT), null, null);
    innerField.addProp(FIELD_ID, 31);
    Schema inner = Schema.createRecord("Inner", null, "ns", false, listOf(innerField));

    Schema nullable = Schema.createUnion(Schema.create(Schema.Type.NULL), inner);
    Schema.Field outer = new Schema.Field("opt", nullable, null, Schema.Field.NULL_DEFAULT_VALUE);
    outer.addProp(FIELD_ID, 30);
    Schema record = Schema.createRecord("R", null, "ns", false, listOf(outer));

    Schema parsed = new Schema.Parser().parse(record.toString());

    Schema.Field parsedOuter = parsed.getField("opt");
    assertEquals(30, ((Number) parsedOuter.getObjectProp(FIELD_ID)).intValue());
    Schema parsedInner = parsedOuter.schema().getTypes().get(1); // [null, Inner]
    Schema.Field parsedInnerField = parsedInner.getField("x");
    assertNotNull(parsedInnerField);
    assertEquals(31, ((Number) parsedInnerField.getObjectProp(FIELD_ID)).intValue());
  }

  private static List<Schema.Field> listOf(Schema.Field... fs) {
    List<Schema.Field> list = new ArrayList<>(fs.length);
    for (Schema.Field f : fs) {
      list.add(f);
    }
    return list;
  }
}
