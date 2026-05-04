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

import static org.apache.hudi.common.schema.HoodieSchema.ELEMENT_ID_PROP;
import static org.apache.hudi.common.schema.HoodieSchema.FIELD_ID_PROP;
import static org.apache.hudi.common.schema.HoodieSchema.KEY_ID_PROP;
import static org.apache.hudi.common.schema.HoodieSchema.VALUE_ID_PROP;

/**
 * Walks a {@link HoodieSchema} and assigns sequential integer ids to every addressable
 * sub-schema (record fields, array elements, map keys and values), writing the result
 * onto the underlying Avro {@link Schema}/{@link Schema.Field} as custom JSON properties.
 *
 * <p>The traversal order is fixed: record fields in declared order; array element id
 * assigned before recursing into the element type; map key id then value id assigned
 * before recursing into the value type. This ordering matches the legacy
 * InternalSchemaBuilder so id assignments stay stable across the
 * InternalSchema → HoodieSchema migration and round-tripping a previously-IDed
 * InternalSchema through HoodieSchema produces the same
 * id mapping.</p>
 *
 * <p>Existing ids are preserved: if a node already has an id property, this assigner uses
 * that id and bumps {@code nextId} past it, so reapplying the assigner to a partially
 * IDed schema is idempotent. {@link #assign(HoodieSchema, int)} returns the new
 * {@code maxColumnId} so callers can persist it on the schema root.</p>
 */
public final class HoodieSchemaIdAssigner {

  private HoodieSchemaIdAssigner() {
  }

  /**
   * Assigns ids onto {@code schema} starting from {@code startId} and writes the resulting
   * {@code max-column-id} onto the schema root.
   *
   * @return the maximum column id present after assignment
   */
  public static int assign(HoodieSchema schema, int startId) {
    int[] nextId = {startId};
    visit(schema, nextId);
    int maxId = nextId[0] - 1;
    schema.setMaxColumnId(maxId);
    return maxId;
  }

  /**
   * Assigns ids onto {@code schema}, starting after the highest id already present
   * (or 0 if none). This is the entry point for first-time id assignment on a schema
   * that has no prior IDs.
   */
  public static int assignFresh(HoodieSchema schema) {
    HoodieSchemaIndex index = HoodieSchemaIndex.of(schema);
    int start = Math.max(0, index.maxColumnIdSeen() + 1);
    return assign(schema, start);
  }

  private static void visit(HoodieSchema schema, int[] nextId) {
    HoodieSchema effective = schema.isNullable() ? schema.getNonNullType() : schema;
    switch (effective.getType()) {
      case RECORD:
        // Mirror InternalSchemaConverter ordering: assign ids to all fields at this level
        // before recursing into any of them.
        for (HoodieSchemaField field : effective.getFields()) {
          assignFieldId(field, nextId);
        }
        for (HoodieSchemaField field : effective.getFields()) {
          visit(field.schema(), nextId);
        }
        return;
      case ARRAY: {
        Schema arr = effective.getAvroSchema();
        assignContainerId(arr, ELEMENT_ID_PROP, nextId);
        visit(effective.getElementType(), nextId);
        return;
      }
      case MAP: {
        Schema map = effective.getAvroSchema();
        assignContainerId(map, KEY_ID_PROP, nextId);
        assignContainerId(map, VALUE_ID_PROP, nextId);
        visit(effective.getValueType(), nextId);
        return;
      }
      default:
        // primitive: nothing to assign
    }
  }

  private static void assignFieldId(HoodieSchemaField field, int[] nextId) {
    Schema.Field avroField = field.getAvroField();
    Object existing = avroField.getObjectProp(FIELD_ID_PROP);
    if (existing instanceof Number) {
      int existingId = ((Number) existing).intValue();
      if (existingId >= nextId[0]) {
        nextId[0] = existingId + 1;
      }
      return;
    }
    avroField.addProp(FIELD_ID_PROP, nextId[0]++);
  }

  private static void assignContainerId(Schema container, String propName, int[] nextId) {
    Object existing = container.getObjectProp(propName);
    if (existing instanceof Number) {
      int existingId = ((Number) existing).intValue();
      if (existingId >= nextId[0]) {
        nextId[0] = existingId + 1;
      }
      return;
    }
    container.addProp(propName, nextId[0]++);
  }
}
