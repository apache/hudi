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

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.hudi.common.schema.HoodieSchema.ELEMENT_ID_PROP;
import static org.apache.hudi.common.schema.HoodieSchema.FIELD_ID_PROP;
import static org.apache.hudi.common.schema.HoodieSchema.KEY_ID_PROP;
import static org.apache.hudi.common.schema.HoodieSchema.VALUE_ID_PROP;

/**
 * Lazily-computed id ↔ full-name ↔ position indices for a {@link HoodieSchema}.
 *
 * <p>This is the HoodieSchema-side replacement for the four transient maps that
 * {@link org.apache.hudi.common.schema.evolution.legacy.InternalSchema} maintains
 * (idToField / nameToId / idToName / nameToPosition). It walks the HoodieSchema once
 * and reads {@code field-id} / {@code element-id} / {@code key-id} / {@code value-id}
 * Avro custom properties to populate the maps.</p>
 *
 * <p>"Full name" is dot-joined from the root, with array elements addressed as
 * {@code parent.element} and map keys / values as {@code parent.key} / {@code parent.value},
 * mirroring the convention already in use across the codebase
 * ({@link org.apache.hudi.common.schema.evolution.legacy.utils.InternalSchemaUtils#createFullName}).</p>
 */
public final class HoodieSchemaIndex {

  static final String ARRAY_ELEMENT = "element";
  static final String MAP_KEY = "key";
  static final String MAP_VALUE = "value";

  private final Map<Integer, String> idToName;
  private final Map<String, Integer> nameToId;
  private final Map<String, Integer> nameToPosition;
  private final Map<Integer, HoodieSchema> idToSchema;
  private final int maxColumnIdSeen;

  private HoodieSchemaIndex(Map<Integer, String> idToName,
                            Map<String, Integer> nameToId,
                            Map<String, Integer> nameToPosition,
                            Map<Integer, HoodieSchema> idToSchema,
                            int maxColumnIdSeen) {
    this.idToName = Collections.unmodifiableMap(idToName);
    this.nameToId = Collections.unmodifiableMap(nameToId);
    this.nameToPosition = Collections.unmodifiableMap(nameToPosition);
    this.idToSchema = Collections.unmodifiableMap(idToSchema);
    this.maxColumnIdSeen = maxColumnIdSeen;
  }

  public static HoodieSchemaIndex of(HoodieSchema schema) {
    Map<Integer, String> idToName = new HashMap<>();
    Map<String, Integer> nameToId = new LinkedHashMap<>();
    Map<String, Integer> nameToPosition = new LinkedHashMap<>();
    Map<Integer, HoodieSchema> idToSchema = new HashMap<>();
    int[] maxColumnId = {-1};
    int[] position = {0};
    Deque<String> path = new ArrayDeque<>();
    walk(schema, path, idToName, nameToId, nameToPosition, idToSchema, maxColumnId, position);
    return new HoodieSchemaIndex(idToName, nameToId, nameToPosition, idToSchema, maxColumnId[0]);
  }

  public Map<Integer, String> idToName() {
    return idToName;
  }

  public Map<String, Integer> nameToId() {
    return nameToId;
  }

  public Map<String, Integer> nameToPosition() {
    return nameToPosition;
  }

  /**
   * Returns the HoodieSchema (the type) of the field/element/key/value at column id
   * {@code id}, or null if no such id exists. Equivalent to
   * {@link org.apache.hudi.common.schema.evolution.legacy.InternalSchema#findType(int)}.
   */
  public Map<Integer, HoodieSchema> idToSchema() {
    return idToSchema;
  }

  public int maxColumnIdSeen() {
    return maxColumnIdSeen;
  }

  private static void walk(HoodieSchema schema,
                           Deque<String> path,
                           Map<Integer, String> idToName,
                           Map<String, Integer> nameToId,
                           Map<String, Integer> nameToPosition,
                           Map<Integer, HoodieSchema> idToSchema,
                           int[] maxColumnId,
                           int[] position) {
    HoodieSchema effective = schema.isNullable() ? schema.getNonNullType() : schema;
    switch (effective.getType()) {
      case RECORD:
        for (HoodieSchemaField field : effective.getFields()) {
          int id = readIntProp(field.getAvroField().getObjectProp(FIELD_ID_PROP), -1);
          String fullName = createFullName(field.name(), path);
          recordEntry(fullName, id, field.schema(), position, nameToId, idToName, nameToPosition, idToSchema, maxColumnId);
          path.push(field.name());
          try {
            walk(field.schema(), path, idToName, nameToId, nameToPosition, idToSchema, maxColumnId, position);
          } finally {
            path.pop();
          }
        }
        return;
      case ARRAY: {
        Schema arrAvro = effective.getAvroSchema();
        int elementId = readIntProp(arrAvro.getObjectProp(ELEMENT_ID_PROP), -1);
        String fullName = createFullName(ARRAY_ELEMENT, path);
        recordEntry(fullName, elementId, effective.getElementType(), position, nameToId, idToName, nameToPosition, idToSchema, maxColumnId);
        path.push(ARRAY_ELEMENT);
        try {
          walk(effective.getElementType(), path, idToName, nameToId, nameToPosition, idToSchema, maxColumnId, position);
        } finally {
          path.pop();
        }
        return;
      }
      case MAP: {
        Schema mapAvro = effective.getAvroSchema();
        int keyId = readIntProp(mapAvro.getObjectProp(KEY_ID_PROP), -1);
        int valueId = readIntProp(mapAvro.getObjectProp(VALUE_ID_PROP), -1);
        String keyName = createFullName(MAP_KEY, path);
        String valueName = createFullName(MAP_VALUE, path);
        recordEntry(keyName, keyId, HoodieSchema.create(HoodieSchemaType.STRING),
            position, nameToId, idToName, nameToPosition, idToSchema, maxColumnId);
        recordEntry(valueName, valueId, effective.getValueType(),
            position, nameToId, idToName, nameToPosition, idToSchema, maxColumnId);
        path.push(MAP_VALUE);
        try {
          walk(effective.getValueType(), path, idToName, nameToId, nameToPosition, idToSchema, maxColumnId, position);
        } finally {
          path.pop();
        }
        return;
      }
      default:
        // primitives have no addressable child name
    }
  }

  private static void recordEntry(String fullName,
                                  int id,
                                  HoodieSchema schema,
                                  int[] position,
                                  Map<String, Integer> nameToId,
                                  Map<Integer, String> idToName,
                                  Map<String, Integer> nameToPosition,
                                  Map<Integer, HoodieSchema> idToSchema,
                                  int[] maxColumnId) {
    nameToPosition.put(fullName, position[0]++);
    if (id >= 0) {
      nameToId.put(fullName, id);
      idToName.put(id, fullName);
      idToSchema.put(id, schema);
      if (id > maxColumnId[0]) {
        maxColumnId[0] = id;
      }
    }
  }

  private static int readIntProp(Object raw, int fallback) {
    if (raw instanceof Number) {
      return ((Number) raw).intValue();
    }
    return fallback;
  }

  static String createFullName(String name, Deque<String> path) {
    if (path.isEmpty()) {
      return name;
    }
    StringBuilder sb = new StringBuilder();
    Iterator<String> iter = path.descendingIterator();
    while (iter.hasNext()) {
      sb.append(iter.next()).append('.');
    }
    sb.append(name);
    return sb.toString();
  }
}
