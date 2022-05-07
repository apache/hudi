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

package org.apache.hudi.internal.schema;

import org.apache.hudi.internal.schema.visitor.InternalSchemaVisitor;
import org.apache.hudi.internal.schema.visitor.NameToIDVisitor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A build class to help build fields for InternalSchema
 */
public class InternalSchemaBuilder implements Serializable {
  private static final InternalSchemaBuilder INSTANCE = new InternalSchemaBuilder();

  public static InternalSchemaBuilder getBuilder() {
    return INSTANCE;
  }

  private InternalSchemaBuilder() {
  }


  /**
   * Build a mapping from id to full field name for a internal Type.
   * if a field y belong to a struct filed x, then the full name of y is x.y
   *
   * @param type hoodie internal type
   * @return a mapping from id to full field name
   */
  public Map<Integer, String> buildIdToName(Type type) {
    Map<Integer, String> result = new HashMap<>();
    buildNameToId(type).forEach((k, v) -> result.put(v, k));
    return result;
  }

  /**
   * Build a mapping from full field name to id for a internal Type.
   * if a field y belong to a struct filed x, then the full name of y is x.y
   *
   * @param type hoodie internal type
   * @return a mapping from full field name to id
   */
  public Map<String, Integer> buildNameToId(Type type) {
    return visit(type, new NameToIDVisitor());
  }

  /**
   * Use to traverse all types in internalSchema with visitor.
   *
   * @param schema hoodie internal schema
   * @return vistor expected result.
   */
  public <T> T visit(InternalSchema schema, InternalSchemaVisitor<T> visitor) {
    return visitor.schema(schema, visit(schema.getRecord(), visitor));
  }

  public <T> T visit(Type type, InternalSchemaVisitor<T> visitor) {
    switch (type.typeId()) {
      case RECORD:
        Types.RecordType record = (Types.RecordType) type;
        List<T> results = new ArrayList<>();
        for (Types.Field f : record.fields()) {
          visitor.beforeField(f);
          T result;
          try {
            result = visit(f.type(), visitor);
          } finally {
            visitor.afterField(f);
          }
          results.add(visitor.field(f, result));
        }
        return visitor.record(record, results);
      case ARRAY:
        Types.ArrayType array = (Types.ArrayType) type;
        T elementResult;
        Types.Field elementField = array.field(array.elementId());
        visitor.beforeArrayElement(elementField);
        try {
          elementResult = visit(elementField.type(), visitor);
        } finally {
          visitor.afterArrayElement(elementField);
        }
        return visitor.array(array, elementResult);
      case MAP:
        Types.MapType map = (Types.MapType) type;
        T keyResult;
        T valueResult;
        Types.Field keyField = map.field(map.keyId());
        visitor.beforeMapKey(keyField);
        try {
          keyResult = visit(map.keyType(), visitor);
        } finally {
          visitor.afterMapKey(keyField);
        }
        Types.Field valueField = map.field(map.valueId());
        visitor.beforeMapValue(valueField);
        try {
          valueResult = visit(map.valueType(), visitor);
        } finally {
          visitor.afterMapValue(valueField);
        }
        return visitor.map(map, keyResult, valueResult);
      default:
        return visitor.primitive((Type.PrimitiveType)type);
    }
  }

  /**
   * Build a mapping from id to field for a internal Type.
   *
   * @param type hoodie internal type
   * @return a mapping from id to field
   */
  public Map<Integer, Types.Field> buildIdToField(Type type) {
    Map<Integer, Types.Field> idToField = new HashMap<>();
    visitIdToField(type, idToField);
    return idToField;
  }

  private void visitIdToField(Type type, Map<Integer, Types.Field> index) {
    switch (type.typeId()) {
      case RECORD:
        Types.RecordType record = (Types.RecordType) type;
        for (Types.Field field : record.fields()) {
          visitIdToField(field.type(), index);
          index.put(field.fieldId(), field);
        }
        return;
      case ARRAY:
        Types.ArrayType array = (Types.ArrayType) type;
        visitIdToField(array.elementType(), index);
        for (Types.Field field : array.fields()) {
          index.put(field.fieldId(), field);
        }
        return;
      case MAP:
        Types.MapType map = (Types.MapType) type;
        visitIdToField(map.keyType(), index);
        visitIdToField(map.valueType(), index);
        for (Types.Field field : map.fields()) {
          index.put(field.fieldId(), field);
        }
        return;
      default:
        return;
    }
  }

  /**
   * Build a mapping which maintain the relation between child field id and it's parent field id.
   * if a child field y(which id is 9) belong to a nest field x(which id is 6), then (9 -> 6) will be added to the result map.
   * if a field has no parent field, nothings will be added.
   *
   * @param record hoodie record type.
   * @return a mapping from id to parentId for a record Type
   */
  public Map<Integer, Integer> index2Parents(Types.RecordType record) {
    Map<Integer, Integer> result = new HashMap<>();
    Deque<Integer> parentIds = new LinkedList<>();
    index2Parents(record, parentIds, result);
    return result;
  }

  private void index2Parents(Type type, Deque<Integer> pids, Map<Integer, Integer> id2p) {
    switch (type.typeId()) {
      case RECORD:
        Types.RecordType record = (Types.RecordType)type;
        for (Types.Field f : record.fields()) {
          pids.push(f.fieldId());
          index2Parents(f.type(), pids, id2p);
          pids.pop();
        }

        for (Types.Field f : record.fields()) {
          // root record has no parent id.
          if (!pids.isEmpty()) {
            Integer pid = pids.peek();
            id2p.put(f.fieldId(), pid);
          }
        }
        return;
      case ARRAY:
        Types.ArrayType array = (Types.ArrayType) type;
        Types.Field elementField = array.field(array.elementId());
        pids.push(elementField.fieldId());
        index2Parents(elementField.type(), pids, id2p);
        pids.pop();
        id2p.put(array.elementId(), pids.peek());
        return;
      case MAP:
        Types.MapType map = (Types.MapType) type;
        Types.Field keyField = map.field(map.keyId());
        Types.Field valueField = map.field(map.valueId());
        // visit key
        pids.push(map.keyId());
        index2Parents(keyField.type(), pids, id2p);
        pids.pop();
        // visit value
        pids.push(map.valueId());
        index2Parents(valueField.type(), pids, id2p);
        pids.pop();
        id2p.put(map.keyId(), pids.peek());
        id2p.put(map.valueId(), pids.peek());
        return;
      default:
    }
  }

  /**
   * Assigns new ids for all fields in a Type, based on initial id.
   *
   * @param type a type.
   * @param nextId initial id which used to fresh ids for all fields in a type
   * @return a new type with new ids
   */
  public Type refreshNewId(Type type, AtomicInteger nextId) {
    switch (type.typeId()) {
      case RECORD:
        Types.RecordType record = (Types.RecordType) type;
        List<Types.Field> oldFields = record.fields();
        int currentId = nextId.get();
        nextId.set(currentId + record.fields().size());
        List<Types.Field> internalFields = new ArrayList<>();
        for (int i = 0; i < oldFields.size(); i++) {
          Types.Field oldField = oldFields.get(i);
          Type fieldType = refreshNewId(oldField.type(), nextId);
          internalFields.add(Types.Field.get(currentId++, oldField.isOptional(), oldField.name(), fieldType, oldField.doc()));
        }
        return Types.RecordType.get(internalFields);
      case ARRAY:
        Types.ArrayType array = (Types.ArrayType) type;
        int elementId = nextId.get();
        nextId.set(elementId + 1);
        Type elementType = refreshNewId(array.elementType(), nextId);
        return Types.ArrayType.get(elementId, array.isElementOptional(), elementType);
      case MAP:
        Types.MapType map = (Types.MapType) type;
        int keyId = nextId.get();
        int valueId = keyId + 1;
        nextId.set(keyId + 2);
        Type keyType = refreshNewId(map.keyType(), nextId);
        Type valueType = refreshNewId(map.valueType(), nextId);
        return Types.MapType.get(keyId, valueId, keyType, valueType, map.isValueOptional());
      default:
        return type;
    }
  }
}
