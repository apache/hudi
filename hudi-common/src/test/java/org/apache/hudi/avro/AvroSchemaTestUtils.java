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

package org.apache.hudi.avro;

import org.apache.hudi.exception.HoodieAvroSchemaException;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.avro.HoodieAvroUtils.createFullName;

public class AvroSchemaTestUtils {
  public static Schema.Field createNestedField(String name, Schema.Type type) {
    return createNestedField(name, Schema.create(type));
  }

  public static Schema.Field createNestedField(String name, Schema schema) {
    return new Schema.Field(name, createRecord(name, new Schema.Field("nested", schema, null, null)), null, null);
  }

  public static Schema.Field createArrayField(String name, Schema.Type type) {
    return createArrayField(name, Schema.create(type));
  }

  public static Schema.Field createArrayField(String name, Schema schema) {
    return new Schema.Field(name, Schema.createArray(schema), null, null);
  }

  public static Schema.Field createNullableArrayField(String name, Schema schema) {
    return new Schema.Field(name, Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.createArray(schema)), null, Schema.Field.NULL_VALUE);
  }

  public static Schema.Field createMapField(String name, Schema.Type type) {
    return createMapField(name, Schema.create(type));
  }

  public static Schema.Field createMapField(String name, Schema schema) {
    return new Schema.Field(name, Schema.createMap(schema), null, null);
  }

  public static Schema.Field createPrimitiveField(String name, Schema.Type type) {
    return new Schema.Field(name, Schema.create(type), null, null);
  }

  public static Schema.Field createNullablePrimitiveField(String name, Schema.Type type) {
    return new Schema.Field(name, AvroSchemaUtils.createNullableSchema(type), null, JsonProperties.NULL_VALUE);
  }

  public static Schema createRecord(String name, Schema.Field... fields) {
    return Schema.createRecord(name, null, null, false, Arrays.asList(fields));
  }

  public static Schema createNullableRecord(String name, Schema.Field... fields) {
    return AvroSchemaUtils.createNullableSchema(Schema.createRecord(name, null, null, false, Arrays.asList(fields)));
  }

  public static void validateRecordsHaveSameData(Object expected, Object actual) {
    validateRecordsHaveSameData(expected, actual, new LinkedList<>());
  }

  private static void validateRecordsHaveSameData(Object expected, Object actual, Deque<String> fieldNames) {
    if (expected instanceof GenericRecord) {
      if (!(actual instanceof GenericRecord)) {
        throw new HoodieAvroSchemaException("Expected record but got " + actual.getClass().getName() + " for " + createFullName(fieldNames));
      }
      GenericRecord expectedRecord = (GenericRecord) expected;
      GenericRecord actualRecord = (GenericRecord) actual;
      if (!AvroSchemaUtils.areSchemasProjectionEquivalent(expectedRecord.getSchema(), actualRecord.getSchema())) {
        throw new HoodieAvroSchemaException("Expected record schema " + expectedRecord.getSchema() + " but got " + actualRecord.getSchema() + " for " + createFullName(fieldNames));
      }
      for (Schema.Field field : expectedRecord.getSchema().getFields()) {
        fieldNames.push(field.name());
        validateRecordsHaveSameData(expectedRecord.get(field.name()), actualRecord.get(field.name()), fieldNames);
        fieldNames.pop();
      }
    } else if (expected instanceof Collection) {
      if (!(actual instanceof Collection)) {
        throw new HoodieAvroSchemaException("Expected collection but got " + actual.getClass().getName());
      }
      Collection expectedCollection = (Collection) expected;
      Collection actualCollection = (Collection) actual;
      if (expectedCollection.size() != actualCollection.size()) {
        throw new HoodieAvroSchemaException("Expected collection size " + expectedCollection.size() + " but got " + actualCollection.size() + " for " + createFullName(fieldNames));
      }
      Iterator<?> expectedIterator = expectedCollection.iterator();
      Iterator<?> actualIterator = actualCollection.iterator();
      fieldNames.push("element");
      while (expectedIterator.hasNext() && actualIterator.hasNext()) {
        validateRecordsHaveSameData(expectedIterator.next(), actualIterator.next(), fieldNames);
      }
      fieldNames.pop();
    } else if (expected instanceof Map) {
      if (!(actual instanceof Map)) {
        throw new HoodieAvroSchemaException("Expected map but got " + actual.getClass().getName() + " for " + createFullName(fieldNames));
      }
      Map expectedMap = (Map) expected;
      Map actualMap = (Map) actual;
      if (expectedMap.size() != actualMap.size()) {
        throw new HoodieAvroSchemaException("Expected map size " + expectedMap.size() + " but got " + actualMap.size() + " for " + createFullName(fieldNames));
      }
      if (!expectedMap.keySet().equals(actualMap.keySet())) {
        Set<String> expectedKeys = (Set<String>) expectedMap.keySet().stream().map(Object::toString).collect(Collectors.toSet());
        Set<String> actualKeys = (Set<String>) actualMap.keySet().stream().map(Object::toString).collect(Collectors.toSet());
        if (!expectedKeys.equals(actualKeys)) {
          throw new HoodieAvroSchemaException("Expected map keys " + expectedMap.keySet() + " but got " + actualMap.keySet() + " for " + createFullName(fieldNames));
        } else {
          Map<String, Object> realExpectedMap = (Map<String, Object>) expectedMap.entrySet().stream()
              .collect(Collectors.toMap(e -> ((Map.Entry) e).getKey().toString(), e -> ((Map.Entry) e).getValue()));
          Map<String, Object> realActualMap = (Map<String, Object>) actualMap.entrySet().stream()
              .collect(Collectors.toMap(e -> ((Map.Entry) e).getKey().toString(), e -> ((Map.Entry) e).getValue()));
          fieldNames.push("value");
          for (String key : realExpectedMap.keySet()) {
            validateRecordsHaveSameData(realExpectedMap.get(key), realActualMap.get(key), fieldNames);
          }
          fieldNames.pop();
        }
      } else {
        fieldNames.push("value");
        for (Object key : expectedMap.keySet()) {
          validateRecordsHaveSameData(expectedMap.get(key), actualMap.get(key), fieldNames);
        }
        fieldNames.pop();
      }
    } else {
      if (!Objects.equals(expected, actual)) {
        if (expected != null && actual != null && (expected instanceof Utf8 || actual instanceof Utf8  || expected instanceof GenericData.EnumSymbol || actual instanceof GenericData.EnumSymbol)) {
          String expectedString = expected.toString();
          String actualString = actual.toString();
          if (expectedString.equals(actualString)) {
            return;
          }
        }
        throw new HoodieAvroSchemaException("For " + createFullName(fieldNames) + " Expected " + expected + " but got " + actual);
      }
    }
  }
}
