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

package org.apache.hudi.internal.schema.utils;

import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.InternalSchemaBuilder;
import org.apache.hudi.internal.schema.Types;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests {@link InternalSchemaUtils}.
 */
public class TestInternalSchemaUtils {
  @Test
  public void testPruneSchema() {
    Types.RecordType record = getSimpleRecordType();
    InternalSchema originSchema = new InternalSchema(Types.RecordType.get(record.fields()));
    List<Integer> prunedCols = new ArrayList<>();
    prunedCols.add(4);
    prunedCols.add(3);
    prunedCols.add(0);
    prunedCols.add(2);
    InternalSchema prunedSchema = InternalSchemaUtils.pruneInternalSchemaByID(originSchema, prunedCols, null);
    InternalSchema checkedSchema = new InternalSchema(Types.RecordType.get(Arrays.asList(new Types.Field[] {
            Types.Field.get(0, "bool", Types.BooleanType.get()),
            Types.Field.get(2, "long", Types.LongType.get()),
            Types.Field.get(3, "float", Types.FloatType.get()),
            Types.Field.get(4, "double", Types.DoubleType.get())
        })));
    assertEquals(prunedSchema, checkedSchema);

    // nest schema
    Types.RecordType nestRecord = getNestRecordType();
    InternalSchema originNestSchema = new InternalSchema(Types.RecordType.get(nestRecord.fields()));
    List<Integer> prunedNestCols = new ArrayList<>();
    prunedNestCols.add(0);
    prunedNestCols.add(1);
    prunedNestCols.add(5);
    prunedNestCols.add(11);
    InternalSchema prunedNestSchema = InternalSchemaUtils.pruneInternalSchemaByID(originNestSchema, prunedNestCols, null);
  }

  @Test
  public void testInternalSchemaVisitor() {
    Types.RecordType nestRecord = getNestRecordType();
    Map<String, Integer> result = InternalSchemaBuilder.getBuilder().buildNameToId(nestRecord);
    assertEquals(result.size(), 12);
    assertEquals(result.get("locations.value.long"), 11);
    assertEquals(result.get("locations.value.lat"), 10);
    assertEquals(result.get("locations.value"), 9);
    assertEquals(result.get("locations.key"), 8);
    assertEquals(result.get("doubles.element"), 7);

    Types.RecordType simpleRecord = getSimpleRecordType();
    Map<String, Integer> result1 = InternalSchemaBuilder.getBuilder().buildNameToId(simpleRecord);
    assertEquals(result1.size(), 5);
    assertEquals(result1.get("double"), 4);
  }

  @Test
  public void testIntTypeEqualsAfterDeserialization() throws Exception {
    Types.IntType intType = Types.IntType.get();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    new ObjectOutputStream(baos).writeObject(intType);
    Types.IntType deserializedIntType = (Types.IntType)
        new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray())).readObject();
    assertEquals(intType, deserializedIntType);
  }

  @Test
  void testFieldOrderingVisitor() {
    Types.RecordType nestRecord = getNestRecordType();
    Map<String, Integer> result = new InternalSchema(nestRecord).getNameToPosition();
    Map<String, Integer> expected = new HashMap<>();
    expected.put("id", 0);
    expected.put("data", 1);
    expected.put("preferences", 2);
    expected.put("preferences.feature1", 3);
    expected.put("preferences.feature2", 4);
    expected.put("doubles", 5);
    expected.put("doubles.element", 6);
    expected.put("locations", 7);
    expected.put("locations.key", 8);
    expected.put("locations.value", 9);
    expected.put("locations.value.lat", 10);
    expected.put("locations.value.long", 11);
    assertEquals(expected, result);
  }

  public Types.RecordType getNestRecordType() {
    return Types.RecordType.get(Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "data", Types.StringType.get()),
        Types.Field.get(2, true, "preferences",
            Types.RecordType.get(Types.Field.get(5, false, "feature1",
                Types.BooleanType.get()), Types.Field.get(6, true, "feature2", Types.BooleanType.get()))),
        Types.Field.get(3, false,"doubles", Types.ArrayType.get(7, false, Types.DoubleType.get())),
        Types.Field.get(4, false, "locations", Types.MapType.get(8, 9, Types.StringType.get(),
            Types.RecordType.get(Types.Field.get(10, false, "lat", Types.FloatType.get()), Types.Field.get(11, false, "long", Types.FloatType.get())), false))
    );
  }

  public Types.RecordType getSimpleRecordType() {
    return Types.RecordType.get(Arrays.asList(new Types.Field[] {
        Types.Field.get(0, "bool", Types.BooleanType.get()),
        Types.Field.get(1, "int", Types.IntType.get()),
        Types.Field.get(2, "long", Types.LongType.get()),
        Types.Field.get(3, "float", Types.FloatType.get()),
        Types.Field.get(4, "double", Types.DoubleType.get())
    }));
  }
}
