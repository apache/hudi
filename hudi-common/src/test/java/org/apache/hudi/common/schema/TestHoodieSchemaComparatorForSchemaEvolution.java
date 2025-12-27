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

import org.apache.hudi.io.util.FileIOUtils;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieSchemaComparatorForSchemaEvolution {
  @Test
  void testAttrsIrrelevantToEquality() throws IOException {
    // Validates that schemas with different non-essential attributes (like doc strings or aliases)
    // are still considered equivalent for schema evolution purposes
    String schemaA = FileIOUtils.readAsUTFString(TestHoodieSchemaComparatorForSchemaEvolution.class.getResourceAsStream("/avro-schema-evo/schema-allshapes-A.txt"));
    String schemaB = FileIOUtils.readAsUTFString(TestHoodieSchemaComparatorForSchemaEvolution.class.getResourceAsStream("/avro-schema-evo/schema-allshapes-B.txt"));

    HoodieSchema schema1 = HoodieSchema.parse(schemaA);
    HoodieSchema schema2 = HoodieSchema.parse(schemaB);
    assertNotEquals(schema1, schema2);
    assertTrue(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(schema1, schema2));
    assertEquals(new HoodieSchemaComparatorForSchemaEvolution.SchemaWrapper(schema1),
        new HoodieSchemaComparatorForSchemaEvolution.SchemaWrapper(schema2));
  }

  @Test
  void testComparingPrimitiveTypes() {
    // Tests comparison of all primitive types against each other
    // Validates that each primitive type is equal only to other schemas sharing the same
    // primitive type.
    HoodieSchemaType[] primitiveTypes = {
        HoodieSchemaType.NULL, HoodieSchemaType.BOOLEAN, HoodieSchemaType.INT,
        HoodieSchemaType.LONG, HoodieSchemaType.FLOAT, HoodieSchemaType.DOUBLE,
        HoodieSchemaType.BYTES, HoodieSchemaType.STRING
    };

    for (HoodieSchemaType primitiveType : primitiveTypes) {
      for (HoodieSchemaType type : primitiveTypes) {
        if (primitiveType == type) {
          assertTrue(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(
              HoodieSchema.create(primitiveType),
              HoodieSchema.create(type)
          ));
        } else {
          assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(
              HoodieSchema.create(primitiveType),
              HoodieSchema.create(type)
          ), String.format("Types %s and %s should not be equal",
              primitiveType, type));
        }
      }
    }
  }

  @Test
  void testEqualToSelf() {
    // Validates that a schema is equal to itself
    // Basic sanity check for schema comparison
    String schema = "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"field1\",\"type\":\"string\"}]}";
    assertTrue(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(
        HoodieSchema.parse(schema),
        HoodieSchema.parse(schema)
    ));
  }

  @Test
  void testIsErrorFieldInRecordSchema() {
    // Tests that a record schema is not equal to an error schema
    // even if they have the same structure
    HoodieSchema record1 = HoodieSchema.createRecord("TestRecord", null, null, false,
        Arrays.asList(
            HoodieSchemaField.of("field1", HoodieSchema.create(HoodieSchemaType.STRING), null, null)
        ));

    HoodieSchema record2 = HoodieSchema.createRecord("TestRecord", null, null, true, // error record
        Arrays.asList(
            HoodieSchemaField.of("field1", HoodieSchema.create(HoodieSchemaType.STRING), null, null)
        ));

    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(record1, record2));
  }

  @Test
  void testRecordFieldTypes() {
    // Validates that records with fields of different types are not considered equal
    // even if the field names are the same
    String schema1 = "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"field1\",\"type\":\"string\"}]}";
    String schema2 = "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"field1\",\"type\":\"int\"}]}";
    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(
        HoodieSchema.parse(schema1),
        HoodieSchema.parse(schema2)
    ));
  }

  @Test
  void testRecordFieldOrderAttribute() {
    // Tests that records with different field order attributes are not equal
    // This is important for schema evolution as order affects serialization
    String schema1 = "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"field1\",\"type\":\"string\",\"order\":\"ascending\"}]}";
    String schema2 = "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"field1\",\"type\":\"string\",\"order\":\"descending\"}]}";
    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(
        HoodieSchema.parse(schema1),
        HoodieSchema.parse(schema2)
    ));
  }

  @Test
  void testArraySchema() {
    // Validates that array schemas with different item types are not equal
    // even if the array structure is the same
    String schema1 = "{\"type\":\"array\",\"items\":\"string\"}";
    String schema2 = "{\"type\":\"array\",\"items\":\"int\"}";
    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(
        HoodieSchema.parse(schema1),
        HoodieSchema.parse(schema2)
    ));
  }

  @Test
  void testMapSchema() {
    // Tests that map schemas with different value types are not equal
    // even if the map structure is the same
    String schema1 = "{\"type\":\"map\",\"values\":\"string\"}";
    String schema2 = "{\"type\":\"map\",\"values\":\"int\"}";
    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(
        HoodieSchema.parse(schema1),
        HoodieSchema.parse(schema2)
    ));
  }

  @Test
  void testFixedSchemaSizeAttr() {
    // Validates that fixed-type schemas with different sizes are not equal
    // Size is a critical attribute for fixed-length fields
    String schema1 = "{\"type\":\"fixed\",\"name\":\"F\",\"size\":16}";
    String schema2 = "{\"type\":\"fixed\",\"name\":\"F\",\"size\":32}";
    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(
        HoodieSchema.parse(schema1),
        HoodieSchema.parse(schema2)
    ));
  }

  @Test
  void testUnionMemberTypes() {
    // Tests that unions with different member types are not equal
    // even if they have the same number of members
    String schema1 = "[\"null\",\"string\"]";
    String schema2 = "[\"null\",\"int\"]";
    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(
        HoodieSchema.parse(schema1),
        HoodieSchema.parse(schema2)
    ));
  }

  @Test
  void testUnionMemberOrdering() {
    // Validates that the order of union members doesn't affect equality
    String schema1 = "[\"null\",\"string\"]";
    String schema2 = "[\"string\",\"null\"]";
    assertTrue(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(
        HoodieSchema.parse(schema1),
        HoodieSchema.parse(schema2)
    ));
  }

  @Test
  void testLogicalTypeDecimalAttr() {
    // Tests that decimal logical types with different precision and scale are not equal
    String schema1 = "{\"type\":\"fixed\",\"name\":\"D\",\"size\":16,"
        + "\"logicalType\":\"decimal\",\"precision\":10,\"scale\":2}";
    String schema2 = "{\"type\":\"fixed\",\"name\":\"D\",\"size\":16,"
        + "\"logicalType\":\"decimal\",\"precision\":8,\"scale\":2}";
    String schema3 = "{\"type\":\"fixed\",\"name\":\"D\",\"size\":16,"
        + "\"logicalType\":\"decimal\",\"precision\":8,\"scale\":3}";
    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(
        HoodieSchema.parse(schema1),
        HoodieSchema.parse(schema2)
    ));

    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(
        HoodieSchema.parse(schema2),
        HoodieSchema.parse(schema3)
    ));
  }

  @Test
  void testLogicalType() {
    // Validates that different logical types on the same underlying type are not equal
    String schema1 = "{\"type\":\"int\",\"logicalType\":\"date\"}";
    String schema2 = "{\"type\":\"int\",\"logicalType\":\"time-millis\"}";
    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(
        HoodieSchema.parse(schema1),
        HoodieSchema.parse(schema2)
    ));
  }

  @Test
  void testLogicalTypesWithDifferentPrimitiveTypes() {
    // Tests that logical types with different underlying types are not equal
    // even if they represent the same logical concept (decimal)
    String decimalFixed = "{\"type\":\"fixed\",\"name\":\"D\",\"size\":16,\"logicalType\":\"decimal\",\"precision\":10,\"scale\":2}";
    String decimalBytes = "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":10,\"scale\":2}";
    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(
        HoodieSchema.parse(decimalFixed),
        HoodieSchema.parse(decimalBytes)
    ));
  }

  @Test
  void testComparingSchemaFieldNames() {
    // Validates that schemas with different names are not equal
    // even if their structure is identical - tests for records, enums, and fixed types
    String record1 = "{\"type\":\"record\",\"name\":\"R1\",\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";
    String record2 = "{\"type\":\"record\",\"name\":\"R2\",\"fields\":[{\"name\":\"f2\",\"type\":\"string\"}]}";
    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(
        HoodieSchema.parse(record1),
        HoodieSchema.parse(record2)
    ));

    // Enum
    String enum1 = "{\"type\":\"enum\",\"name\":\"E1\",\"symbols\":[\"A\"]}";
    String enum2 = "{\"type\":\"enum\",\"name\":\"E2\",\"symbols\":[\"A\"]}";
    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(
        HoodieSchema.parse(enum1),
        HoodieSchema.parse(enum2)
    ));

    // Fixed
    String fixed1 = "{\"type\":\"fixed\",\"name\":\"F1\",\"size\":16}";
    String fixed2 = "{\"type\":\"fixed\",\"name\":\"F2\",\"size\":16}";
    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(
        HoodieSchema.parse(fixed1),
        HoodieSchema.parse(fixed2)
    ));
  }

  @Test
  void testEnumSchemaName() {
    // Tests that enum schemas with different names are not equal
    // even if they have the same symbols
    HoodieSchema schema1 = HoodieSchema.createEnum("enum1", null, null, Arrays.asList("A", "B", "C"));
    HoodieSchema schema2 = HoodieSchema.createEnum("enum2", null, null, Arrays.asList("A", "B", "C"));
    HoodieSchema schema3 = HoodieSchema.createEnum("enum1", null, null, Arrays.asList("A", "B", "C"));

    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(schema1, schema2));
    assertTrue(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(schema1, schema3));
  }

  @Test
  void testEnumSchema() {
    // Validates that enum schemas with different symbol sets are not equal
    // even if one is a subset of the other
    HoodieSchema schema1 = HoodieSchema.createEnum("enum", null, null, Arrays.asList("A", "C"));
    HoodieSchema schema2 = HoodieSchema.createEnum("enum", null, null, Arrays.asList("A", "B", "C"));

    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(schema1, schema2));
  }

  @Test
  void testEnumSymbolsOrder() {
    // Tests that enum schemas with different symbol orders are not equal
    // Order matters for enum serialization
    String schema1 = "{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"A\",\"B\"]}";
    String schema2 = "{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"B\",\"A\"]}";
    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(
        HoodieSchema.parse(schema1),
        HoodieSchema.parse(schema2)
    ));
  }

  @Test
  void testDefaultValueEquality() {
    // Tests comparison of schemas with different default values
    HoodieSchemaField field1 = HoodieSchemaField.of("field", HoodieSchema.create(HoodieSchemaType.STRING), null, "default1");
    HoodieSchemaField field2 = HoodieSchemaField.of("field", HoodieSchema.create(HoodieSchemaType.STRING), null, "default2");
    HoodieSchemaField field3 = HoodieSchemaField.of("field", HoodieSchema.create(HoodieSchemaType.STRING), null, "default1");
    HoodieSchemaField fieldNoDefault = HoodieSchemaField.of("field", HoodieSchema.create(HoodieSchemaType.STRING), null, null);

    HoodieSchema record1 = HoodieSchema.createRecord("test", null, null, false, Collections.singletonList(field1));
    HoodieSchema record2 = HoodieSchema.createRecord("test", null, null, false, Collections.singletonList(field2));
    HoodieSchema record3 = HoodieSchema.createRecord("test", null, null, false, Collections.singletonList(field3));
    HoodieSchema recordNoDefault = HoodieSchema.createRecord("test", null, null, false, Collections.singletonList(fieldNoDefault));

    // Different default values should not be equal
    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(record1, record2));

    // Same default values should be equal
    assertTrue(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(record1, record3));

    // No default value vs default value should not be equal
    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(record1, recordNoDefault));

    // No default values should be equal to each other
    assertTrue(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(recordNoDefault, recordNoDefault));
  }

  @Test
  void testComplexDefaultValueEquality() {
    // Tests equality comparison of schemas with complex default values (nested records)
    // Validates that default value comparison works correctly for nested structures
    HoodieSchema innerSchema = HoodieSchema.createRecord("inner", null, null, false,
        Collections.singletonList(
            HoodieSchemaField.of("value", HoodieSchema.create(HoodieSchemaType.STRING), null, null)
        ));

    // Create default values as JSON-compatible Maps
    Map<String, Object> defaultValue1 = new HashMap<>();
    defaultValue1.put("value", "test");

    Map<String, Object> defaultValue2 = new HashMap<>();
    defaultValue2.put("value", "test");

    Map<String, Object> defaultValue3 = new HashMap<>();
    defaultValue3.put("value", "different");

    HoodieSchemaField field1 = HoodieSchemaField.of("field", innerSchema, null, defaultValue1);
    HoodieSchemaField field2 = HoodieSchemaField.of("field", innerSchema, null, defaultValue2);
    HoodieSchemaField field3 = HoodieSchemaField.of("field", innerSchema, null, defaultValue3);

    HoodieSchema record1 = HoodieSchema.createRecord("test", null, null, false, Collections.singletonList(field1));
    HoodieSchema record2 = HoodieSchema.createRecord("test", null, null, false, Collections.singletonList(field2));
    HoodieSchema record3 = HoodieSchema.createRecord("test", null, null, false, Collections.singletonList(field3));

    // Same complex default values should be equal
    assertTrue(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(record1, record2));

    // Different complex default values should not be equal
    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(record1, record3));
  }

  @Test
  void testArrayDefaultValueEquality() {
    // Tests equality comparison of schemas with array default values
    // Validates that default value comparison works correctly for array types
    List<String> defaultArray1 = Arrays.asList("a", "b", "c");
    List<String> defaultArray2 = Arrays.asList("a", "b", "c");
    List<String> defaultArray3 = Arrays.asList("x", "y", "z");

    HoodieSchema arraySchema = HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.STRING));

    HoodieSchemaField field1 = HoodieSchemaField.of("field", arraySchema, null, defaultArray1);
    HoodieSchemaField field2 = HoodieSchemaField.of("field", arraySchema, null, defaultArray2);
    HoodieSchemaField field3 = HoodieSchemaField.of("field", arraySchema, null, defaultArray3);

    HoodieSchema record1 = HoodieSchema.createRecord("test", null, null, false, Collections.singletonList(field1));
    HoodieSchema record2 = HoodieSchema.createRecord("test", null, null, false, Collections.singletonList(field2));
    HoodieSchema record3 = HoodieSchema.createRecord("test", null, null, false, Collections.singletonList(field3));

    // Same array default values should be equal
    assertTrue(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(record1, record2));

    // Different array default values should not be equal
    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(record1, record3));
  }

  @Test
  void testCompareWithNull() {
    // Tests schema comparison behavior when one or both schemas are null
    // Validates proper handling of null cases in the comparator
    HoodieSchema schema = HoodieSchema.create(HoodieSchemaType.STRING);
    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(schema, null));
    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(null, schema));
    assertTrue(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(null, null));
  }

  @Test
  void testRecordFieldCountMismatch() {
    // Tests that records with different number of fields are not equal
    // even if all common fields match
    HoodieSchema record1 = HoodieSchema.createRecord("TestRecord", null, null, false,
        Collections.singletonList(
            HoodieSchemaField.of("field1", HoodieSchema.create(HoodieSchemaType.STRING), null, null)
        ));

    HoodieSchema record2 = HoodieSchema.createRecord("TestRecord", null, null, false,
        Arrays.asList(
            HoodieSchemaField.of("field1", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
            HoodieSchemaField.of("field2", HoodieSchema.create(HoodieSchemaType.STRING), null, null)
        ));

    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(record1, record2));
  }

  @Test
  void testUnionSizeMismatch() {
    // Tests that unions with different number of types are not equal
    // even if all common types match
    HoodieSchema union1 = HoodieSchema.createUnion(Arrays.asList(
        HoodieSchema.create(HoodieSchemaType.NULL),
        HoodieSchema.create(HoodieSchemaType.STRING)
    ));

    HoodieSchema union2 = HoodieSchema.createUnion(Arrays.asList(
        HoodieSchema.create(HoodieSchemaType.NULL),
        HoodieSchema.create(HoodieSchemaType.STRING),
        HoodieSchema.create(HoodieSchemaType.INT)
    ));

    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(union1, union2));
  }

  @Test
  void testUnionOrder() {
    // Tests that the order of types in a union doesn't affect equality
    // Important for schema evolution as union member order shouldn't matter
    HoodieSchema union1 = HoodieSchema.createUnion(Arrays.asList(
        HoodieSchema.create(HoodieSchemaType.NULL),
        HoodieSchema.create(HoodieSchemaType.STRING)
    ));

    HoodieSchema union2 = HoodieSchema.createUnion(Arrays.asList(
        HoodieSchema.create(HoodieSchemaType.STRING),
        HoodieSchema.create(HoodieSchemaType.NULL)
    ));

    assertTrue(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(union1, union2));
  }

  @Test
  void testLogicalTypeOneNull() {
    // Tests comparison of schemas where one has a logical type and the other doesn't
    // Validates that logical type presence affects equality
    String schema1 = "{\"type\":\"int\",\"logicalType\":\"date\"}";
    String schema2 = "{\"type\":\"int\"}";

    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(
        HoodieSchema.parse(schema1),
        HoodieSchema.parse(schema2)
    ));

    // Swap the 2 schema position should have no effect.
    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(
        HoodieSchema.parse(schema2),
        HoodieSchema.parse(schema1)
    ));
  }

  @Test
  void testSchemaWrapperNullAndTypeMismatch() {
    // Tests SchemaWrapper's null handling and type comparison behavior
    // Validates proper handling of edge cases in the wrapper class
    HoodieSchema schema = HoodieSchema.create(HoodieSchemaType.STRING);
    HoodieSchemaComparatorForSchemaEvolution.SchemaWrapper wrapper = new HoodieSchemaComparatorForSchemaEvolution.SchemaWrapper(schema);

    assertNotNull(wrapper);
    assertNotEquals(wrapper, new Object());
  }

  @Test
  void testTimestampLogicalTypeEquality() {
    // Tests that timestamp logical types with different precisions are not equal
    String timestampMillis = "{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}";
    String timestampMicros = "{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}";

    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(
        HoodieSchema.parse(timestampMillis),
        HoodieSchema.parse(timestampMicros)
    ));
  }

  @Test
  void testTimeLogicalTypeEquality() {
    // Tests that time logical types with different precisions are not equal
    String timeMillis = "{\"type\":\"int\",\"logicalType\":\"time-millis\"}";
    String timeMicros = "{\"type\":\"long\",\"logicalType\":\"time-micros\"}";

    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(
        HoodieSchema.parse(timeMillis),
        HoodieSchema.parse(timeMicros)
    ));
  }
}