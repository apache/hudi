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

package org.apache.hudi.avro;

import org.apache.hudi.io.util.FileIOUtils;

import org.apache.avro.Schema;
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

class TestAvroSchemaComparatorForSchemaEvolution {
  @Test
  void testAttrsIrrelevantToEquality() throws IOException {
    // Validates that schemas with different non-essential attributes (like doc strings or aliases)
    // are still considered equivalent for schema evolution purposes
    String schemaA = FileIOUtils.readAsUTFString(TestAvroSchemaComparatorForSchemaEvolution.class.getResourceAsStream("/avro-schema-evo/schema-allshapes-A.txt"));
    String schemaB = FileIOUtils.readAsUTFString(TestAvroSchemaComparatorForSchemaEvolution.class.getResourceAsStream("/avro-schema-evo/schema-allshapes-B.txt"));

    Schema schema1 = new Schema.Parser().parse(schemaA);
    Schema schema2 = new Schema.Parser().parse(schemaB);
    assertNotEquals(schema1, schema2);
    assertTrue(AvroSchemaComparatorForSchemaEvolution.schemaEquals(schema1, schema2));
    assertEquals(new AvroSchemaComparatorForSchemaEvolution.SchemaWrapper(schema1),
        new AvroSchemaComparatorForSchemaEvolution.SchemaWrapper(schema2));
  }

  @Test
  void testComparingPrimitiveTypes() {
    // Tests comparison of all primitive Avro types against each other
    // Validates that each primitive type is equal only to other avro schema sharing the same
    // primitive type.
    Schema.Type[] primitiveTypes = {
        Schema.Type.NULL, Schema.Type.BOOLEAN, Schema.Type.INT,
        Schema.Type.LONG, Schema.Type.FLOAT, Schema.Type.DOUBLE,
        Schema.Type.BYTES, Schema.Type.STRING
    };

    for (Schema.Type primitiveType : primitiveTypes) {
      for (Schema.Type type : primitiveTypes) {
        if (primitiveType == type) {
          assertTrue(AvroSchemaComparatorForSchemaEvolution.schemaEquals(
              Schema.create(primitiveType),
              Schema.create(type)
          ));
        } else {
          assertFalse(AvroSchemaComparatorForSchemaEvolution.schemaEquals(
              Schema.create(primitiveType),
              Schema.create(type)
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
    assertTrue(AvroSchemaComparatorForSchemaEvolution.schemaEquals(
        new Schema.Parser().parse(schema),
        new Schema.Parser().parse(schema)
    ));
  }

  @Test
  void testIsErrorFieldInRecordSchema() {
    // Tests that a record schema is not equal to an error schema
    // even if they have the same structure
    Schema record1 = Schema.createRecord("TestRecord", null, null, false);
    record1.setFields(Arrays.asList(
        new Schema.Field("field1", Schema.create(Schema.Type.STRING), null, null)
    ));

    Schema record2 = Schema.createRecord("TestRecord", null, null, true); // error record
    record2.setFields(Arrays.asList(
        new Schema.Field("field1", Schema.create(Schema.Type.STRING), null, null)
    ));

    assertFalse(AvroSchemaComparatorForSchemaEvolution.schemaEquals(record1, record2));
  }

  @Test
  void testRecordFieldTypes() {
    // Validates that records with fields of different types are not considered equal
    // even if the field names are the same
    String schema1 = "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"field1\",\"type\":\"string\"}]}";
    String schema2 = "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"field1\",\"type\":\"int\"}]}";
    assertFalse(AvroSchemaComparatorForSchemaEvolution.schemaEquals(
        new Schema.Parser().parse(schema1),
        new Schema.Parser().parse(schema2)
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
    assertFalse(AvroSchemaComparatorForSchemaEvolution.schemaEquals(
        new Schema.Parser().parse(schema1),
        new Schema.Parser().parse(schema2)
    ));
  }

  @Test
  void testArraySchema() {
    // Validates that array schemas with different item types are not equal
    // even if the array structure is the same
    String schema1 = "{\"type\":\"array\",\"items\":\"string\"}";
    String schema2 = "{\"type\":\"array\",\"items\":\"int\"}";
    assertFalse(AvroSchemaComparatorForSchemaEvolution.schemaEquals(
        new Schema.Parser().parse(schema1),
        new Schema.Parser().parse(schema2)
    ));
  }

  @Test
  void testMapSchema() {
    // Tests that map schemas with different value types are not equal
    // even if the map structure is the same
    String schema1 = "{\"type\":\"map\",\"values\":\"string\"}";
    String schema2 = "{\"type\":\"map\",\"values\":\"int\"}";
    assertFalse(AvroSchemaComparatorForSchemaEvolution.schemaEquals(
        new Schema.Parser().parse(schema1),
        new Schema.Parser().parse(schema2)
    ));
  }

  @Test
  void testFixedSchemaSizeAttr() {
    // Validates that fixed-type schemas with different sizes are not equal
    // Size is a critical attribute for fixed-length fields
    String schema1 = "{\"type\":\"fixed\",\"name\":\"F\",\"size\":16}";
    String schema2 = "{\"type\":\"fixed\",\"name\":\"F\",\"size\":32}";
    assertFalse(AvroSchemaComparatorForSchemaEvolution.schemaEquals(
        new Schema.Parser().parse(schema1),
        new Schema.Parser().parse(schema2)
    ));
  }

  @Test
  void testUnionMemberTypes() {
    // Tests that unions with different member types are not equal
    // even if they have the same number of members
    String schema1 = "[\"null\",\"string\"]";
    String schema2 = "[\"null\",\"int\"]";
    assertFalse(AvroSchemaComparatorForSchemaEvolution.schemaEquals(
        new Schema.Parser().parse(schema1),
        new Schema.Parser().parse(schema2)
    ));
  }

  @Test
  void testUnionMemberOrdering() {
    // Validates that the order of union members doesn't affect equality
    String schema1 = "[\"null\",\"string\"]";
    String schema2 = "[\"string\",\"null\"]";
    assertTrue(AvroSchemaComparatorForSchemaEvolution.schemaEquals(
        new Schema.Parser().parse(schema1),
        new Schema.Parser().parse(schema2)
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
    assertFalse(AvroSchemaComparatorForSchemaEvolution.schemaEquals(
        new Schema.Parser().parse(schema1),
        new Schema.Parser().parse(schema2)
    ));

    assertFalse(AvroSchemaComparatorForSchemaEvolution.schemaEquals(
        new Schema.Parser().parse(schema2),
        new Schema.Parser().parse(schema3)
    ));
  }

  @Test
  void testLogicalType() {
    // Validates that different logical types on the same underlying type are not equal
    String schema1 = "{\"type\":\"int\",\"logicalType\":\"date\"}";
    String schema2 = "{\"type\":\"int\",\"logicalType\":\"time-millis\"}";
    assertFalse(AvroSchemaComparatorForSchemaEvolution.schemaEquals(
        new Schema.Parser().parse(schema1),
        new Schema.Parser().parse(schema2)
    ));
  }

  @Test
  void testLogicalTypesWithDifferentPrimitiveTypes() {
    // Tests that logical types with different underlying types are not equal
    // even if they represent the same logical concept (decimal)
    String decimalFixed = "{\"type\":\"fixed\",\"name\":\"D\",\"size\":16,\"logicalType\":\"decimal\",\"precision\":10,\"scale\":2}";
    String decimalBytes = "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":10,\"scale\":2}";
    assertFalse(AvroSchemaComparatorForSchemaEvolution.schemaEquals(
        new Schema.Parser().parse(decimalFixed),
        new Schema.Parser().parse(decimalBytes)
    ));
  }

  @Test
  void testComparingSchemaFieldNames() {
    // Validates that schemas with different names are not equal
    // even if their structure is identical - tests for records, enums, and fixed types
    String record1 = "{\"type\":\"record\",\"name\":\"R1\",\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";
    String record2 = "{\"type\":\"record\",\"name\":\"R2\",\"fields\":[{\"name\":\"f2\",\"type\":\"string\"}]}";
    assertFalse(AvroSchemaComparatorForSchemaEvolution.schemaEquals(
        new Schema.Parser().parse(record1),
        new Schema.Parser().parse(record2)
    ));

    // Enum
    String enum1 = "{\"type\":\"enum\",\"name\":\"E1\",\"symbols\":[\"A\"]}";
    String enum2 = "{\"type\":\"enum\",\"name\":\"E2\",\"symbols\":[\"A\"]}";
    assertFalse(AvroSchemaComparatorForSchemaEvolution.schemaEquals(
        new Schema.Parser().parse(enum1),
        new Schema.Parser().parse(enum2)
    ));

    // Fixed
    String fixed1 = "{\"type\":\"fixed\",\"name\":\"F1\",\"size\":16}";
    String fixed2 = "{\"type\":\"fixed\",\"name\":\"F2\",\"size\":16}";
    assertFalse(AvroSchemaComparatorForSchemaEvolution.schemaEquals(
        new Schema.Parser().parse(fixed1),
        new Schema.Parser().parse(fixed2)
    ));
  }

  @Test
  void testEnumSchemaName() {
    // Tests that enum schemas with different names are not equal
    // even if they have the same symbols
    Schema schema1 = Schema.createEnum("enum1", null, null, Arrays.asList("A", "B", "C"));
    Schema schema2 = Schema.createEnum("enum2", null, null, Arrays.asList("A", "B", "C"));
    Schema schema3 = Schema.createEnum("enum1", null, null, Arrays.asList("A", "B", "C"));

    assertFalse(AvroSchemaComparatorForSchemaEvolution.schemaEquals(schema1, schema2));
    assertTrue(AvroSchemaComparatorForSchemaEvolution.schemaEquals(schema1, schema3));
  }

  @Test
  void testEnumSchema() {
    // Validates that enum schemas with different symbol sets are not equal
    // even if one is a subset of the other
    Schema schema1 = Schema.createEnum("enum", null, null, Arrays.asList("A", "C"));
    Schema schema2 = Schema.createEnum("enum", null, null, Arrays.asList("A", "B", "C"));

    assertFalse(AvroSchemaComparatorForSchemaEvolution.schemaEquals(schema1, schema2));
  }

  @Test
  void testEnumSymbolsOrder() {
    // Tests that enum schemas with different symbol orders are not equal
    // Order matters for enum serialization
    String schema1 = "{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"A\",\"B\"]}";
    String schema2 = "{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"B\",\"A\"]}";
    assertFalse(AvroSchemaComparatorForSchemaEvolution.schemaEquals(
        new Schema.Parser().parse(schema1),
        new Schema.Parser().parse(schema2)
    ));
  }

  @Test
  void testDefaultValueEquality() {
    // Tests comparison of schemas with different default values
    Schema.Field field1 = new Schema.Field("field", Schema.create(Schema.Type.STRING), null, "default1");
    Schema.Field field2 = new Schema.Field("field", Schema.create(Schema.Type.STRING), null, "default2");
    Schema.Field field3 = new Schema.Field("field", Schema.create(Schema.Type.STRING), null, "default1");
    Schema.Field fieldNoDefault = new Schema.Field("field", Schema.create(Schema.Type.STRING), null, null);

    Schema record1 = Schema.createRecord("test", null, null, false);
    record1.setFields(Collections.singletonList(field1));

    Schema record2 = Schema.createRecord("test", null, null, false);
    record2.setFields(Collections.singletonList(field2));

    Schema record3 = Schema.createRecord("test", null, null, false);
    record3.setFields(Collections.singletonList(field3));

    Schema recordNoDefault = Schema.createRecord("test", null, null, false);
    recordNoDefault.setFields(Collections.singletonList(fieldNoDefault));

    // Different default values should not be equal
    assertFalse(AvroSchemaComparatorForSchemaEvolution.schemaEquals(record1, record2));

    // Same default values should be equal
    assertTrue(AvroSchemaComparatorForSchemaEvolution.schemaEquals(record1, record3));

    // No default value vs default value should not be equal
    assertFalse(AvroSchemaComparatorForSchemaEvolution.schemaEquals(record1, recordNoDefault));

    // No default values should be equal to each other
    assertTrue(AvroSchemaComparatorForSchemaEvolution.schemaEquals(recordNoDefault, recordNoDefault));
  }

  @Test
  void testComplexDefaultValueEquality() {
    // Tests equality comparison of schemas with complex default values (nested records)
    // Validates that default value comparison works correctly for nested structures
    Schema innerSchema = Schema.createRecord("inner", null, null, false);
    innerSchema.setFields(Collections.singletonList(
        new Schema.Field("value", Schema.create(Schema.Type.STRING), null, null)
    ));

    // Create default values as JSON-compatible Maps
    Map<String, Object> defaultValue1 = new HashMap<>();
    defaultValue1.put("value", "test");

    Map<String, Object> defaultValue2 = new HashMap<>();
    defaultValue2.put("value", "test");

    Map<String, Object> defaultValue3 = new HashMap<>();
    defaultValue3.put("value", "different");

    Schema.Field field1 = new Schema.Field("field", innerSchema, null, defaultValue1);
    Schema.Field field2 = new Schema.Field("field", innerSchema, null, defaultValue2);
    Schema.Field field3 = new Schema.Field("field", innerSchema, null, defaultValue3);

    Schema record1 = Schema.createRecord("test", null, null, false);
    record1.setFields(Collections.singletonList(field1));

    Schema record2 = Schema.createRecord("test", null, null, false);
    record2.setFields(Collections.singletonList(field2));

    Schema record3 = Schema.createRecord("test", null, null, false);
    record3.setFields(Collections.singletonList(field3));

    // Same complex default values should be equal
    assertTrue(AvroSchemaComparatorForSchemaEvolution.schemaEquals(record1, record2));

    // Different complex default values should not be equal
    assertFalse(AvroSchemaComparatorForSchemaEvolution.schemaEquals(record1, record3));
  }

  @Test
  void testArrayDefaultValueEquality() {
    // Tests equality comparison of schemas with array default values
    // Validates that default value comparison works correctly for array types
    List<String> defaultArray1 = Arrays.asList("a", "b", "c");
    List<String> defaultArray2 = Arrays.asList("a", "b", "c");
    List<String> defaultArray3 = Arrays.asList("x", "y", "z");

    Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.STRING));

    Schema.Field field1 = new Schema.Field("field", arraySchema, null, defaultArray1);
    Schema.Field field2 = new Schema.Field("field", arraySchema, null, defaultArray2);
    Schema.Field field3 = new Schema.Field("field", arraySchema, null, defaultArray3);

    Schema record1 = Schema.createRecord("test", null, null, false);
    record1.setFields(Collections.singletonList(field1));

    Schema record2 = Schema.createRecord("test", null, null, false);
    record2.setFields(Collections.singletonList(field2));

    Schema record3 = Schema.createRecord("test", null, null, false);
    record3.setFields(Collections.singletonList(field3));

    // Same array default values should be equal
    assertTrue(AvroSchemaComparatorForSchemaEvolution.schemaEquals(record1, record2));

    // Different array default values should not be equal
    assertFalse(AvroSchemaComparatorForSchemaEvolution.schemaEquals(record1, record3));
  }

  @Test
  void testCompareWithNull() {
    // Tests schema comparison behavior when one or both schemas are null
    // Validates proper handling of null cases in the comparator
    Schema schema = Schema.create(Schema.Type.STRING);
    assertFalse(AvroSchemaComparatorForSchemaEvolution.schemaEquals(schema, null));
    assertFalse(AvroSchemaComparatorForSchemaEvolution.schemaEquals(null, schema));
    assertTrue(AvroSchemaComparatorForSchemaEvolution.schemaEquals(null, null));
  }

  @Test
  void testRecordFieldCountMismatch() {
    // Tests that records with different number of fields are not equal
    // even if all common fields match
    Schema record1 = Schema.createRecord("TestRecord", null, null, false);
    record1.setFields(Collections.singletonList(
        new Schema.Field("field1", Schema.create(Schema.Type.STRING), null, null)
    ));

    Schema record2 = Schema.createRecord("TestRecord", null, null, false);
    record2.setFields(Arrays.asList(
        new Schema.Field("field1", Schema.create(Schema.Type.STRING), null, null),
        new Schema.Field("field2", Schema.create(Schema.Type.STRING), null, null)
    ));

    assertFalse(AvroSchemaComparatorForSchemaEvolution.schemaEquals(record1, record2));
  }

  @Test
  void testUnionSizeMismatch() {
    // Tests that unions with different number of types are not equal
    // even if all common types match
    Schema union1 = Schema.createUnion(Arrays.asList(
        Schema.create(Schema.Type.NULL),
        Schema.create(Schema.Type.STRING)
    ));

    Schema union2 = Schema.createUnion(Arrays.asList(
        Schema.create(Schema.Type.NULL),
        Schema.create(Schema.Type.STRING),
        Schema.create(Schema.Type.INT)
    ));

    assertFalse(AvroSchemaComparatorForSchemaEvolution.schemaEquals(union1, union2));
  }

  @Test
  void testUnionOrder() {
    // Tests that the order of types in a union doesn't affect equality
    // Important for schema evolution as union member order shouldn't matter
    Schema union1 = Schema.createUnion(Arrays.asList(
        Schema.create(Schema.Type.NULL),
        Schema.create(Schema.Type.STRING)
    ));

    Schema union2 = Schema.createUnion(Arrays.asList(
        Schema.create(Schema.Type.STRING),
        Schema.create(Schema.Type.NULL)
    ));

    assertTrue(AvroSchemaComparatorForSchemaEvolution.schemaEquals(union1, union2));
  }

  @Test
  void testLogicalTypeOneNull() {
    // Tests comparison of schemas where one has a logical type and the other doesn't
    // Validates that logical type presence affects equality
    String schema1 = "{\"type\":\"int\",\"logicalType\":\"date\"}";
    String schema2 = "{\"type\":\"int\"}";

    assertFalse(AvroSchemaComparatorForSchemaEvolution.schemaEquals(
        new Schema.Parser().parse(schema1),
        new Schema.Parser().parse(schema2)
    ));

    // Swap the 2 schema position should have no effect.
    assertFalse(AvroSchemaComparatorForSchemaEvolution.schemaEquals(
        new Schema.Parser().parse(schema2),
        new Schema.Parser().parse(schema1)
    ));
  }

  @Test
  void testSchemaWrapperNullAndTypeMismatch() {
    // Tests SchemaWrapper's null handling and type comparison behavior
    // Validates proper handling of edge cases in the wrapper class
    Schema schema = Schema.create(Schema.Type.STRING);
    AvroSchemaComparatorForSchemaEvolution.SchemaWrapper wrapper = new AvroSchemaComparatorForSchemaEvolution.SchemaWrapper(schema);

    assertNotNull(wrapper);
    assertNotEquals(wrapper, new Object());
  }
}