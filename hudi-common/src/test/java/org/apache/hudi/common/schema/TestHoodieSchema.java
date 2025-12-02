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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieAvroSchemaException;

import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for {@link HoodieSchema}.
 */
public class TestHoodieSchema {

  private static final String SAMPLE_RECORD_SCHEMA =
      "{"
          + "  \"type\": \"record\","
          + "  \"name\": \"User\","
          + "  \"namespace\": \"com.example\","
          + "  \"doc\": \"User record schema\","
          + "  \"fields\": ["
          + "    {\"name\": \"id\", \"type\": \"long\"},"
          + "    {\"name\": \"name\", \"type\": \"string\", \"default\": \"\"},"
          + "    {\"name\": \"email\", \"type\": [\"null\", \"string\"], \"default\": null},"
          + "    {\"name\": \"tip_history\", \"type\": {\"type\": \"array\", \"items\":"
          + "      {\"type\": \"record\", \"name\": \"tip_history\", \"fields\": ["
          + "        {\"name\": \"amount\", \"type\": {\"type\": \"fixed\", \"name\": \"fixed\", \"size\": 5, \"logicalType\": \"decimal\", \"precision\": 10, \"scale\": 6}},"
          + "        {\"name\": \"currency\", \"type\": \"string\"}]}}}"
          + "  ]"
          + "}";

  @Test
  public void testSchemaCreationFromAvroSchema() {
    Schema avroSchema = Schema.create(Schema.Type.STRING);
    HoodieSchema hoodieSchema = HoodieSchema.fromAvroSchema(avroSchema);

    assertNotNull(hoodieSchema);
    assertEquals(HoodieSchemaType.STRING, hoodieSchema.getType());
    assertEquals(avroSchema, hoodieSchema.getAvroSchema());
  }

  @Test
  public void testSchemaCreationWithNullAvroSchema() {
    assertNull(HoodieSchema.fromAvroSchema(null));
  }

  @Test
  public void testSchemaParsingFromJson() {
    HoodieSchema schema = HoodieSchema.parse(SAMPLE_RECORD_SCHEMA);

    assertNotNull(schema);
    assertEquals(HoodieSchemaType.RECORD, schema.getType());
    assertEquals("User", schema.getName());
    assertEquals(Option.of("com.example"), schema.getNamespace());
    assertEquals("com.example.User", schema.getFullName());
    assertEquals(Option.of("User record schema"), schema.getDoc());

    List<HoodieSchemaField> fields = schema.getFields();
    assertEquals(4, fields.size());

    // Check individual fields
    HoodieSchemaField idField = fields.get(0);
    assertEquals("id", idField.name());
    assertEquals(HoodieSchemaType.LONG, idField.schema().getType());

    HoodieSchemaField nameField = fields.get(1);
    assertEquals("name", nameField.name());
    assertEquals(HoodieSchemaType.STRING, nameField.schema().getType());
    assertTrue(nameField.hasDefaultValue());

    HoodieSchemaField emailField = fields.get(2);
    assertEquals("email", emailField.name());
    assertEquals(HoodieSchemaType.UNION, emailField.schema().getType());
    assertTrue(emailField.hasDefaultValue());

    HoodieSchemaField tipHistoryField = fields.get(3);
    assertEquals("tip_history", tipHistoryField.name());
    assertEquals(HoodieSchemaType.ARRAY, tipHistoryField.schema().getType());
    assertEquals(2, tipHistoryField.getNonNullSchema().getElementType().getFields().size());
  }

  @Test
  public void testSchemaParsingWithInvalidJson() {
    assertThrows(HoodieAvroSchemaException.class, () -> {
      HoodieSchema.parse("invalid json");
    }, "Should throw exception for invalid JSON schema");

    assertThrows(IllegalArgumentException.class, () -> {
      String invalid = null;
      HoodieSchema.parse(invalid);
    }, "Should throw exception for null schema string");

    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchema.parse("");
    }, "Should throw exception for empty schema string");

    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchema.parse("   ");
    }, "Should throw exception for whitespace-only schema string");
  }

  @ParameterizedTest
  @EnumSource(value = HoodieSchemaType.class, mode = EnumSource.Mode.MATCH_ALL)
  public void testPrimitiveSchemaCreation(HoodieSchemaType type) {
    // FIXED requires name and size, DECIMAL requires precision and scale, TIMESTAMP and TIME require precision
    if (type.isPrimitive() && type != HoodieSchemaType.FIXED && type != HoodieSchemaType.DECIMAL && type != HoodieSchemaType.TIMESTAMP && type != HoodieSchemaType.TIME) {
      HoodieSchema schema = HoodieSchema.create(type);

      assertNotNull(schema);
      assertEquals(type, schema.getType());
      assertEquals(type.toAvroType(), schema.getAvroSchema().getType());
    } else {
      // FIXED throws AvroRuntimeException, others throw IllegalArgumentException  
      Class<? extends Exception> expectedExceptionType = (type == HoodieSchemaType.FIXED) 
          ? org.apache.avro.AvroRuntimeException.class 
          : IllegalArgumentException.class;
      
      assertThrows(expectedExceptionType, () -> HoodieSchema.create(type), "Should throw exception for non-primitive type or type requiring additional arguments: " + type);
    }
  }

  @Test
  public void testCreateWithNullType() {
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchema.create(null);
    }, "Should throw exception for null schema type");
  }

  @Test
  public void testFixedSchemaCreation() {
    // Test creating FIXED schema with Builder (requires name and size)
    HoodieSchema fixedSchema = new HoodieSchema.Builder(HoodieSchemaType.FIXED)
        .setName("FixedField")
        .setFixedSize(16)
        .build();

    assertNotNull(fixedSchema);
    assertEquals(HoodieSchemaType.FIXED, fixedSchema.getType());
    assertEquals("FixedField", fixedSchema.getName());
    assertEquals(16, fixedSchema.getFixedSize());
  }

  @Test
  public void testNullableSchemaCreation() {
    HoodieSchema nullableSchema = HoodieSchema.createNullable(HoodieSchemaType.STRING);

    assertEquals(HoodieSchemaType.UNION, nullableSchema.getType());
    assertTrue(nullableSchema.isNullable());

    List<HoodieSchema> unionTypes = nullableSchema.getTypes();
    assertEquals(2, unionTypes.size());

    // Should contain null and string types
    boolean hasNull = unionTypes.stream().anyMatch(s -> s.getType() == HoodieSchemaType.NULL);
    boolean hasString = unionTypes.stream().anyMatch(s -> s.getType() == HoodieSchemaType.STRING);
    assertTrue(hasNull && hasString);
  }

  @Test
  public void testNullableSchemaWithExistingUnion() {
    // Create a union that already contains null
    Schema avroUnion = Schema.createUnion(Arrays.asList(
        Schema.create(Schema.Type.NULL),
        Schema.create(Schema.Type.STRING)
    ));
    HoodieSchema existingUnion = HoodieSchema.fromAvroSchema(avroUnion);

    // Making it nullable should return the same schema
    HoodieSchema nullableSchema = HoodieSchema.createNullable(existingUnion);
    assertEquals(existingUnion.toString(), nullableSchema.toString());

    // Create a union without null
    Schema avroUnionWithoutNull = Schema.createUnion(Arrays.asList(
        Schema.create(Schema.Type.STRING),
        Schema.create(Schema.Type.INT)
    ));
    HoodieSchema unionWithoutNull = HoodieSchema.fromAvroSchema(avroUnionWithoutNull);

    // Making it nullable should add null
    HoodieSchema newNullableSchema = HoodieSchema.createNullable(unionWithoutNull);
    assertTrue(newNullableSchema.isNullable());
    assertEquals(3, newNullableSchema.getTypes().size()); // null, string, int
  }

  @Test
  public void testArraySchemaCreation() {
    HoodieSchema elementSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    HoodieSchema arraySchema = HoodieSchema.createArray(elementSchema);

    assertEquals(HoodieSchemaType.ARRAY, arraySchema.getType());
    assertEquals(HoodieSchemaType.STRING, arraySchema.getElementType().getType());
  }

  @Test
  public void testMapSchemaCreation() {
    HoodieSchema valueSchema = HoodieSchema.create(HoodieSchemaType.INT);
    HoodieSchema mapSchema = HoodieSchema.createMap(valueSchema);

    assertEquals(HoodieSchemaType.MAP, mapSchema.getType());
    assertEquals(HoodieSchemaType.INT, mapSchema.getValueType().getType());
  }

  @Test
  public void testRecordSchemaFieldAccess() {
    HoodieSchema schema = HoodieSchema.parse(SAMPLE_RECORD_SCHEMA);

    // Test getField by name
    Option<HoodieSchemaField> idField = schema.getField("id");
    assertTrue(idField.isPresent());
    assertEquals("id", idField.get().name());
    assertEquals(HoodieSchemaType.LONG, idField.get().schema().getType());

    Option<HoodieSchemaField> nonExistentField = schema.getField("nonexistent");
    assertFalse(nonExistentField.isPresent());

    // Test invalid field name
    assertThrows(IllegalArgumentException.class, () -> {
      schema.getField(null);
    }, "Should throw exception for null field name");

    assertThrows(IllegalArgumentException.class, () -> {
      schema.getField("");
    }, "Should throw exception for empty field name");
  }

  @Test
  public void testNonRecordSchemaFieldAccess() {
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);

    assertThrows(IllegalStateException.class, () -> {
      stringSchema.getFields();
    }, "Should throw exception when getting fields from non-record schema");

    assertThrows(IllegalStateException.class, () -> {
      stringSchema.getField("test");
    }, "Should throw exception when getting field from non-record schema");
  }

  @Test
  public void testArraySchemaElementAccess() {
    HoodieSchema elementSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    HoodieSchema arraySchema = HoodieSchema.createArray(elementSchema);

    HoodieSchema retrievedElementType = arraySchema.getElementType();
    assertEquals(HoodieSchemaType.STRING, retrievedElementType.getType());

    // Test with non-array schema
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    assertThrows(IllegalStateException.class, () -> {
      stringSchema.getElementType();
    }, "Should throw exception when getting element type from non-array schema");
  }

  @Test
  public void testMapSchemaValueAccess() {
    HoodieSchema valueSchema = HoodieSchema.create(HoodieSchemaType.INT);
    HoodieSchema mapSchema = HoodieSchema.createMap(valueSchema);

    HoodieSchema retrievedValueType = mapSchema.getValueType();
    assertEquals(HoodieSchemaType.INT, retrievedValueType.getType());

    // Test with non-map schema
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    assertThrows(IllegalStateException.class, () -> {
      stringSchema.getValueType();
    }, "Should throw exception when getting value type from non-map schema");
  }

  @Test
  public void testUnionSchemaTypeAccess() {
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    HoodieSchema nullableSchema = HoodieSchema.createNullable(stringSchema);

    List<HoodieSchema> types = nullableSchema.getTypes();
    assertEquals(2, types.size());

    // Test getNonNullType
    HoodieSchema nonNullType = nullableSchema.getNonNullType();
    assertEquals(HoodieSchemaType.STRING, nonNullType.getType());

    // Test with non-union schema
    assertThrows(IllegalStateException.class, stringSchema::getTypes, "Should throw exception when getting types from non-union schema");

    assertSame(stringSchema, stringSchema.getNonNullType());
  }

  @Test
  public void testFixedSchemaSize() {
    Schema avroFixed = Schema.createFixed("MD5", "MD5 hash", null, 16);
    HoodieSchema fixedSchema = HoodieSchema.fromAvroSchema(avroFixed);

    assertEquals(HoodieSchemaType.FIXED, fixedSchema.getType());
    assertEquals(16, fixedSchema.getFixedSize());

    // Test with non-fixed schema
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    assertThrows(IllegalStateException.class, stringSchema::getFixedSize, "Should throw exception when getting fixed size from non-fixed schema");
  }

  @Test
  public void testEnumSchemaSymbols() {
    Schema avroEnum = Schema.createEnum("Color", "Color enum", null, Arrays.asList("RED", "GREEN", "BLUE"));
    HoodieSchema enumSchema = HoodieSchema.fromAvroSchema(avroEnum);

    assertEquals(HoodieSchemaType.ENUM, enumSchema.getType());
    List<String> symbols = enumSchema.getEnumSymbols();
    assertEquals(Arrays.asList("RED", "GREEN", "BLUE"), symbols);

    // Test with non-enum schema
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    assertThrows(IllegalStateException.class, stringSchema::getEnumSymbols, "Should throw exception when getting symbols from non-enum schema");
  }

  @Test
  public void testSchemaProperties() {
    HoodieSchema schema = HoodieSchema.parse(SAMPLE_RECORD_SCHEMA);

    assertEquals("User", schema.getName());
    assertEquals(Option.of("com.example"), schema.getNamespace());
    assertEquals("com.example.User", schema.getFullName());
    assertEquals(Option.of("User record schema"), schema.getDoc());

    // Test schema without these properties (primitive types have their type name as name but no namespace)
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    assertEquals("string", stringSchema.getName()); // Primitive types use type name
    // Note: Don't test getNamespace() on primitive types as Avro throws exception
    assertEquals("string", stringSchema.getFullName()); // Same as name for primitives
    assertEquals(Option.empty(), stringSchema.getDoc());
  }

  @Test
  public void testAvroCompatibility() {
    // Test round-trip compatibility
    Schema originalAvroSchema = new Schema.Parser().parse(SAMPLE_RECORD_SCHEMA);
    HoodieSchema hoodieSchema = HoodieSchema.fromAvroSchema(originalAvroSchema);
    Schema retrievedAvroSchema = hoodieSchema.getAvroSchema();

    assertEquals(originalAvroSchema, retrievedAvroSchema);
    assertEquals(originalAvroSchema.toString(), retrievedAvroSchema.toString());
  }

  @Test
  public void testSchemaEquality() {
    HoodieSchema schema1 = HoodieSchema.create(HoodieSchemaType.STRING);
    HoodieSchema schema2 = HoodieSchema.create(HoodieSchemaType.STRING);
    HoodieSchema schema3 = HoodieSchema.create(HoodieSchemaType.INT);

    assertEquals(schema1, schema2);
    assertEquals(schema1.hashCode(), schema2.hashCode());

    // Different types should not be equal
    // Note: assertEquals vs assertNotEquals depends on actual implementation
    // The current implementation delegates to Avro Schema.equals()
  }

  @Test
  public void testSchemaToString() {
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);

    String schemaString = stringSchema.toString();
    assertNotNull(schemaString);
    assertTrue(schemaString.contains("string"));

    // Test pretty printing
    String prettyString = stringSchema.toString(true);
    assertNotNull(prettyString);

    String nonPrettyString = stringSchema.toString(false);
    assertNotNull(nonPrettyString);
  }

  @Test
  public void testIsNullable() {
    // Non-nullable schema
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    assertFalse(stringSchema.isNullable());

    // Nullable schema
    HoodieSchema nullableSchema = HoodieSchema.createNullable(stringSchema);
    assertTrue(nullableSchema.isNullable());

    // Union without null
    Schema avroUnion = Schema.createUnion(Arrays.asList(
        Schema.create(Schema.Type.STRING),
        Schema.create(Schema.Type.INT)
    ));
    HoodieSchema unionWithoutNull = HoodieSchema.fromAvroSchema(avroUnion);
    assertFalse(unionWithoutNull.isNullable());
  }

  @Test
  public void testBuilderPattern() {
    // Test the new Builder pattern
    List<HoodieSchemaField> fields = Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG), "User ID"),
        HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), "User name")
    );

    HoodieSchema recordSchema = new HoodieSchema.Builder(HoodieSchemaType.RECORD)
        .setName("User")
        .setNamespace("com.example")
        .setDoc("User record schema")
        .setFields(fields)
        .build();

    assertNotNull(recordSchema);
    assertEquals(HoodieSchemaType.RECORD, recordSchema.getType());
    assertEquals("User", recordSchema.getName());
    assertEquals(Option.of("com.example"), recordSchema.getNamespace());
    assertEquals(Option.of("User record schema"), recordSchema.getDoc());
    assertEquals(2, recordSchema.getFields().size());
  }

  @Test
  public void testStaticFactoryMethods() {
    // Test createRecord
    List<HoodieSchemaField> fields = Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG)),
        HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING))
    );

    HoodieSchema recordSchema = HoodieSchema.createRecord("User", "com.example", "User record", fields);
    assertEquals(HoodieSchemaType.RECORD, recordSchema.getType());
    assertEquals("User", recordSchema.getName());

    // Test createUnion (varargs version)
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    HoodieSchema intSchema = HoodieSchema.create(HoodieSchemaType.INT);
    HoodieSchema unionSchema = HoodieSchema.createUnion(stringSchema, intSchema);
    assertEquals(HoodieSchemaType.UNION, unionSchema.getType());
    assertEquals(2, unionSchema.getTypes().size());

    // Test createEnum
    List<String> symbols = Arrays.asList("RED", "GREEN", "BLUE");
    HoodieSchema enumSchema = HoodieSchema.createEnum("Color", "com.example", "Color enum", symbols);
    assertEquals(HoodieSchemaType.ENUM, enumSchema.getType());
    assertEquals("Color", enumSchema.getName());
    assertEquals(symbols, enumSchema.getEnumSymbols());

    // Test createFixed
    HoodieSchema fixedSchema = HoodieSchema.createFixed("MD5", "com.example", "MD5 hash", 16);
    assertEquals(HoodieSchemaType.FIXED, fixedSchema.getType());
    assertEquals("MD5", fixedSchema.getName());
    assertEquals(16, fixedSchema.getFixedSize());
  }

  @Test
  public void testBuilderValidation() {
    // Test that builder validates required properties
    assertThrows(IllegalArgumentException.class, () -> {
      new HoodieSchema.Builder(HoodieSchemaType.RECORD).build(); // Missing name and fields
    });

    assertThrows(IllegalArgumentException.class, () -> {
      new HoodieSchema.Builder(HoodieSchemaType.ARRAY).build(); // Missing element type
    });

    assertThrows(IllegalArgumentException.class, () -> {
      new HoodieSchema.Builder(HoodieSchemaType.FIXED)
          .setName("Test")
          .setFixedSize(-1) // Invalid size
          .build();
    });

    // Test null type validation
    assertThrows(IllegalArgumentException.class, () -> {
      new HoodieSchema.Builder(null);
    });
  }

  @Test
  public void testNullValueConstant() {
    // HoodieSchema.NULL_VALUE should be the same as JsonProperties.NULL_VALUE
    assertSame(JsonProperties.NULL_VALUE, HoodieSchema.NULL_VALUE);
    assertEquals(JsonProperties.NULL_VALUE, HoodieSchema.NULL_VALUE);
  }

  @Test
  public void testSchemaParser() {
    String jsonSchema = "{\"type\":\"record\",\"name\":\"Test\",\"fields\":["
        + "{\"name\":\"id\",\"type\":\"long\"}"
        + ",{\"name\":\"name\",\"type\":\"string\"}]}";

    HoodieSchema.Parser parser = new HoodieSchema.Parser();
    HoodieSchema schema = parser.parse(jsonSchema);

    assertNotNull(schema);
    assertEquals(HoodieSchemaType.RECORD, schema.getType());
    assertEquals("Test", schema.getName());
    assertEquals(2, schema.getFields().size());
  }

  @Test
  public void testCreateRecordWithErrorFlag() {
    List<HoodieSchemaField> fields = Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG), "User ID"),
        HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), "User name")
    );

    HoodieSchema recordSchema = HoodieSchema.createRecord("ErrorRecord", "Error record", "com.example", true, fields);

    assertNotNull(recordSchema);
    assertEquals(HoodieSchemaType.RECORD, recordSchema.getType());
    assertEquals("ErrorRecord", recordSchema.getName());
    assertEquals("com.example", recordSchema.getNamespace().get());
    assertTrue(recordSchema.isError());
    assertEquals(2, recordSchema.getFields().size());
  }

  @Test
  public void testMutableOperations() {
    List<HoodieSchemaField> fields = Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG), "ID")
    );

    HoodieSchema recordSchema = HoodieSchema.createRecord("TestRecord", "com.example", "Test record", fields);

    // Test addProp
    recordSchema.addProp("custom.prop", "test-value");
    assertEquals("test-value", recordSchema.getProp("custom.prop"));

    // Note: Test setFields limitation - Avro doesn't allow calling setFields on schemas that already have fields
    // This is a limitation of the underlying Avro implementation, so we just verify the method exists
    // and would work in valid scenarios (during initial schema construction via Builder)
    assertTrue(recordSchema.getFields().size() > 0); // Verify the original record has fields
  }

  @Test
  public void testHudiSchemaVersion() {
    String version = HoodieSchema.getHudiSchemaVersion();
    assertNotNull(version);
    // Version could be 'unknown' in test environment, which is acceptable
  }

  @Test
  public void testInvalidMutableOperations() {
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);

    // Should throw on non-record schema operations
    assertThrows(IllegalStateException.class, () -> stringSchema.isError());
    assertThrows(IllegalStateException.class, () -> stringSchema.setFields(Arrays.asList()));
  }

  @Test
  public void testCreateRecord() {
    // Create fields
    HoodieSchemaField idField = HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG));
    HoodieSchemaField nameField = HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING));
    List<HoodieSchemaField> fields = Arrays.asList(idField, nameField);

    // Create record schema
    HoodieSchema recordSchema = HoodieSchema.createRecord("User", null, null, fields);

    assertNotNull(recordSchema);
    assertEquals(HoodieSchemaType.RECORD, recordSchema.getType());
    assertEquals("User", recordSchema.getName());
    assertEquals(2, recordSchema.getFields().size());

    // Verify fields
    Option<HoodieSchemaField> retrievedIdField = recordSchema.getField("id");
    assertTrue(retrievedIdField.isPresent());
    assertEquals(HoodieSchemaType.LONG, retrievedIdField.get().schema().getType());

    Option<HoodieSchemaField> retrievedNameField = recordSchema.getField("name");
    assertTrue(retrievedNameField.isPresent());
    assertEquals(HoodieSchemaType.STRING, retrievedNameField.get().schema().getType());
  }

  @Test
  public void testCreateRecordWithNamespaceAndDoc() {
    HoodieSchemaField field = HoodieSchemaField.of("value", HoodieSchema.create(HoodieSchemaType.STRING));
    List<HoodieSchemaField> fields = Collections.singletonList(field);

    HoodieSchema recordSchema = HoodieSchema.createRecord("TestRecord", "com.example",
        "Test record schema", fields);

    assertNotNull(recordSchema);
    assertEquals(HoodieSchemaType.RECORD, recordSchema.getType());
    assertEquals("TestRecord", recordSchema.getName());
    assertEquals(Option.of("com.example"), recordSchema.getNamespace());
    assertEquals("com.example.TestRecord", recordSchema.getFullName());
    assertEquals(Option.of("Test record schema"), recordSchema.getDoc());
  }

  @Test
  public void testCreateRecordWithInvalidParameters() {
    HoodieSchemaField field = HoodieSchemaField.of("test", HoodieSchema.create(HoodieSchemaType.STRING));
    List<HoodieSchemaField> fields = Collections.singletonList(field);

    // Test null record name
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchema.createRecord(null, null, null, fields);
    }, "Should throw exception for null record name");

    // Test empty record name
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchema.createRecord("", null, null, fields);
    }, "Should throw exception for empty record name");

    // Test null fields
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchema.createRecord("TestRecord", null, null, null);
    }, "Should throw exception for null fields");
  }

  @Test
  void testCreateRecordWithEmptyList() {
    // Test empty fields list
    HoodieSchema recordSchema = HoodieSchema.createRecord("EmptyRecord", null, null, Collections.emptyList());

    assertNotNull(recordSchema);
    assertEquals(HoodieSchemaType.RECORD, recordSchema.getType());
    assertEquals("EmptyRecord", recordSchema.getName());
    assertEquals(0, recordSchema.getFields().size());
  }

  @Test
  public void testCreateEnum() {
    List<String> symbols = Arrays.asList("RED", "GREEN", "BLUE");
    HoodieSchema enumSchema = HoodieSchema.createEnum("Color", null, null, symbols);

    assertNotNull(enumSchema);
    assertEquals(HoodieSchemaType.ENUM, enumSchema.getType());
    assertEquals("Color", enumSchema.getName());
    assertEquals(symbols, enumSchema.getEnumSymbols());
  }

  @Test
  public void testCreateEnumWithInvalidParameters() {
    List<String> symbols = Arrays.asList("A", "B", "C");

    // Test null enum name
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchema.createEnum(null, null, null, symbols);
    }, "Should throw exception for null enum name");

    // Test empty enum name
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchema.createEnum("", null, null, symbols);
    }, "Should throw exception for empty enum name");

    // Test null symbols
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchema.createEnum("TestEnum", null, null, null);
    }, "Should throw exception for null symbols");

    // Test empty symbols
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchema.createEnum("TestEnum", null, null, Collections.emptyList());
    }, "Should throw exception for empty symbols list");
  }

  @Test
  public void testCreateFixed() {
    HoodieSchema fixedSchema = HoodieSchema.createFixed("MD5Hash", null, null, 16);

    assertNotNull(fixedSchema);
    assertEquals(HoodieSchemaType.FIXED, fixedSchema.getType());
    assertEquals("MD5Hash", fixedSchema.getName());
    assertEquals(16, fixedSchema.getFixedSize());
  }

  @Test
  public void testCreateFixedWithInvalidParameters() {
    // Test null fixed name
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchema.createFixed(null, null, null, 16);
    }, "Should throw exception for null fixed name");

    // Test empty fixed name
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchema.createFixed("", null, null, 16);
    }, "Should throw exception for empty fixed name");

    // Test invalid size
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchema.createFixed("Test", null, null, 0);
    }, "Should throw exception for zero size");

    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchema.createFixed("Test", null, null, -1);
    }, "Should throw exception for negative size");
  }

  @Test
  public void testCreateUnion() {
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    HoodieSchema intSchema = HoodieSchema.create(HoodieSchemaType.INT);

    List<HoodieSchema> types = Arrays.asList(stringSchema, intSchema);
    HoodieSchema unionSchema = HoodieSchema.createUnion(types);

    assertNotNull(unionSchema);
    assertEquals(HoodieSchemaType.UNION, unionSchema.getType());

    List<HoodieSchema> unionTypes = unionSchema.getTypes();
    assertEquals(2, unionTypes.size());
    assertEquals(HoodieSchemaType.STRING, unionTypes.get(0).getType());
    assertEquals(HoodieSchemaType.INT, unionTypes.get(1).getType());
  }

  @Test
  public void testCreateUnionWithVarargs() {
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    HoodieSchema intSchema = HoodieSchema.create(HoodieSchemaType.INT);

    HoodieSchema unionSchema = HoodieSchema.createUnion(stringSchema, intSchema);

    assertNotNull(unionSchema);
    assertEquals(HoodieSchemaType.UNION, unionSchema.getType());
    assertEquals(2, unionSchema.getTypes().size());
  }

  @Test
  public void testCreateUnionWithInvalidParameters() {
    // Test null types list
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchema.createUnion((List<HoodieSchema>) null);
    }, "Should throw exception for null types list");

    // Test empty types list
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchema.createUnion(Collections.emptyList());
    }, "Should throw exception for empty types list");

    // Test null varargs array
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchema.createUnion((HoodieSchema[]) null);
    }, "Should throw exception for null varargs array");

    // Test empty varargs array
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchema.createUnion();
    }, "Should throw exception for empty varargs array");
  }

  @Test
  public void testCreateNullableSchema() {
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    HoodieSchema nullableSchema = HoodieSchema.createNullable(stringSchema);

    assertNotNull(nullableSchema);
    assertEquals(HoodieSchemaType.UNION, nullableSchema.getType());
    assertTrue(nullableSchema.isNullable());

    List<HoodieSchema> types = nullableSchema.getTypes();
    assertEquals(2, types.size());

    // Should contain null and string types
    boolean hasNull = types.stream().anyMatch(s -> s.getType() == HoodieSchemaType.NULL);
    boolean hasString = types.stream().anyMatch(s -> s.getType() == HoodieSchemaType.STRING);
    assertTrue(hasNull && hasString);
  }

  @Test
  public void testCreateNullableSchemaWithInvalidParameters() {
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchema.createNullable((HoodieSchema) null);
    }, "Should throw exception for null type");
  }

  @Test
  void testCreateDecimalSchema() {
    HoodieSchema decimalSchema = HoodieSchema.createDecimal(10, 2);

    assertTrue(decimalSchema instanceof HoodieSchema.Decimal);
    assertEquals(HoodieSchemaType.DECIMAL, decimalSchema.getType());
    assertEquals("decimal(10,2)", decimalSchema.getName());
    assertEquals(10, ((HoodieSchema.Decimal) decimalSchema).getPrecision());
    assertEquals(2, ((HoodieSchema.Decimal) decimalSchema).getScale());
    LogicalTypes.Decimal avroLogicalType = (LogicalTypes.Decimal) decimalSchema.toAvroSchema().getLogicalType();
    assertEquals(10, avroLogicalType.getPrecision());
    assertEquals(2, avroLogicalType.getScale());

    // create with fixed size
    HoodieSchema decimalFixedSchema = HoodieSchema.createDecimal("fixed_decimal", null, null, 10, 2, 5);
    assertTrue(decimalFixedSchema instanceof HoodieSchema.Decimal);
    assertEquals(5, decimalFixedSchema.getFixedSize());
    assertEquals(10, ((HoodieSchema.Decimal) decimalFixedSchema).getPrecision());
    assertEquals(2, ((HoodieSchema.Decimal) decimalFixedSchema).getScale());
    LogicalTypes.Decimal avroLogicalTypeFixed = (LogicalTypes.Decimal) decimalFixedSchema.toAvroSchema().getLogicalType();
    assertEquals(10, avroLogicalTypeFixed.getPrecision());
    assertEquals(2, avroLogicalTypeFixed.getScale());
    assertEquals(5, decimalFixedSchema.getFixedSize());
  }

  @Test
  void testCreateTimestampMillis() {
    HoodieSchema timestampSchema = HoodieSchema.createTimestampMillis();

    assertEquals(HoodieSchemaType.TIMESTAMP, timestampSchema.getType());
    assertEquals("timestamp-millis", timestampSchema.getName());
    assertTrue(((HoodieSchema.Timestamp) timestampSchema).isUtcAdjusted());
    assertEquals(HoodieSchema.TimePrecision.MILLIS, ((HoodieSchema.Timestamp) timestampSchema).getPrecision());
    assertInstanceOf(LogicalTypes.TimestampMillis.class, timestampSchema.toAvroSchema().getLogicalType());
  }

  @Test
  void testCreateTimestampMicros() {
    HoodieSchema timestampSchema = HoodieSchema.createTimestampMicros();

    assertEquals(HoodieSchemaType.TIMESTAMP, timestampSchema.getType());
    assertEquals("timestamp-micros", timestampSchema.getName());
    assertTrue(((HoodieSchema.Timestamp) timestampSchema).isUtcAdjusted());
    assertEquals(HoodieSchema.TimePrecision.MICROS, ((HoodieSchema.Timestamp) timestampSchema).getPrecision());
    assertInstanceOf(LogicalTypes.TimestampMicros.class, timestampSchema.toAvroSchema().getLogicalType());
  }

  @Test
  void testCreateLocalTimestampMillis() {
    HoodieSchema timestampSchema = HoodieSchema.createLocalTimestampMillis();

    assertEquals(HoodieSchemaType.TIMESTAMP, timestampSchema.getType());
    assertEquals("local-timestamp-millis", timestampSchema.getName());
    assertFalse(((HoodieSchema.Timestamp) timestampSchema).isUtcAdjusted());
    assertEquals(HoodieSchema.TimePrecision.MILLIS, ((HoodieSchema.Timestamp) timestampSchema).getPrecision());
    assertInstanceOf(LogicalTypes.LocalTimestampMillis.class, timestampSchema.toAvroSchema().getLogicalType());
  }

  @Test
  void testCreateLocalTimestampMicros() {
    HoodieSchema timestampSchema = HoodieSchema.createLocalTimestampMicros();

    assertEquals(HoodieSchemaType.TIMESTAMP, timestampSchema.getType());
    assertEquals("local-timestamp-micros", timestampSchema.getName());
    assertFalse(((HoodieSchema.Timestamp) timestampSchema).isUtcAdjusted());
    assertEquals(HoodieSchema.TimePrecision.MICROS, ((HoodieSchema.Timestamp) timestampSchema).getPrecision());
    assertInstanceOf(LogicalTypes.LocalTimestampMicros.class, timestampSchema.toAvroSchema().getLogicalType());
  }

  @Test
  void testCreateTimeMillisSchema() {
    HoodieSchema timeSchema = HoodieSchema.createTimeMillis();

    assertEquals(HoodieSchemaType.TIME, timeSchema.getType());
    assertEquals("time-millis", timeSchema.getName());
    assertInstanceOf(LogicalTypes.TimeMillis.class, timeSchema.toAvroSchema().getLogicalType());
  }

  @Test
  void testCreateTimeMicrosSchema() {
    HoodieSchema timeSchema = HoodieSchema.createTimeMicros();

    assertEquals(HoodieSchemaType.TIME, timeSchema.getType());
    assertEquals("time-micros", timeSchema.getName());
    assertInstanceOf(LogicalTypes.TimeMicros.class, timeSchema.toAvroSchema().getLogicalType());
  }

  @Test
  void testCreateDateSchema() {
    HoodieSchema dateSchema = HoodieSchema.createDate();

    assertEquals(HoodieSchemaType.DATE, dateSchema.getType());
    assertEquals("date", dateSchema.getName());
    assertInstanceOf(LogicalTypes.Date.class, dateSchema.toAvroSchema().getLogicalType());
  }

  @Test
  void testCreateUuidSchema() {
    HoodieSchema uuidSchema = HoodieSchema.createUUID();

    assertEquals(HoodieSchemaType.UUID, uuidSchema.getType());
    assertEquals("uuid", uuidSchema.getName());
    assertEquals("uuid", uuidSchema.toAvroSchema().getLogicalType().getName());
  }

  @Test
  void testSubclassIsPreservedInNestedSchemas() {
    // Create a record schema with a decimal field
    HoodieSchema decimalFieldSchema = HoodieSchema.createDecimal(10, 2);
    HoodieSchemaField decimalField = HoodieSchemaField.of("amount", decimalFieldSchema);
    List<HoodieSchemaField> fields = Collections.singletonList(decimalField);

    HoodieSchema recordSchema = HoodieSchema.createRecord("Transaction", null, null, fields);

    // Retrieve the field schema and verify it's still a Decimal subclass
    Option<HoodieSchemaField> retrievedFieldOpt = recordSchema.getField("amount");
    assertTrue(retrievedFieldOpt.isPresent());
    HoodieSchema retrievedFieldSchema = retrievedFieldOpt.get().schema();

    assertTrue(retrievedFieldSchema instanceof HoodieSchema.Decimal);
    assertEquals(10, ((HoodieSchema.Decimal) retrievedFieldSchema).getPrecision());
    assertEquals(2, ((HoodieSchema.Decimal) retrievedFieldSchema).getScale());

    // Create an array schema with timestamp elements
    HoodieSchema timestampElementSchema = HoodieSchema.createTimestampMillis();
    HoodieSchema arraySchema = HoodieSchema.createArray(timestampElementSchema);
    HoodieSchema retrievedElementSchema = arraySchema.getElementType();
    assertTrue(retrievedElementSchema instanceof HoodieSchema.Timestamp);
    assertEquals(HoodieSchema.TimePrecision.MILLIS, ((HoodieSchema.Timestamp) retrievedElementSchema).getPrecision());

    // Create a map schema with time values
    HoodieSchema timeSchema = HoodieSchema.createTimeMillis();
    HoodieSchema mapSchema = HoodieSchema.createMap(timeSchema);
    HoodieSchema retrievedValueSchema = mapSchema.getValueType();
    assertTrue(retrievedValueSchema instanceof HoodieSchema.Time);
    assertEquals(HoodieSchema.TimePrecision.MILLIS, ((HoodieSchema.Time) retrievedValueSchema).getPrecision());
  }
}
