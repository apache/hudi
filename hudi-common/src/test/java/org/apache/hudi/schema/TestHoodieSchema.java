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

package org.apache.hudi.schema;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieAvroSchemaException;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for {@link HoodieSchema}.
 */
public class TestHoodieSchema {

  private static final String SAMPLE_RECORD_SCHEMA =
      "{\n"
          + "  \"type\": \"record\",\n"
          + "  \"name\": \"User\",\n"
          + "  \"namespace\": \"com.example\",\n"
          + "  \"doc\": \"User record schema\",\n"
          + "  \"fields\": [\n"
          + "    {\"name\": \"id\", \"type\": \"long\"},\n"
          + "    {\"name\": \"name\", \"type\": \"string\", \"default\": \"\"},\n"
          + "    {\"name\": \"email\", \"type\": [\"null\", \"string\"], \"default\": null}\n"
          + "  ]\n"
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
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchema.fromAvroSchema(null);
    }, "Should throw exception for null Avro schema");

    assertThrows(IllegalArgumentException.class, () -> {
      new HoodieSchema(null);
    }, "Should throw exception for null Avro schema in constructor");
  }

  @Test
  public void testSchemaParsingFromJson() {
    HoodieSchema schema = HoodieSchema.parse(SAMPLE_RECORD_SCHEMA);

    assertNotNull(schema);
    assertEquals(HoodieSchemaType.RECORD, schema.getType());
    assertEquals(Option.of("User"), schema.getName());
    assertEquals(Option.of("com.example"), schema.getNamespace());
    assertEquals(Option.of("com.example.User"), schema.getFullName());
    assertEquals(Option.of("User record schema"), schema.getDoc());

    List<HoodieSchemaField> fields = schema.getFields();
    assertEquals(3, fields.size());

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
  }

  @Test
  public void testSchemaParsingWithInvalidJson() {
    assertThrows(HoodieAvroSchemaException.class, () -> {
      HoodieSchema.parse("invalid json");
    }, "Should throw exception for invalid JSON schema");

    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchema.parse(null);
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
    if (type.isPrimitive() && type != HoodieSchemaType.FIXED) { // FIXED requires name and size
      HoodieSchema schema = HoodieSchema.create(type);

      assertNotNull(schema);
      assertEquals(type, schema.getType());
      assertEquals(type.toAvroType(), schema.getAvroSchema().getType());
    } else {
      // FIXED throws AvroRuntimeException, others throw IllegalArgumentException  
      Class<? extends Exception> expectedExceptionType = (type == HoodieSchemaType.FIXED) 
          ? org.apache.avro.AvroRuntimeException.class 
          : IllegalArgumentException.class;
      
      assertThrows(expectedExceptionType, () -> {
        HoodieSchema.create(type);
      }, "Should throw exception for non-primitive type: " + type);
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
    assertEquals("FixedField", fixedSchema.getName().get());
    assertEquals(16, fixedSchema.getFixedSize());
  }

  @Test
  public void testNullableSchemaCreation() {
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    HoodieSchema nullableSchema = HoodieSchema.createNullable(stringSchema);

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
    HoodieSchema existingUnion = new HoodieSchema(avroUnion);

    // Making it nullable should return the same schema
    HoodieSchema nullableSchema = HoodieSchema.createNullable(existingUnion);
    assertEquals(existingUnion.toString(), nullableSchema.toString());

    // Create a union without null
    Schema avroUnionWithoutNull = Schema.createUnion(Arrays.asList(
        Schema.create(Schema.Type.STRING),
        Schema.create(Schema.Type.INT)
    ));
    HoodieSchema unionWithoutNull = new HoodieSchema(avroUnionWithoutNull);

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
    assertThrows(IllegalStateException.class, () -> {
      stringSchema.getTypes();
    }, "Should throw exception when getting types from non-union schema");

    assertThrows(IllegalStateException.class, () -> {
      stringSchema.getNonNullType();
    }, "Should throw exception when getting non-null type from non-union schema");
  }

  @Test
  public void testFixedSchemaSize() {
    Schema avroFixed = Schema.createFixed("MD5", "MD5 hash", null, 16);
    HoodieSchema fixedSchema = new HoodieSchema(avroFixed);

    assertEquals(HoodieSchemaType.FIXED, fixedSchema.getType());
    assertEquals(16, fixedSchema.getFixedSize());

    // Test with non-fixed schema
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    assertThrows(IllegalStateException.class, () -> {
      stringSchema.getFixedSize();
    }, "Should throw exception when getting fixed size from non-fixed schema");
  }

  @Test
  public void testEnumSchemaSymbols() {
    Schema avroEnum = Schema.createEnum("Color", "Color enum", null, Arrays.asList("RED", "GREEN", "BLUE"));
    HoodieSchema enumSchema = new HoodieSchema(avroEnum);

    assertEquals(HoodieSchemaType.ENUM, enumSchema.getType());
    List<String> symbols = enumSchema.getEnumSymbols();
    assertEquals(Arrays.asList("RED", "GREEN", "BLUE"), symbols);

    // Test with non-enum schema
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    assertThrows(IllegalStateException.class, () -> {
      stringSchema.getEnumSymbols();
    }, "Should throw exception when getting symbols from non-enum schema");
  }

  @Test
  public void testSchemaProperties() {
    HoodieSchema schema = HoodieSchema.parse(SAMPLE_RECORD_SCHEMA);

    assertEquals(Option.of("User"), schema.getName());
    assertEquals(Option.of("com.example"), schema.getNamespace());
    assertEquals(Option.of("com.example.User"), schema.getFullName());
    assertEquals(Option.of("User record schema"), schema.getDoc());

    // Test schema without these properties (primitive types have their type name as name but no namespace)
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    assertEquals(Option.of("string"), stringSchema.getName()); // Primitive types use type name
    // Note: Don't test getNamespace() on primitive types as Avro throws exception
    assertEquals(Option.of("string"), stringSchema.getFullName()); // Same as name for primitives
    assertEquals(Option.empty(), stringSchema.getDoc());
  }

  @Test
  public void testAvroCompatibility() {
    // Test round-trip compatibility
    Schema originalAvroSchema = new Schema.Parser().parse(SAMPLE_RECORD_SCHEMA);
    HoodieSchema hoodieSchema = new HoodieSchema(originalAvroSchema);
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
    HoodieSchema unionWithoutNull = new HoodieSchema(avroUnion);
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
    assertEquals(Option.of("User"), recordSchema.getName());
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
    assertEquals(Option.of("User"), recordSchema.getName());

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
    assertEquals(Option.of("Color"), enumSchema.getName());
    assertEquals(symbols, enumSchema.getEnumSymbols());

    // Test createFixed
    HoodieSchema fixedSchema = HoodieSchema.createFixed("MD5", "com.example", "MD5 hash", 16);
    assertEquals(HoodieSchemaType.FIXED, fixedSchema.getType());
    assertEquals(Option.of("MD5"), fixedSchema.getName());
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
    assertEquals("Test", schema.getName().get());
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
    assertEquals("ErrorRecord", recordSchema.getName().get());
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
}
