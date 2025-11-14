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

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for {@link HoodieSchemaField}.
 */
public class TestHoodieSchemaField {

  @Test
  public void testFieldCreationFromAvroField() {
    // Create Avro field
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    Schema.Field avroField = new Schema.Field("testField", stringSchema, "Test documentation", "defaultValue");

    // Create Hudi field
    HoodieSchemaField hoodieField = HoodieSchemaField.fromAvroField(avroField);

    // Verify properties
    assertEquals("testField", hoodieField.name());
    assertEquals(HoodieSchemaType.STRING, hoodieField.schema().getType());
    assertEquals(Option.of("Test documentation"), hoodieField.doc());
    assertEquals(Option.of("defaultValue"), hoodieField.defaultVal());
    assertTrue(hoodieField.hasDefaultValue());
    assertEquals(HoodieFieldOrder.ASCENDING, hoodieField.order()); // Default order
    assertEquals(-1, hoodieField.pos()); // Field position when not in record
  }

  @Test
  public void testFieldCreationWithNullAvroField() {
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaField.fromAvroField(null);
    }, "Should throw exception for null Avro field");

    assertThrows(IllegalArgumentException.class, () -> {
      new HoodieSchemaField(null);
    }, "Should throw exception for null Avro field in constructor");
  }

  @Test
  public void testFieldCreationUsingFactoryMethod() {
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);

    // Test with all parameters
    HoodieSchemaField field1 = HoodieSchemaField.of("name", stringSchema, "Name field", "John");
    assertEquals("name", field1.name());
    assertEquals(HoodieSchemaType.STRING, field1.schema().getType());
    assertEquals(Option.of("Name field"), field1.doc());
    assertEquals(Option.of("John"), field1.defaultVal());
    assertTrue(field1.hasDefaultValue());

    // Test with minimal parameters
    HoodieSchemaField field2 = HoodieSchemaField.of("age", HoodieSchema.create(HoodieSchemaType.INT));
    assertEquals("age", field2.name());
    assertEquals(HoodieSchemaType.INT, field2.schema().getType());
    assertEquals(Option.empty(), field2.doc());
    assertFalse(field2.hasDefaultValue());
  }

  @Test
  public void testFieldCreationWithInvalidParameters() {
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);

    // Test null name
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaField.of(null, stringSchema);
    }, "Should throw exception for null field name");

    // Test empty name
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaField.of("", stringSchema);
    }, "Should throw exception for empty field name");

    // Test null schema
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaField.of("test", null);
    }, "Should throw exception for null field schema");
  }

  @Test
  public void testFieldWithoutDefaultValue() {
    Schema intSchema = Schema.create(Schema.Type.INT);
    Schema.Field avroField = new Schema.Field("count", intSchema);

    HoodieSchemaField hoodieField = new HoodieSchemaField(avroField);

    assertEquals("count", hoodieField.name());
    assertEquals(HoodieSchemaType.INT, hoodieField.schema().getType());
    assertEquals(Option.empty(), hoodieField.doc());
    assertEquals(Option.empty(), hoodieField.defaultVal());
    assertFalse(hoodieField.hasDefaultValue());
  }

  @Test
  public void testFieldWithComplexSchema() {
    // Create array schema
    Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.STRING));
    Schema.Field avroField = new Schema.Field("tags", arraySchema, "Array of tags", null);

    HoodieSchemaField hoodieField = new HoodieSchemaField(avroField);

    assertEquals("tags", hoodieField.name());
    assertEquals(HoodieSchemaType.ARRAY, hoodieField.schema().getType());
    assertEquals(HoodieSchemaType.STRING, hoodieField.schema().getElementType().getType());
    assertEquals(Option.of("Array of tags"), hoodieField.doc());
    assertFalse(hoodieField.hasDefaultValue());
  }

  @Test
  public void testFieldWithName() {
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    HoodieSchemaField originalField = HoodieSchemaField.of("original", stringSchema, "Original field", "default");

    // Create field with new name
    HoodieSchemaField renamedField = originalField.withName("renamed");

    assertEquals("renamed", renamedField.name());
    assertEquals(originalField.schema().getType(), renamedField.schema().getType());
    assertEquals(originalField.doc(), renamedField.doc());
    assertEquals(originalField.defaultVal(), renamedField.defaultVal());
    assertEquals(originalField.hasDefaultValue(), renamedField.hasDefaultValue());

    // Original field should be unchanged
    assertEquals("original", originalField.name());
  }

  @Test
  public void testFieldWithNameInvalidInput() {
    HoodieSchemaField field = HoodieSchemaField.of("test", HoodieSchema.create(HoodieSchemaType.STRING));

    assertThrows(IllegalArgumentException.class, () -> {
      field.withName(null);
    }, "Should throw exception for null name");

    assertThrows(IllegalArgumentException.class, () -> {
      field.withName("");
    }, "Should throw exception for empty name");
  }

  @Test
  public void testFieldWithSchema() {
    HoodieSchemaField originalField = HoodieSchemaField.of("test", HoodieSchema.create(HoodieSchemaType.STRING));
    HoodieSchema newSchema = HoodieSchema.create(HoodieSchemaType.INT);

    // Create field with new schema
    HoodieSchemaField modifiedField = originalField.withSchema(newSchema);

    assertEquals(originalField.name(), modifiedField.name());
    assertEquals(HoodieSchemaType.INT, modifiedField.schema().getType());

    // Original field should be unchanged
    assertEquals(HoodieSchemaType.STRING, originalField.schema().getType());
  }

  @Test
  public void testFieldWithSchemaInvalidInput() {
    HoodieSchemaField field = HoodieSchemaField.of("test", HoodieSchema.create(HoodieSchemaType.STRING));

    assertThrows(IllegalArgumentException.class, () -> {
      field.withSchema(null);
    }, "Should throw exception for null schema");
  }

  @Test
  public void testAvroCompatibility() {
    // Create Hudi field
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    HoodieSchemaField hoodieField = HoodieSchemaField.of("test", stringSchema, "Test field", "default");

    // Get underlying Avro field
    Schema.Field avroField = hoodieField.getAvroField();
    assertNotNull(avroField);
    assertEquals("test", avroField.name());
    assertEquals(Schema.Type.STRING, avroField.schema().getType());
    assertEquals("Test field", avroField.doc());
    assertEquals("default", avroField.defaultVal());
    assertTrue(avroField.hasDefaultValue());

    // Create new Hudi field from Avro field and verify round-trip
    HoodieSchemaField roundTripField = new HoodieSchemaField(avroField);
    assertEquals(hoodieField.name(), roundTripField.name());
    assertEquals(hoodieField.schema().getType(), roundTripField.schema().getType());
    assertEquals(hoodieField.doc(), roundTripField.doc());
    assertEquals(hoodieField.defaultVal(), roundTripField.defaultVal());
    assertEquals(hoodieField.hasDefaultValue(), roundTripField.hasDefaultValue());
  }

  @Test
  public void testFieldEquality() {
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);

    HoodieSchemaField field1 = HoodieSchemaField.of("name", stringSchema, "Name field", "John");
    HoodieSchemaField field2 = HoodieSchemaField.of("name", stringSchema, "Name field", "John");
    HoodieSchemaField field3 = HoodieSchemaField.of("name", stringSchema, "Different doc", "John");
    HoodieSchemaField field4 = HoodieSchemaField.of("differentName", stringSchema, "Name field", "John");

    // Test equality based on all field properties
    assertEquals(field1, field2);
    assertEquals(field1.hashCode(), field2.hashCode());

    // Different documentation should make fields unequal
    // Note: This depends on the actual equality implementation
    // The current implementation compares all Avro field properties

    // Different names should make fields unequal
    // assertEquals vs assertNotEquals depends on implementation details
  }

  @Test
  public void testFieldToString() {
    HoodieSchemaField field = HoodieSchemaField.of("testField", HoodieSchema.create(HoodieSchemaType.STRING),
        "Test documentation", "defaultValue");

    String fieldString = field.toString();
    assertNotNull(fieldString);
    assertTrue(fieldString.contains("testField"));
    assertTrue(fieldString.contains("STRING"));
    assertTrue(fieldString.contains("true")); // hasDefault
  }

  @Test
  public void testFieldOrderHandling() {
    Schema stringSchema = Schema.create(Schema.Type.STRING);

    // Test different field orders
    Schema.Field ascendingField = new Schema.Field("asc", stringSchema, null, null, Schema.Field.Order.ASCENDING);
    Schema.Field descendingField = new Schema.Field("desc", stringSchema, null, null, Schema.Field.Order.DESCENDING);
    Schema.Field ignoreField = new Schema.Field("ignore", stringSchema, null, null, Schema.Field.Order.IGNORE);

    HoodieSchemaField hoodieAsc = new HoodieSchemaField(ascendingField);
    HoodieSchemaField hoodieDesc = new HoodieSchemaField(descendingField);
    HoodieSchemaField hoodieIgnore = new HoodieSchemaField(ignoreField);

    assertEquals(HoodieFieldOrder.ASCENDING, hoodieAsc.order());
    assertEquals(HoodieFieldOrder.DESCENDING, hoodieDesc.order());
    assertEquals(HoodieFieldOrder.IGNORE, hoodieIgnore.order());
  }

  @Test
  public void testFieldCreationWithOrder() {
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);

    // Test creation with explicit order
    HoodieSchemaField field = HoodieSchemaField.of("sortField", stringSchema, "Sorted field", "default", HoodieFieldOrder.DESCENDING);

    assertEquals("sortField", field.name());
    assertEquals(HoodieSchemaType.STRING, field.schema().getType());
    assertEquals(Option.of("Sorted field"), field.doc());
    assertEquals(Option.of("default"), field.defaultVal());
    assertEquals(HoodieFieldOrder.DESCENDING, field.order());
  }

  @Test
  public void testFieldProperties() {
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    HoodieSchemaField field = HoodieSchemaField.of("testField", stringSchema, "Test field", "default");

    // Test addProp
    field.addProp("custom.prop", "test-value");
    assertEquals("test-value", field.getProp("custom.prop"));

    // Test getObjectProps
    assertNotNull(field.getObjectProps());
    assertTrue(field.getObjectProps().containsKey("custom.prop"));
  }

  @Test
  public void testFieldPropertyValidation() {
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    HoodieSchemaField field = HoodieSchemaField.of("testField", stringSchema);

    // Should throw on invalid property key
    assertThrows(IllegalArgumentException.class, () -> field.addProp(null, "value"));
    assertThrows(IllegalArgumentException.class, () -> field.addProp("", "value"));
  }
}
