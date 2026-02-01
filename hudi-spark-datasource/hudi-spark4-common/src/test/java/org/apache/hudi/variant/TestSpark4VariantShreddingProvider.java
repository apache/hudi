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

package org.apache.hudi.variant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for Spark4VariantShreddingProvider.
 *
 * <p>Note: These tests verify the provider's structure and API contracts.
 * Full end-to-end tests with actual Spark Variant parsing are in the integration test suite.</p>
 */
public class TestSpark4VariantShreddingProvider {

  private Spark4VariantShreddingProvider provider;

  @BeforeEach
  public void setup() {
    provider = new Spark4VariantShreddingProvider();
  }

  /**
   * Test that the provider can be instantiated successfully.
   */
  @Test
  public void testProviderInstantiation() {
    assertNotNull(provider, "Provider should be instantiated");
  }

  /**
   * Test null variant handling - provider should return null for null inputs.
   */
  @Test
  public void testShredNullVariant() {
    Schema unshreddedSchema = createUnshreddedVariantSchema();
    GenericRecord unshreddedVariant = new GenericData.Record(unshreddedSchema);
    unshreddedVariant.put("value", null);
    unshreddedVariant.put("metadata", null);

    Schema shreddedSchema = createShreddedVariantSchema(Schema.create(Schema.Type.INT));
    HoodieSchema.Variant variantSchema = HoodieSchema.createVariantShredded(
        "test", null, null, HoodieSchema.create(HoodieSchemaType.INT));

    GenericRecord result = provider.shredVariantRecord(unshreddedVariant, shreddedSchema, variantSchema);

    assertNull(result, "Provider should return null for null variant inputs");
  }

  /**
   * Test that the provider handles object shredding schema structure.
   */
  @Test
  public void testObjectShreddingSchemaStructure() {
    // Create shredded object schema with fields a:int, b:string
    Map<String, HoodieSchema> typedFields = new LinkedHashMap<>();
    typedFields.put("a", HoodieSchema.create(HoodieSchemaType.INT));
    typedFields.put("b", HoodieSchema.create(HoodieSchemaType.STRING));

    HoodieSchema.Variant variantSchema = HoodieSchema.createVariantShreddedObject(
        "test", null, null, typedFields);

    assertNotNull(variantSchema, "Variant schema should be created");
    assertTrue(variantSchema.getTypedValueField().isPresent(), "Should have typed_value field");
  }

  /**
   * Test that provider correctly constructs Avro schemas for basic types.
   */
  @Test
  public void testAvroSchemaConstruction() {
    // Test INT type
    HoodieSchema intSchema = HoodieSchema.create(HoodieSchemaType.INT);
    HoodieSchema.Variant intVariant = HoodieSchema.createVariantShredded("test", null, null, intSchema);
    assertNotNull(intVariant);
    assertNotNull(intVariant.getAvroSchema());

    // Test STRING type
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    HoodieSchema.Variant stringVariant = HoodieSchema.createVariantShredded("test", null, null, stringSchema);
    assertNotNull(stringVariant);

    // Test LONG type
    HoodieSchema longSchema = HoodieSchema.create(HoodieSchemaType.LONG);
    HoodieSchema.Variant longVariant = HoodieSchema.createVariantShredded("test", null, null, longSchema);
    assertNotNull(longVariant);

    // Test DOUBLE type
    HoodieSchema doubleSchema = HoodieSchema.create(HoodieSchemaType.DOUBLE);
    HoodieSchema.Variant doubleVariant = HoodieSchema.createVariantShredded("test", null, null, doubleSchema);
    assertNotNull(doubleVariant);

    // Test BOOLEAN type
    HoodieSchema boolSchema = HoodieSchema.create(HoodieSchemaType.BOOLEAN);
    HoodieSchema.Variant boolVariant = HoodieSchema.createVariantShredded("test", null, null, boolSchema);
    assertNotNull(boolVariant);
  }

  /**
   * Test decimal schema construction.
   */
  @Test
  public void testDecimalSchemaConstruction() {
    HoodieSchema decimalSchema = HoodieSchema.createDecimal(10, 2);
    HoodieSchema.Variant decimalVariant = HoodieSchema.createVariantShredded("test", null, null, decimalSchema);

    assertNotNull(decimalVariant);
    assertNotNull(decimalVariant.getAvroSchema());
    assertTrue(decimalVariant.getTypedValueField().isPresent());
  }

  /**
   * Test object schema with multiple fields.
   */
  @Test
  public void testMultiFieldObjectSchema() {
    Map<String, HoodieSchema> fields = new LinkedHashMap<>();
    fields.put("field1", HoodieSchema.create(HoodieSchemaType.INT));
    fields.put("field2", HoodieSchema.create(HoodieSchemaType.STRING));
    fields.put("field3", HoodieSchema.create(HoodieSchemaType.DOUBLE));
    fields.put("field4", HoodieSchema.create(HoodieSchemaType.BOOLEAN));

    HoodieSchema.Variant variant = HoodieSchema.createVariantShreddedObject("test", null, null, fields);

    assertNotNull(variant);
    assertNotNull(variant.getAvroSchema());
    assertTrue(variant.isShredded());
    assertTrue(variant.getTypedValueField().isPresent());

    // Verify the typed_value field is a RECORD with the correct fields
    HoodieSchema typedValueSchema = variant.getTypedValueField().get();
    assertEquals(HoodieSchemaType.RECORD, typedValueSchema.getType());

    List<HoodieSchemaField> typedFields = typedValueSchema.getFields();
    assertEquals(4, typedFields.size(), "Should have 4 fields");
  }

  /**
   * Test that array shredding schema can be created with correct structure.
   */
  @Test
  public void testArrayShreddingSchemaStructure() {
    // Create element schema wrapped in the shredded field struct
    HoodieSchema elementType = HoodieSchema.create(HoodieSchemaType.INT);
    HoodieSchema arrayElementSchema = HoodieSchema.createShreddedFieldStruct("element", elementType);

    // Create array schema
    HoodieSchema arraySchema = HoodieSchema.createArray(arrayElementSchema);

    // Create variant with array typed_value
    HoodieSchema.Variant arrayVariant = HoodieSchema.createVariantShredded("test", null, null, arraySchema);

    assertNotNull(arrayVariant);
    assertTrue(arrayVariant.isShredded());
    assertTrue(arrayVariant.getTypedValueField().isPresent());

    HoodieSchema typedValue = arrayVariant.getTypedValueField().get();
    assertEquals(HoodieSchemaType.ARRAY, typedValue.getType());
  }

  /**
   * Test schema structure with nested objects.
   */
  @Test
  public void testNestedObjectSchemaStructure() {
    // Create inner object
    List<HoodieSchemaField> innerFields = new java.util.ArrayList<>();
    innerFields.add(HoodieSchemaField.of("x",
        HoodieSchema.create(HoodieSchemaType.INT)));
    innerFields.add(HoodieSchemaField.of("y",
        HoodieSchema.create(HoodieSchemaType.INT)));

    HoodieSchema innerObject = HoodieSchema.createRecord("inner", null, null, innerFields);

    // Create outer object with the inner object as a field
    Map<String, HoodieSchema> outerFields = new LinkedHashMap<>();
    outerFields.put("name", HoodieSchema.create(HoodieSchemaType.STRING));
    outerFields.put("point", innerObject);

    HoodieSchema.Variant variant = HoodieSchema.createVariantShreddedObject("test", null, null, outerFields);

    assertNotNull(variant);
    assertTrue(variant.isShredded());
  }

  /**
   * Test that provider respects nullable wrapper for variant fields.
   */
  @Test
  public void testNullableVariantSchema() {
    HoodieSchema intSchema = HoodieSchema.create(HoodieSchemaType.INT);
    HoodieSchema.Variant variant = HoodieSchema.createVariantShredded("test", null, null, intSchema);
    HoodieSchema nullableVariant = HoodieSchema.createNullable(variant);

    assertNotNull(nullableVariant);
    assertTrue(nullableVariant.isNullable());

    HoodieSchema unwrapped = nullableVariant.getNonNullType();
    assertEquals(HoodieSchemaType.VARIANT, unwrapped.getType());
  }

  /**
   * Test complex DDL-style schema with various types.
   */
  @Test
  public void testComplexDDLSchema() {
    Map<String, HoodieSchema> fields = new LinkedHashMap<>();
    fields.put("id", HoodieSchema.create(HoodieSchemaType.LONG));
    fields.put("name", HoodieSchema.create(HoodieSchemaType.STRING));
    fields.put("price", HoodieSchema.createDecimal(15, 2));
    fields.put("quantity", HoodieSchema.create(HoodieSchemaType.INT));
    fields.put("active", HoodieSchema.create(HoodieSchemaType.BOOLEAN));
    fields.put("rating", HoodieSchema.create(HoodieSchemaType.DOUBLE));

    HoodieSchema.Variant variant = HoodieSchema.createVariantShreddedObject("test", null, null, fields);

    assertNotNull(variant);
    assertTrue(variant.isShredded());

    HoodieSchema typedValue = variant.getTypedValueField().get();
    List<HoodieSchemaField> typedFields = typedValue.getFields();
    assertEquals(6, typedFields.size(), "Should have all 6 fields");
  }

  /**
   * Helper method to create an unshredded variant schema.
   */
  private Schema createUnshreddedVariantSchema() {
    return SchemaBuilder.record("variant")
        .fields()
        .requiredBytes("value")
        .requiredBytes("metadata")
        .endRecord();
  }

  /**
   * Helper method to create a shredded variant schema with the given typed_value type.
   */
  private Schema createShreddedVariantSchema(Schema typedValueType) {
    return SchemaBuilder.record("variant")
        .fields()
        .optionalBytes("value")
        .requiredBytes("metadata")
        .name("typed_value").type().unionOf().nullType().and().type(typedValueType).endUnion().noDefault()
        .endRecord();
  }
}
