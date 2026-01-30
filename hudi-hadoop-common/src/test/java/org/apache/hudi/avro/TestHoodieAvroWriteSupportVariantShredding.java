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

import java.lang.reflect.Method;
import java.util.Collections;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.LogicalTypes.Decimal;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST;
import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_VARIANT_WRITE_SHREDDING_ENABLED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for variant shredding functionality in HoodieAvroWriteSupport.
 */
public class TestHoodieAvroWriteSupportVariantShredding {

  /**
   * Test parseShreddingDDL with various valid inputs.
   */
  @ParameterizedTest
  @MethodSource("provideParseShreddingDDLTestCases")
  public void testParseShreddingDDL(String ddl, Map<String, HoodieSchemaType> expectedTypes) throws Exception {
    // Use reflection to call the private static method
    Method method = HoodieAvroWriteSupport.class.getDeclaredMethod(
        "parseShreddingDDL", String.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, HoodieSchema> result = (Map<String, HoodieSchema>) method.invoke(null, ddl);

    assertEquals(expectedTypes.size(), result.size(), "Number of fields should match");

    for (Map.Entry<String, HoodieSchemaType> entry : expectedTypes.entrySet()) {
      String fieldName = entry.getKey();
      HoodieSchemaType expectedType = entry.getValue();

      assertTrue(result.containsKey(fieldName), "Should contain field: " + fieldName);
      assertEquals(expectedType, result.get(fieldName).getType(),
          "Type for field " + fieldName + " should match");
    }
  }

  private static Stream<Arguments> provideParseShreddingDDLTestCases() {
    Map<String, HoodieSchemaType> case1 = new LinkedHashMap<>();
    case1.put("a", HoodieSchemaType.INT);
    case1.put("b", HoodieSchemaType.STRING);

    Map<String, HoodieSchemaType> case2 = new LinkedHashMap<>();
    case2.put("x", HoodieSchemaType.LONG);
    case2.put("y", HoodieSchemaType.DOUBLE);
    case2.put("z", HoodieSchemaType.BOOLEAN);

    Map<String, HoodieSchemaType> case3 = new LinkedHashMap<>();
    case3.put("field1", HoodieSchemaType.FLOAT);
    case3.put("field2", HoodieSchemaType.BYTES);

    Map<String, HoodieSchemaType> case4 = new LinkedHashMap<>();
    case4.put("id", HoodieSchemaType.INT);

    Map<String, HoodieSchemaType> case5 = new LinkedHashMap<>();
    case5.put("value", HoodieSchemaType.DECIMAL);

    return Stream.of(
        Arguments.of("a int, b string", case1),
        Arguments.of("x long, y double, z boolean", case2),
        Arguments.of("field1 float, field2 binary", case3),
        Arguments.of("id integer", case4),
        Arguments.of("value decimal(10,2)", case5)
    );
  }

  /**
   * Test parseShreddingDDL with whitespace variations.
   */
  @Test
  public void testParseShreddingDDLWithWhitespace() throws Exception {
    Method method = HoodieAvroWriteSupport.class.getDeclaredMethod(
        "parseShreddingDDL", String.class);
    method.setAccessible(true);

    // Extra spaces
    @SuppressWarnings("unchecked")
    Map<String, HoodieSchema> result1 = (Map<String, HoodieSchema>) method.invoke(null,
        "  a    int  ,   b   string  ");
    assertEquals(2, result1.size());
    assertEquals(HoodieSchemaType.INT, result1.get("a").getType());
    assertEquals(HoodieSchemaType.STRING, result1.get("b").getType());

    // Tabs
    @SuppressWarnings("unchecked")
    Map<String, HoodieSchema> result2 = (Map<String, HoodieSchema>) method.invoke(null,
        "a\tint,\tb\tstring");
    assertEquals(2, result2.size());

    // Mixed
    @SuppressWarnings("unchecked")
    Map<String, HoodieSchema> result3 = (Map<String, HoodieSchema>) method.invoke(null,
        " a int , b   string,  c\tdouble ");
    assertEquals(3, result3.size());
  }

  /**
   * Test parseShreddingDDL with empty/invalid inputs.
   */
  @Test
  public void testParseShreddingDDLInvalidInputs() throws Exception {
    Method method = HoodieAvroWriteSupport.class.getDeclaredMethod(
        "parseShreddingDDL", String.class);
    method.setAccessible(true);

    // Empty string
    @SuppressWarnings("unchecked")
    Map<String, HoodieSchema> result1 = (Map<String, HoodieSchema>) method.invoke(null, "");
    assertEquals(0, result1.size());

    // Only whitespace
    @SuppressWarnings("unchecked")
    Map<String, HoodieSchema> result2 = (Map<String, HoodieSchema>) method.invoke(null, "   ");
    assertEquals(0, result2.size());

    // Only comma
    @SuppressWarnings("unchecked")
    Map<String, HoodieSchema> result3 = (Map<String, HoodieSchema>) method.invoke(null, ",,,");
    assertEquals(0, result3.size());

    // Invalid format - missing type
    assertThrows(Exception.class, () -> {
      method.invoke(null, "a");
    });

    // Invalid format - only type
    assertThrows(Exception.class, () -> {
      method.invoke(null, "int");
    });
  }

  /**
   * Test parseShreddingDDL with decimal types containing commas.
   * This verifies that commas inside decimal(p,s) are not treated as field separators.
   */
  @Test
  public void testParseShreddingDDLWithDecimal() throws Exception {
    Method method = HoodieAvroWriteSupport.class.getDeclaredMethod(
        "parseShreddingDDL", String.class);
    method.setAccessible(true);

    // Test with decimal type that has comma in it
    @SuppressWarnings("unchecked")
    Map<String, HoodieSchema> result = (Map<String, HoodieSchema>) method.invoke(null,
        "a int, b decimal(10,2), c string");

    assertEquals(3, result.size(), "Should have 3 fields");
    assertTrue(result.containsKey("a"));
    assertTrue(result.containsKey("b"));
    assertTrue(result.containsKey("c"));

    assertEquals(HoodieSchemaType.INT, result.get("a").getType());
    assertEquals(HoodieSchemaType.DECIMAL, result.get("b").getType());
    assertEquals(HoodieSchemaType.STRING, result.get("c").getType());

    // Test with multiple decimals
    @SuppressWarnings("unchecked")
    Map<String, HoodieSchema> result2 = (Map<String, HoodieSchema>) method.invoke(null,
        "price decimal(15,2), quantity int, discount decimal(5,3)");

    assertEquals(3, result2.size());
    assertEquals(HoodieSchemaType.DECIMAL, result2.get("price").getType());
    assertEquals(HoodieSchemaType.INT, result2.get("quantity").getType());
    assertEquals(HoodieSchemaType.DECIMAL, result2.get("discount").getType());
  }

  /**
   * Test parseSimpleType with all supported types.
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "int", "INT", "integer", "INTEGER",
      "long", "LONG", "bigint", "BIGINT",
      "string", "STRING",
      "double", "DOUBLE",
      "float", "FLOAT",
      "boolean", "BOOLEAN",
      "binary", "BINARY"
  })
  public void testParseSimpleTypeBasicTypes(String typeStr) throws Exception {
    Method method = HoodieAvroWriteSupport.class.getDeclaredMethod(
        "parseSimpleType", String.class);
    method.setAccessible(true);

    HoodieSchema result = (HoodieSchema) method.invoke(null, typeStr);
    assertNotNull(result);

    String lower = typeStr.toLowerCase();
    if (lower.equals("int") || lower.equals("integer")) {
      assertEquals(HoodieSchemaType.INT, result.getType());
    } else if (lower.equals("long") || lower.equals("bigint")) {
      assertEquals(HoodieSchemaType.LONG, result.getType());
    } else if (lower.equals("string")) {
      assertEquals(HoodieSchemaType.STRING, result.getType());
    } else if (lower.equals("double")) {
      assertEquals(HoodieSchemaType.DOUBLE, result.getType());
    } else if (lower.equals("float")) {
      assertEquals(HoodieSchemaType.FLOAT, result.getType());
    } else if (lower.equals("boolean")) {
      assertEquals(HoodieSchemaType.BOOLEAN, result.getType());
    } else if (lower.equals("binary")) {
      assertEquals(HoodieSchemaType.BYTES, result.getType());
    }
  }

  /**
   * Test parseSimpleType with decimal types.
   */
  @ParameterizedTest
  @MethodSource("provideDecimalTestCases")
  public void testParseSimpleTypeDecimal(String typeStr, int expectedPrecision, int expectedScale)
      throws Exception {
    Method method = HoodieAvroWriteSupport.class.getDeclaredMethod(
        "parseSimpleType", String.class);
    method.setAccessible(true);

    HoodieSchema result = (HoodieSchema) method.invoke(null, typeStr);
    assertEquals(HoodieSchemaType.DECIMAL, result.getType());

    // Verify precision and scale via Avro schema
    Schema avroSchema = result.getAvroSchema();
    LogicalType logicalType = avroSchema.getLogicalType();
    assertInstanceOf(Decimal.class, logicalType);
    LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logicalType;
    assertEquals(expectedPrecision, decimal.getPrecision());
    assertEquals(expectedScale, decimal.getScale());
  }

  private static Stream<Arguments> provideDecimalTestCases() {
    return Stream.of(
        Arguments.of("decimal(10,2)", 10, 2),
        Arguments.of("decimal(38,18)", 38, 18),
        Arguments.of("decimal(5,0)", 5, 0),
        Arguments.of("DECIMAL(15,3)", 15, 3),
        Arguments.of("decimal( 20 , 5 )", 20, 5)  // with spaces
    );
  }

  /**
   * Test parseSimpleType with unsupported types.
   */
  @ParameterizedTest
  @ValueSource(strings = {"unsupported", "array", "map", "struct", "invalid"})
  public void testParseSimpleTypeUnsupported(String typeStr) throws Exception {
    Method method = HoodieAvroWriteSupport.class.getDeclaredMethod(
        "parseSimpleType", String.class);
    method.setAccessible(true);

    assertThrows(Exception.class, () -> {
      method.invoke(null, typeStr);
    });
  }

  /**
   * Test generateEffectiveSchema with shredding disabled.
   */
  @Test
  public void testGenerateEffectiveSchemaWithShreddingDisabled() {
    // Create a schema with a shredded variant field
    Map<String, HoodieSchema> typedValueFields = new LinkedHashMap<>();
    typedValueFields.put("a", HoodieSchema.create(HoodieSchemaType.INT));
    typedValueFields.put("b", HoodieSchema.create(HoodieSchemaType.STRING));

    HoodieSchema.Variant shreddedVariant = HoodieSchema.createVariantShreddedObject(
        "myVariant", null, null, typedValueFields);

    List<HoodieSchemaField> fields = new ArrayList<>();
    fields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)));
    fields.add(HoodieSchemaField.of("v", shreddedVariant));

    HoodieSchema inputSchema = HoodieSchema.createRecord("TestRecord", null, null, fields);

    // Configure shredding disabled
    Properties props = new Properties();
    props.setProperty(PARQUET_VARIANT_WRITE_SHREDDING_ENABLED.key(), "false");

    HoodieSchema effectiveSchema = HoodieAvroWriteSupport.generateEffectiveSchema(inputSchema, props);

    // Verify the variant field is unshredded
    HoodieSchemaField variantField = effectiveSchema.getFields().get(1);
    HoodieSchema variantSchema = variantField.schema();
    assertEquals(HoodieSchemaType.VARIANT, variantSchema.getType());

    HoodieSchema.Variant variant = (HoodieSchema.Variant) variantSchema;
    assertFalse(variant.isShredded(), "Variant should be unshredded when shredding is disabled");
  }

  /**
   * Test generateEffectiveSchema with shredding enabled and forced schema.
   */
  @Test
  public void testGenerateEffectiveSchemaWithForcedShreddingSchema() {
    // Create a schema with an unshredded variant
    HoodieSchema.Variant unshreddedVariant = HoodieSchema.createVariant("myVariant", null, null);

    List<HoodieSchemaField> fields = new ArrayList<>();
    fields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)));
    fields.add(HoodieSchemaField.of("v", unshreddedVariant));

    HoodieSchema inputSchema = HoodieSchema.createRecord("TestRecord", null, null, fields);

    // Configure shredding enabled with forced schema
    Properties props = new Properties();
    props.setProperty(PARQUET_VARIANT_WRITE_SHREDDING_ENABLED.key(), "true");
    props.setProperty(PARQUET_VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST.key(), "x int, y string");

    HoodieSchema effectiveSchema = HoodieAvroWriteSupport.generateEffectiveSchema(inputSchema, props);

    // Verify the variant field is now shredded
    HoodieSchemaField variantField = effectiveSchema.getFields().get(1);
    HoodieSchema variantSchema = variantField.schema();
    assertEquals(HoodieSchemaType.VARIANT, variantSchema.getType());

    HoodieSchema.Variant variant = (HoodieSchema.Variant) variantSchema;
    assertTrue(variant.isShredded(), "Variant should be shredded with forced schema");
    assertTrue(variant.getTypedValueField().isPresent(), "Should have typed_value field");
  }

  /**
   * Test generateEffectiveSchema with nullable variant fields.
   */
  @Test
  public void testGenerateEffectiveSchemaWithNullableVariant() {
    HoodieSchema.Variant variant = HoodieSchema.createVariant("myVariant", null, null);
    HoodieSchema nullableVariant = HoodieSchema.createNullable(variant);

    List<HoodieSchemaField> fields = new ArrayList<>();
    fields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)));
    fields.add(HoodieSchemaField.of("v", nullableVariant));

    HoodieSchema inputSchema = HoodieSchema.createRecord("TestRecord", null, null, fields);

    Properties props = new Properties();
    props.setProperty(PARQUET_VARIANT_WRITE_SHREDDING_ENABLED.key(), "true");
    props.setProperty(PARQUET_VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST.key(), "a int");

    HoodieSchema effectiveSchema = HoodieAvroWriteSupport.generateEffectiveSchema(inputSchema, props);

    HoodieSchemaField variantField = effectiveSchema.getFields().get(1);
    HoodieSchema variantSchema = variantField.schema();

    assertTrue(variantSchema.isNullable(), "Variant should remain nullable");

    HoodieSchema unwrapped = variantSchema.getNonNullType();
    assertEquals(HoodieSchemaType.VARIANT, unwrapped.getType());
    assertTrue(((HoodieSchema.Variant) unwrapped).isShredded());
  }

  /**
   * Test generateEffectiveSchema with multiple variant fields.
   */
  @Test
  public void testGenerateEffectiveSchemaWithMultipleVariantFields() {
    HoodieSchema.Variant v1 = HoodieSchema.createVariant("variant1", null, null);
    HoodieSchema.Variant v2 = HoodieSchema.createVariant("variant2", null, null);

    List<HoodieSchemaField> fields = new ArrayList<>();
    fields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)));
    fields.add(HoodieSchemaField.of("v1", v1));
    fields.add(HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING)));
    fields.add(HoodieSchemaField.of("v2", v2));

    HoodieSchema inputSchema = HoodieSchema.createRecord("TestRecord", null, null, fields);

    Properties props = new Properties();
    props.setProperty(PARQUET_VARIANT_WRITE_SHREDDING_ENABLED.key(), "true");
    props.setProperty(PARQUET_VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST.key(), "field1 int, field2 string");

    HoodieSchema effectiveSchema = HoodieAvroWriteSupport.generateEffectiveSchema(inputSchema, props);

    // Both variant fields should be shredded
    HoodieSchemaField v1Field = effectiveSchema.getFields().get(1);
    HoodieSchemaField v2Field = effectiveSchema.getFields().get(3);

    assertTrue(((HoodieSchema.Variant) v1Field.schema()).isShredded());
    assertTrue(((HoodieSchema.Variant) v2Field.schema()).isShredded());

    // Non-variant fields should be unchanged
    assertEquals(HoodieSchemaType.INT, effectiveSchema.getFields().get(0).schema().getType());
    assertEquals(HoodieSchemaType.STRING, effectiveSchema.getFields().get(2).schema().getType());
  }

  /**
   * Test generateEffectiveSchema with HoodieConfig overload.
   */
  @Test
  public void testGenerateEffectiveSchemaWithHoodieConfig() {
    HoodieSchema.Variant variant = HoodieSchema.createVariant("myVariant", null, null);

    List<HoodieSchemaField> fields = Collections.singletonList(
        HoodieSchemaField.of("v", variant)
    );

    HoodieSchema inputSchema = HoodieSchema.createRecord("TestRecord", null, null, fields);

    HoodieConfig config = new HoodieConfig();
    config.setValue(PARQUET_VARIANT_WRITE_SHREDDING_ENABLED.key(), "true");
    config.setValue(PARQUET_VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST.key(), "a int");

    HoodieSchema effectiveSchema = HoodieAvroWriteSupport.generateEffectiveSchema(inputSchema, config);

    HoodieSchemaField variantField = effectiveSchema.getFields().get(0);
    assertTrue(((HoodieSchema.Variant) variantField.schema()).isShredded());
  }

  /**
   * Test that non-record schemas are returned unchanged.
   */
  @Test
  public void testGenerateEffectiveSchemaWithNonRecordSchema() {
    HoodieSchema intSchema = HoodieSchema.create(HoodieSchemaType.INT);

    Properties props = new Properties();
    props.setProperty(PARQUET_VARIANT_WRITE_SHREDDING_ENABLED.key(), "true");

    HoodieSchema effectiveSchema = HoodieAvroWriteSupport.generateEffectiveSchema(intSchema, props);

    assertEquals(intSchema, effectiveSchema, "Non-record schema should be returned unchanged");
  }

  /**
   * Test schema with no variant fields is returned unchanged.
   */
  @Test
  public void testGenerateEffectiveSchemaWithNoVariantFields() {
    List<HoodieSchemaField> fields = new ArrayList<>();
    fields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)));
    fields.add(HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING)));

    HoodieSchema inputSchema = HoodieSchema.createRecord("TestRecord", null, null, fields);

    Properties props = new Properties();
    props.setProperty(PARQUET_VARIANT_WRITE_SHREDDING_ENABLED.key(), "true");
    props.setProperty(PARQUET_VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST.key(), "a int");

    HoodieSchema effectiveSchema = HoodieAvroWriteSupport.generateEffectiveSchema(inputSchema, props);

    assertEquals(inputSchema, effectiveSchema, "Schema with no variant fields should be unchanged");
  }

  /**
   * Test decimal parsing with various formats.
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "decimal(1,0)",
      "decimal(38,38)",
      "decimal(10,5)",
      "DECIMAL(20,10)",
      "decimal( 15 , 7 )"
  })
  public void testDecimalParsingVariations(String decimalType) throws Exception {
    Method method = HoodieAvroWriteSupport.class.getDeclaredMethod(
        "parseSimpleType", String.class);
    method.setAccessible(true);

    HoodieSchema result = (HoodieSchema) method.invoke(null, decimalType);
    assertEquals(HoodieSchemaType.DECIMAL, result.getType());
  }
}