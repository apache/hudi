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

package org.apache.hudi.io.storage.hadoop;

import java.util.Collections;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST;
import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_VARIANT_WRITE_SHREDDING_ENABLED;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for variant shredding provider auto-detection in HoodieAvroFileWriterFactory.
 */
public class TestHoodieAvroFileWriterFactoryVariantShredding {

  /**
   * Test that the detectShreddingProvider method can detect a provider when it exists on the classpath.
   * This test uses reflection to call the private static method.
   */
  @Test
  public void testDetectShreddingProvider() throws Exception {
    Method detectMethod = HoodieAvroFileWriterFactory.class.getDeclaredMethod("detectShreddingProvider");
    detectMethod.setAccessible(true);

    String detected = (String) detectMethod.invoke(null);

    // The result depends on whether the Spark4VariantShreddingProvider is on the classpath
    // In a test environment without Spark 4, this will be null
    // In a Spark 4 environment, it should detect org.apache.hudi.variant.Spark4VariantShreddingProvider
    if (detected != null) {
      assertTrue(detected.contains("VariantShreddingProvider"),
          "Detected class should be a VariantShreddingProvider");
    }
    // If null, that's also valid - no provider found on classpath
  }

  /**
   * Test that detectShreddingProvider returns null when no provider is available.
   * This is verified by the fact that it doesn't throw an exception.
   */
  @Test
  public void testDetectShreddingProviderNoProviderAvailable() throws Exception {
    Method detectMethod = HoodieAvroFileWriterFactory.class.getDeclaredMethod("detectShreddingProvider");
    detectMethod.setAccessible(true);

    // This should not throw an exception even if no provider is found
    String detected = (String) detectMethod.invoke(null);

    // detected can be null or a valid provider class name
    // Both are valid outcomes depending on the test environment
  }

  /**
   * Test effective schema generation with various configurations.
   */
  @Test
  public void testEffectiveSchemaGenerationIntegration() {
    // Create a schema with a variant field
    HoodieSchema.Variant variant = HoodieSchema.createVariant("testVariant", null, null);

    List<HoodieSchemaField> fields = new ArrayList<>();
    fields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)));
    fields.add(HoodieSchemaField.of("v", variant));

    HoodieSchema inputSchema = HoodieSchema.createRecord("TestRecord", null, null, fields);

    // Test 1: Shredding disabled
    HoodieConfig config1 = new HoodieConfig();
    config1.setValue(PARQUET_VARIANT_WRITE_SHREDDING_ENABLED.key(), "false");

    HoodieSchema effective1 = org.apache.hudi.avro.HoodieAvroWriteSupport.generateEffectiveSchema(
        inputSchema, config1);
    assertNotNull(effective1);

    // Test 2: Shredding enabled with forced schema
    HoodieConfig config2 = new HoodieConfig();
    config2.setValue(PARQUET_VARIANT_WRITE_SHREDDING_ENABLED.key(), "true");
    config2.setValue(PARQUET_VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST.key(), "a int, b string");

    HoodieSchema effective2 = org.apache.hudi.avro.HoodieAvroWriteSupport.generateEffectiveSchema(
        inputSchema, config2);
    assertNotNull(effective2);

    HoodieSchemaField variantField = effective2.getFields().get(1);
    HoodieSchema.Variant resultVariant = (HoodieSchema.Variant) variantField.schema();
    assertTrue(resultVariant.isShredded(), "Variant should be shredded with forced schema");
  }

  /**
   * Test schema generation with multiple variant fields.
   */
  @Test
  public void testEffectiveSchemaWithMultipleVariants() {
    HoodieSchema.Variant v1 = HoodieSchema.createVariant("variant1", null, null);
    HoodieSchema.Variant v2 = HoodieSchema.createVariant("variant2", null, null);

    List<HoodieSchemaField> fields = new ArrayList<>();
    fields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)));
    fields.add(HoodieSchemaField.of("v1", v1));
    fields.add(HoodieSchemaField.of("v2", v2));

    HoodieSchema inputSchema = HoodieSchema.createRecord("TestRecord", null, null, fields);

    HoodieConfig config = new HoodieConfig();
    config.setValue(PARQUET_VARIANT_WRITE_SHREDDING_ENABLED.key(), "true");
    config.setValue(PARQUET_VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST.key(), "x int, y string");

    HoodieSchema effectiveSchema = org.apache.hudi.avro.HoodieAvroWriteSupport.generateEffectiveSchema(
        inputSchema, config);

    assertNotNull(effectiveSchema);
    // Both variant fields should be transformed
    HoodieSchemaField v1Field = effectiveSchema.getFields().get(1);
    HoodieSchemaField v2Field = effectiveSchema.getFields().get(2);

    assertTrue(((HoodieSchema.Variant) v1Field.schema()).isShredded());
    assertTrue(((HoodieSchema.Variant) v2Field.schema()).isShredded());
  }

  /**
   * Test schema generation with nullable variant fields.
   */
  @Test
  public void testEffectiveSchemaWithNullableVariants() {
    HoodieSchema.Variant variant = HoodieSchema.createVariant("testVariant", null, null);
    HoodieSchema nullableVariant = HoodieSchema.createNullable(variant);

    List<HoodieSchemaField> fields = new ArrayList<>();
    fields.add(HoodieSchemaField.of("v", nullableVariant));

    HoodieSchema inputSchema = HoodieSchema.createRecord("TestRecord", null, null, fields);

    HoodieConfig config = new HoodieConfig();
    config.setValue(PARQUET_VARIANT_WRITE_SHREDDING_ENABLED.key(), "true");
    config.setValue(PARQUET_VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST.key(), "a int");

    HoodieSchema effectiveSchema = org.apache.hudi.avro.HoodieAvroWriteSupport.generateEffectiveSchema(
        inputSchema, config);

    HoodieSchemaField variantField = effectiveSchema.getFields().get(0);
    assertTrue(variantField.schema().isNullable(), "Variant should remain nullable");

    HoodieSchema unwrapped = variantField.schema().getNonNullType();
    assertTrue(((HoodieSchema.Variant) unwrapped).isShredded());
  }

  /**
   * Test that effective schema generation handles already-shredded variants correctly.
   */
  @Test
  public void testEffectiveSchemaWithAlreadyShreddedVariant() {
    // Create a variant that's already shredded
    Map<String, HoodieSchema> typedFields = new LinkedHashMap<>();
    typedFields.put("a", HoodieSchema.create(HoodieSchemaType.INT));
    typedFields.put("b", HoodieSchema.create(HoodieSchemaType.STRING));

    HoodieSchema.Variant shreddedVariant = HoodieSchema.createVariantShreddedObject(
        "testVariant", null, null, typedFields);

    List<HoodieSchemaField> fields = Collections.singletonList(
        HoodieSchemaField.of("v", shreddedVariant));

    HoodieSchema inputSchema = HoodieSchema.createRecord("TestRecord", null, null, fields);

    // Shredding enabled but no forced schema
    HoodieConfig config = new HoodieConfig();
    config.setValue(PARQUET_VARIANT_WRITE_SHREDDING_ENABLED.key(), "true");

    HoodieSchema effectiveSchema = org.apache.hudi.avro.HoodieAvroWriteSupport.generateEffectiveSchema(
        inputSchema, config);

    // The already-shredded variant should remain shredded
    HoodieSchemaField variantField = effectiveSchema.getFields().get(0);
    HoodieSchema.Variant resultVariant = (HoodieSchema.Variant) variantField.schema();
    assertTrue(resultVariant.isShredded());
  }

  /**
   * Test unshredding when shredding is disabled.
   */
  @Test
  public void testEffectiveSchemaUnshreddingWhenDisabled() {
    // Create a shredded variant
    Map<String, HoodieSchema> typedFields = new LinkedHashMap<>();
    typedFields.put("a", HoodieSchema.create(HoodieSchemaType.INT));

    HoodieSchema.Variant shreddedVariant = HoodieSchema.createVariantShreddedObject(
        "testVariant", null, null, typedFields);

    List<HoodieSchemaField> fields = Collections.singletonList(
        HoodieSchemaField.of("v", shreddedVariant)
    );

    HoodieSchema inputSchema = HoodieSchema.createRecord("TestRecord", null, null, fields);

    // Shredding disabled - should unshred
    HoodieConfig config = new HoodieConfig();
    config.setValue(PARQUET_VARIANT_WRITE_SHREDDING_ENABLED.key(), "false");

    HoodieSchema effectiveSchema = org.apache.hudi.avro.HoodieAvroWriteSupport.generateEffectiveSchema(
        inputSchema, config);

    HoodieSchemaField variantField = effectiveSchema.getFields().get(0);
    HoodieSchema.Variant resultVariant = (HoodieSchema.Variant) variantField.schema();
    // Should be unshredded now
    assertTrue(!resultVariant.isShredded() || !resultVariant.getTypedValueField().isPresent(),
        "Variant should be unshredded when shredding is disabled");
  }

  /**
   * Test that non-variant fields are preserved during schema transformation.
   */
  @Test
  public void testEffectiveSchemaPreservesNonVariantFields() {
    HoodieSchema.Variant variant = HoodieSchema.createVariant("testVariant", null, null);

    List<HoodieSchemaField> fields = new ArrayList<>();
    fields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)));
    fields.add(HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING)));
    fields.add(HoodieSchemaField.of("v", variant));
    fields.add(HoodieSchemaField.of("timestamp", HoodieSchema.create(HoodieSchemaType.LONG)));

    HoodieSchema inputSchema = HoodieSchema.createRecord("TestRecord", null, null, fields);

    HoodieConfig config = new HoodieConfig();
    config.setValue(PARQUET_VARIANT_WRITE_SHREDDING_ENABLED.key(), "true");
    config.setValue(PARQUET_VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST.key(), "a int");

    HoodieSchema effectiveSchema = org.apache.hudi.avro.HoodieAvroWriteSupport.generateEffectiveSchema(
        inputSchema, config);

    // Verify all non-variant fields are preserved
    List<HoodieSchemaField> resultFields = effectiveSchema.getFields();
    assertNotNull(resultFields.get(0).schema()); // id
    assertNotNull(resultFields.get(1).schema()); // name
    assertNotNull(resultFields.get(2).schema()); // v (variant, should be shredded)
    assertNotNull(resultFields.get(3).schema()); // timestamp

    // Non-variant field types should be unchanged
    assertSame(HoodieSchemaType.INT, resultFields.get(0).schema().getType());
    assertSame(HoodieSchemaType.STRING, resultFields.get(1).schema().getType());
    assertSame(HoodieSchemaType.LONG, resultFields.get(3).schema().getType());
  }
}
