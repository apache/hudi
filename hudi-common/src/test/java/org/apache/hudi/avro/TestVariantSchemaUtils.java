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

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestVariantSchemaUtils {

  private static HoodieSchema schemaWithVariants() {
    return HoodieSchema.createRecord("rec", "ns", null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING)),
        HoodieSchemaField.of("v1", HoodieSchema.createNullable(HoodieSchema.createVariant())),
        HoodieSchemaField.of("v2", HoodieSchema.createVariant("v2_variant", null, null)),
        HoodieSchemaField.of("shredded", HoodieSchema.createVariantShredded(
            "pre_shredded", null, null, HoodieSchema.create(HoodieSchemaType.LONG)))));
  }

  private static HoodieConfig inferenceEnabledConfig() {
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieStorageConfig.PARQUET_VARIANT_SHREDDING_SCHEMA_INFERENCE_ENABLED, "true");
    return config;
  }

  @Test
  public void testGetInferableVariantColumns() {
    HoodieSchema schema = schemaWithVariants();
    // Unshredded variants only; explicitly shredded columns keep schema-driven shredding.
    assertEquals(Arrays.asList("v1", "v2"),
        VariantSchemaUtils.getInferableVariantColumns(inferenceEnabledConfig(), schema));
  }

  @Test
  public void testGetInferableVariantColumnsGating() {
    HoodieSchema schema = schemaWithVariants();

    // Inference disabled (default)
    assertTrue(VariantSchemaUtils.getInferableVariantColumns(new HoodieConfig(), schema).isEmpty());

    // Write shredding disabled
    HoodieConfig shreddingOff = inferenceEnabledConfig();
    shreddingOff.setValue(HoodieStorageConfig.PARQUET_VARIANT_WRITE_SHREDDING_ENABLED, "false");
    assertTrue(VariantSchemaUtils.getInferableVariantColumns(shreddingOff, schema).isEmpty());

    // Forced test DDL wins over inference
    HoodieConfig forced = inferenceEnabledConfig();
    forced.setValue(HoodieStorageConfig.PARQUET_VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST, "a int");
    assertTrue(VariantSchemaUtils.getInferableVariantColumns(forced, schema).isEmpty());

    // Internal schema present: the Spark write support would ignore the splice
    HoodieConfig internal = inferenceEnabledConfig();
    internal.setValue("hoodie.internal.schema", "{}");
    assertTrue(VariantSchemaUtils.getInferableVariantColumns(internal, schema).isEmpty());

    // No variant columns
    HoodieSchema noVariants = HoodieSchema.createRecord("rec", null, null,
        Collections.singletonList(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING))));
    assertTrue(VariantSchemaUtils.getInferableVariantColumns(inferenceEnabledConfig(), noVariants).isEmpty());
  }

  @Test
  public void testApplyInferredShredding() {
    HoodieSchema schema = schemaWithVariants();
    Map<String, HoodieSchema> inferred = new LinkedHashMap<>();
    // Spec-form object typed_value for v1; v2 intentionally absent (declined).
    Map<String, HoodieSchema> typedFields = new LinkedHashMap<>();
    typedFields.put("a", HoodieSchema.create(HoodieSchemaType.INT));
    inferred.put("v1", HoodieSchema.createVariantShreddedObject(typedFields).getTypedValueField().get());

    HoodieSchema spliced = VariantSchemaUtils.applyInferredShredding(schema, inferred);
    assertNotEquals(schema, spliced);

    // v1: shredded, nullability preserved, typed_value nullable
    HoodieSchema v1 = spliced.getField("v1").get().schema();
    assertTrue(v1.isNullable());
    HoodieSchema.Variant v1Variant = (HoodieSchema.Variant) v1.getNonNullType();
    assertTrue(v1Variant.isShredded());
    assertTrue(v1Variant.getTypedValueField().isPresent());
    assertTrue(v1Variant.getTypedValueField().get().isNullable());

    // v2: untouched (declined)
    HoodieSchema v2 = spliced.getField("v2").get().schema();
    assertFalse(((HoodieSchema.Variant) v2).isShredded());

    // Pre-shredded column untouched
    HoodieSchema.Variant preShredded = (HoodieSchema.Variant) spliced.getField("shredded").get().schema();
    assertEquals(HoodieSchemaType.LONG,
        preShredded.getTypedValueField().get().getType());

    // Non-variant column untouched
    assertEquals(HoodieSchemaType.STRING, spliced.getField("id").get().schema().getType());

    // Strip is the inverse: stripping the spliced schema yields unshredded v1 again
    HoodieSchema stripped = VariantSchemaUtils.stripVariantShredding(spliced);
    assertFalse(((HoodieSchema.Variant) stripped.getField("v1").get().schema().getNonNullType()).isShredded());
  }

  @Test
  public void testApplyInferredShreddingIdentityWhenNothingMatches() {
    HoodieSchema schema = schemaWithVariants();
    assertSame(schema, VariantSchemaUtils.applyInferredShredding(schema, Collections.emptyMap()));
    // Map entry for a non-variant column does not match anything
    assertSame(schema, VariantSchemaUtils.applyInferredShredding(schema,
        Collections.singletonMap("id", HoodieSchema.create(HoodieSchemaType.LONG))));
  }

  @Test
  public void testStripVariantShreddingByShape() {
    // Footer-derived schemas lose the variant logical type: a shredded variant comes back as a
    // plain record {metadata: bytes, value: nullable bytes, typed_value}.
    HoodieSchema footerVariant = HoodieSchema.createRecord("v", null, null, Arrays.asList(
        HoodieSchemaField.of("metadata", HoodieSchema.create(HoodieSchemaType.BYTES)),
        HoodieSchemaField.of("value", HoodieSchema.createNullable(HoodieSchemaType.BYTES)),
        HoodieSchemaField.of("typed_value", HoodieSchema.createRecord("tv", null, null,
            Collections.singletonList(HoodieSchemaField.of("a", HoodieSchema.create(HoodieSchemaType.LONG)))))));
    HoodieSchema schema = HoodieSchema.createRecord("rec", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING)),
        HoodieSchemaField.of("v", HoodieSchema.createNullable(footerVariant))));

    HoodieSchema stripped = VariantSchemaUtils.stripVariantShreddingByShape(schema);
    HoodieSchema v = stripped.getField("v").get().schema().getNonNullType();
    assertEquals(2, v.getFields().size());
    assertFalse(v.getField("typed_value").isPresent());
    assertTrue(v.getField("metadata").isPresent());
    assertTrue(v.getField("value").isPresent());
    // Non-variant-shaped records pass through; identity when nothing matches.
    assertEquals(HoodieSchemaType.STRING, stripped.getField("id").get().schema().getType());
    assertSame(stripped, VariantSchemaUtils.stripVariantShreddingByShape(stripped));
  }

  @Test
  public void testApplyInferredShreddingToConfig() {
    HoodieSchema schema = schemaWithVariants();
    HoodieConfig config = new HoodieConfig();
    config.setValue("hoodie.avro.schema", schema.getAvroSchema().toString());
    String originalString = config.getString("hoodie.avro.schema");

    Map<String, HoodieSchema> inferred = Collections.singletonMap("v2",
        HoodieSchema.create(HoodieSchemaType.STRING));
    HoodieConfig spliced = VariantSchemaUtils.applyInferredShreddingToConfig(config, inferred);

    // The original config is never mutated.
    assertEquals(originalString, config.getString("hoodie.avro.schema"));
    HoodieSchema splicedSchema = HoodieSchema.parse(spliced.getString("hoodie.avro.schema"));
    HoodieSchema.Variant v2 = (HoodieSchema.Variant) splicedSchema.getField("v2").get().schema();
    assertTrue(v2.isShredded());

    // Identity when there is nothing to splice.
    assertSame(config, VariantSchemaUtils.applyInferredShreddingToConfig(config, Collections.emptyMap()));
  }
}
