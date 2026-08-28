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

package org.apache.hudi.common.avro;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaTestUtils;
import org.apache.hudi.common.schema.HoodieSchemaType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
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
  public void testGetInferableVariantColumnsIsTopLevelOnly() {
    // Inference never shreds a nested variant (struct member, array element, map value): the
    // footer-fallback strip and the write supports' hooks share that top-level scope, so a
    // recursive walk here would shred what nothing else accounts for.
    HoodieSchema schema = HoodieSchema.createRecord("rec", "ns", null, Arrays.asList(
        HoodieSchemaField.of("top", HoodieSchema.createVariant()),
        HoodieSchemaField.of("s", HoodieSchema.createRecord("s_rec", "ns", null,
            Collections.singletonList(HoodieSchemaField.of("v", HoodieSchema.createVariant("s_variant", null, null))))),
        HoodieSchemaField.of("a", HoodieSchema.createArray(HoodieSchema.createVariant("a_variant", null, null))),
        HoodieSchemaField.of("m", HoodieSchema.createMap(HoodieSchema.createVariant("m_variant", null, null)))));

    assertEquals(Collections.singletonList("top"),
        VariantSchemaUtils.getInferableVariantColumns(inferenceEnabledConfig(), schema));
  }

  @Test
  public void testGetInferableVariantColumnsFromConfig() {
    HoodieSchema schema = schemaWithVariants();
    String schemaString = schema.getAvroSchema().toString();
    // Only the id column: distinguishes the write-schema key from the avro-schema key below.
    String noVariantString = HoodieSchema.createRecord("rec", null, null,
        Collections.singletonList(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING))))
        .getAvroSchema().toString();

    // hoodie.write.schema takes precedence over hoodie.avro.schema, as in the row write support.
    HoodieConfig writeOverridesAvro = inferenceEnabledConfig();
    writeOverridesAvro.setValue("hoodie.avro.schema", schemaString);
    writeOverridesAvro.setValue("hoodie.write.schema", noVariantString);
    assertTrue(VariantSchemaUtils.getInferableVariantColumnsFromConfig(writeOverridesAvro).isEmpty());

    HoodieConfig avroOnly = inferenceEnabledConfig();
    avroOnly.setValue("hoodie.avro.schema", schemaString);
    assertEquals(Arrays.asList("v1", "v2"), VariantSchemaUtils.getInferableVariantColumnsFromConfig(avroOnly));

    HoodieConfig writeOnly = inferenceEnabledConfig();
    writeOnly.setValue("hoodie.write.schema", schemaString);
    assertEquals(Arrays.asList("v1", "v2"), VariantSchemaUtils.getInferableVariantColumnsFromConfig(writeOnly));

    // No schema in the config, or inference disabled: nothing, and no schema parse attempted.
    assertTrue(VariantSchemaUtils.getInferableVariantColumnsFromConfig(inferenceEnabledConfig()).isEmpty());
    HoodieConfig disabled = new HoodieConfig();
    disabled.setValue("hoodie.avro.schema", "not a schema");
    assertTrue(VariantSchemaUtils.getInferableVariantColumnsFromConfig(disabled).isEmpty());
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

    // Splicing the non-nullable v2 keeps it non-nullable: a bare shredded variant, no union.
    HoodieSchema v2Spliced = VariantSchemaUtils.applyInferredShredding(schema,
        Collections.singletonMap("v2", HoodieSchema.create(HoodieSchemaType.STRING))).getField("v2").get().schema();
    assertFalse(v2Spliced.isNullable());
    assertTrue(((HoodieSchema.Variant) v2Spliced).isShredded());

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
    HoodieSchema footerVariant = HoodieSchemaTestUtils.createPlainShreddedVariantRecord("v",
        HoodieSchema.createRecord("tv", null, null,
            Collections.singletonList(HoodieSchemaField.of("a", HoodieSchema.create(HoodieSchemaType.LONG)))));
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

    // Documented limitation: the footer fallback has no table schema to anchor on, so unlike the
    // read path's isShreddedVariantTarget / alignShreddedVariants (which keep a same-shaped user
    // struct intact, see TestHoodieSchemaCompatibility) this strip cannot tell a shredded variant
    // from a plain user struct of the same shape and strips both. Pinned so that adding an anchor
    // later is a deliberate change.
    HoodieSchema userStruct = HoodieSchemaTestUtils.createPlainShreddedVariantRecord("user_struct",
        HoodieSchema.create(HoodieSchemaType.LONG));
    HoodieSchema userStructStripped = VariantSchemaUtils.stripVariantShreddingByShape(HoodieSchema.createRecord(
        "rec", null, null, Collections.singletonList(HoodieSchemaField.of("s", userStruct)))).getField("s").get().schema();
    assertEquals(Arrays.asList("metadata", "value"),
        userStructStripped.getFields().stream().map(HoodieSchemaField::name).collect(Collectors.toList()));
  }

  @Test
  public void testStripVariantShreddingByShapeRestoresOmittedValue() {
    // The spec lets a writer omit `value` when every row is typed; the stripped column must still
    // come out in the unshredded {metadata, value} shape rather than as a one-field record.
    HoodieSchema twoFieldVariant = HoodieSchema.createRecord("v", null, null, Arrays.asList(
        HoodieSchemaField.of("metadata", HoodieSchema.create(HoodieSchemaType.BYTES)),
        HoodieSchemaField.of("typed_value", HoodieSchema.createNullable(HoodieSchemaType.LONG))));
    HoodieSchema schema = HoodieSchema.createRecord("rec", null, null,
        Collections.singletonList(HoodieSchemaField.of("v", twoFieldVariant)));

    HoodieSchema v = VariantSchemaUtils.stripVariantShreddingByShape(schema).getField("v").get().schema();
    assertEquals(Arrays.asList("metadata", "value"),
        v.getFields().stream().map(HoodieSchemaField::name).collect(Collectors.toList()));
    assertTrue(v.getField("value").get().schema().isNullable());
    assertEquals(HoodieSchemaType.BYTES, v.getField("value").get().schema().getNonNullType().getType());
    // The synthesized field must carry a null default like every other footer-derived optional field.
    assertEquals(HoodieSchema.NULL_VALUE, v.getField("value").get().defaultVal().get());
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

  @Test
  public void testApplyInferredShreddingToConfigKeepsSameNamedVariantColumnsApart() {
    // Variant columns commonly share one record type (the default name "variant"), which Avro
    // serializes as a name reference after the first occurrence. Splicing one column's record
    // under that shared name used to alias every other variant column to the shredded definition
    // once the schema was re-parsed from the config (and two inferred columns could not coexist).
    HoodieSchema schema = HoodieSchema.createRecord("rec", "ns", null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING)),
        HoodieSchemaField.of("a", HoodieSchema.createVariant()),
        HoodieSchemaField.of("b", HoodieSchema.createVariant())));
    HoodieConfig config = new HoodieConfig();
    config.setValue("hoodie.avro.schema", schema.getAvroSchema().toString());

    // One column inferred, the sibling declined: the sibling must come back unshredded.
    HoodieSchema oneSpliced = HoodieSchema.parse(VariantSchemaUtils.applyInferredShreddingToConfig(config,
        Collections.singletonMap("a", HoodieSchema.create(HoodieSchemaType.LONG))).getString("hoodie.avro.schema"));
    assertTrue(((HoodieSchema.Variant) oneSpliced.getField("a").get().schema()).isShredded());
    assertFalse(((HoodieSchema.Variant) oneSpliced.getField("b").get().schema()).isShredded());

    // Both inferred with different typed_values: each column keeps its own.
    Map<String, HoodieSchema> both = new LinkedHashMap<>();
    both.put("a", HoodieSchema.create(HoodieSchemaType.LONG));
    both.put("b", HoodieSchema.create(HoodieSchemaType.STRING));
    HoodieSchema bothSpliced = HoodieSchema.parse(
        VariantSchemaUtils.applyInferredShreddingToConfig(config, both).getString("hoodie.avro.schema"));
    HoodieSchema.Variant a = (HoodieSchema.Variant) bothSpliced.getField("a").get().schema();
    HoodieSchema.Variant b = (HoodieSchema.Variant) bothSpliced.getField("b").get().schema();
    assertEquals(HoodieSchemaType.LONG, a.getTypedValueField().get().getNonNullType().getType());
    assertEquals(HoodieSchemaType.STRING, b.getTypedValueField().get().getNonNullType().getType());
    assertNotEquals(a.getFullName(), b.getFullName());
  }

  @Test
  public void testApplyInferredShreddingToConfigSplicesEverySchemaKey() {
    // The row write support resolves hoodie.write.schema first and falls back to
    // hoodie.avro.schema, so whichever of the two is set (or both) must carry the splice.
    HoodieSchema schema = schemaWithVariants();
    Map<String, HoodieSchema> inferred = Collections.singletonMap("v2", HoodieSchema.create(HoodieSchemaType.STRING));

    HoodieConfig bothKeys = new HoodieConfig();
    bothKeys.setValue("hoodie.write.schema", schema.getAvroSchema().toString());
    bothKeys.setValue("hoodie.avro.schema", schema.getAvroSchema().toString());
    HoodieConfig bothSpliced = VariantSchemaUtils.applyInferredShreddingToConfig(bothKeys, inferred);
    for (String key : Arrays.asList("hoodie.write.schema", "hoodie.avro.schema")) {
      HoodieSchema.Variant v2 = (HoodieSchema.Variant) HoodieSchema.parse(bothSpliced.getString(key)).getField("v2").get().schema();
      assertTrue(v2.isShredded(), key + " should carry the splice");
    }

    HoodieConfig writeOnly = new HoodieConfig();
    writeOnly.setValue("hoodie.write.schema", schema.getAvroSchema().toString());
    HoodieConfig writeSpliced = VariantSchemaUtils.applyInferredShreddingToConfig(writeOnly, inferred);
    assertTrue(((HoodieSchema.Variant) HoodieSchema.parse(writeSpliced.getString("hoodie.write.schema"))
        .getField("v2").get().schema()).isShredded());
    // An absent key stays absent: the splice never invents a schema.
    assertNull(writeSpliced.getString("hoodie.avro.schema"));

    // Detection reads hoodie.write.schema first while the splice writes every key that is set:
    // when the two keys carry different schemas, each key is spliced on its own columns, so a key
    // without the inferred column is left exactly as it was.
    HoodieSchema noV2 = HoodieSchema.createRecord("rec", "ns", null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING)),
        HoodieSchemaField.of("v1", HoodieSchema.createNullable(HoodieSchema.createVariant()))));
    HoodieConfig differentKeys = new HoodieConfig();
    differentKeys.setValue("hoodie.write.schema", schema.getAvroSchema().toString());
    differentKeys.setValue("hoodie.avro.schema", noV2.getAvroSchema().toString());
    HoodieConfig differentSpliced = VariantSchemaUtils.applyInferredShreddingToConfig(differentKeys, inferred);
    assertTrue(((HoodieSchema.Variant) HoodieSchema.parse(differentSpliced.getString("hoodie.write.schema"))
        .getField("v2").get().schema()).isShredded());
    HoodieSchema avroSide = HoodieSchema.parse(differentSpliced.getString("hoodie.avro.schema"));
    assertFalse(avroSide.getField("v2").isPresent());
    assertFalse(((HoodieSchema.Variant) avroSide.getField("v1").get().schema().getNonNullType()).isShredded());
  }
}
