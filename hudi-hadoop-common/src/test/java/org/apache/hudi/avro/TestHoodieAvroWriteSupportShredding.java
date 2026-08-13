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

import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class TestHoodieAvroWriteSupportShredding {

  /**
   * The forced-shredding DDL must tolerate the commas inside parameterized types such as
   * {@code decimal(15, 1)}, which is the documented example on
   * {@link HoodieStorageConfig#PARQUET_VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST}. A naive
   * {@code ddl.split(",")} tore that field apart and threw "Unsupported shredding type: decimal(15".
   */
  @Test
  void forcedShreddingDdlTreatsDecimalParensAsOneField() {
    HoodieSchema record = HoodieSchema.createRecord(
        "test_record", "org.apache.hudi.test", null,
        Collections.singletonList(HoodieSchemaField.of("v", HoodieSchema.createVariant())));

    Properties props = new Properties();
    props.setProperty(HoodieStorageConfig.PARQUET_VARIANT_WRITE_SHREDDING_ENABLED.key(), "true");
    props.setProperty(HoodieStorageConfig.PARQUET_VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST.key(),
        "a int, b string, c decimal(15, 1)");

    HoodieSchema effective = HoodieAvroWriteSupport.generateEffectiveSchema(record, props);

    HoodieSchema variantField = effective.getFields().get(0).schema();
    HoodieSchema variant = variantField.isNullable() ? variantField.getNonNullType() : variantField;
    HoodieSchema typedValueField = variant.getFields().stream()
        .filter(f -> HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD.equals(f.name()))
        .map(HoodieSchemaField::schema)
        .findFirst()
        .orElseThrow(() -> new AssertionError("shredded variant is missing typed_value: " + variant));
    HoodieSchema typedValue = typedValueField.isNullable() ? typedValueField.getNonNullType() : typedValueField;

    List<String> shreddedFieldNames = typedValue.getFields().stream()
        .map(HoodieSchemaField::name)
        .collect(Collectors.toList());
    assertEquals(Arrays.asList("a", "b", "c"), shreddedFieldNames);
  }

  /**
   * Disabling shredding over an already-shredded schema - the clustering/compaction case
   * {@link HoodieAvroWriteSupport#generateEffectiveSchema} calls out - has to strip typed_value
   * without tripping Avro's "Field already used". Rebuilding a record while reusing a
   * {@code Schema.Field} still bound to the source record throws, so any table with a shredded
   * variant AND at least one other column failed here. #18938 fixed exactly that defect in the
   * sibling HoodieVariantReconstruction and left this twin behind. Nested variants must be
   * stripped too, since the row writer shreds at any depth.
   */
  @Test
  void disablingShreddingStripsTypedValueAtEveryDepth() {
    HoodieSchema record = HoodieSchema.createRecord(
        "test_record", "org.apache.hudi.test", null, Arrays.asList(
            HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("v", HoodieSchema.createVariantShredded(
                "v", "org.apache.hudi.test", null, HoodieSchema.create(HoodieSchemaType.INT))),
            HoodieSchemaField.of("nested", HoodieSchema.createRecord(
                "nested_record", "org.apache.hudi.test", null,
                Collections.singletonList(HoodieSchemaField.of("nv", HoodieSchema.createVariantShredded(
                    "nv", "org.apache.hudi.test", null, HoodieSchema.create(HoodieSchemaType.INT))))))));

    Properties props = new Properties();
    props.setProperty(HoodieStorageConfig.PARQUET_VARIANT_WRITE_SHREDDING_ENABLED.key(), "false");

    HoodieSchema effective = assertDoesNotThrow(
        () -> HoodieAvroWriteSupport.generateEffectiveSchema(record, props),
        "stripping shredding must rebuild the record with fresh Avro fields");

    assertEquals("id", effective.getFields().get(0).name(), "non-variant fields must survive the rebuild");
    assertUnshredded(effective.getField("v").get().schema(), "top-level variant");
    assertUnshredded(effective.getField("nested").get().schema().getField("nv").get().schema(), "nested variant");
  }

  private static void assertUnshredded(HoodieSchema fieldSchema, String label) {
    HoodieSchema unwrapped = fieldSchema.isNullable() ? fieldSchema.getNonNullType() : fieldSchema;
    assertInstanceOf(HoodieSchema.Variant.class, unwrapped, label + " should still be a variant");
    assertFalse(((HoodieSchema.Variant) unwrapped).isShredded(), label + " should no longer be shredded");
  }
}
