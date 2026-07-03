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

import org.apache.parquet.avro.AvroSchemaConverterWithTimestampNTZ;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
   * A shredded variant written via the Avro path must carry the parquet VARIANT logical type on the
   * variant group so external readers recognize it (mirroring the row-writer path and native Spark).
   * The annotation type only exists in parquet 1.16+ (Spark 4.1+); on older parquet it stays absent.
   */
  @Test
  void shreddedVariantParquetGroupCarriesVariantLogicalType() {
    HoodieSchema record = HoodieSchema.createRecord(
        "test_record", "org.apache.hudi.test", null,
        Collections.singletonList(HoodieSchemaField.of("v", HoodieSchema.createVariant())));

    Properties props = new Properties();
    props.setProperty(HoodieStorageConfig.PARQUET_VARIANT_WRITE_SHREDDING_ENABLED.key(), "true");
    props.setProperty(HoodieStorageConfig.PARQUET_VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST.key(),
        "a int, b string");

    HoodieSchema effective = HoodieAvroWriteSupport.generateEffectiveSchema(record, props);
    MessageType parquet = new AvroSchemaConverterWithTimestampNTZ().convert(effective);
    GroupType variantGroup = parquet.getType("v").asGroupType();
    assertTrue(variantGroup.containsField("typed_value"),
        "expected a shredded typed_value group: " + variantGroup);

    LogicalTypeAnnotation annotation = variantGroup.getLogicalTypeAnnotation();
    if (variantLogicalTypeSupported()) {
      assertNotNull(annotation,
          "shredded variant group should carry the VARIANT logical type: " + variantGroup);
      assertTrue(annotation.toString().contains("VARIANT"),
          "expected the VARIANT logical type, got: " + annotation);
    } else {
      assertNull(annotation, "parquet < 1.16 has no VARIANT logical type: " + variantGroup);
    }
  }

  private static boolean variantLogicalTypeSupported() {
    try {
      LogicalTypeAnnotation.class.getMethod("variantType", byte.class);
      return true;
    } catch (NoSuchMethodException e) {
      return false;
    }
  }
}
