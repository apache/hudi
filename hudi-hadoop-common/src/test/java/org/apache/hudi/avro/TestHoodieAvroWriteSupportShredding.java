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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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
    HoodieSchema effective = HoodieAvroWriteSupport.generateEffectiveSchema(
        singleVariantRecord(), forcedShreddingProps("a int, b string, c decimal(15, 1)"));

    assertShredded(effective.getFields().get(0).schema(), "forced top-level variant",
        Arrays.asList("a", "b", "c"));
  }

  /**
   * The forced-shredding DDL has to reach every variant that is a record member, at any depth and
   * including records carried by an array or a map, so that the AVRO write path lays out the same
   * file the row writer does for the same table and config. A variant that is directly an array
   * element or a map value is left unshredded on purpose on both paths: the DDL says what
   * typed_value holds, not where, and forcing a collection leaf is out of scope.
   */
  @Test
  void forcedShreddingReachesVariantRecordMembersAtEveryDepth() {
    HoodieSchema effective = HoodieAvroWriteSupport.generateEffectiveSchema(
        nestedVariantRecord(), forcedShreddingProps("a int, b string"));

    List<String> ddlFields = Arrays.asList("a", "b");
    assertShredded(effective.getField("s").get().schema().getField("inner").get().schema(),
        "variant under a struct", ddlFields);
    assertShredded(effective.getField("items").get().schema().getElementType()
        .getField("v").get().schema(), "variant under a struct in an array", ddlFields);
    assertShredded(effective.getField("m").get().schema().getValueType()
        .getField("v").get().schema(), "variant under a struct in a map", ddlFields);
    assertUnshredded(effective.getField("arr").get().schema().getElementType(), "bare array element");
    assertUnshredded(effective.getField("mv").get().schema().getValueType(), "bare map value");

    // The MessageType is derived from the effective schema by the generic parquet-avro converter,
    // so the spliced typed_value has to survive the list/map wrappers it generates as well.
    MessageType parquet = new AvroSchemaConverterWithTimestampNTZ().convert(effective);
    assertTrue(parquet.getType("s").asGroupType().getType("inner").asGroupType().containsField("typed_value"),
        "expected a shredded group at s.inner: " + parquet);
    assertTrue(listElementOf(parquet.getType("items").asGroupType()).getType("v").asGroupType()
        .containsField("typed_value"), "expected a shredded group at the items element: " + parquet);
    assertTrue(mapValueOf(parquet.getType("m").asGroupType()).getType("v").asGroupType()
        .containsField("typed_value"), "expected a shredded group at the m value: " + parquet);
    assertFalse(listElementOf(parquet.getType("arr").asGroupType()).containsField("typed_value"),
        "a bare array element must stay unshredded: " + parquet);
    assertFalse(mapValueOf(parquet.getType("mv").asGroupType()).containsField("typed_value"),
        "a bare map value must stay unshredded: " + parquet);
  }

  /**
   * Disabling shredding over an already-shredded schema - the clustering/compaction case
   * {@link HoodieAvroWriteSupport#generateEffectiveSchema} calls out - has to strip typed_value
   * without tripping Avro's "Field already used". Rebuilding a record while reusing a
   * {@code Schema.Field} still bound to the source record throws, so any table with a shredded
   * variant AND at least one other column failed here. #18938 fixed exactly that defect in the
   * sibling HoodieVariantReconstruction and left this twin behind. The strip is the exact inverse
   * of the forced splice and has to reach as far: both writers shred variant record members at any
   * depth off the forced-shredding DDL, and a hand-authored write schema can put typed_value on a
   * bare array element or map value too, which no DDL forces. A clustering/compaction schema read
   * back from such a file carries typed_value below the top level, so every arm is pinned here.
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
                    "nv", "org.apache.hudi.test", null, HoodieSchema.create(HoodieSchemaType.INT)))))),
            HoodieSchemaField.of("items", HoodieSchema.createArray(HoodieSchema.createVariantShredded(
                "iv", "org.apache.hudi.test", null, HoodieSchema.create(HoodieSchemaType.INT)))),
            HoodieSchemaField.of("tags", HoodieSchema.createMap(HoodieSchema.createVariantShredded(
                "tv", "org.apache.hudi.test", null, HoodieSchema.create(HoodieSchemaType.INT))))));

    Properties props = new Properties();
    props.setProperty(HoodieStorageConfig.PARQUET_VARIANT_WRITE_SHREDDING_ENABLED.key(), "false");

    HoodieSchema effective = assertDoesNotThrow(
        () -> HoodieAvroWriteSupport.generateEffectiveSchema(record, props),
        "stripping shredding must rebuild the record with fresh Avro fields");

    assertEquals("id", effective.getFields().get(0).name(), "non-variant fields must survive the rebuild");
    assertUnshredded(effective.getField("v").get().schema(), "top-level variant");
    assertUnshredded(effective.getField("nested").get().schema().getField("nv").get().schema(), "nested variant");
    assertUnshredded(effective.getField("items").get().schema().getElementType(), "array element variant");
    assertUnshredded(effective.getField("tags").get().schema().getValueType(), "map value variant");
  }

  private static void assertUnshredded(HoodieSchema fieldSchema, String label) {
    HoodieSchema unwrapped = fieldSchema.isNullable() ? fieldSchema.getNonNullType() : fieldSchema;
    assertInstanceOf(HoodieSchema.Variant.class, unwrapped, label + " should still be a variant");
    assertFalse(((HoodieSchema.Variant) unwrapped).isShredded(), label + " should no longer be shredded");
  }

  private static void assertShredded(HoodieSchema fieldSchema, String label,
                                     List<String> expectedTypedValueFields) {
    HoodieSchema unwrapped = fieldSchema.isNullable() ? fieldSchema.getNonNullType() : fieldSchema;
    assertInstanceOf(HoodieSchema.Variant.class, unwrapped, label + " should be a variant");
    HoodieSchema typedValueField = unwrapped.getFields().stream()
        .filter(f -> HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD.equals(f.name()))
        .map(HoodieSchemaField::schema)
        .findFirst()
        .orElseThrow(() -> new AssertionError(label + " is missing typed_value: " + unwrapped));
    HoodieSchema typedValue = typedValueField.isNullable() ? typedValueField.getNonNullType() : typedValueField;

    List<String> shreddedFieldNames = typedValue.getFields().stream()
        .map(HoodieSchemaField::name)
        .collect(Collectors.toList());
    assertEquals(expectedTypedValueFields, shreddedFieldNames, label + " typed_value members");
  }

  /**
   * The element type of a parquet LIST group, under either layout: the converter emits the 2-level
   * form ({@code repeated group array}) by default, and the 3-level {@code list/element} form when
   * {@code parquet.avro.write-old-list-structure} is off.
   */
  private static GroupType listElementOf(GroupType listGroup) {
    GroupType repeated = listGroup.getType(0).asGroupType();
    return repeated.getFieldCount() == 1 && "list".equals(repeated.getName())
        && "element".equals(repeated.getType(0).getName())
        ? repeated.getType(0).asGroupType() : repeated;
  }

  /** The value type of a parquet MAP group ({@code key_value/value}). */
  private static GroupType mapValueOf(GroupType mapGroup) {
    return mapGroup.getType(0).asGroupType().getType("value").asGroupType();
  }

  /**
   * A shredded variant written via the Avro path must carry the parquet VARIANT logical type on the
   * variant group so external readers recognize it (mirroring the row-writer path and native Spark).
   * The annotation type only exists in parquet 1.16+ (Spark 4.1+); on older parquet it stays absent.
   */
  @Test
  void shreddedVariantParquetGroupCarriesVariantLogicalType() {
    HoodieSchema effective = HoodieAvroWriteSupport.generateEffectiveSchema(
        singleVariantRecord(), forcedShreddingProps("a int, b string"));
    MessageType parquet = new AvroSchemaConverterWithTimestampNTZ().convert(effective);
    GroupType variantGroup = parquet.getType("v").asGroupType();
    // The converter tags unshredded groups too, so pin that the group under test is the shredded
    // one; on parquet < 1.16 (every CI lane today) this is the assertion that keeps the test honest.
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

  /** A record with one unshredded variant column {@code v}. */
  private static HoodieSchema singleVariantRecord() {
    return HoodieSchema.createRecord(
        "test_record", "org.apache.hudi.test", null,
        Collections.singletonList(HoodieSchemaField.of("v", HoodieSchema.createVariant())));
  }

  /**
   * A record carrying a variant at every position the write path can meet one: a struct member,
   * a struct member reached through an array and through a map, and the two positions the forced
   * DDL must leave alone - a bare array element and a bare map value.
   */
  private static HoodieSchema nestedVariantRecord() {
    HoodieSchema struct = HoodieSchema.createRecord("s_record", "org.apache.hudi.test", null,
        Collections.singletonList(HoodieSchemaField.of("inner", HoodieSchema.createVariant())));
    HoodieSchema itemStruct = HoodieSchema.createRecord("item_record", "org.apache.hudi.test", null,
        Collections.singletonList(HoodieSchemaField.of("v", HoodieSchema.createVariant())));
    HoodieSchema mapValueStruct = HoodieSchema.createRecord("map_value_record", "org.apache.hudi.test", null,
        Collections.singletonList(HoodieSchemaField.of("v", HoodieSchema.createVariant())));
    return HoodieSchema.createRecord("test_nested_record", "org.apache.hudi.test", null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
        HoodieSchemaField.of("s", struct),
        HoodieSchemaField.of("items", HoodieSchema.createArray(itemStruct)),
        HoodieSchemaField.of("m", HoodieSchema.createMap(mapValueStruct)),
        HoodieSchemaField.of("arr", HoodieSchema.createArray(HoodieSchema.createVariant())),
        HoodieSchemaField.of("mv", HoodieSchema.createMap(HoodieSchema.createVariant()))));
  }

  /** Write-support properties that shred every variant column with the given forced test DDL. */
  private static Properties forcedShreddingProps(String forcedShreddingDdl) {
    Properties props = new Properties();
    props.setProperty(HoodieStorageConfig.PARQUET_VARIANT_WRITE_SHREDDING_ENABLED.key(), "true");
    props.setProperty(HoodieStorageConfig.PARQUET_VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST.key(), forcedShreddingDdl);
    return props;
  }

  private static boolean variantLogicalTypeSupported() {
    // Probes parquet directly, not the converter's own probe: the expectation must be an independent oracle.
    try {
      LogicalTypeAnnotation.class.getMethod("variantType", byte.class);
      return true;
    } catch (NoSuchMethodException e) {
      return false;
    }
  }
}
