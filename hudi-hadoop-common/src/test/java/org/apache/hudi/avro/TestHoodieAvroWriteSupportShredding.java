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

import org.apache.hudi.common.avro.VariantShreddingProvider;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.MetaFieldsMode;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.core.io.storage.HoodieAvroFileWriter;
import org.apache.hudi.core.io.storage.HoodieFileWriter;
import org.apache.hudi.core.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroSchemaConverterWithTimestampNTZ;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.common.avro.AvroSchemaUtils.getNonNullTypeFromUnion;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieAvroWriteSupportShredding {

  /** The variant value bytes {@link StubShreddingProvider} shreds; anything else stays a residual. */
  private static final byte[] TYPED_MARKER = "typed".getBytes(StandardCharsets.UTF_8);
  /** The variant value bytes {@link StubShreddingProvider} declines outright, returning null. */
  private static final byte[] DECLINE_MARKER = "decline".getBytes(StandardCharsets.UTF_8);
  private static final byte[] RESIDUAL_VALUE = "residual".getBytes(StandardCharsets.UTF_8);
  private static final byte[] VARIANT_METADATA = new byte[] {1, 0, 0};
  private static final String SHREDDED_LEAF = "shredded";

  @TempDir
  java.nio.file.Path tmpDir;

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

    // Which of the six positions the DDL reaches is the position matrix in
    // TestVariantSchemaUtils#testApplyForcedShreddingReachesRecordMembersAtAnyDepth, and restating it
    // here would only pin the same splice twice. What this test owns is the leg past it: the DDL
    // string reaches the splice through generateEffectiveSchema, and the spliced typed_value
    // survives the list and map wrappers the generic parquet-avro converter generates.
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
   * The value-level half of the same contract. Splicing typed_value into the schema only pays off if
   * the writer also rewrites the records it is handed, at every position the effective schema
   * declares shredded: {@link org.apache.parquet.avro.AvroWriteSupport} reads fields positionally, so
   * a variant left in its {@code {metadata, value}} shape under a schema that declares typed_value
   * fails the write outright. This drives a real parquet write with a stub provider - no Spark on
   * this module's classpath - and reads the file back raw so typed_value is visible exactly as it
   * was stored.
   *
   * <p>The positions come from the schema, which the test above pins; what the rows add are the
   * cases the value walk itself can get wrong: the residual fallback, a provider that declines a
   * variant and hands back null, a null nullable struct, a null array column and a null map column,
   * a null map value, a null variant carried by an array element, a nested record whose fields
   * arrive in a different order, and one that does not carry a field at all.</p>
   */
  @Test
  void writesNestedVariantsShreddedAtEveryPositionTheEffectiveSchemaDeclares() throws Exception {
    HoodieSchema table = roundTripRecord();
    Properties props = forcedShreddingProps("k string");
    // The write support loads the provider reflectively and this module ships none, so point it at
    // the stub below instead of the Spark implementation.
    props.setProperty(HoodieStorageConfig.PARQUET_VARIANT_SHREDDING_PROVIDER_CLASS.key(),
        StubShreddingProvider.class.getName());

    // The writer factory converts its own generateEffectiveSchema result into the file's MessageType
    // while the write support constructor recomputes the effective schema for the records it builds.
    // A cheap guard that the two calls agree, down to the generated record names - which the Avro
    // schema string carries and a MessageType would drop.
    assertEquals(
        HoodieAvroWriteSupport.generateEffectiveSchema(table, props).toAvroSchema().toString(),
        HoodieAvroWriteSupport.generateEffectiveSchema(table, props).toAvroSchema().toString(),
        "generateEffectiveSchema must be deterministic for the same schema and properties");

    Schema tableAvro = table.toAvroSchema();
    Schema structAvro = getNonNullTypeFromUnion(tableAvro.getField("s").schema());
    Schema itemAvro = getNonNullTypeFromUnion(tableAvro.getField("items").schema()).getElementType();
    Schema mapValueAvro = getNonNullTypeFromUnion(
        getNonNullTypeFromUnion(tableAvro.getField("m").schema()).getValueType());
    Schema variantAvro = tableAvro.getField("v").schema();
    // Input records reach the writer untransformed and carry their own schema, so the same field
    // names in another order - and a record missing one entirely - have to resolve by name.
    Schema reorderedAvro = HoodieSchema.createRecord("s_reordered", "org.apache.hudi.test", null,
        Arrays.asList(
            HoodieSchemaField.of("n", HoodieSchema.createNullable(HoodieSchemaType.INT)),
            HoodieSchemaField.of("inner", HoodieSchema.createVariant()))).toAvroSchema();
    Schema withoutNAvro = HoodieSchema.createRecord("s_without_n", "org.apache.hudi.test", null,
        Collections.singletonList(HoodieSchemaField.of("inner", HoodieSchema.createVariant()))).toAvroSchema();

    List<GenericRecord> rows = Arrays.asList(
        // Every declared position shredded, plus the bare array element that is not declared.
        recordOf(tableAvro,
            "id", 0,
            "s", recordOf(structAvro, "inner", variant(variantAvro, TYPED_MARKER), "n", 7),
            "items", Arrays.asList(
                recordOf(itemAvro, "v", variant(variantAvro, TYPED_MARKER), "label", "first"),
                recordOf(itemAvro, "v", variant(variantAvro, TYPED_MARKER), "label", "second")),
            "m", Collections.singletonMap("a", recordOf(mapValueAvro, "v", variant(variantAvro, TYPED_MARKER))),
            "arr", Collections.singletonList(variant(variantAvro, TYPED_MARKER)),
            "v", variant(variantAvro, TYPED_MARKER)),
        // The residual fallback at depth, a null variant under an array element, and a variant the
        // provider declines at a nullable position.
        recordOf(tableAvro,
            "id", 1,
            "s", recordOf(structAvro, "inner", variant(variantAvro, RESIDUAL_VALUE), "n", 1),
            "items", Arrays.asList(
                recordOf(itemAvro, "v", null, "label", "no variant"),
                recordOf(itemAvro, "v", variant(variantAvro, DECLINE_MARKER), "label", "declined")),
            "m", Collections.emptyMap(),
            "arr", Collections.emptyList(),
            "v", variant(variantAvro, TYPED_MARKER)),
        // A null nullable struct, and a null array and a null map beside it: there is no record,
        // element or map value below any of them to walk, so each has to pass through untouched.
        recordOf(tableAvro,
            "id", 2,
            "s", null,
            "items", null,
            "m", null,
            "arr", Collections.emptyList(),
            "v", variant(variantAvro, TYPED_MARKER)),
        // The same struct with its fields declared in the other order.
        recordOf(tableAvro,
            "id", 3,
            "s", recordOf(reorderedAvro, "n", 3, "inner", variant(variantAvro, TYPED_MARKER)),
            "items", Collections.emptyList(),
            "m", Collections.emptyMap(),
            "arr", Collections.emptyList(),
            "v", variant(variantAvro, TYPED_MARKER)),
        // An input struct that does not carry n at all.
        recordOf(tableAvro,
            "id", 4,
            "s", recordOf(withoutNAvro, "inner", variant(variantAvro, TYPED_MARKER)),
            "items", Collections.emptyList(),
            "m", Collections.emptyMap(),
            "arr", Collections.emptyList(),
            "v", variant(variantAvro, TYPED_MARKER)),
        // A null map value: like the null struct, there is no record below it to walk.
        recordOf(tableAvro,
            "id", 5,
            "s", null,
            "items", Collections.emptyList(),
            "m", Collections.singletonMap("a", null),
            "arr", Collections.emptyList(),
            "v", variant(variantAvro, TYPED_MARKER)));

    List<GenericRecord> readBack = writeAndReadBack(table, props, "shredded.parquet", rows);
    assertEquals(rows.size(), readBack.size(), "every row must round trip");

    GenericRecord shredded = readBack.get(0);
    GenericRecord struct = (GenericRecord) shredded.get("s");
    assertTypedValue((GenericRecord) struct.get("inner"), "s.inner");
    assertEquals(Integer.valueOf(7), struct.get("n"), "a non-variant sibling must survive the rewrite");
    List<?> items = (List<?>) shredded.get("items");
    assertTypedValue((GenericRecord) ((GenericRecord) items.get(0)).get("v"), "items[0].v");
    assertTypedValue((GenericRecord) ((GenericRecord) items.get(1)).get("v"), "items[1].v");
    assertEquals("second", String.valueOf(((GenericRecord) items.get(1)).get("label")),
        "a non-variant sibling inside an array element must survive the rewrite");
    assertTypedValue((GenericRecord) ((GenericRecord) mapValue((Map<?, ?>) shredded.get("m"), "a")).get("v"),
        "m[a].v");
    assertTypedValue((GenericRecord) shredded.get("v"), "top-level v");
    // The bare array element is the one position the DDL leaves alone: the file declares no
    // typed_value column there at all, and the value has to arrive untouched.
    GenericRecord bareElement = (GenericRecord) ((List<?>) shredded.get("arr")).get(0);
    assertNull(bareElement.getSchema().getField(HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD),
        "a bare array element must stay unshredded on disk: " + bareElement.getSchema());
    assertEquals(ByteBuffer.wrap(TYPED_MARKER), bareElement.get("value"), "bare array element value");

    GenericRecord residual = readBack.get(1);
    GenericRecord residualInner = (GenericRecord) ((GenericRecord) residual.get("s")).get("inner");
    assertNull(residualInner.get("typed_value"), "a value the provider does not match must stay a residual");
    assertEquals(ByteBuffer.wrap(RESIDUAL_VALUE), residualInner.get("value"), "residual value at depth");
    List<?> residualItems = (List<?>) residual.get("items");
    assertNull(((GenericRecord) residualItems.get(0)).get("v"),
        "a null variant under an array element must pass through as null");
    assertNull(((GenericRecord) residualItems.get(1)).get("v"),
        "a variant the provider declines must land as null at a nullable position");

    GenericRecord nullCollections = readBack.get(2);
    assertNull(nullCollections.get("s"), "a null nullable struct must pass through as null");
    assertNull(nullCollections.get("items"), "a null array column must pass through as null");
    assertNull(nullCollections.get("m"), "a null map column must pass through as null");

    GenericRecord reordered = (GenericRecord) readBack.get(3).get("s");
    assertTypedValue((GenericRecord) reordered.get("inner"), "s.inner from a reordered input record");
    assertEquals(Integer.valueOf(3), reordered.get("n"), "input fields must be matched by name, not position");

    GenericRecord withoutN = (GenericRecord) readBack.get(4).get("s");
    assertTypedValue((GenericRecord) withoutN.get("inner"), "s.inner from an input record without n");
    assertNull(withoutN.get("n"), "a field the input does not carry must be left null");

    assertNull(mapValue((Map<?, ?>) readBack.get(5).get("m"), "a"),
        "a null map value must pass through as null");

    // The same declined variant at a NON-nullable position is not a null the file can hold:
    // rt_map_value_record.v is a required group, so parquet-avro fails the write outright rather
    // than storing anything. Its own file, so the rows above stay unaffected.
    GenericRecord declinedAtRequiredPosition = recordOf(tableAvro,
        "id", 6,
        "s", null,
        "items", Collections.emptyList(),
        "m", Collections.singletonMap("a",
            recordOf(mapValueAvro, "v", variant(variantAvro, DECLINE_MARKER))),
        "arr", Collections.emptyList(),
        "v", variant(variantAvro, TYPED_MARKER));
    Exception failure = assertThrows(Exception.class,
        () -> writeAndReadBack(table, props, "declined.parquet",
            Collections.singletonList(declinedAtRequiredPosition)),
        "a declined variant at a required position must fail the write, not write a null");
    assertTrue(messageChain(failure).contains("Null-value for required field"),
        "expected parquet-avro's required-field failure, got: " + messageChain(failure));
  }

  /**
   * The provider gate is keyed off the whole schema tree, not the root fields. A table whose only
   * shredded variant sits below the top level needs a provider just as much as one shredded at the
   * root, and having none configured has to fail at writer construction rather than NPE on the
   * first record. Master only threw when a ROOT field was shredded, so a nested-only schema is
   * exactly the failure mode this change introduces.
   */
  @Test
  void nestedOnlyShreddedSchemaWithoutAProviderFailsAtConstruction() {
    // Hand-built rather than nestedVariantRecord() plus a forced DDL, which would shred the same
    // positions: a schema that already carries typed_value with no DDL to apply is the only input
    // that reaches generateEffectiveSchema's "enabled, no forced DDL, take the schema as-is" arm.
    Map<String, HoodieSchema> ddlFields = new LinkedHashMap<>();
    ddlFields.put("k", HoodieSchema.create(HoodieSchemaType.STRING));
    HoodieSchema table = HoodieSchema.createRecord(
        "nested_only_record", "org.apache.hudi.test", null,
        Collections.singletonList(HoodieSchemaField.of("s", HoodieSchema.createRecord(
            "nested_only_s_record", "org.apache.hudi.test", null,
            Collections.singletonList(HoodieSchemaField.of("inner",
                HoodieSchema.createVariantShreddedObject(
                    "inner_variant", "org.apache.hudi.test", null, ddlFields)))))));

    // Shredding on, no forced DDL (the schema is already shredded) and no provider named: this
    // module ships none, and the write support does not auto-detect - only the factory does.
    Properties props = new Properties();
    props.setProperty(HoodieStorageConfig.PARQUET_VARIANT_WRITE_SHREDDING_ENABLED.key(), "true");
    MessageType messageType = new AvroSchemaConverterWithTimestampNTZ()
        .convert(HoodieAvroWriteSupport.generateEffectiveSchema(table, props));

    HoodieException failure = assertThrows(HoodieException.class,
        () -> new HoodieAvroWriteSupport(messageType, table, Option.empty(), props),
        "a variant shredded only below the top level still needs a shredding provider");
    assertTrue(failure.getMessage().contains("no VariantShreddingProvider"),
        "expected the missing-provider message, got: " + failure.getMessage());
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
   * No arm here is evidence for this change. The top-level and nested-record arms are master's test
   * verbatim, and master already recursed into records; the array-element and map-value arms pin
   * pre-existing behaviour of {@code VariantSchemaUtils#stripVariantShreddingAt} and pass on master
   * too. The whole test is regression cover for the strip direction while the forced splice grows.
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
   * The element type of a parquet LIST group. One converter, one layout: every MessageType here
   * comes from the no-arg {@link AvroSchemaConverterWithTimestampNTZ}, which leaves
   * {@code parquet.avro.write-old-list-structure} at its default and so always emits the 2-level
   * form ({@code repeated group array}); a 3-level {@code list/element} arm would be dead code.
   */
  private static GroupType listElementOf(GroupType listGroup) {
    return listGroup.getType(0).asGroupType();
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

  /**
   * The round-trip table schema: a variant under a nullable struct (next to a plain sibling), under
   * a struct in an array and under a nullable struct in a map, one directly under an array - the
   * position the DDL leaves unshredded - and one at the top level as the control.
   */
  private static HoodieSchema roundTripRecord() {
    HoodieSchema struct = HoodieSchema.createRecord("rt_s_record", "org.apache.hudi.test", null, Arrays.asList(
        HoodieSchemaField.of("inner", HoodieSchema.createVariant()),
        // Nullable so that an input record without it is legal rather than a broken write.
        HoodieSchemaField.of("n", HoodieSchema.createNullable(HoodieSchemaType.INT))));
    // The sibling is not decoration, and not a shredding concern. The Avro write path emits 2-level
    // parquet lists (parquet-avro's default parquet.avro.write-old-list-structure=true; the row
    // writer emits 3-level), and parquet-avro before 1.14 decides whether a single-field repeated
    // group is the element or a synthetic wrapper with a check that is sensitive to the Avro record
    // NAME (AvroRecordConverter#isElementType), which never matches the name Hudi gives the element
    // record. So on the parquet-avro this module builds against (1.13.1) Hudi's own
    // HoodieAvroParquetReader cannot read array<struct<single field>> at all, shredded or not: a
    // known reader limitation on parquet-avro < 1.14, closed as won't-fix in apache/hudi#19782 with
    // the workarounds recorded there (write 3-level lists via
    // parquet.avro.write-old-list-structure=false, or give the element a second field).
    // parquet-avro 1.14+ recognizes the group by its "array" name instead (PARQUET-2450). Hence the
    // sibling here.
    HoodieSchema itemStruct = HoodieSchema.createRecord("rt_item_record", "org.apache.hudi.test", null, Arrays.asList(
        HoodieSchemaField.of("v", HoodieSchema.createNullable(HoodieSchema.createVariant())),
        HoodieSchemaField.of("label", HoodieSchema.createNullable(HoodieSchemaType.STRING))));
    // The struct is nullable below so a null map value is a legal row; the variant inside it is not,
    // which makes it the required position a declined variant has to fail the write at.
    HoodieSchema mapValueStruct = HoodieSchema.createRecord("rt_map_value_record", "org.apache.hudi.test", null,
        Collections.singletonList(HoodieSchemaField.of("v", HoodieSchema.createVariant())));
    // items and m are nullable columns, which is the ordinary shape a Spark table gives a collection
    // and the only input that reaches the array and map shredders' pass-through arms: a null there
    // is neither a Collection nor a Map, so it has to be handed on rather than walked.
    return HoodieSchema.createRecord("test_round_trip_record", "org.apache.hudi.test", null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
        HoodieSchemaField.of("s", HoodieSchema.createNullable(struct)),
        HoodieSchemaField.of("items", HoodieSchema.createNullable(HoodieSchema.createArray(itemStruct))),
        HoodieSchemaField.of("m", HoodieSchema.createNullable(
            HoodieSchema.createMap(HoodieSchema.createNullable(mapValueStruct)))),
        HoodieSchemaField.of("arr", HoodieSchema.createArray(HoodieSchema.createVariant())),
        HoodieSchemaField.of("v", HoodieSchema.createVariant())));
  }

  /**
   * Writes the rows through the public writer factory and reads the file back raw. Going through
   * {@link HoodieFileWriterFactory} rather than a hand-built write support is the point: it is the
   * production route, and it owns both halves the round trip depends on - it carries the properties
   * into {@link HoodieAvroWriteSupport#generateEffectiveSchema} and converts that same effective
   * schema into the file's MessageType. {@link MetaFieldsMode#NONE} keeps the writer from adding the
   * Hudi meta columns, so the file holds only the table's own.
   *
   * <p>The read is deliberately not HoodieAvroParquetReader: that one rebuilds the variants through
   * the provider, which would hide the typed_value columns under test. parquet-avro stores the write
   * schema in the footer, so the records come back at the effective (shredded) schema.</p>
   */
  private List<GenericRecord> writeAndReadBack(HoodieSchema table, Properties props, String fileName,
                                               List<GenericRecord> rows) throws Exception {
    HoodieStorage storage = HoodieTestUtils.getStorage(tmpDir.toString());
    StoragePath path = new StoragePath(tmpDir.resolve(fileName).toAbsolutePath().toString());
    HoodieConfig config = new HoodieConfig(TypedProperties.copy(props));
    config.setValue(HoodieTableConfig.META_FIELDS_MODE, MetaFieldsMode.NONE.name());
    // Bypassing HoodieWriteConfig means no engine sets the codec, and the factory no longer
    // defaults it (#19685), so this low-level path has to.
    config.setValue(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME, "zstd");
    try (HoodieFileWriter writer = HoodieFileWriterFactory.getFileWriter(
        "000", path, storage, config, table, new LocalTaskContextSupplier(),
        HoodieRecord.HoodieRecordType.AVRO)) {
      for (GenericRecord row : rows) {
        ((HoodieAvroFileWriter) writer).writeAvro(String.valueOf(row.get("id")), row);
      }
    }

    Configuration conf = new Configuration();
    List<GenericRecord> readBack = new ArrayList<>();
    try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
        HadoopInputFile.fromPath(new Path(path.toUri()), conf)).withConf(conf).build()) {
      for (GenericRecord next = reader.read(); next != null; next = reader.read()) {
        readBack.add(next);
      }
    }
    return readBack;
  }

  /** A record of {@code schema} built from {@code name, value} pairs. */
  private static GenericRecord recordOf(Schema schema, Object... nameValuePairs) {
    GenericRecord record = new GenericData.Record(schema);
    for (int i = 0; i < nameValuePairs.length; i += 2) {
      record.put((String) nameValuePairs[i], nameValuePairs[i + 1]);
    }
    return record;
  }

  /** An unshredded variant record, the shape every input record carries. */
  private static GenericRecord variant(Schema variantSchema, byte[] valueBytes) {
    return recordOf(variantSchema,
        HoodieSchema.Variant.VARIANT_METADATA_FIELD, ByteBuffer.wrap(VARIANT_METADATA),
        HoodieSchema.Variant.VARIANT_VALUE_FIELD, ByteBuffer.wrap(valueBytes));
  }

  private static void assertTypedValue(GenericRecord variantRecord, String label) {
    GenericRecord typedValue = (GenericRecord) variantRecord.get("typed_value");
    assertNotNull(typedValue, label + " should have been shredded into typed_value");
    assertNull(variantRecord.get("value"), label + " residual must be null once typed_value holds the value");
    GenericRecord ddlField = (GenericRecord) typedValue.get("k");
    assertNotNull(ddlField, label + " is missing the DDL field k: " + typedValue);
    assertEquals(SHREDDED_LEAF, String.valueOf(ddlField.get("typed_value")), label + " typed_value leaf");
  }

  /**
   * The value under {@code key}, which may be null - the entry is looked up first so that a present
   * key with a null value is distinguishable from a missing key. parquet-avro hands map keys back as
   * Utf8, which never equals a String key.
   */
  private static Object mapValue(Map<?, ?> map, String key) {
    return map.entrySet().stream()
        .filter(entry -> key.equals(String.valueOf(entry.getKey())))
        .findFirst()
        .orElseThrow(() -> new AssertionError("no entry " + key + " in " + map))
        .getValue();
  }

  /** The whole cause chain rendered, so an assertion does not depend on which layer wraps a failure. */
  private static String messageChain(Throwable throwable) {
    StringBuilder chain = new StringBuilder();
    for (Throwable current = throwable; current != null && current != current.getCause();
         current = current.getCause()) {
      chain.append(current).append('\n');
    }
    return chain.toString();
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

  /**
   * Stands in for the engine provider the write support loads reflectively, so the value walk can be
   * driven without Spark on the classpath. The rule is deterministic and exercises all three
   * outcomes a real provider has: a variant whose value bytes are {@link #TYPED_MARKER} shreds -
   * every DDL field struct gets its typed_value leaf and the top-level residual goes null;
   * {@link #DECLINE_MARKER} returns null, which is what {@code Spark4VariantShreddingProvider} does
   * for a variant whose value or metadata is null, and the caller writes that null straight into the
   * record; anything else keeps its value in the residual with typed_value left null. Must be public
   * with a no-arg constructor for {@code ReflectionUtils.loadClass}.
   */
  public static class StubShreddingProvider implements VariantShreddingProvider {

    @Override
    public GenericRecord shredVariantRecord(GenericRecord unshreddedVariant, Schema shreddedSchema,
                                            HoodieSchema.Variant variantSchema) {
      ByteBuffer value = (ByteBuffer) unshreddedVariant.get(HoodieSchema.Variant.VARIANT_VALUE_FIELD);
      if (isMarker(value, DECLINE_MARKER)) {
        // The real provider declines the same way (null value or metadata), and VariantShredder
        // passes the null straight into the record it is building.
        return null;
      }
      GenericRecord shredded = new GenericData.Record(shreddedSchema);
      shredded.put(HoodieSchema.Variant.VARIANT_METADATA_FIELD,
          unshreddedVariant.get(HoodieSchema.Variant.VARIANT_METADATA_FIELD));
      if (!isMarker(value, TYPED_MARKER)) {
        // The residual fallback: typed_value stays null and the binary is carried as-is.
        shredded.put(HoodieSchema.Variant.VARIANT_VALUE_FIELD, value);
        return shredded;
      }
      Schema typedValueSchema = getNonNullTypeFromUnion(shreddedSchema.getField(
          HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD).schema());
      GenericRecord typedValue = new GenericData.Record(typedValueSchema);
      for (Schema.Field field : typedValueSchema.getFields()) {
        // Each DDL field is a {value, typed_value} struct; only the typed leaf is populated.
        Schema fieldStruct = getNonNullTypeFromUnion(field.schema());
        // Every DDL this test forces is `k string`, so the leaf is always a string.
        Schema leafSchema = getNonNullTypeFromUnion(
            fieldStruct.getField(HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD).schema());
        if (leafSchema.getType() != Schema.Type.STRING) {
          throw new UnsupportedOperationException("no stub value for " + leafSchema);
        }
        GenericRecord leaf = new GenericData.Record(fieldStruct);
        leaf.put(HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD, SHREDDED_LEAF);
        typedValue.put(field.name(), leaf);
      }
      shredded.put(HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD, typedValue);
      return shredded;
    }

    @Override
    public GenericRecord rebuildVariantRecord(GenericRecord shreddedVariant, Schema shreddedSchema,
                                              Schema unshreddedSchema) {
      throw new UnsupportedOperationException(
          "the round trip reads raw parquet; the read half is TestHoodieVariantReconstruction's");
    }

    private static boolean isMarker(ByteBuffer value, byte[] marker) {
      if (value == null) {
        return false;
      }
      ByteBuffer copy = value.duplicate();
      byte[] bytes = new byte[copy.remaining()];
      copy.get(bytes);
      return Arrays.equals(bytes, marker);
    }
  }
}
