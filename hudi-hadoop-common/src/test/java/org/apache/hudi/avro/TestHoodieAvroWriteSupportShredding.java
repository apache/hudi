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
import org.apache.hudi.common.config.HoodieParquetConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.model.MetaFieldsMode;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.io.storage.hadoop.HoodieAvroParquetWriter;
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
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
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
import java.util.List;
import java.util.Map;
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

  /** The variant value bytes {@link StubShreddingProvider} shreds; anything else stays a residual. */
  private static final byte[] TYPED_MARKER = "typed".getBytes(StandardCharsets.UTF_8);
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
   * cases the value walk itself can get wrong: the residual fallback, a null nullable struct, a null
   * variant carried by an array element, a nested record whose fields arrive in a different order,
   * and one that does not carry a field at all.</p>
   */
  @Test
  void writesNestedVariantsShreddedAtEveryPositionTheEffectiveSchemaDeclares() throws Exception {
    HoodieSchema table = roundTripRecord();
    Properties props = forcedShreddingProps("k string");
    // The write support loads the provider reflectively and this module ships none, so point it at
    // the stub below instead of the Spark implementation.
    props.setProperty(HoodieStorageConfig.PARQUET_VARIANT_SHREDDING_PROVIDER_CLASS.key(),
        StubShreddingProvider.class.getName());

    AvroSchemaConverterWithTimestampNTZ converter = new AvroSchemaConverterWithTimestampNTZ();
    MessageType messageType = converter.convert(HoodieAvroWriteSupport.generateEffectiveSchema(table, props));
    // The factory converts its own generateEffectiveSchema result into the MessageType while the
    // write support recomputes the effective schema for the records it builds. A splice that minted
    // different record names on the second call would write records against a file layout they do
    // not match, so the two computations have to agree exactly.
    assertEquals(messageType.toString(),
        converter.convert(HoodieAvroWriteSupport.generateEffectiveSchema(table, props)).toString(),
        "generateEffectiveSchema must be deterministic for the same schema and properties");

    Schema tableAvro = table.toAvroSchema();
    Schema structAvro = nonNull(tableAvro.getField("s").schema());
    Schema itemAvro = tableAvro.getField("items").schema().getElementType();
    Schema mapValueAvro = tableAvro.getField("m").schema().getValueType();
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
        // The residual fallback at depth, and a null variant under an array element.
        recordOf(tableAvro,
            "id", 1,
            "s", recordOf(structAvro, "inner", variant(variantAvro, RESIDUAL_VALUE), "n", 1),
            "items", Collections.singletonList(recordOf(itemAvro, "v", null, "label", "no variant")),
            "m", Collections.emptyMap(),
            "arr", Collections.emptyList(),
            "v", variant(variantAvro, TYPED_MARKER)),
        // A null nullable struct: there is no record below it to walk.
        recordOf(tableAvro,
            "id", 2,
            "s", null,
            "items", Collections.emptyList(),
            "m", Collections.emptyMap(),
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
            "v", variant(variantAvro, TYPED_MARKER)));

    List<GenericRecord> readBack = writeAndReadBack(table, props, messageType, rows);
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
    assertTypedValue((GenericRecord) mapValue((Map<?, ?>) shredded.get("m"), "a").get("v"), "m[a].v");
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
    assertNull(((GenericRecord) ((List<?>) residual.get("items")).get(0)).get("v"),
        "a null variant under an array element must pass through as null");

    assertNull(readBack.get(2).get("s"), "a null nullable struct must pass through as null");

    GenericRecord reordered = (GenericRecord) readBack.get(3).get("s");
    assertTypedValue((GenericRecord) reordered.get("inner"), "s.inner from a reordered input record");
    assertEquals(Integer.valueOf(3), reordered.get("n"), "input fields must be matched by name, not position");

    GenericRecord withoutN = (GenericRecord) readBack.get(4).get("s");
    assertTypedValue((GenericRecord) withoutN.get("inner"), "s.inner from an input record without n");
    assertNull(withoutN.get("n"), "a field the input does not carry must be left null");
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
   * Only the top-level and nested-record arms are the fix: the array-element and map-value arms pin
   * pre-existing behaviour of {@code VariantSchemaUtils#stripVariantShreddingAt} and pass on master
   * too, so they are regression cover, not evidence for this change.
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
   * a struct in an array and in a map, one directly under an array - the position the DDL leaves
   * unshredded - and one at the top level as the control.
   */
  private static HoodieSchema roundTripRecord() {
    HoodieSchema struct = HoodieSchema.createRecord("rt_s_record", "org.apache.hudi.test", null, Arrays.asList(
        HoodieSchemaField.of("inner", HoodieSchema.createVariant()),
        // Nullable so that an input record without it is legal rather than a broken write.
        HoodieSchemaField.of("n", HoodieSchema.createNullable(HoodieSchemaType.INT))));
    // The sibling is not decoration: parquet-avro reads the 2-level list layout by guessing whether
    // the repeated group is the element or a synthetic wrapper, and a single-field element record
    // makes that guess ambiguous. A second field settles it, so the raw read below stays readable.
    HoodieSchema itemStruct = HoodieSchema.createRecord("rt_item_record", "org.apache.hudi.test", null, Arrays.asList(
        HoodieSchemaField.of("v", HoodieSchema.createNullable(HoodieSchema.createVariant())),
        HoodieSchemaField.of("label", HoodieSchema.createNullable(HoodieSchemaType.STRING))));
    HoodieSchema mapValueStruct = HoodieSchema.createRecord("rt_map_value_record", "org.apache.hudi.test", null,
        Collections.singletonList(HoodieSchemaField.of("v", HoodieSchema.createVariant())));
    return HoodieSchema.createRecord("test_round_trip_record", "org.apache.hudi.test", null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
        HoodieSchemaField.of("s", HoodieSchema.createNullable(struct)),
        HoodieSchemaField.of("items", HoodieSchema.createArray(itemStruct)),
        HoodieSchemaField.of("m", HoodieSchema.createMap(mapValueStruct)),
        HoodieSchemaField.of("arr", HoodieSchema.createArray(HoodieSchema.createVariant())),
        HoodieSchemaField.of("v", HoodieSchema.createVariant())));
  }

  /**
   * Writes the rows through a real {@link HoodieAvroWriteSupport} and reads the file back raw. The
   * read is deliberately not HoodieAvroParquetReader: that one rebuilds the variants through the
   * provider, which would hide the typed_value columns under test. parquet-avro stores the write
   * schema in the footer, so the records come back at the effective (shredded) schema.
   */
  private List<GenericRecord> writeAndReadBack(HoodieSchema table, Properties props, MessageType messageType,
                                               List<GenericRecord> rows) throws Exception {
    HoodieStorage storage = HoodieTestUtils.getStorage(tmpDir.toString());
    StoragePath path = new StoragePath(tmpDir.resolve("shredded.parquet").toAbsolutePath().toString());
    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(messageType, table, Option.empty(), props);
    HoodieParquetConfig<HoodieAvroWriteSupport> parquetConfig = new HoodieParquetConfig<>(
        writeSupport, CompressionCodecName.UNCOMPRESSED, ParquetWriter.DEFAULT_BLOCK_SIZE,
        ParquetWriter.DEFAULT_PAGE_SIZE, 1024 * 1024 * 1024L, storage.getConf(), 0.1, true);
    try (HoodieAvroParquetWriter writer = new HoodieAvroParquetWriter(
        path, parquetConfig, "000", new LocalTaskContextSupplier(), MetaFieldsMode.NONE)) {
      for (GenericRecord row : rows) {
        writer.writeAvro(String.valueOf(row.get("id")), row);
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

  /** parquet-avro hands map keys back as Utf8, which never equals a String key. */
  private static GenericRecord mapValue(Map<?, ?> map, String key) {
    return (GenericRecord) map.entrySet().stream()
        .filter(entry -> key.equals(String.valueOf(entry.getKey())))
        .map(Map.Entry::getValue)
        .findFirst()
        .orElseThrow(() -> new AssertionError("no entry " + key + " in " + map));
  }

  /** The non-null branch of a nullable Avro union, or the schema itself. */
  private static Schema nonNull(Schema schema) {
    if (schema.getType() != Schema.Type.UNION) {
      return schema;
    }
    return schema.getTypes().stream()
        .filter(type -> type.getType() != Schema.Type.NULL)
        .findFirst()
        .orElseThrow(() -> new AssertionError("union with no non-null branch: " + schema));
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
   * driven without Spark on the classpath. The rule is deterministic and exercises both outcomes: a
   * variant whose value bytes are {@link #TYPED_MARKER} shreds - every DDL field struct gets its
   * typed_value leaf and the top-level residual goes null - and anything else keeps its value in the
   * residual with typed_value left null. Must be public with a no-arg constructor for
   * {@code ReflectionUtils.loadClass}.
   */
  public static class StubShreddingProvider implements VariantShreddingProvider {

    @Override
    public GenericRecord shredVariantRecord(GenericRecord unshreddedVariant, Schema shreddedSchema,
                                            HoodieSchema.Variant variantSchema) {
      GenericRecord shredded = new GenericData.Record(shreddedSchema);
      shredded.put(HoodieSchema.Variant.VARIANT_METADATA_FIELD,
          unshreddedVariant.get(HoodieSchema.Variant.VARIANT_METADATA_FIELD));
      ByteBuffer value = (ByteBuffer) unshreddedVariant.get(HoodieSchema.Variant.VARIANT_VALUE_FIELD);
      if (!isTypedMarker(value)) {
        // The residual fallback: typed_value stays null and the binary is carried as-is.
        shredded.put(HoodieSchema.Variant.VARIANT_VALUE_FIELD, value);
        return shredded;
      }
      Schema typedValueSchema = nonNull(shreddedSchema.getField(
          HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD).schema());
      GenericRecord typedValue = new GenericData.Record(typedValueSchema);
      for (Schema.Field field : typedValueSchema.getFields()) {
        // Each DDL field is a {value, typed_value} struct; only the typed leaf is populated.
        Schema fieldStruct = nonNull(field.schema());
        GenericRecord leaf = new GenericData.Record(fieldStruct);
        leaf.put(HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD, leafValue(
            nonNull(fieldStruct.getField(HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD).schema())));
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

    private static Object leafValue(Schema leafSchema) {
      switch (leafSchema.getType()) {
        case STRING:
          return SHREDDED_LEAF;
        case INT:
          return 1;
        default:
          throw new UnsupportedOperationException("no stub value for " + leafSchema);
      }
    }

    private static boolean isTypedMarker(ByteBuffer value) {
      if (value == null) {
        return false;
      }
      ByteBuffer copy = value.duplicate();
      byte[] bytes = new byte[copy.remaining()];
      copy.get(bytes);
      return Arrays.equals(bytes, TYPED_MARKER);
    }
  }
}
