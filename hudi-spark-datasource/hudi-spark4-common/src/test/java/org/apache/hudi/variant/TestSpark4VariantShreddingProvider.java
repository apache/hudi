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

import org.apache.hudi.common.avro.ConvertingGenericData;
import org.apache.hudi.common.avro.VariantShreddingProvider;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroSchemaConverterWithTimestampNTZ;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.spark.types.variant.Variant;
import org.apache.spark.types.variant.VariantBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hudi.common.avro.HoodieAvroUtils.unwrapNullable;
import static org.apache.hudi.common.schema.HoodieSchema.Variant.VARIANT_METADATA_FIELD;
import static org.apache.hudi.common.schema.HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD;
import static org.apache.hudi.common.schema.HoodieSchema.Variant.VARIANT_VALUE_FIELD;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.NANOS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.bsonType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.decimalType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.float16Type;
import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.intervalType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.jsonType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timeType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timestampType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.uuidType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trip and behavior-pinning coverage for {@link Spark4VariantShreddingProvider}: shred an
 * unshredded variant, then reconstruct it, asserting both the intermediate shredded schema/values and
 * the reconstructed variant. This exercises {@code shredVariantRecord}, {@code avroTypeToScalarType},
 * {@code convertScalarToAvro}, {@code prepareRebuild} (the read schema built from the file's parquet
 * type, with each scalar leaf's {@code LeafDecoding}), and the
 * {@code AvroVariantRow}/{@code AvroObjectRow}/{@code AvroArrayRow} accessors across every reachable
 * scalar leaf type, object/array shapes, partial (residual) shredding, and the null/error guards - the
 * AVRO read-path reconstruction that the Spark MOR SQL test cannot reach (Spark compaction reads base
 * files via the InternalRow reader, not HoodieAvroParquetReader). Parquet encodings Hudi's writer never
 * produces are read straight from a parquet group and pinned against what Spark 4's reader returns.
 */
class TestSpark4VariantShreddingProvider {

  private final Spark4VariantShreddingProvider provider = new Spark4VariantShreddingProvider();
  private final Schema unshreddedSchema = HoodieSchema.createVariant().getAvroSchema();

  /** Wrap a fully built {@link Variant} into the unshredded {metadata, value} Avro record. */
  private GenericRecord unshredded(Variant variant) {
    GenericRecord record = new GenericData.Record(unshreddedSchema);
    record.put(VARIANT_METADATA_FIELD, ByteBuffer.wrap(variant.getMetadata()));
    record.put(VARIANT_VALUE_FIELD, ByteBuffer.wrap(variant.getValue()));
    return record;
  }

  private GenericRecord shred(Variant variant, HoodieSchema.Variant shredded) {
    GenericRecord record = provider.shredVariantRecord(unshredded(variant), shredded.getAvroSchema(), shredded);
    // ConvertingGenericData.INSTANCE is the data model HoodieAvroWriteSupport hands to parquet-avro,
    // so this pins that every shredded record produced here is writable at its declared schema.
    assertTrue(ConvertingGenericData.INSTANCE.validate(shredded.getAvroSchema(), record),
        "shredded record is not writable at its declared schema");
    return record;
  }

  /** Prepares the rebuild of {@code shredded} from the parquet group it is written as. */
  private VariantShreddingProvider.VariantRebuilder rebuilder(HoodieSchema.Variant shredded) {
    HoodieSchema record = HoodieSchema.createRecord("r", "org.apache.hudi.test", null,
        Collections.singletonList(HoodieSchemaField.of("v", shredded)));
    GroupType parquetType = new AvroSchemaConverterWithTimestampNTZ().convert(record).getType("v").asGroupType();
    return provider.prepareRebuild("v", parquetType, unshreddedSchema);
  }

  private Variant rebuild(GenericRecord shreddedRecord, HoodieSchema.Variant shredded) {
    return toVariant(rebuilder(shredded).rebuild(shreddedRecord));
  }

  private static Variant toVariant(GenericRecord rebuilt) {
    return new Variant(toBytes(rebuilt.get(VARIANT_VALUE_FIELD)), toBytes(rebuilt.get(VARIANT_METADATA_FIELD)));
  }

  private void assertRoundTrips(Variant variant, HoodieSchema.Variant shredded) {
    assertRoundTrips(variant, shredded, "");
  }

  private void assertRoundTrips(Variant variant, HoodieSchema.Variant shredded, String context) {
    Variant rebuilt = rebuild(shred(variant, shredded), shredded);
    assertEquals(variant.toJson(ZoneOffset.UTC), rebuilt.toJson(ZoneOffset.UTC),
        "variant did not round-trip through shred/rebuild" + context);
  }

  /** Parse json to a variant, shred it, rebuild it, assert the json round-trips. */
  private void assertRoundTrips(String json, HoodieSchema.Variant shredded) throws Exception {
    assertRoundTrips(VariantBuilder.parseJson(json, false), shredded, " for: " + json);
  }

  /**
   * Wrap a scalar leaf into a shredded variant schema with a nullable {@code typed_value}, the shape
   * production always emits ({@code createShreddedFieldStruct} wraps every leaf in
   * {@code createNullable}, and the parquet shredding spec requires {@code typed_value} optional).
   */
  private static HoodieSchema.Variant shreddedLeaf(HoodieSchema typedValue) {
    return HoodieSchema.createVariantShredded(HoodieSchema.createNullable(typedValue));
  }

  private void assertScalarRoundTrips(String json, HoodieSchema typedValue) throws Exception {
    assertRoundTrips(json, shreddedLeaf(typedValue));
  }

  /**
   * Shred {@code variant} into a scalar {@code typedValue} schema, assert the value fully shredded
   * (into {@code typed_value}, exactly {@code expectedTypedValue}, no residual {@code value}) and
   * round-trips back to the original variant.
   */
  private void assertScalarShredsTo(Variant variant, HoodieSchema typedValue, Object expectedTypedValue) {
    HoodieSchema.Variant shredded = shreddedLeaf(typedValue);
    GenericRecord shreddedRecord = shred(variant, shredded);
    assertNotNull(shreddedRecord.get(VARIANT_METADATA_FIELD), "shredded record must carry metadata");
    assertNull(shreddedRecord.get(VARIANT_VALUE_FIELD), "a matching scalar leaves no residual value");
    assertEquals(expectedTypedValue, shreddedRecord.get(VARIANT_TYPED_VALUE_FIELD),
        "scalar was not shredded into typed_value as expected");
    assertEquals(variant.toJson(ZoneOffset.UTC), rebuild(shreddedRecord, shredded).toJson(ZoneOffset.UTC),
        "scalar did not round-trip through shred/rebuild");
  }

  private void assertScalarShredsTo(String json, HoodieSchema typedValue, Object expectedTypedValue) throws Exception {
    assertScalarShredsTo(VariantBuilder.parseJson(json, false), typedValue, expectedTypedValue);
  }

  /**
   * Shred {@code variant} against a scalar {@code typedValue} it does not match: assert it is NOT
   * shredded (typed_value stays null, the value lands in the residual {@code value}) yet still
   * round-trips. Exercises the "decline to shred, keep in residual" fallbacks.
   */
  private void assertStaysInResidual(Variant variant, HoodieSchema typedValue) {
    HoodieSchema.Variant shredded = shreddedLeaf(typedValue);
    GenericRecord shreddedRecord = shred(variant, shredded);
    assertNull(shreddedRecord.get(VARIANT_TYPED_VALUE_FIELD), "value should not have been shredded");
    assertNotNull(shreddedRecord.get(VARIANT_VALUE_FIELD), "unshredded value must survive in residual");
    assertEquals(variant.toJson(ZoneOffset.UTC), rebuild(shreddedRecord, shredded).toJson(ZoneOffset.UTC),
        "residual value did not round-trip through shred/rebuild");
  }

  private static Variant scalar(java.util.function.Consumer<VariantBuilder> append) {
    VariantBuilder builder = new VariantBuilder(false);
    append.accept(builder);
    return builder.result();
  }

  // ---------------------------------------------------------------------------
  // Scalar leaf types: one per branch of avroTypeToScalarType / convertScalarToAvro, each read back
  // from the parquet type Hudi's writer gives it.
  // ---------------------------------------------------------------------------

  @Test
  void intLeafShredsToInteger() {
    // An integral variant shreds into an INT typed_value as a boxed Integer (Avro has no byte/short,
    // so the write side only ever emits IntegralSize.INT here; the narrower parquet INT(8) / INT(16)
    // leaves only come from other writers, see rebuildsLeafFromItsParquetEncodingLikeSpark).
    assertScalarShredsTo(scalar(b -> b.appendLong(1000)), HoodieSchema.create(HoodieSchemaType.INT), 1000);
  }

  @Test
  void longLeafShredsToLong() throws Exception {
    assertScalarShredsTo(scalar(b -> b.appendLong(100000)), HoodieSchema.create(HoodieSchemaType.LONG), 100000L);
    // Same branch through the json entry point.
    assertScalarShredsTo("42", HoodieSchema.create(HoodieSchemaType.LONG), 42L);
  }

  @Test
  void floatShredsAndRoundTrips() {
    assertScalarShredsTo(scalar(b -> b.appendFloat(1.5f)), HoodieSchema.create(HoodieSchemaType.FLOAT), 1.5f);
  }

  @Test
  void doubleShredsAndRoundTrips() {
    assertScalarShredsTo(scalar(b -> b.appendDouble(2.5d)), HoodieSchema.create(HoodieSchemaType.DOUBLE), 2.5d);
  }

  @Test
  void stringLeafShredsToString() throws Exception {
    assertScalarShredsTo("\"hello world\"", HoodieSchema.create(HoodieSchemaType.STRING), "hello world");
  }

  @Test
  void booleanLeafShredsToBoolean() throws Exception {
    assertScalarShredsTo("true", HoodieSchema.create(HoodieSchemaType.BOOLEAN), true);
  }

  @Test
  void binaryShredsToByteBuffer() {
    byte[] payload = {'n', 'o', 't', '-', 'u', 't', 'f', '8', (byte) 0xC0, (byte) 0xFF};
    assertScalarShredsTo(scalar(b -> b.appendBinary(payload)),
        HoodieSchema.create(HoodieSchemaType.BYTES), ByteBuffer.wrap(payload));
  }

  @Test
  void dateShredsToDaysSinceEpoch() {
    assertScalarShredsTo(scalar(b -> b.appendDate(19000)), HoodieSchema.createDate(), 19000);
  }

  @Test
  void timestampMicrosShredsToMicros() {
    long micros = 1_700_000_000_000_000L;
    assertScalarShredsTo(scalar(b -> b.appendTimestamp(micros)), HoodieSchema.createTimestampMicros(), micros);
  }

  @Test
  void localTimestampMicrosShredsToMicros() {
    long micros = 1_700_000_000_000_000L;
    assertScalarShredsTo(scalar(b -> b.appendTimestampNtz(micros)), HoodieSchema.createLocalTimestampMicros(), micros);
  }

  @Test
  void decimalLeafShredsToBigDecimal() throws Exception {
    assertScalarShredsTo("123.45", HoodieSchema.createDecimal(10, 2), new BigDecimal("123.45"));
    // Decimal-on-fixed still shreds: a fixed typed_value is rejected unless it carries a decimal.
    assertScalarShredsTo("123.45", HoodieSchema.createDecimal("dec_fixed", "org.apache.hudi.test", null, 10, 2, 8),
        new BigDecimal("123.45"));
  }

  @Test
  void numericScaleChangesShredWhenExact() {
    // allowNumericScaleChanges() lets Spark shred an integral variant into a decimal leaf (and a
    // decimal variant into an integral leaf) when the value is exactly representable.
    assertScalarShredsTo(scalar(b -> b.appendLong(5)), HoodieSchema.createDecimal(10, 2), new BigDecimal("5.00"));
    assertScalarShredsTo(scalar(b -> b.appendDecimal(new BigDecimal("5.00"))),
        HoodieSchema.create(HoodieSchemaType.LONG), 5L);
  }

  // ---------------------------------------------------------------------------
  // "Decline to shred" fallbacks: value stays in the residual binary.
  // ---------------------------------------------------------------------------

  @Test
  void lossyDecimalIsNotShredded() {
    // decimal(10,2) cannot represent 123.456 exactly, so the value must fall to the residual.
    assertStaysInResidual(scalar(b -> b.appendDecimal(new BigDecimal("123.456"))),
        HoodieSchema.createDecimal(10, 2));
  }

  @Test
  void noTypedValueSchemaRoundTrips() throws Exception {
    // A shredded schema without typed_value keeps the whole variant in the residual value.
    assertRoundTrips("{\"a\":1}", HoodieSchema.createVariantShredded(null));
  }

  // ---------------------------------------------------------------------------
  // Leaf types Spark 4 cannot shred: rejected on write, as Spark's own
  // SparkShreddingUtils.buildVariantSchema rejects them (GH 19442).
  // ---------------------------------------------------------------------------

  private static Stream<Arguments> typedValueLeavesSparkCannotShred() {
    UUID uuid = UUID.fromString("12345678-1234-1234-1234-123456789abc");
    long micros = 1_700_000_000_000_000L;
    return Stream.of(
        Arguments.of("fixed", HoodieSchema.createFixed("fixed4", "org.apache.hudi.test", null, 4),
            scalar(b -> b.appendBinary(new byte[] {1, 2, 3, 4}))),
        Arguments.of("uuid on fixed", HoodieSchema.fromAvroSchema(
            LogicalTypes.uuid().addToSchema(Schema.createFixed("uuid_fixed", null, "org.apache.hudi.test", 16))),
            scalar(b -> b.appendUuid(uuid))),
        Arguments.of("uuid on string", HoodieSchema.createUUID(), scalar(b -> b.appendUuid(uuid))),
        Arguments.of("timestamp-millis", HoodieSchema.createTimestampMillis(),
            scalar(b -> b.appendTimestamp(micros))),
        Arguments.of("local-timestamp-millis", HoodieSchema.createLocalTimestampMillis(),
            scalar(b -> b.appendTimestampNtz(micros))),
        Arguments.of("timestamp-nanos", HoodieSchema.fromAvroSchema(
            LogicalTypes.timestampNanos().addToSchema(Schema.create(Schema.Type.LONG))),
            scalar(b -> b.appendTimestamp(micros))),
        Arguments.of("local-timestamp-nanos", HoodieSchema.fromAvroSchema(
            LogicalTypes.localTimestampNanos().addToSchema(Schema.create(Schema.Type.LONG))),
            scalar(b -> b.appendTimestampNtz(micros))),
        Arguments.of("time-millis", HoodieSchema.createTimeMillis(), scalar(b -> b.appendLong(1000))),
        Arguments.of("time-micros", HoodieSchema.createTimeMicros(), scalar(b -> b.appendLong(1000))),
        Arguments.of("enum", HoodieSchema.createEnum("e", "org.apache.hudi.test", null, Arrays.asList("A", "B")),
            scalar(b -> b.appendString("A"))),
        Arguments.of("map", HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.LONG)),
            scalar(b -> b.appendLong(1))));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("typedValueLeavesSparkCannotShred")
  void rejectsTypedValueLeafSparkCannotShred(String leaf, HoodieSchema typedValue, Variant variant) {
    HoodieException e = assertThrows(HoodieException.class, () -> shred(variant, shreddedLeaf(typedValue)));
    assertTrue(e.getMessage().contains("Spark 4 cannot shred"), "unexpected message: " + e.getMessage());
  }

  // ---------------------------------------------------------------------------
  // Object and array shapes, including partial (residual) object shredding.
  // ---------------------------------------------------------------------------

  @Test
  void objectRoundTrips() throws Exception {
    Map<String, HoodieSchema> shreddedFields = new LinkedHashMap<>();
    shreddedFields.put("a", HoodieSchema.create(HoodieSchemaType.STRING));
    shreddedFields.put("b", HoodieSchema.create(HoodieSchemaType.LONG));
    assertRoundTrips("{\"a\":\"x\",\"b\":5}", HoodieSchema.createVariantShreddedObject(null, null, null, shreddedFields));
  }

  @Test
  void partialObjectShreddingKeepsExtraFieldsInResidual() throws Exception {
    // Shredded schema declares {a, b} but the variant provides {a, c}: "a" shreds into typed_value,
    // "b" is absent (null value + null typed_value), and the extra "c" lands in the residual value.
    Map<String, HoodieSchema> shreddedFields = new LinkedHashMap<>();
    shreddedFields.put("a", HoodieSchema.create(HoodieSchemaType.STRING));
    shreddedFields.put("b", HoodieSchema.create(HoodieSchemaType.LONG));
    HoodieSchema.Variant shredded = HoodieSchema.createVariantShreddedObject(null, null, null, shreddedFields);

    Variant variant = VariantBuilder.parseJson("{\"a\":\"x\",\"c\":99}", false);
    GenericRecord shreddedRecord = shred(variant, shredded);

    // The unmatched field forces a non-null residual value at the top level.
    assertNotNull(shreddedRecord.get(VARIANT_VALUE_FIELD), "extra field must be captured in residual value");
    GenericRecord typedValue = (GenericRecord) shreddedRecord.get(VARIANT_TYPED_VALUE_FIELD);
    GenericRecord aField = (GenericRecord) typedValue.get("a");
    assertEquals("x", aField.get(VARIANT_TYPED_VALUE_FIELD), "declared field a must shred into typed_value");
    assertNull(aField.get(VARIANT_VALUE_FIELD), "matched field a carries no residual");
    GenericRecord bField = (GenericRecord) typedValue.get("b");
    assertNull(bField.get(VARIANT_VALUE_FIELD), "absent field b carries no residual value");
    assertNull(bField.get(VARIANT_TYPED_VALUE_FIELD), "absent field b carries no typed_value");

    assertEquals(variant.toJson(ZoneOffset.UTC), rebuild(shreddedRecord, shredded).toJson(ZoneOffset.UTC));
  }

  @Test
  void typeMismatchedObjectFieldFallsToFieldResidual() throws Exception {
    // The field is declared LONG but the variant supplies a string: the field's own value carries
    // the residual, its typed_value stays null, and the top-level value stays null.
    Map<String, HoodieSchema> shreddedFields = new LinkedHashMap<>();
    shreddedFields.put("a", HoodieSchema.create(HoodieSchemaType.LONG));
    HoodieSchema.Variant shredded = HoodieSchema.createVariantShreddedObject(null, null, null, shreddedFields);

    Variant variant = VariantBuilder.parseJson("{\"a\":\"str\"}", false);
    GenericRecord shreddedRecord = shred(variant, shredded);

    assertNull(shreddedRecord.get(VARIANT_VALUE_FIELD), "no unmatched field, so the top-level residual stays null");
    GenericRecord aField = (GenericRecord) ((GenericRecord) shreddedRecord.get(VARIANT_TYPED_VALUE_FIELD)).get("a");
    assertNotNull(aField.get(VARIANT_VALUE_FIELD), "mismatched field a keeps its value in the field residual");
    assertNull(aField.get(VARIANT_TYPED_VALUE_FIELD), "mismatched field a must not shred");

    assertEquals(variant.toJson(ZoneOffset.UTC), rebuild(shreddedRecord, shredded).toJson(ZoneOffset.UTC));
  }

  @Test
  void arrayRoundTrips() throws Exception {
    // typed_value for an array is array<{value, typed_value}>: each element is itself a shredded struct.
    HoodieSchema element = HoodieSchema.createRecord("v_array_element", "org.apache.hudi.test", null, Arrays.asList(
        HoodieSchemaField.of(VARIANT_VALUE_FIELD, HoodieSchema.createNullable(HoodieSchemaType.BYTES)),
        HoodieSchemaField.of(VARIANT_TYPED_VALUE_FIELD, HoodieSchema.create(HoodieSchemaType.LONG))));
    assertScalarRoundTrips("[1,2,3]", HoodieSchema.createArray(element));
  }

  // ---------------------------------------------------------------------------
  // Decimal reconstruction from the on-disk (avro-decoded) encodings a parquet reader produces:
  // the shred path emits a BigDecimal, but a base file feeds rebuild a ByteBuffer / GenericFixed.
  // ---------------------------------------------------------------------------

  @Test
  void rebuildDecimalFromBytesEncoding() {
    assertDecimalRebuildsFromEncoding(HoodieSchema.createDecimal(10, 2));
  }

  @Test
  void rebuildDecimalFromFixedEncoding() {
    assertDecimalRebuildsFromEncoding(
        HoodieSchema.createDecimal("dec_fixed", "org.apache.hudi.test", null, 10, 2, 8));
  }

  private void assertDecimalRebuildsFromEncoding(HoodieSchema decimalType) {
    BigDecimal value = new BigDecimal("123.45");
    HoodieSchema.Variant shredded = shreddedLeaf(decimalType);
    GenericRecord shreddedRecord = shred(scalar(b -> b.appendDecimal(value)), shredded);

    // Encode against the concrete decimal branch (typed_value itself is a nullable union).
    Schema tvSchema = decimalType.getAvroSchema();
    Conversions.DecimalConversion conversion = new Conversions.DecimalConversion();
    Object encoded = tvSchema.getType() == Schema.Type.FIXED
        ? conversion.toFixed(value, tvSchema, tvSchema.getLogicalType())
        : conversion.toBytes(value, tvSchema, tvSchema.getLogicalType());
    shreddedRecord.put(VARIANT_TYPED_VALUE_FIELD, encoded);

    Variant original = scalar(b -> b.appendDecimal(value));
    assertEquals(original.toJson(ZoneOffset.UTC), rebuild(shreddedRecord, shredded).toJson(ZoneOffset.UTC));
  }

  @Test
  void rebuildDecimalRejectsUnexpectedEncoding() {
    HoodieSchema.Variant shredded = shreddedLeaf(HoodieSchema.createDecimal(10, 2));
    GenericRecord shreddedRecord = shred(scalar(b -> b.appendDecimal(new BigDecimal("1.00"))), shredded);
    shreddedRecord.put(VARIANT_TYPED_VALUE_FIELD, "not-a-decimal");
    assertThrows(IllegalStateException.class,
        () -> rebuilder(shredded).rebuild(shreddedRecord));
  }

  // ---------------------------------------------------------------------------
  // Reading a shredded file: each typed_value leaf is decoded by its parquet type, as Spark 4's own
  // reader does. The footer's Avro conversion drops the encodings that matter here (unsigned and
  // decimal INT32 / INT64, JSON, millisecond timestamps), so these leaves are built straight from a
  // parquet group, fed the value parquet-avro reads for them, and compared with Spark's variant.
  // ---------------------------------------------------------------------------

  private static final byte[] EMPTY_METADATA = scalar(VariantBuilder::appendNull).getMetadata();

  /** A typed_value leaf, built directly because Types refuses some encodings under test (e.g. DECIMAL(10, 0) on INT32). */
  private static PrimitiveType leaf(PrimitiveTypeName type, int length, LogicalTypeAnnotation annotation) {
    return new PrimitiveType(Type.Repetition.OPTIONAL, type, length, VARIANT_TYPED_VALUE_FIELD)
        .withLogicalTypeAnnotation(annotation);
  }

  private static PrimitiveType leaf(PrimitiveTypeName type, LogicalTypeAnnotation annotation) {
    return leaf(type, 0, annotation);
  }

  /** A top-level shredded variant column {@code v} with a scalar typed_value. */
  private static GroupType shreddedColumn(PrimitiveType typedValue) {
    return new GroupType(Type.Repetition.OPTIONAL, "v",
        new PrimitiveType(Type.Repetition.REQUIRED, BINARY, VARIANT_METADATA_FIELD),
        new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, VARIANT_VALUE_FIELD),
        typedValue);
  }

  /** The variant column {@code v} declared by a parquet schema snippet. */
  private static GroupType column(String group) {
    return MessageTypeParser.parseMessageType("message r { " + group + " }").getType("v").asGroupType();
  }

  /** The Avro schema HoodieAvroParquetReader reads {@code column} at: its footer conversion, lists unwrapped. */
  private static Schema avroReadSchema(GroupType column) {
    Configuration conf = new Configuration(false);
    conf.setBoolean(AvroSchemaConverterWithTimestampNTZ.ADD_LIST_ELEMENT_RECORDS, false);
    Schema record = new AvroSchemaConverterWithTimestampNTZ(conf).convert(new MessageType("r", column)).toAvroSchema();
    return unwrapNullable(record.getField(column.getName()).schema());
  }

  private static Schema fieldSchema(Schema record, String field) {
    return unwrapNullable(record.getField(field).schema());
  }

  private static GenericRecord record(Schema schema, String field, Object value) {
    GenericRecord record = new GenericData.Record(schema);
    record.put(field, value);
    return record;
  }

  private static void assertSameVariant(Variant expected, Variant actual) {
    assertEquals(expected.toJson(ZoneOffset.UTC), actual.toJson(ZoneOffset.UTC));
    // The binary also pins the variant type, which the json can hide.
    assertArrayEquals(expected.getValue(), actual.getValue(), "variant type or encoding differs");
  }

  private static Stream<Arguments> typedValueEncodingsSparkReads() {
    byte[] bytes = {1, 2, 3, (byte) 0xFF};
    String json = "{\"a\":[1,2]}";
    long millis = 1_700_000_000_123L;
    BigDecimal negative = new BigDecimal("-123.45");
    Schema fixed5 = Schema.createFixed(VARIANT_TYPED_VALUE_FIELD, null, null, 5);
    return Stream.of(
        // Narrow and unsigned ints widen to the next signed size; INT(64, false) becomes DECIMAL(20, 0).
        Arguments.of(leaf(INT32, intType(8, true)), -5, scalar(b -> b.appendLong(-5))),
        Arguments.of(leaf(INT32, intType(16, false)), 60000, scalar(b -> b.appendLong(60000))),
        Arguments.of(leaf(INT32, intType(32, false)), -1, scalar(b -> b.appendLong(4294967295L))),
        Arguments.of(leaf(INT64, intType(64, false)), -1L,
            scalar(b -> b.appendDecimal(new BigDecimal("18446744073709551615")))),
        // A decimal on INT32 / INT64 arrives as its unscaled value.
        Arguments.of(leaf(INT32, decimalType(2, 9)), 150, scalar(b -> b.appendDecimal(new BigDecimal("1.50")))),
        Arguments.of(leaf(INT32, decimalType(2, 9)), -12345, scalar(b -> b.appendDecimal(negative))),
        Arguments.of(leaf(INT64, decimalType(4, 18)), 123456789012345678L,
            scalar(b -> b.appendDecimal(new BigDecimal("12345678901234.5678")))),
        Arguments.of(leaf(FIXED_LEN_BYTE_ARRAY, 5, decimalType(2, 10)),
            new Conversions.DecimalConversion().toFixed(negative, fixed5, LogicalTypes.decimal(10, 2)),
            scalar(b -> b.appendDecimal(negative))),
        // Millisecond timestamps scale to micros and keep the UTC / local distinction.
        Arguments.of(leaf(INT64, timestampType(true, MILLIS)), millis, scalar(b -> b.appendTimestamp(millis * 1000))),
        Arguments.of(leaf(INT64, timestampType(false, MILLIS)), millis, scalar(b -> b.appendTimestampNtz(millis * 1000))),
        // JSON and ENUM read as strings; BSON and a plain FIXED_LEN_BYTE_ARRAY as binary.
        Arguments.of(leaf(BINARY, jsonType()), ByteBuffer.wrap(json.getBytes(UTF_8)), scalar(b -> b.appendString(json))),
        Arguments.of(leaf(BINARY, enumType()), new Utf8("A"), scalar(b -> b.appendString("A"))),
        Arguments.of(leaf(BINARY, bsonType()), ByteBuffer.wrap(bytes), scalar(b -> b.appendBinary(bytes))),
        Arguments.of(leaf(FIXED_LEN_BYTE_ARRAY, 4, null),
            new GenericData.Fixed(Schema.createFixed(VARIANT_TYPED_VALUE_FIELD, null, null, 4), bytes),
            scalar(b -> b.appendBinary(bytes))));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("typedValueEncodingsSparkReads")
  void rebuildsLeafFromItsParquetEncodingLikeSpark(PrimitiveType leaf, Object typedValue, Variant expected) {
    GroupType column = shreddedColumn(leaf);
    GenericRecord shreddedRecord = record(avroReadSchema(column), VARIANT_TYPED_VALUE_FIELD, typedValue);
    shreddedRecord.put(VARIANT_METADATA_FIELD, ByteBuffer.wrap(EMPTY_METADATA));
    assertSameVariant(expected, toVariant(provider.prepareRebuild("v", column, unshreddedSchema).rebuild(shreddedRecord)));
  }

  private static Stream<Arguments> typedValueEncodingsSparkRejects() {
    String sparkRejects = "which Spark 4 does not support reading";
    return Stream.of(
        Arguments.of(leaf(INT32, timeType(true, MILLIS)), sparkRejects),
        Arguments.of(leaf(INT64, timeType(true, MICROS)), sparkRejects),
        Arguments.of(leaf(INT64, timeType(false, MICROS)), sparkRejects),
        Arguments.of(leaf(INT64, timeType(true, NANOS)), sparkRejects),
        Arguments.of(leaf(INT64, timestampType(true, NANOS)), sparkRejects),
        Arguments.of(leaf(INT64, timestampType(false, NANOS)), sparkRejects),
        Arguments.of(leaf(FIXED_LEN_BYTE_ARRAY, 2, float16Type()), sparkRejects),
        Arguments.of(leaf(FIXED_LEN_BYTE_ARRAY, 16, uuidType()), sparkRejects),
        Arguments.of(leaf(FIXED_LEN_BYTE_ARRAY, 12, intervalType()), sparkRejects),
        Arguments.of(leaf(INT32, decimalType(0, 10)), sparkRejects),
        Arguments.of(leaf(INT64, decimalType(0, 19)), sparkRejects),
        Arguments.of(leaf(INT96, null), "INT96 typed_value is not supported by Hudi's Avro reader"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("typedValueEncodingsSparkRejects")
  void rejectsLeafSparkCannotReadWhenPrepared(PrimitiveType leaf, String reason) {
    // Fails when the file is opened, before any record, naming the column and the parquet type.
    HoodieException e = assertThrows(HoodieException.class,
        () -> provider.prepareRebuild("v", shreddedColumn(leaf), unshreddedSchema));
    String message = e.getMessage();
    assertTrue(message.contains("column 'v'") && message.contains("'" + leaf + "'") && message.contains(reason), message);
  }

  @Test
  void rebuildsAndRejectsNestedLeavesByTheirParquetEncoding() throws Exception {
    // DECIMAL(9, 2) on INT32 as an object field and as the element of a 3-level list.
    GroupType column = column("optional group v { required binary metadata; optional binary value;"
        + " optional group typed_value {"
        + "   required group price { optional binary value; optional int32 typed_value (DECIMAL(9,2)); }"
        + "   required group history { optional binary value; optional group typed_value (LIST) {"
        + "     repeated group list { required group element {"
        + "       optional binary value; optional int32 typed_value (DECIMAL(9,2)); } } } } } }");
    Variant expected = VariantBuilder.parseJson("{\"price\":1.50,\"history\":[1.25,-0.75]}", false);

    Schema v = avroReadSchema(column);
    Schema typedValue = fieldSchema(v, VARIANT_TYPED_VALUE_FIELD);
    Schema history = fieldSchema(typedValue, "history");
    Schema element = unwrapNullable(fieldSchema(history, VARIANT_TYPED_VALUE_FIELD).getElementType());
    GenericRecord typedValueRecord = new GenericData.Record(typedValue);
    typedValueRecord.put("price", record(fieldSchema(typedValue, "price"), VARIANT_TYPED_VALUE_FIELD, 150));
    typedValueRecord.put("history", record(history, VARIANT_TYPED_VALUE_FIELD, Arrays.asList(
        record(element, VARIANT_TYPED_VALUE_FIELD, 125), record(element, VARIANT_TYPED_VALUE_FIELD, -75))));
    GenericRecord shreddedRecord = record(v, VARIANT_TYPED_VALUE_FIELD, typedValueRecord);
    shreddedRecord.put(VARIANT_METADATA_FIELD, ByteBuffer.wrap(expected.getMetadata()));
    assertSameVariant(expected, toVariant(provider.prepareRebuild("v", column, unshreddedSchema).rebuild(shreddedRecord)));

    // A leaf Spark rejects below an object field: the error carries the column's and the leaf's paths.
    GroupType rejected = column("optional group v { required binary metadata; optional binary value;"
        + " optional group typed_value {"
        + "   required group t { optional binary value; optional int64 typed_value (TIME(MICROS,true)); } } }");
    HoodieException e = assertThrows(HoodieException.class,
        () -> provider.prepareRebuild("s.v", rejected, unshreddedSchema));
    assertTrue(e.getMessage().contains("column 's.v'")
        && e.getMessage().contains("typed_value 's.v.typed_value.t.typed_value'"), e.getMessage());
  }

  private static Stream<Arguments> invalidShreddingLayouts() {
    String header = "optional group v { required binary metadata; optional binary value;";
    return Stream.of(
        Arguments.of("unknown field in an object field group", header
            + " optional group typed_value { required group a {"
            + "   optional binary value; optional int32 typed_value; optional binary extra; } } }", "v.typed_value.a"),
        Arguments.of("MAP typed_value", header
            + " optional group typed_value (MAP) { repeated group key_value { required binary key (STRING);"
            + "   required group value { optional binary value; optional int32 typed_value; } } } }", "v.typed_value"),
        Arguments.of("LIST typed_value of non-struct elements", header
            + " optional group typed_value (LIST) { repeated group list { optional int32 element; } } }", "v.typed_value"),
        Arguments.of("no top-level metadata",
            "optional group v { optional binary value; optional int32 typed_value; }", "v"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("invalidShreddingLayouts")
  void rejectsInvalidShreddingLayoutWhenPrepared(String layout, String group, String offendingPath) {
    HoodieException e = assertThrows(HoodieException.class,
        () -> provider.prepareRebuild("v", column(group), unshreddedSchema));
    assertTrue(e.getMessage().contains("'" + offendingPath + "' is not a valid shredded variant layout"), e.getMessage());
  }

  // ---------------------------------------------------------------------------
  // Null / error guards.
  // ---------------------------------------------------------------------------

  @Test
  void shredReturnsNullWhenValueOrMetadataMissing() {
    HoodieSchema.Variant shredded = shreddedLeaf(HoodieSchema.create(HoodieSchemaType.LONG));
    Variant variant = scalar(b -> b.appendLong(1));

    GenericRecord missingValue = unshredded(variant);
    missingValue.put(VARIANT_VALUE_FIELD, null);
    assertNull(provider.shredVariantRecord(missingValue, shredded.getAvroSchema(), shredded));

    GenericRecord missingMetadata = unshredded(variant);
    missingMetadata.put(VARIANT_METADATA_FIELD, null);
    assertNull(provider.shredVariantRecord(missingMetadata, shredded.getAvroSchema(), shredded));
  }

  @Test
  void rebuildReturnsNullForNullRecord() {
    // Coverage of a defensive guard only: the sole production caller (HoodieVariantReconstruction)
    // already checks instanceof GenericRecord before calling the rebuilder.
    HoodieSchema.Variant shredded = shreddedLeaf(HoodieSchema.create(HoodieSchemaType.LONG));
    assertNull(rebuilder(shredded).rebuild(null));
  }

  @Test
  void rebuildThrowsWhenMetadataMissing() {
    HoodieSchema.Variant shredded = shreddedLeaf(HoodieSchema.create(HoodieSchemaType.LONG));
    GenericRecord shreddedRecord = shred(scalar(b -> b.appendLong(7)), shredded);
    shreddedRecord.put(VARIANT_METADATA_FIELD, null);
    assertThrows(HoodieException.class,
        () -> rebuilder(shredded).rebuild(shreddedRecord));
  }

  private static byte[] toBytes(Object byteBuffer) {
    ByteBuffer buf = ((ByteBuffer) byteBuffer).duplicate();
    byte[] out = new byte[buf.remaining()];
    buf.get(out);
    return out;
  }
}
