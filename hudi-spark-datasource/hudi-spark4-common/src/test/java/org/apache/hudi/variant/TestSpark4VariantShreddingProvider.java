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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.types.variant.Variant;
import org.apache.spark.types.variant.VariantBuilder;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.hudi.common.schema.HoodieSchema.Variant.VARIANT_METADATA_FIELD;
import static org.apache.hudi.common.schema.HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD;
import static org.apache.hudi.common.schema.HoodieSchema.Variant.VARIANT_VALUE_FIELD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Round-trip and behavior-pinning coverage for {@link Spark4VariantShreddingProvider}: shred an
 * unshredded variant, then reconstruct it, asserting both the intermediate shredded schema/values and
 * the reconstructed variant. This exercises {@code shredVariantRecord}, {@code rebuildVariantRecord},
 * {@code avroTypeToScalarType}, {@code convertScalarToAvro}, and the
 * {@code AvroVariantRow}/{@code AvroObjectRow}/{@code AvroArrayRow} accessors across every scalar leaf
 * type, object/array shapes, partial (residual) shredding, and the null/error guards - the AVRO
 * read-path reconstruction that the Spark MOR SQL test cannot reach (Spark compaction reads base
 * files via the InternalRow reader, not HoodieAvroParquetReader).
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
    return provider.shredVariantRecord(unshredded(variant), shredded.getAvroSchema(), shredded);
  }

  private Variant rebuild(GenericRecord shreddedRecord, HoodieSchema.Variant shredded) {
    GenericRecord rebuilt =
        provider.rebuildVariantRecord(shreddedRecord, shredded.getAvroSchema(), unshreddedSchema);
    return new Variant(toBytes(rebuilt.get(VARIANT_VALUE_FIELD)), toBytes(rebuilt.get(VARIANT_METADATA_FIELD)));
  }

  private void assertRoundTrips(Variant variant, HoodieSchema.Variant shredded) {
    Variant rebuilt = rebuild(shred(variant, shredded), shredded);
    assertEquals(variant.toJson(ZoneOffset.UTC), rebuilt.toJson(ZoneOffset.UTC),
        "variant did not round-trip through shred/rebuild");
  }

  /** Parse json to a variant, shred it, rebuild it, assert the json round-trips. */
  private void assertRoundTrips(String json, HoodieSchema.Variant shredded) throws Exception {
    assertRoundTrips(VariantBuilder.parseJson(json, false), shredded);
  }

  private void assertScalarRoundTrips(String json, HoodieSchema typedValue) throws Exception {
    assertRoundTrips(json, HoodieSchema.createVariantShredded(typedValue));
  }

  /**
   * Shred {@code variant} into a scalar {@code typedValue} schema, assert the value fully shredded
   * (into {@code typed_value}, exactly {@code expectedTypedValue}, no residual {@code value}) and
   * round-trips back to the original variant.
   */
  private void assertScalarShredsTo(Variant variant, HoodieSchema typedValue, Object expectedTypedValue) {
    HoodieSchema.Variant shredded = HoodieSchema.createVariantShredded(typedValue);
    GenericRecord shreddedRecord = shred(variant, shredded);
    assertNotNull(shreddedRecord.get(VARIANT_METADATA_FIELD), "shredded record must carry metadata");
    assertNull(shreddedRecord.get(VARIANT_VALUE_FIELD), "a matching scalar leaves no residual value");
    assertEquals(expectedTypedValue, shreddedRecord.get(VARIANT_TYPED_VALUE_FIELD),
        "scalar was not shredded into typed_value as expected");
    assertEquals(variant.toJson(ZoneOffset.UTC), rebuild(shreddedRecord, shredded).toJson(ZoneOffset.UTC),
        "scalar did not round-trip through shred/rebuild");
  }

  /**
   * Shred {@code variant} against a scalar {@code typedValue} it does not match: assert it is NOT
   * shredded (typed_value stays null, the value lands in the residual {@code value}) yet still
   * round-trips. Exercises the "decline to shred, keep in residual" fallbacks.
   */
  private void assertStaysInResidual(Variant variant, HoodieSchema typedValue) {
    HoodieSchema.Variant shredded = HoodieSchema.createVariantShredded(typedValue);
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
  // Scalar leaf types: one per branch of avroTypeToScalarType / convertScalarToAvro.
  // ---------------------------------------------------------------------------

  @Test
  void numericRoundTrips() throws Exception {
    assertScalarRoundTrips("42", HoodieSchema.create(HoodieSchemaType.LONG));
  }

  @Test
  void intLeafShredsToInteger() {
    // An integral variant shreds into an INT typed_value as a boxed Integer (Avro has no byte/short,
    // so avroTypeToScalarType only ever emits IntegralSize.INT here).
    assertScalarShredsTo(scalar(b -> b.appendLong(1000)), HoodieSchema.create(HoodieSchemaType.INT), 1000);
  }

  @Test
  void longLeafShredsToLong() {
    assertScalarShredsTo(scalar(b -> b.appendLong(100000)), HoodieSchema.create(HoodieSchemaType.LONG), 100000L);
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
  void stringRoundTrips() throws Exception {
    assertScalarRoundTrips("\"hello world\"", HoodieSchema.create(HoodieSchemaType.STRING));
  }

  @Test
  void booleanRoundTrips() throws Exception {
    assertScalarRoundTrips("true", HoodieSchema.create(HoodieSchemaType.BOOLEAN));
  }

  @Test
  void binaryShredsToByteBuffer() {
    byte[] payload = "not-utf8- ÿ".getBytes(StandardCharsets.ISO_8859_1);
    assertScalarShredsTo(scalar(b -> b.appendBinary(payload)),
        HoodieSchema.create(HoodieSchemaType.BYTES), ByteBuffer.wrap(payload));
  }

  @Test
  void uuidShredsToString() {
    UUID uuid = UUID.fromString("12345678-1234-1234-1234-123456789abc");
    assertScalarShredsTo(scalar(b -> b.appendUuid(uuid)), HoodieSchema.createUUID(), uuid.toString());
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
  void decimalRoundTrips() throws Exception {
    assertScalarRoundTrips("123.45", HoodieSchema.createDecimal(10, 2));
  }

  // ---------------------------------------------------------------------------
  // "Decline to shred" fallbacks: value stays in the residual binary.
  // ---------------------------------------------------------------------------

  @Test
  void millisTimestampIsNotShreddedIntoMicrosLeaf() {
    // A millisecond-precision typed_value cannot represent a micros variant timestamp, so
    // avroTypeToScalarType returns null and the value is left unshredded in the residual.
    assertStaysInResidual(scalar(b -> b.appendTimestamp(1_700_000_000_000_000L)),
        HoodieSchema.createTimestampMillis());
    assertStaysInResidual(scalar(b -> b.appendTimestampNtz(1_700_000_000_000_000L)),
        HoodieSchema.createLocalTimestampMillis());
  }

  @Test
  void fixedLeafShredsBinaryToByteBuffer() {
    // A FIXED typed_value maps to BinaryType, so a binary variant is shredded into typed_value (had
    // avroTypeToScalarType returned null for FIXED, the value would fall to the residual instead).
    byte[] payload = {1, 2, 3, 4};
    assertScalarShredsTo(scalar(b -> b.appendBinary(payload)),
        HoodieSchema.createFixed("fx", "org.apache.hudi.test", null, 4), ByteBuffer.wrap(payload));
  }

  // ---------------------------------------------------------------------------
  // Object and array shapes, including partial (residual) object shredding.
  // ---------------------------------------------------------------------------

  @Test
  void objectRoundTrips() throws Exception {
    Map<String, HoodieSchema> shreddedFields = new LinkedHashMap<>();
    shreddedFields.put("a", HoodieSchema.create(HoodieSchemaType.STRING));
    shreddedFields.put("b", HoodieSchema.create(HoodieSchemaType.LONG));
    assertRoundTrips("{\"a\":\"x\",\"b\":5}", HoodieSchema.createVariantShreddedObject(shreddedFields));
  }

  @Test
  void partialObjectShreddingKeepsExtraFieldsInResidual() throws Exception {
    // Shredded schema declares {a, b} but the variant provides {a, c}: "a" shreds into typed_value,
    // "b" is absent (null value + null typed_value), and the extra "c" lands in the residual value.
    Map<String, HoodieSchema> shreddedFields = new LinkedHashMap<>();
    shreddedFields.put("a", HoodieSchema.create(HoodieSchemaType.STRING));
    shreddedFields.put("b", HoodieSchema.create(HoodieSchemaType.LONG));
    HoodieSchema.Variant shredded = HoodieSchema.createVariantShreddedObject(shreddedFields);

    Variant variant = VariantBuilder.parseJson("{\"a\":\"x\",\"c\":99}", false);
    GenericRecord shreddedRecord = shred(variant, shredded);

    // The unmatched field forces a non-null residual value at the top level.
    assertNotNull(shreddedRecord.get(VARIANT_VALUE_FIELD), "extra field must be captured in residual value");
    GenericRecord typedValue = (GenericRecord) shreddedRecord.get(VARIANT_TYPED_VALUE_FIELD);
    GenericRecord bField = (GenericRecord) typedValue.get("b");
    assertNull(bField.get(VARIANT_VALUE_FIELD), "absent field b carries no residual value");
    assertNull(bField.get(VARIANT_TYPED_VALUE_FIELD), "absent field b carries no typed_value");

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
    assertDecimalRebuildsFromEncoding(HoodieSchema.createDecimal(10, 2), false);
  }

  @Test
  void rebuildDecimalFromFixedEncoding() {
    assertDecimalRebuildsFromEncoding(
        HoodieSchema.createDecimal("dec_fixed", "org.apache.hudi.test", null, 10, 2, 8), true);
  }

  private void assertDecimalRebuildsFromEncoding(HoodieSchema decimalType, boolean fixed) {
    BigDecimal value = new BigDecimal("123.45");
    HoodieSchema.Variant shredded = HoodieSchema.createVariantShredded(decimalType);
    GenericRecord shreddedRecord = shred(scalar(b -> b.appendDecimal(value)), shredded);

    Schema tvSchema = shredded.getAvroSchema().getField(VARIANT_TYPED_VALUE_FIELD).schema();
    Conversions.DecimalConversion conversion = new Conversions.DecimalConversion();
    Object encoded = fixed
        ? conversion.toFixed(value, tvSchema, tvSchema.getLogicalType())
        : conversion.toBytes(value, tvSchema, tvSchema.getLogicalType());
    shreddedRecord.put(VARIANT_TYPED_VALUE_FIELD, encoded);

    Variant original = scalar(b -> b.appendDecimal(value));
    assertEquals(original.toJson(ZoneOffset.UTC), rebuild(shreddedRecord, shredded).toJson(ZoneOffset.UTC));
  }

  @Test
  void rebuildDecimalRejectsUnexpectedEncoding() {
    HoodieSchema.Variant shredded = HoodieSchema.createVariantShredded(HoodieSchema.createDecimal(10, 2));
    GenericRecord shreddedRecord = shred(scalar(b -> b.appendDecimal(new BigDecimal("1.00"))), shredded);
    shreddedRecord.put(VARIANT_TYPED_VALUE_FIELD, "not-a-decimal");
    assertThrows(IllegalStateException.class,
        () -> provider.rebuildVariantRecord(shreddedRecord, shredded.getAvroSchema(), unshreddedSchema));
  }

  // ---------------------------------------------------------------------------
  // Null / error guards.
  // ---------------------------------------------------------------------------

  @Test
  void shredReturnsNullWhenValueOrMetadataMissing() {
    HoodieSchema.Variant shredded = HoodieSchema.createVariantShredded(HoodieSchema.create(HoodieSchemaType.LONG));
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
    HoodieSchema.Variant shredded = HoodieSchema.createVariantShredded(HoodieSchema.create(HoodieSchemaType.LONG));
    assertNull(provider.rebuildVariantRecord(null, shredded.getAvroSchema(), unshreddedSchema));
  }

  @Test
  void rebuildThrowsWhenMetadataMissing() {
    HoodieSchema.Variant shredded = HoodieSchema.createVariantShredded(HoodieSchema.create(HoodieSchemaType.LONG));
    GenericRecord shreddedRecord = shred(scalar(b -> b.appendLong(7)), shredded);
    shreddedRecord.put(VARIANT_METADATA_FIELD, null);
    assertThrows(HoodieException.class,
        () -> provider.rebuildVariantRecord(shreddedRecord, shredded.getAvroSchema(), unshreddedSchema));
  }

  private static byte[] toBytes(Object byteBuffer) {
    ByteBuffer buf = ((ByteBuffer) byteBuffer).duplicate();
    byte[] out = new byte[buf.remaining()];
    buf.get(out);
    return out;
  }
}
