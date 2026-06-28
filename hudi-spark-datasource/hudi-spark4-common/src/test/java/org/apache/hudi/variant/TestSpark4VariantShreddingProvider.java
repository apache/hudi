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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.types.variant.Variant;
import org.apache.spark.types.variant.VariantBuilder;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Round-trip coverage for {@link Spark4VariantShreddingProvider}: shred an unshredded variant, then
 * reconstruct it, and assert it round-trips. This exercises {@code rebuildVariantRecord} and the
 * {@code AvroVariantRow}/{@code AvroObjectRow}/{@code AvroArrayRow} accessors across scalar, object,
 * and array shapes - the AVRO read-path reconstruction (#18931) that the Spark MOR SQL test cannot
 * reach (Spark compaction reads base files via the InternalRow reader, not HoodieAvroParquetReader).
 */
class TestSpark4VariantShreddingProvider {

  private final Spark4VariantShreddingProvider provider = new Spark4VariantShreddingProvider();
  private final Schema unshreddedSchema = HoodieSchema.createVariant().getAvroSchema();

  /** Parse json to a variant, shred it to {@code shredded}, rebuild it, assert the json round-trips. */
  private void assertRoundTrips(String json, HoodieSchema.Variant shredded) throws Exception {
    Variant variant = VariantBuilder.parseJson(json, false);
    GenericRecord unshreddedRecord = new GenericData.Record(unshreddedSchema);
    unshreddedRecord.put(HoodieSchema.Variant.VARIANT_METADATA_FIELD, ByteBuffer.wrap(variant.getMetadata()));
    unshreddedRecord.put(HoodieSchema.Variant.VARIANT_VALUE_FIELD, ByteBuffer.wrap(variant.getValue()));

    Schema shreddedSchema = shredded.getAvroSchema();
    GenericRecord shreddedRecord = provider.shredVariantRecord(unshreddedRecord, shreddedSchema, shredded);
    GenericRecord rebuilt = provider.rebuildVariantRecord(shreddedRecord, shreddedSchema, unshreddedSchema);

    Variant rebuiltVariant = new Variant(
        toBytes(rebuilt.get(HoodieSchema.Variant.VARIANT_VALUE_FIELD)),
        toBytes(rebuilt.get(HoodieSchema.Variant.VARIANT_METADATA_FIELD)));
    assertEquals(variant.toJson(ZoneOffset.UTC), rebuiltVariant.toJson(ZoneOffset.UTC),
        "variant did not round-trip through shred/rebuild for: " + json);
  }

  private void assertScalarRoundTrips(String json, HoodieSchema typedValue) throws Exception {
    assertRoundTrips(json, HoodieSchema.createVariantShredded(typedValue));
  }

  @Test
  void numericRoundTrips() throws Exception {
    assertScalarRoundTrips("42", HoodieSchema.create(HoodieSchemaType.LONG));
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
  void decimalRoundTrips() throws Exception {
    assertScalarRoundTrips("123.45", HoodieSchema.createDecimal(10, 2));
  }

  @Test
  void objectRoundTrips() throws Exception {
    Map<String, HoodieSchema> shreddedFields = new LinkedHashMap<>();
    shreddedFields.put("a", HoodieSchema.create(HoodieSchemaType.STRING));
    shreddedFields.put("b", HoodieSchema.create(HoodieSchemaType.LONG));
    assertRoundTrips("{\"a\":\"x\",\"b\":5}", HoodieSchema.createVariantShreddedObject(shreddedFields));
  }

  @Test
  void arrayRoundTrips() throws Exception {
    // typed_value for an array is array<{value, typed_value}>: each element is itself a shredded struct.
    HoodieSchema element = HoodieSchema.createRecord("v_array_element", "org.apache.hudi.test", null, Arrays.asList(
        HoodieSchemaField.of(HoodieSchema.Variant.VARIANT_VALUE_FIELD, HoodieSchema.createNullable(HoodieSchemaType.BYTES)),
        HoodieSchemaField.of(HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD, HoodieSchema.create(HoodieSchemaType.LONG))));
    assertScalarRoundTrips("[1,2,3]", HoodieSchema.createArray(element));
  }

  private static byte[] toBytes(Object byteBuffer) {
    ByteBuffer buf = ((ByteBuffer) byteBuffer).duplicate();
    byte[] out = new byte[buf.remaining()];
    buf.get(out);
    return out;
  }
}
