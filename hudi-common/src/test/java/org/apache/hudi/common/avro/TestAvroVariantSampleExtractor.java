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

import org.apache.hudi.common.avro.VariantShreddingSchemaInferrer.VariantSample;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Properties;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

public class TestAvroVariantSampleExtractor {

  private static final HoodieSchema SCHEMA = HoodieSchema.createRecord("rec", "ns", null, Arrays.asList(
      HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING)),
      HoodieSchemaField.of("v", HoodieSchema.createNullable(HoodieSchema.createVariant()))));
  private static final Properties PROPS = new Properties();
  private static final HoodieKey KEY = new HoodieKey("r1", "p");

  private static GenericRecord variant(Object value, Object metadata) {
    HoodieSchema variantSchema = SCHEMA.getField("v").get().schema().getNonNullType();
    GenericRecord record = new GenericData.Record(variantSchema.toAvroSchema());
    record.put(HoodieSchema.Variant.VARIANT_VALUE_FIELD, value);
    record.put(HoodieSchema.Variant.VARIANT_METADATA_FIELD, metadata);
    return record;
  }

  private static GenericRecord row(GenericRecord variantValue) {
    GenericRecord record = new GenericData.Record(SCHEMA.toAvroSchema());
    record.put("id", "r1");
    record.put("v", variantValue);
    return record;
  }

  @Test
  public void testExtractsDefensiveCopiesOfByteBufferAndByteArrayPayloads() throws IOException {
    AvroVariantSampleExtractor extractor = new AvroVariantSampleExtractor(singletonList("v"));

    // ByteBuffer payloads, the usual Avro representation of bytes.
    byte[] valueBytes = {1, 2};
    byte[] metadataBytes = {3};
    VariantSample[] fromBuffers = extractor.extract(
        new HoodieAvroIndexedRecord(KEY, row(variant(ByteBuffer.wrap(valueBytes), ByteBuffer.wrap(metadataBytes)))),
        SCHEMA, PROPS);
    assertEquals(1, fromBuffers.length);
    assertArrayEquals(new byte[] {1, 2}, fromBuffers[0].getValue());
    assertArrayEquals(new byte[] {3}, fromBuffers[0].getMetadata());
    // Samples must not alias the record's backing arrays: they outlive the buffered record.
    valueBytes[0] = 9;
    metadataBytes[0] = 9;
    assertArrayEquals(new byte[] {1, 2}, fromBuffers[0].getValue());
    assertArrayEquals(new byte[] {3}, fromBuffers[0].getMetadata());

    // Raw byte[] payloads are accepted too.
    byte[] rawValue = {4};
    byte[] rawMetadata = {5};
    VariantSample[] fromArrays = extractor.extract(
        new HoodieAvroIndexedRecord(KEY, row(variant(rawValue, rawMetadata))), SCHEMA, PROPS);
    rawValue[0] = 9;
    assertArrayEquals(new byte[] {4}, fromArrays[0].getValue());
    assertArrayEquals(new byte[] {5}, fromArrays[0].getMetadata());
  }

  @Test
  public void testAbsentColumnNullVariantAndMissingMemberYieldNullSamples() throws IOException {
    // "w" is not in the record's own schema (per-call schemas can differ from the writer schema);
    // it is skipped rather than failing the write.
    AvroVariantSampleExtractor extractor = new AvroVariantSampleExtractor(Arrays.asList("v", "w"));

    VariantSample[] nullVariant = extractor.extract(new HoodieAvroIndexedRecord(KEY, row(null)), SCHEMA, PROPS);
    assertEquals(2, nullVariant.length);
    assertNull(nullVariant[0]);
    assertNull(nullVariant[1]);

    // A variant record without a value payload contributes nothing either.
    VariantSample[] missingValue = extractor.extract(
        new HoodieAvroIndexedRecord(KEY, row(variant(null, ByteBuffer.wrap(new byte[] {1})))), SCHEMA, PROPS);
    assertNull(missingValue[0]);
  }

  @Test
  public void testPrepareMaterializesPayloadOnceAndPassesIndexedRecordsThrough() throws IOException {
    AvroVariantSampleExtractor extractor = new AvroVariantSampleExtractor(singletonList("v"));
    GenericRecord data = row(variant(ByteBuffer.wrap(new byte[] {1}), ByteBuffer.wrap(new byte[] {2})));

    // A payload-backed record is deserialized by prepare(); the result is what the decorator
    // buffers and replays, so the writer's own toIndexedRecord at replay is a pass-through.
    HoodieRecord payloadBacked = new HoodieAvroRecord<>(KEY, new HoodieAvroPayload(Option.of(data)));
    HoodieRecord prepared = extractor.prepare(payloadBacked, SCHEMA, PROPS);
    assertInstanceOf(HoodieAvroIndexedRecord.class, prepared);
    assertSame(prepared, extractor.prepare(prepared, SCHEMA, PROPS), "already materialized: identity");
    assertNotNull(extractor.extract(prepared, SCHEMA, PROPS)[0]);
    assertArrayEquals(new byte[] {1}, extractor.extract(prepared, SCHEMA, PROPS)[0].getValue());

    // A record without data (delete payload) has nothing to materialize: buffered as it is, no sample.
    HoodieRecord delete = new HoodieAvroRecord<>(KEY, new HoodieAvroPayload(Option.empty()));
    assertSame(delete, extractor.prepare(delete, SCHEMA, PROPS));
    assertNull(extractor.extract(delete, SCHEMA, PROPS)[0]);
  }
}
