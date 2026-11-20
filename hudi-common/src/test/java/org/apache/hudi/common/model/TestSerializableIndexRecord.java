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

package org.apache.hudi.common.model;

import org.apache.hudi.common.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.SerializationUtils;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestSerializableIndexRecord {
  private final Schema schema = SchemaBuilder.record("test")
      .fields().optionalString("field_1").requiredInt("field_2").endRecord();

  @Test
  void testEqualsWithGenericRecord() {
    GenericRecord record1 = new GenericRecordBuilder(schema)
        .set("field_1", "value1")
        .set("field_2", 42)
        .build();

    GenericRecord record2 = new GenericRecordBuilder(schema)
        .set("field_1", "value2")
        .set("field_2", 42)
        .build();

    SerializableIndexedRecord serializableIndexedRecord = SerializableIndexedRecord.createInstance(record1);

    assertEquals(record1, serializableIndexedRecord.getRecord());
    assertEquals(serializableIndexedRecord, record1);

    assertNotEquals(record2, serializableIndexedRecord.getRecord());
    assertNotEquals(serializableIndexedRecord, record2);

    assertNotEquals(serializableIndexedRecord, null);
  }

  @Test
  void testEncodeDecode() throws IOException {
    GenericRecord originalRecord = new GenericRecordBuilder(schema)
        .set("field_1", "value1")
        .set("field_2", 42)
        .build();
    SerializableIndexedRecord serializableIndexedRecord = SerializableIndexedRecord.createInstance(originalRecord);
    byte[] encoded = serializableIndexedRecord.encodeRecord();

    assertEquals(originalRecord, HoodieAvroUtils.bytesToAvro(encoded, schema));
    byte[] reEncoded = serializableIndexedRecord.encodeRecord();
    // a decoded record is encoded again on each call and keeps no encoded copy of itself
    assertNotSame(encoded, reEncoded);
    assertArrayEquals(encoded, reEncoded);
    // similarly decode should not change the underlying record if it is present
    serializableIndexedRecord.decodeRecord(schema);
    assertSame(originalRecord, serializableIndexedRecord.getRecord());

    // serialize and deserialize the record with Kryo by using the SerializationUtils
    byte[] serialized = SerializationUtils.serialize(serializableIndexedRecord);
    SerializableIndexedRecord deserialized = SerializationUtils.deserialize(serialized);
    deserialized.decodeRecord(schema);
    IndexedRecord parsedRecord = deserialized.getRecord();
    assertEquals(originalRecord, parsedRecord);

    // decoding again should not produce a new record
    deserialized.decodeRecord(schema);
    assertSame(parsedRecord, deserialized.getRecord());
  }

  @Test
  void testFromAvroBytes() {
    GenericRecord originalRecord = new GenericRecordBuilder(schema)
        .set("field_1", "value1")
        .set("field_2", 42)
        .build();
    byte[] encoded = HoodieAvroUtils.avroToBytes(originalRecord);
    SerializableIndexedRecord serializableIndexedRecord = SerializableIndexedRecord.fromAvroBytes(schema, encoded);
    assertEquals(originalRecord, serializableIndexedRecord.getRecord());

    assertNull(SerializableIndexedRecord.fromAvroBytes(schema, new byte[0]));
  }

  /**
   * Serializing a decoded record (Kryo, as a Spark shuffle spill does, or Java serialization) must not leave its encoded bytes on
   * it: a buffer of decoded records being written out would otherwise hold each payload twice until the buffer is released.
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void serializingADecodedRecordKeepsOneCopyOfItsPayload(boolean javaSerialization) throws Exception {
    Schema payloadSchema = SchemaBuilder.record("payload").fields().requiredBytes("bytes").endRecord();
    byte[] content = new byte[64 * 1024];
    for (int i = 0; i < content.length; i++) {
      content[i] = (byte) (i % 251 + 1);
    }
    GenericRecord payload = new GenericRecordBuilder(payloadSchema).set("bytes", ByteBuffer.wrap(content)).build();
    SerializableIndexedRecord record = SerializableIndexedRecord.createInstance(payload);

    byte[] serialized = serialize(record, javaSerialization);
    assertFalse(record.holdsEncodedBytes(), "serialization keeps no encoded copy on the decoded record");
    assertSame(payload, record.getRecord(), "the decoded record is kept as it is");
    SerializableIndexedRecord read = deserialize(serialized, javaSerialization);
    read.decodeRecord(payloadSchema);
    assertEquals(ByteBuffer.wrap(content), read.get(0), "the serialized payload remains intact");
  }

  /** A record still in its encoded form writes the bytes it was read from, without decoding them. */
  @Test
  void encodedRecordWritesItsBytesWithoutDecoding() throws Exception {
    GenericRecord original = new GenericRecordBuilder(schema).set("field_1", "value1").set("field_2", 42).build();
    byte[] bytes = HoodieAvroUtils.avroToBytes(original);
    SerializableIndexedRecord encoded = SerializableIndexedRecord.fromAvroBytes(schema, bytes);
    assertSame(bytes, encoded.encodeRecord(), "the bytes it was read from");

    for (boolean javaSerialization : new boolean[] {false, true}) {
      byte[] serialized = serialize(encoded, javaSerialization);
      assertTrue(encoded.holdsEncodedBytes(), "serializing an encoded record does not decode it");
      assertSame(bytes, encoded.encodeRecord());
      SerializableIndexedRecord read = deserialize(serialized, javaSerialization);
      read.decodeRecord(schema);
      assertEquals("value1", read.get("field_1").toString());
      assertEquals(42, read.get("field_2"));
    }
  }

  /** Java serialization round-trips a record larger than one block of the object stream's block data. */
  @Test
  void javaSerializationRoundTripsALargeRecord() throws IOException, ClassNotFoundException {
    Schema payloadSchema = SchemaBuilder.record("payload").fields().requiredBytes("bytes").endRecord();
    byte[] content = new byte[64 * 1024];
    for (int i = 0; i < content.length; i++) {
      content[i] = (byte) (i % 251 + 1);
    }
    GenericRecord payload = new GenericRecordBuilder(payloadSchema).set("bytes", ByteBuffer.wrap(content)).build();

    ByteArrayOutputStream serialized = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(serialized)) {
      out.writeObject(SerializableIndexedRecord.createInstance(payload));
    }
    SerializableIndexedRecord read;
    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(serialized.toByteArray()))) {
      read = (SerializableIndexedRecord) in.readObject();
    }
    read.decodeRecord(payloadSchema);
    ByteBuffer readContent = (ByteBuffer) read.getRecord().get(0);
    byte[] readBytes = new byte[readContent.remaining()];
    readContent.get(readBytes);
    assertArrayEquals(content, readBytes, "every byte of the payload comes back");
  }

  /** A decoded record updated after it was serialized serializes with the update. */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void recordUpdatedAfterSerializationSerializesTheUpdate(boolean javaSerialization) throws Exception {
    GenericRecord original = new GenericRecordBuilder(schema).set("field_1", "value1").set("field_2", 42).build();
    SerializableIndexedRecord record = SerializableIndexedRecord.createInstance(original);
    serialize(record, javaSerialization);

    record.put("field_2", 43);
    SerializableIndexedRecord deserialized = deserialize(serialize(record, javaSerialization), javaSerialization);
    deserialized.decodeRecord(schema);
    assertEquals(43, deserialized.getRecord().get(1), "the second serialization carries the update");
  }

  private byte[] serialize(SerializableIndexedRecord record, boolean javaSerialization) throws IOException {
    if (!javaSerialization) {
      return SerializationUtils.serialize(record);
    }
    ByteArrayOutputStream serialized = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(serialized)) {
      out.writeObject(record);
    }
    return serialized.toByteArray();
  }

  private SerializableIndexedRecord deserialize(byte[] serialized, boolean javaSerialization) throws IOException, ClassNotFoundException {
    if (!javaSerialization) {
      return SerializationUtils.deserialize(serialized);
    }
    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(serialized))) {
      return (SerializableIndexedRecord) in.readObject();
    }
  }
}
