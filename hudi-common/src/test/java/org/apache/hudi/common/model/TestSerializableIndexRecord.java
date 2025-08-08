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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.SerializationUtils;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

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
    // encode should use existing bytes
    assertSame(encoded, reEncoded);
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
}
