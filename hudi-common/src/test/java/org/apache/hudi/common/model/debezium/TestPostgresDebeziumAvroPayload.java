/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.model.debezium;

import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestPostgresDebeziumAvroPayload {

  private static final String AVRO_SCHEMA_STRING = "{\"type\": \"record\","
      + "\"name\": \"events\"," + "\"fields\": [ "
      + "{\"name\": \"Key\", \"type\" : \"int\"},"
      + "{\"name\": \"_change_operation_type\", \"type\": \"string\"},"
      + "{\"name\": \"_event_lsn\", \"type\" : \"long\"}"
      + "]}";

  @Test
  public void testInsert() throws IOException {
    Schema avroSchema = new Schema.Parser().parse(AVRO_SCHEMA_STRING);
    GenericRecord record = new GenericData.Record(avroSchema);
    record.put("Key", 0);
    record.put(DebeziumConstants.MODIFIED_OP_COL_NAME, "c");
    record.put(DebeziumConstants.MODIFIED_LSN_COL_NAME, 100L);

    PostgresDebeziumAvroPayload payload = new PostgresDebeziumAvroPayload(record, 100L);

    Option<IndexedRecord> outputPayload = payload.getInsertValue(avroSchema);
    assertTrue((int) outputPayload.get().get(0) == 0);
    assertTrue(outputPayload.get().get(1).toString().equals("c"));
    assertTrue((long) outputPayload.get().get(2) == 100L);
  }

  @Test
  public void testUpdate() throws Exception {
    Schema avroSchema = new Schema.Parser().parse(AVRO_SCHEMA_STRING);
    GenericRecord newRecord = new GenericData.Record(avroSchema);
    newRecord.put("Key", 1);
    newRecord.put(DebeziumConstants.MODIFIED_OP_COL_NAME, "u");
    newRecord.put(DebeziumConstants.MODIFIED_LSN_COL_NAME, 100L);

    GenericRecord oldRecord = new GenericData.Record(avroSchema);
    oldRecord.put("Key", 0);
    oldRecord.put(DebeziumConstants.MODIFIED_OP_COL_NAME, "c");
    oldRecord.put(DebeziumConstants.MODIFIED_LSN_COL_NAME, 99L);

    PostgresDebeziumAvroPayload payload = new PostgresDebeziumAvroPayload(newRecord, 100L);

    Option<IndexedRecord> outputPayload = payload.combineAndGetUpdateValue(oldRecord, avroSchema);
    assertTrue((int) outputPayload.get().get(0) == 1);
    assertTrue(outputPayload.get().get(1).toString().equals("u"));
    assertTrue((long) outputPayload.get().get(2) == 100L);

    GenericRecord lateRecord = new GenericData.Record(avroSchema);
    lateRecord.put("Key", 1);
    lateRecord.put(DebeziumConstants.MODIFIED_OP_COL_NAME, "u");
    lateRecord.put(DebeziumConstants.MODIFIED_LSN_COL_NAME, 98L);
    payload = new PostgresDebeziumAvroPayload(lateRecord, 98L);
    outputPayload = payload.combineAndGetUpdateValue(oldRecord, avroSchema);
    assertTrue((int) outputPayload.get().get(0) == 0);
    assertTrue(outputPayload.get().get(1).toString().equals("c"));
    assertTrue((long) outputPayload.get().get(2) == 99L);
  }

  @Test
  public void testDelete() throws Exception {
    Schema avroSchema = new Schema.Parser().parse(AVRO_SCHEMA_STRING);
    GenericRecord deleteRecord = new GenericData.Record(avroSchema);
    deleteRecord.put("Key", 2);
    deleteRecord.put(DebeziumConstants.MODIFIED_OP_COL_NAME, "d");
    deleteRecord.put(DebeziumConstants.MODIFIED_LSN_COL_NAME, 100L);

    GenericRecord oldRecord = new GenericData.Record(avroSchema);
    oldRecord.put("Key", 3);
    oldRecord.put(DebeziumConstants.MODIFIED_OP_COL_NAME, "u");
    oldRecord.put(DebeziumConstants.MODIFIED_LSN_COL_NAME, 99L);

    PostgresDebeziumAvroPayload payload = new PostgresDebeziumAvroPayload(deleteRecord, 100L);

    Option<IndexedRecord> outputPayload = payload.combineAndGetUpdateValue(oldRecord, avroSchema);
    // expect nothing to be committed to table
    assertFalse(outputPayload.isPresent());

    GenericRecord lateRecord = new GenericData.Record(avroSchema);
    lateRecord.put("Key", 1);
    lateRecord.put(DebeziumConstants.MODIFIED_OP_COL_NAME, "d");
    lateRecord.put(DebeziumConstants.MODIFIED_LSN_COL_NAME, 98L);
    payload = new PostgresDebeziumAvroPayload(lateRecord, 98L);
    outputPayload = payload.combineAndGetUpdateValue(oldRecord, avroSchema);
    assertTrue((int) outputPayload.get().get(0) == 3);
    assertTrue(outputPayload.get().get(1).toString().equals("u"));
    assertTrue((long) outputPayload.get().get(2) == 99L);
  }

  @Test
  public void testMergeWithToastedValues() throws IOException {
    Schema avroSchema = SchemaBuilder.builder()
        .record("test_schema")
        .namespace("test_namespace")
        .fields()
        .name(DebeziumConstants.MODIFIED_LSN_COL_NAME).type().longType().noDefault()
        .name("string_col").type().stringType().noDefault()
        .name("byte_col").type().bytesType().noDefault()
        .name("string_null_col_1").type().nullable().stringType().noDefault()
        .name("byte_null_col_1").type().nullable().bytesType().noDefault()
        .name("string_null_col_2").type().nullable().stringType().noDefault()
        .name("byte_null_col_2").type().nullable().bytesType().noDefault()
        .endRecord();

    GenericRecord oldVal = new GenericData.Record(avroSchema);
    oldVal.put(DebeziumConstants.MODIFIED_LSN_COL_NAME, 100L);
    oldVal.put("string_col", "valid string value");
    oldVal.put("byte_col", ByteBuffer.wrap("valid byte value".getBytes()));
    oldVal.put("string_null_col_1", "valid string value");
    oldVal.put("byte_null_col_1", ByteBuffer.wrap("valid byte value".getBytes()));
    oldVal.put("string_null_col_2", null);
    oldVal.put("byte_null_col_2", null);

    GenericRecord newVal = new GenericData.Record(avroSchema);
    newVal.put(DebeziumConstants.MODIFIED_LSN_COL_NAME, 105L);
    newVal.put("string_col", PostgresDebeziumAvroPayload.DEBEZIUM_TOASTED_VALUE);
    newVal.put("byte_col", ByteBuffer.wrap(PostgresDebeziumAvroPayload.DEBEZIUM_TOASTED_VALUE.getBytes()));
    newVal.put("string_null_col_1", null);
    newVal.put("byte_null_col_1", null);
    newVal.put("string_null_col_2", "valid string value");
    newVal.put("byte_null_col_2", ByteBuffer.wrap("valid byte value".getBytes()));

    PostgresDebeziumAvroPayload payload = new PostgresDebeziumAvroPayload(Option.of(newVal));

    GenericRecord outputRecord = (GenericRecord) payload
        .combineAndGetUpdateValue(oldVal, avroSchema).get();

    assertEquals("valid string value", outputRecord.get("string_col"));
    assertEquals("valid byte value", new String(((ByteBuffer) outputRecord.get("byte_col")).array(), StandardCharsets.UTF_8));
    assertEquals(null, outputRecord.get("string_null_col_1"));
    assertEquals(null, outputRecord.get("byte_null_col_1"));
    assertEquals("valid string value", ((Utf8) outputRecord.get("string_null_col_2")).toString());
    assertEquals("valid byte value", new String(((ByteBuffer) outputRecord.get("byte_null_col_2")).array(), StandardCharsets.UTF_8));
  }
}
