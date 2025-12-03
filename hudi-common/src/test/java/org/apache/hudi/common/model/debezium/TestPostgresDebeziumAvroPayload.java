/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or mo contributor license agreements.  See the NOTICE file
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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieDebeziumAvroPayloadException;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;

import static org.apache.hudi.common.util.StringUtils.fromUTF8Bytes;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link PostgresDebeziumAvroPayload}.
 */
public class TestPostgresDebeziumAvroPayload {

  private static final String KEY_FIELD_NAME = "Key";
  private Schema avroSchema;

  @BeforeEach
  void setUp() {
    this.avroSchema = Schema.createRecord(Arrays.asList(
        new Schema.Field(KEY_FIELD_NAME, Schema.create(Schema.Type.INT), "", 0),
        new Schema.Field(DebeziumConstants.FLATTENED_OP_COL_NAME,
            Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)), "", null),
        new Schema.Field(DebeziumConstants.FLATTENED_LSN_COL_NAME,
            Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG)), "", null)
    ));
  }

  @Test
  public void testInsert() throws IOException {
    GenericRecord insertRecord = createRecord(0, Operation.INSERT, 100L);
    PostgresDebeziumAvroPayload payload = new PostgresDebeziumAvroPayload(insertRecord, 100L);
    validateRecord(payload.getInsertValue(avroSchema), 0, Operation.INSERT, 100L);
  }

  @Test
  public void testPreCombine() {
    GenericRecord insertRecord = createRecord(0, Operation.INSERT, 120L);
    PostgresDebeziumAvroPayload insertPayload = new PostgresDebeziumAvroPayload(insertRecord, 120L);

    GenericRecord updateRecord = createRecord(0, Operation.UPDATE, 99L);
    PostgresDebeziumAvroPayload updatePayload = new PostgresDebeziumAvroPayload(updateRecord, 99L);

    GenericRecord deleteRecord = createRecord(0, Operation.DELETE, 111L);
    PostgresDebeziumAvroPayload deletePayload = new PostgresDebeziumAvroPayload(deleteRecord, 111L);

    assertEquals(insertPayload, insertPayload.preCombine(updatePayload));
    assertEquals(deletePayload, deletePayload.preCombine(updatePayload));
    assertEquals(insertPayload, deletePayload.preCombine(insertPayload));
  }

  @Test
  public void testMergeWithUpdate() throws IOException {
    GenericRecord updateRecord = createRecord(1, Operation.UPDATE, 100L);
    PostgresDebeziumAvroPayload payload = new PostgresDebeziumAvroPayload(updateRecord, 100L);

    GenericRecord existingRecord = createRecord(1, Operation.INSERT, 99L);
    Option<IndexedRecord> mergedRecord = payload.combineAndGetUpdateValue(existingRecord, avroSchema);
    validateRecord(mergedRecord, 1, Operation.UPDATE, 100L);

    GenericRecord lateRecord = createRecord(1, Operation.UPDATE, 98L);
    payload = new PostgresDebeziumAvroPayload(lateRecord, 98L);
    mergedRecord = payload.combineAndGetUpdateValue(existingRecord, avroSchema);
    validateRecord(mergedRecord, 1, Operation.INSERT, 99L);
  }

  @Test
  public void testMergeWithDelete() throws IOException {
    GenericRecord deleteRecord = createRecord(2, Operation.DELETE, 100L);
    PostgresDebeziumAvroPayload payload = new PostgresDebeziumAvroPayload(deleteRecord, 100L);
    assertTrue(payload.isDeleted(avroSchema, new Properties()));

    GenericRecord existingRecord = createRecord(2, Operation.UPDATE, 99L);
    Option<IndexedRecord> mergedRecord = payload.combineAndGetUpdateValue(existingRecord, avroSchema);
    // expect nothing to be committed to table
    assertFalse(mergedRecord.isPresent());

    GenericRecord lateRecord = createRecord(2, Operation.DELETE, 98L);
    payload = new PostgresDebeziumAvroPayload(lateRecord, 98L);
    mergedRecord = payload.combineAndGetUpdateValue(existingRecord, avroSchema);
    validateRecord(mergedRecord, 2, Operation.UPDATE, 99L);
  }

  @Test
  public void testMergeWithDeleteUsingEmptyRecord() throws IOException {
    // empty record being merged with current record.
    HoodieRecord<PostgresDebeziumAvroPayload> emptyRecord = new HoodieAvroRecord(new HoodieKey(), new PostgresDebeziumAvroPayload(Option.empty()));
    GenericRecord existingRecord = createRecord(2, Operation.UPDATE, 99L);
    Option<IndexedRecord> mergedRecord = emptyRecord.getData().combineAndGetUpdateValue(existingRecord, avroSchema, new TypedProperties());
    // expect nothing to be committed to table
    assertFalse(mergedRecord.isPresent());
    // Insert record being merged with empty record.
    GenericRecord insertedRecord = createRecord(1, Operation.INSERT, 100L);
    PostgresDebeziumAvroPayload insertPayload = new PostgresDebeziumAvroPayload(insertedRecord, 100L);
    PostgresDebeziumAvroPayload combinedPayload = (PostgresDebeziumAvroPayload) insertPayload.preCombine(emptyRecord.getData(), avroSchema, new TypedProperties());
    assertEquals(insertPayload, combinedPayload);
  }

  @Test
  public void testMergeWithBootstrappedExistingRecords() throws IOException {
    GenericRecord incomingRecord = createRecord(3, Operation.UPDATE, 100L);
    PostgresDebeziumAvroPayload payload = new PostgresDebeziumAvroPayload(incomingRecord, 100L);

    GenericRecord existingRecord = createRecord(3, null, null);
    Option<IndexedRecord> mergedRecord = payload.combineAndGetUpdateValue(existingRecord, avroSchema);
    validateRecord(mergedRecord, 3, Operation.UPDATE, 100L);
  }

  @Test
  public void testInvalidIncomingRecord() {
    GenericRecord incomingRecord = createRecord(4, null, null);
    PostgresDebeziumAvroPayload payload = new PostgresDebeziumAvroPayload(incomingRecord, 100L);

    GenericRecord existingRecord = createRecord(4, Operation.INSERT, 99L);
    assertThrows(HoodieDebeziumAvroPayloadException.class,
        () -> payload.combineAndGetUpdateValue(existingRecord, avroSchema),
        "should have thrown because LSN value of the incoming record is null");
  }

  @Test
  public void testMergeWithToastedValues() throws IOException {
    Schema avroSchema = SchemaBuilder.builder()
        .record("test_schema")
        .namespace("test_namespace")
        .fields()
        .name(DebeziumConstants.FLATTENED_LSN_COL_NAME).type().longType().noDefault()
        .name("string_col").type().stringType().noDefault()
        .name("byte_col").type().bytesType().noDefault()
        .name("string_null_col_1").type().nullable().stringType().noDefault()
        .name("byte_null_col_1").type().nullable().bytesType().noDefault()
        .name("string_null_col_2").type().nullable().stringType().noDefault()
        .name("byte_null_col_2").type().nullable().bytesType().noDefault()
        .endRecord();

    GenericRecord oldVal = new GenericData.Record(avroSchema);
    oldVal.put(DebeziumConstants.FLATTENED_LSN_COL_NAME, 100L);
    oldVal.put("string_col", "valid string value");
    oldVal.put("byte_col", ByteBuffer.wrap(getUTF8Bytes("valid byte value")));
    oldVal.put("string_null_col_1", "valid string value");
    oldVal.put("byte_null_col_1", ByteBuffer.wrap(getUTF8Bytes("valid byte value")));
    oldVal.put("string_null_col_2", null);
    oldVal.put("byte_null_col_2", null);

    GenericRecord newVal = new GenericData.Record(avroSchema);
    newVal.put(DebeziumConstants.FLATTENED_LSN_COL_NAME, 105L);
    newVal.put("string_col", PostgresDebeziumAvroPayload.DEBEZIUM_TOASTED_VALUE);
    newVal.put("byte_col", ByteBuffer.wrap(getUTF8Bytes(PostgresDebeziumAvroPayload.DEBEZIUM_TOASTED_VALUE)));
    newVal.put("string_null_col_1", null);
    newVal.put("byte_null_col_1", null);
    newVal.put("string_null_col_2", "valid string value");
    newVal.put("byte_null_col_2", ByteBuffer.wrap(getUTF8Bytes("valid byte value")));

    PostgresDebeziumAvroPayload payload = new PostgresDebeziumAvroPayload(Option.of(newVal));

    GenericRecord outputRecord = (GenericRecord) payload
        .combineAndGetUpdateValue(oldVal, avroSchema).get();

    assertEquals("valid string value", outputRecord.get("string_col"));
    assertEquals("valid byte value", fromUTF8Bytes(((ByteBuffer) outputRecord.get("byte_col")).array()));
    assertNull(outputRecord.get("string_null_col_1"));
    assertNull(outputRecord.get("byte_null_col_1"));
    assertEquals("valid string value", outputRecord.get("string_null_col_2").toString());
    assertEquals("valid byte value", fromUTF8Bytes(((ByteBuffer) outputRecord.get("byte_null_col_2")).array()));
  }

  private GenericRecord createRecord(int primaryKeyValue, @Nullable Operation op, @Nullable Long lsnValue) {
    GenericRecord record = new GenericData.Record(avroSchema);
    record.put(KEY_FIELD_NAME, primaryKeyValue);
    record.put(DebeziumConstants.FLATTENED_OP_COL_NAME, Objects.toString(op, null));
    record.put(DebeziumConstants.FLATTENED_LSN_COL_NAME, lsnValue);
    return record;
  }

  private void validateRecord(Option<IndexedRecord> iRecord, int primaryKeyValue, Operation op, long lsnValue) {
    IndexedRecord record = iRecord.get();
    assertEquals(primaryKeyValue, (int) record.get(0));
    assertEquals(op.op, record.get(1).toString());
    assertEquals(lsnValue, (long) record.get(2));
  }

  private enum Operation {
    INSERT("c"),
    UPDATE("u"),
    DELETE("d");

    public final String op;

    Operation(String op) {
      this.op = op;
    }

    @Override
    public String toString() {
      return op;
    }
  }
}
