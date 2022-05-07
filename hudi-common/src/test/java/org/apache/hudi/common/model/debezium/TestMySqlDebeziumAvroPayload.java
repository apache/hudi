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

import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestMySqlDebeziumAvroPayload {

  private static final String KEY_FIELD_NAME = "Key";

  private Schema avroSchema;

  @BeforeEach
  void setUp() {
    this.avroSchema = Schema.createRecord(Arrays.asList(
        new Schema.Field(KEY_FIELD_NAME, Schema.create(Schema.Type.INT), "", 0),
        new Schema.Field(DebeziumConstants.FLATTENED_OP_COL_NAME, Schema.create(Schema.Type.STRING), "", null),
        new Schema.Field(DebeziumConstants.ADDED_SEQ_COL_NAME, Schema.create(Schema.Type.STRING), "", null)
    ));
  }

  @Test
  public void testInsert() throws IOException {
    GenericRecord insertRecord = createRecord(0, Operation.INSERT, "00001.111");
    MySqlDebeziumAvroPayload payload = new MySqlDebeziumAvroPayload(insertRecord, "00001.111");
    validateRecord(payload.getInsertValue(avroSchema), 0, Operation.INSERT, "00001.111");
  }

  @Test
  public void testPreCombine() {
    GenericRecord insertRecord = createRecord(0, Operation.INSERT, "00002.111");
    MySqlDebeziumAvroPayload insertPayload = new MySqlDebeziumAvroPayload(insertRecord, "00002.111");

    GenericRecord updateRecord = createRecord(0, Operation.UPDATE, "00001.111");
    MySqlDebeziumAvroPayload updatePayload = new MySqlDebeziumAvroPayload(updateRecord, "00001.111");

    GenericRecord deleteRecord = createRecord(0, Operation.DELETE, "00002.11");
    MySqlDebeziumAvroPayload deletePayload = new MySqlDebeziumAvroPayload(deleteRecord, "00002.11");

    assertEquals(insertPayload, insertPayload.preCombine(updatePayload));
    assertEquals(deletePayload, deletePayload.preCombine(updatePayload));
    assertEquals(insertPayload, deletePayload.preCombine(insertPayload));
  }

  @Test
  public void testMergeWithUpdate() throws IOException {
    GenericRecord updateRecord = createRecord(1, Operation.UPDATE, "00002.11");
    MySqlDebeziumAvroPayload payload = new MySqlDebeziumAvroPayload(updateRecord, "00002.11");

    GenericRecord existingRecord = createRecord(1, Operation.INSERT, "00001.111");
    Option<IndexedRecord> mergedRecord = payload.combineAndGetUpdateValue(existingRecord, avroSchema);
    validateRecord(mergedRecord, 1, Operation.UPDATE, "00002.11");

    GenericRecord lateRecord = createRecord(1, Operation.UPDATE, "00000.222");
    payload = new MySqlDebeziumAvroPayload(lateRecord, "00000.222");
    mergedRecord = payload.combineAndGetUpdateValue(existingRecord, avroSchema);
    validateRecord(mergedRecord, 1, Operation.INSERT, "00001.111");
  }

  @Test
  public void testMergeWithDelete() throws IOException {
    GenericRecord deleteRecord = createRecord(2, Operation.DELETE, "00002.11");
    MySqlDebeziumAvroPayload payload = new MySqlDebeziumAvroPayload(deleteRecord, "00002.11");

    GenericRecord existingRecord = createRecord(2, Operation.UPDATE, "00001.111");
    Option<IndexedRecord> mergedRecord = payload.combineAndGetUpdateValue(existingRecord, avroSchema);
    // expect nothing to be committed to table
    assertFalse(mergedRecord.isPresent());

    GenericRecord lateRecord = createRecord(2, Operation.DELETE, "00000.222");
    payload = new MySqlDebeziumAvroPayload(lateRecord, "00000.222");
    mergedRecord = payload.combineAndGetUpdateValue(existingRecord, avroSchema);
    validateRecord(mergedRecord, 2, Operation.UPDATE, "00001.111");
  }

  private GenericRecord createRecord(int primaryKeyValue, Operation op, String seqValue) {
    GenericRecord record = new GenericData.Record(avroSchema);
    record.put(KEY_FIELD_NAME, primaryKeyValue);
    record.put(DebeziumConstants.FLATTENED_OP_COL_NAME, op.op);
    record.put(DebeziumConstants.ADDED_SEQ_COL_NAME, seqValue);
    return record;
  }

  private void validateRecord(Option<IndexedRecord> iRecord, int primaryKeyValue, Operation op, String seqValue) {
    IndexedRecord record = iRecord.get();
    assertEquals(primaryKeyValue, (int) record.get(0));
    assertEquals(op.op, record.get(1).toString());
    assertEquals(seqValue, record.get(2).toString());
  }

  private enum Operation {
    INSERT("c"),
    UPDATE("u"),
    DELETE("d");

    public final String op;

    Operation(String op) {
      this.op = op;
    }
  }
}
