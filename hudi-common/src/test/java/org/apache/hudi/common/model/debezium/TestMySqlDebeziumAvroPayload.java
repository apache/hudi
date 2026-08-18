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
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.exception.HoodieDebeziumAvroPayloadException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link MySqlDebeziumAvroPayload}.
 */
public class TestMySqlDebeziumAvroPayload {

  private static final String KEY_FIELD_NAME = "Key";

  private Schema avroSchema;

  @BeforeEach
  void setUp() {
    this.avroSchema = Schema.createRecord(Arrays.asList(
        new Schema.Field(KEY_FIELD_NAME, Schema.create(Schema.Type.INT), "", 0),
        new Schema.Field(DebeziumConstants.FLATTENED_OP_COL_NAME,
            Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)), "", null),
        new Schema.Field(DebeziumConstants.ADDED_SEQ_COL_NAME,
            Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)), "", null)
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
    GenericRecord insertRecord = createRecord(0, Operation.INSERT, "00002.123");
    MySqlDebeziumAvroPayload insertPayload = new MySqlDebeziumAvroPayload(insertRecord, "00002.123");

    GenericRecord updateRecord = createRecord(0, Operation.UPDATE, "00001.111");
    MySqlDebeziumAvroPayload updatePayload = new MySqlDebeziumAvroPayload(updateRecord, "00001.111");

    GenericRecord deleteRecord = createRecord(0, Operation.DELETE, "00002.23");
    MySqlDebeziumAvroPayload deletePayload = new MySqlDebeziumAvroPayload(deleteRecord, "00002.23");

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

    GenericRecord originalRecord = createRecord(1, Operation.INSERT, "00000.23");
    payload = new MySqlDebeziumAvroPayload(originalRecord, "00000.23");
    updateRecord = createRecord(1, Operation.UPDATE, "00000.123");
    mergedRecord = payload.combineAndGetUpdateValue(updateRecord, avroSchema);
    validateRecord(mergedRecord, 1, Operation.UPDATE, "00000.123");
  }

  @Test
  public void testMergeWithDelete() throws IOException {
    GenericRecord deleteRecord = createRecord(2, Operation.DELETE, "00002.11");
    MySqlDebeziumAvroPayload payload = new MySqlDebeziumAvroPayload(deleteRecord, "00002.11");
    assertTrue(payload.isDeleted(avroSchema, new Properties()));

    GenericRecord existingRecord = createRecord(2, Operation.UPDATE, "00001.111");
    Option<IndexedRecord> mergedRecord = payload.combineAndGetUpdateValue(existingRecord, avroSchema);
    // expect nothing to be committed to table
    assertFalse(mergedRecord.isPresent());

    GenericRecord lateRecord = createRecord(2, Operation.DELETE, "00000.222");
    payload = new MySqlDebeziumAvroPayload(lateRecord, "00000.222");
    mergedRecord = payload.combineAndGetUpdateValue(existingRecord, avroSchema);
    validateRecord(mergedRecord, 2, Operation.UPDATE, "00001.111");
  }

  @Test
  public void testMergeWithBootstrappedExistingRecords() throws IOException {
    GenericRecord incomingRecord = createRecord(3, Operation.UPDATE, "00002.111");
    MySqlDebeziumAvroPayload payload = new MySqlDebeziumAvroPayload(incomingRecord, "00002.111");

    GenericRecord existingRecord = createRecord(3, null, null);
    Option<IndexedRecord> mergedRecord = payload.combineAndGetUpdateValue(existingRecord, avroSchema);
    validateRecord(mergedRecord, 3, Operation.UPDATE, "00002.111");
  }

  @Test
  public void testInvalidIncomingRecord() {
    GenericRecord incomingRecord = createRecord(4, null, null);
    MySqlDebeziumAvroPayload payload = new MySqlDebeziumAvroPayload(incomingRecord, "00002.111");

    GenericRecord existingRecord = createRecord(4, Operation.INSERT, "00001.111");
    assertThrows(HoodieDebeziumAvroPayloadException.class,
        () -> payload.combineAndGetUpdateValue(existingRecord, avroSchema),
        "should have thrown because event seq value of the incoming record is null");
  }

  @ParameterizedTest
  @CsvSource({
      // Different file numbers - current file is latest
      "'00002.100', '00001.200', true",
      // Different file numbers - new file is latest
      "'00001.200', '00002.100', false",
      // Same file number, position comparison
      "'00001.100', '00001.50', true",
      "'00000.23', '00000.123', false",
      "'00000.1', '00000.10', false",
      "'00000.10', '00000.1', true",
      // Same file number and position - should pick the incoming new record
      "'00001.100', '00001.100', false"})
  public void testIsCurrentSeqLatest(String currentSeq, String newSeq, boolean expectedResult) {
    assertEquals(expectedResult, MySqlDebeziumAvroPayload.isCurrentSeqLatest(currentSeq, newSeq));
  }

  @Test
  public void testMergeWithConfiguredOrderingFieldStoredRecordWins() throws IOException {
    Schema schema = createSchemaWithOrderingField();
    // Incoming has a HIGHER seq but a LOWER configured ordering value -> stored record must win,
    // proving the configured field (not the hardcoded seq) decides.
    MySqlDebeziumAvroPayload payload = new MySqlDebeziumAvroPayload(createRecordWithOrdering(schema, 1, Operation.UPDATE, "00005.100", 50L), 50L);
    GenericRecord existing = createRecordWithOrdering(schema, 1, Operation.INSERT, "00001.100", 99L);
    Option<IndexedRecord> merged = payload.combineAndGetUpdateValue(existing, schema, orderingProps("event_ts"));
    DebeziumOrderingTestFixtures.validateOrderingRecord(merged, 1, Operation.INSERT, DebeziumConstants.ADDED_SEQ_COL_NAME, "00001.100", 99L);
  }

  @Test
  public void testMergeWithConfiguredOrderingFieldIncomingRecordWins() throws IOException {
    Schema schema = createSchemaWithOrderingField();
    // Incoming has a LOWER seq but a HIGHER configured ordering value -> incoming wins.
    MySqlDebeziumAvroPayload payload = new MySqlDebeziumAvroPayload(createRecordWithOrdering(schema, 1, Operation.UPDATE, "00000.100", 120L), 120L);
    GenericRecord existing = createRecordWithOrdering(schema, 1, Operation.INSERT, "00001.100", 99L);
    Option<IndexedRecord> merged = payload.combineAndGetUpdateValue(existing, schema, orderingProps("event_ts"));
    DebeziumOrderingTestFixtures.validateOrderingRecord(merged, 1, Operation.UPDATE, DebeziumConstants.ADDED_SEQ_COL_NAME, "00000.100", 120L);
  }

  @Test
  public void testMergeWithWhitespacePaddedConfiguredOrderingFieldIsTrimmed() throws IOException {
    Schema schema = createSchemaWithOrderingField();
    // The other trimming direction: a padded NON-connector field must route to the configured-field
    // compare. Without the trim, " event_ts " resolves no field (null ordering value) and throws,
    // so this asserts the trim rather than accidentally passing through the connector path.
    MySqlDebeziumAvroPayload payload = new MySqlDebeziumAvroPayload(createRecordWithOrdering(schema, 1, Operation.UPDATE, "00005.100", 50L), 50L);
    GenericRecord existing = createRecordWithOrdering(schema, 1, Operation.INSERT, "00001.100", 99L);
    Option<IndexedRecord> merged = payload.combineAndGetUpdateValue(existing, schema, orderingProps(" event_ts "));
    // Stored row wins on event_ts (99 > 50); the seq order (00005 > 00001) would have picked the incoming
    DebeziumOrderingTestFixtures.validateOrderingRecord(merged, 1, Operation.INSERT, DebeziumConstants.ADDED_SEQ_COL_NAME, "00001.100", 99L);
  }

  @Test
  public void testMergeWithCompositeOrderingFieldsComparesElementWise() throws IOException {
    Schema schema = createSchemaWithOrderingField();
    Properties props = orderingProps("event_ts,event_ts2");
    // First ordering field ties (99 == 99); the second decides (7 > 5) -> incoming wins,
    // even though the seq order (00001 < 00005) says otherwise.
    MySqlDebeziumAvroPayload payload = new MySqlDebeziumAvroPayload(
        DebeziumOrderingTestFixtures.recordWithCompositeOrdering(schema, 1, Operation.UPDATE, DebeziumConstants.ADDED_SEQ_COL_NAME, "00001.100", 99L, 7L), 99L);
    GenericRecord existing = DebeziumOrderingTestFixtures.recordWithCompositeOrdering(schema, 1, Operation.INSERT, DebeziumConstants.ADDED_SEQ_COL_NAME, "00005.100", 99L, 5L);
    Option<IndexedRecord> merged = payload.combineAndGetUpdateValue(existing, schema, props);
    DebeziumOrderingTestFixtures.validateOrderingRecord(merged, 1, Operation.UPDATE, DebeziumConstants.ADDED_SEQ_COL_NAME, "00001.100", 99L);

    // First ordering field decides outright when it differs (98 < 99) -> stored record kept.
    MySqlDebeziumAvroPayload older = new MySqlDebeziumAvroPayload(
        DebeziumOrderingTestFixtures.recordWithCompositeOrdering(schema, 1, Operation.UPDATE, DebeziumConstants.ADDED_SEQ_COL_NAME, "00009.100", 98L, 100L), 98L);
    merged = older.combineAndGetUpdateValue(existing, schema, props);
    DebeziumOrderingTestFixtures.validateOrderingRecord(merged, 1, Operation.INSERT, DebeziumConstants.ADDED_SEQ_COL_NAME, "00005.100", 99L);
  }

  @Test
  public void testPreCombineWithCompositeOrderingFields() {
    Schema schema = createSchemaWithOrderingField();
    Properties props = orderingProps("event_ts,event_ts2");
    // Composite orderingVal, first field tied: the second field decides the dedup winner.
    MySqlDebeziumAvroPayload lower = new MySqlDebeziumAvroPayload(
        DebeziumOrderingTestFixtures.recordWithCompositeOrdering(schema, 1, Operation.INSERT, DebeziumConstants.ADDED_SEQ_COL_NAME, "00005.100", 99L, 5L),
        OrderingValues.create(new Comparable[] {99L, 5L}));
    MySqlDebeziumAvroPayload higher = new MySqlDebeziumAvroPayload(
        DebeziumOrderingTestFixtures.recordWithCompositeOrdering(schema, 1, Operation.UPDATE, DebeziumConstants.ADDED_SEQ_COL_NAME, "00001.100", 99L, 7L),
        OrderingValues.create(new Comparable[] {99L, 7L}));
    assertEquals(higher, higher.preCombine(lower, props));
    assertEquals(higher, lower.preCombine(higher, props));
  }

  @Test
  public void testMergeWithoutOrderingFieldFallsBackToSeq() throws IOException {
    // Properties present but no ordering field configured -> legacy hardcoded seq comparison.
    GenericRecord lateRecord = createRecord(3, Operation.UPDATE, "00000.222");
    MySqlDebeziumAvroPayload payload = new MySqlDebeziumAvroPayload(lateRecord, "00000.222");
    GenericRecord existingRecord = createRecord(3, Operation.INSERT, "00001.111");
    Option<IndexedRecord> mergedRecord = payload.combineAndGetUpdateValue(existingRecord, avroSchema, new Properties());
    validateRecord(mergedRecord, 3, Operation.INSERT, "00001.111");

    GenericRecord freshRecord = createRecord(3, Operation.UPDATE, "00002.11");
    payload = new MySqlDebeziumAvroPayload(freshRecord, "00002.11");
    mergedRecord = payload.combineAndGetUpdateValue(existingRecord, avroSchema, new Properties());
    validateRecord(mergedRecord, 3, Operation.UPDATE, "00002.11");
  }

  @Test
  public void testPreCombineEqualConfiguredOrderingValuesTieGoesToNewer() {
    Schema schema = createSchemaWithOrderingField();
    // Two records with EQUAL configured ordering values: the seq parser would throw here
    // ("99".split(".") has no position segment -> ArrayIndexOutOfBoundsException); the
    // configured-field compare must not throw, and the newer payload wins the tie.
    MySqlDebeziumAvroPayload older = new MySqlDebeziumAvroPayload(createRecordWithOrdering(schema, 1, Operation.INSERT, "00001.111", 99L), 99L);
    MySqlDebeziumAvroPayload newer = new MySqlDebeziumAvroPayload(createRecordWithOrdering(schema, 1, Operation.UPDATE, "00002.111", 99L), 99L);
    assertEquals(newer, newer.preCombine(older, orderingProps("event_ts")));
  }

  @Test
  public void testPreCombineConfiguredOrderingFieldOverridesSeq() {
    Schema schema = createSchemaWithOrderingField();
    // The configured field decides, in both directions, even when the seq order says otherwise.
    MySqlDebeziumAvroPayload lowerTsHigherSeq = new MySqlDebeziumAvroPayload(createRecordWithOrdering(schema, 1, Operation.UPDATE, "00005.100", 50L), 50L);
    MySqlDebeziumAvroPayload higherTsLowerSeq = new MySqlDebeziumAvroPayload(createRecordWithOrdering(schema, 1, Operation.UPDATE, "00000.100", 120L), 120L);
    assertEquals(higherTsLowerSeq, higherTsLowerSeq.preCombine(lowerTsHigherSeq, orderingProps("event_ts")));
    assertEquals(higherTsLowerSeq, lowerTsHigherSeq.preCombine(higherTsLowerSeq, orderingProps("event_ts")));
  }

  @Test
  public void testPreCombineDeleteRecordKeepsNaturalOrder() {
    Schema schema = createSchemaWithOrderingField();
    MySqlDebeziumAvroPayload newer = new MySqlDebeziumAvroPayload(createRecordWithOrdering(schema, 1, Operation.UPDATE, "00002.111", 99L), 99L);
    MySqlDebeziumAvroPayload empty = new MySqlDebeziumAvroPayload(Option.empty());
    assertEquals(newer, newer.preCombine(empty, orderingProps("event_ts")));
  }

  @Test
  public void testPreCombineWithoutOrderingFieldUsesSeqCompare() {
    Schema schema = createSchemaWithOrderingField();
    // No ordering field configured -> the legacy numeric seq comparison still applies. On this
    // path real ingestion sets orderingVal from the seq column (precombine = _event_seq), so
    // construct accordingly.
    MySqlDebeziumAvroPayload olderSeq = new MySqlDebeziumAvroPayload(createRecordWithOrdering(schema, 1, Operation.INSERT, "00001.111", 99L), "00001.111");
    MySqlDebeziumAvroPayload newerSeq = new MySqlDebeziumAvroPayload(createRecordWithOrdering(schema, 1, Operation.UPDATE, "00002.111", 99L), "00002.111");
    assertEquals(newerSeq, newerSeq.preCombine(olderSeq, new Properties()));
  }

  private Schema createSchemaWithOrderingField() {
    return DebeziumOrderingTestFixtures.schemaWithOrderingField(DebeziumConstants.ADDED_SEQ_COL_NAME, Schema.Type.STRING);
  }

  private GenericRecord createRecordWithOrdering(Schema schema, int primaryKeyValue, @Nullable Operation op,
                                                 @Nullable String seqValue, @Nullable Long orderingValue) {
    return DebeziumOrderingTestFixtures.recordWithOrdering(schema, primaryKeyValue, op, DebeziumConstants.ADDED_SEQ_COL_NAME, seqValue, orderingValue);
  }

  private Properties orderingProps(String orderingField) {
    return DebeziumOrderingTestFixtures.orderingProps(orderingField);
  }

  private GenericRecord createRecord(int primaryKeyValue, @Nullable Operation op, @Nullable String seqValue) {
    GenericRecord record = new GenericData.Record(avroSchema);
    record.put(KEY_FIELD_NAME, primaryKeyValue);
    record.put(DebeziumConstants.FLATTENED_OP_COL_NAME, Objects.toString(op, null));
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

    @Override
    public String toString() {
      return op;
    }
  }
}
