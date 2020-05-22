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

package org.apache.hudi.utilities.mongo;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.UtilitiesTestBase;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests against {@link MongoAvroPayload}.
 */
public class TestMongoAvroPayload {

  private static final String PAST_TRACKING_INFO = "past_tracking_info";
  private static final String TAX_COST = "tax_cost";
  private static final String ESCROW_PAYMENT_IDS = "escrow_payment_ids";
  private static final String ESCROW_PAYMENT_AMOUNTS = "escrow_payment_amounts";
  private Schema schema;

  @Before
  public void setup() throws Exception {
    String sampleSchemaStr = UtilitiesTestBase.Helpers
        .readFile("unitTest/TestMongoAvroConverterSampleSchema.avsc");
    schema = new Schema.Parser().parse(sampleSchemaStr);
  }

  @After
  public void teardown() throws Exception {
  }

  private static BsonDocument toDoc(String key, String value) {
    return new BsonDocument(key, new BsonString(value));
  }

  private static BsonValue toBsonInt32(int value) {
    return new BsonInt32(value);
  }

  private static BsonValue toBsonDouble(double value) {
    return new BsonDouble(value);
  }

  private static MongoAvroPayload toPayload(GenericRecord record) {
    return new MongoAvroPayload(record, (Long) record.get(SchemaUtils.TS_MS_FIELD));
  }

  private void assertEqualsPayload(MongoAvroPayload expected, MongoAvroPayload value)
      throws IOException {
    if (expected != value) {
      assertEquals(expected.getInsertValue(schema), value.getInsertValue(schema));
    }
  }

  private void assertEqualsRecord(GenericRecord expected, Option<IndexedRecord> value)
      throws IOException {
    Option<IndexedRecord> expectedOption =
        (expected != null) ? Option.of((IndexedRecord) expected) : Option.empty();
    assertEquals(expectedOption, value);
  }

  private GenericRecord makeUpdateRecord(String id, Long time, BsonDocument patch) {
    GenericRecord genericRecord = new GenericData.Record(schema);
    genericRecord.put(SchemaUtils.OP_FIELD, Operation.UPDATE.code());
    genericRecord.put(SchemaUtils.ID_FIELD, id);
    genericRecord.put(SchemaUtils.TS_MS_FIELD, time);
    genericRecord.put(SchemaUtils.PATCH_FIELD, patch.toJson(SchemaUtils.STRICT_JSON));
    return genericRecord;
  }

  private MongoAvroPayload makeUpdatePayload(String id, Long time, BsonDocument patch) {
    return toPayload(makeUpdateRecord(id, time, patch));
  }

  private GenericRecord makeDeleteRecord(String id, Long time) {
    GenericRecord genericRecord = new GenericData.Record(schema);
    genericRecord.put(SchemaUtils.OP_FIELD, Operation.DELETE.code());
    genericRecord.put(SchemaUtils.ID_FIELD, id);
    genericRecord.put(SchemaUtils.TS_MS_FIELD, time);
    return genericRecord;
  }

  private MongoAvroPayload makeDeletePayload(String id, Long time) {
    return toPayload(makeDeleteRecord(id, time));
  }

  private GenericRecord makeGenericRecord(Operation op, String id, Long time,
      String pastTrackingInfo, Double taxCost, String[] escrowPaymentIds,
      Float[] escrowPaymentAmounts) {
    GenericRecord genericRecord = new GenericData.Record(schema);
    genericRecord.put(SchemaUtils.OP_FIELD, op.code());
    genericRecord.put(SchemaUtils.ID_FIELD, id);
    genericRecord.put(SchemaUtils.TS_MS_FIELD, time);
    if (pastTrackingInfo != null) {
      genericRecord.put(PAST_TRACKING_INFO, pastTrackingInfo);
    }
    if (taxCost != null) {
      genericRecord.put(TAX_COST, taxCost);
    }
    if (escrowPaymentIds != null) {
      genericRecord.put(ESCROW_PAYMENT_IDS, Arrays.asList(escrowPaymentIds));
    }
    if (escrowPaymentAmounts != null) {
      genericRecord.put(ESCROW_PAYMENT_AMOUNTS, Arrays.asList(escrowPaymentAmounts));
    }
    return genericRecord;
  }

  private MongoAvroPayload makeGenericPayload(Operation op, String id, Long time,
      String pastTrackingInfo, Double taxCost, String[] escrowPaymentIds,
      Float[] escrowPaymentAmounts) {
    return toPayload(makeGenericRecord(op, id, time, pastTrackingInfo, taxCost, escrowPaymentIds,
        escrowPaymentAmounts));
  }

  @Test
  public void testPreCombine() throws IOException {
    String alan = "{\"name\": {\"first\": \"Alan\", \"last\": \"Turing\"}}";
    String james = "{\"name\": {\"first\": \"James\", \"last\": \"Bond\"}}";
    MongoAvroPayload payload1 = makeGenericPayload(Operation.READ, "1", 1000L, "N/A", 300.0d,
        new String[]{alan, james}, new Float[]{1.0f, 3.0f});
    MongoAvroPayload payload2 = makeGenericPayload(Operation.CREATE, "1", 1500L, "", 500.0d,
        new String[]{james, alan}, new Float[]{3.0f, 5.0f});
    assertEqualsPayload(payload2, payload2.preCombine(payload1, schema));

    MongoAvroPayload payload3 = makeDeletePayload("1", 2000L);
    assertEqualsPayload(payload3, payload3.preCombine(payload1, schema));

    BsonDocument patch = new BsonDocument("past_tracking_info", toDoc("zone", "us-east"))
        .append("escrow_payment_ids.0.age", toBsonInt32(50))
        .append("escrow_payment_ids.1.age", toBsonInt32(55))
        .append("escrow_payment_amounts.1", toBsonDouble(10.0f));
    MongoAvroPayload payload4 = makeUpdatePayload("1", 2500L, new BsonDocument("$set", patch));

    String alan2 = "{\"name\": {\"first\": \"Alan\", \"last\": \"Turing\"}, \"age\": 50}";
    String james2 = "{\"name\": {\"first\": \"James\", \"last\": \"Bond\"}, \"age\": 55}";
    MongoAvroPayload expected = makeGenericPayload(Operation.READ, "1", 2500L,
        "{\"zone\": \"us-east\"}", 300.0d, new String[]{alan2, james2}, new Float[]{1.0f, 10.0f});
    assertEqualsPayload(expected, payload4.preCombine(payload1, schema));

    BsonDocument patch2 = new BsonDocument("past_tracking_info", BsonBoolean.TRUE)
        .append("escrow_payment_ids.0", BsonBoolean.TRUE);
    MongoAvroPayload payload5 = makeUpdatePayload("1", 3000L, new BsonDocument("$unset", patch2));

    MongoAvroPayload expected2 = makeGenericPayload(Operation.READ, "1", 3000L,
        null, 300.0d, new String[]{null, james}, new Float[]{1.0f, 3.0f});
    assertEqualsPayload(expected2, payload5.preCombine(payload1, schema));

    BsonDocument patch3 = new BsonDocument("past_tracking_info", BsonNull.VALUE)
        .append("escrow_payment_ids.0.age", toBsonInt32(50))
        .append("escrow_payment_ids.1.age", toBsonInt32(55))
        .append("escrow_payment_amounts.1", toBsonDouble(10.0f))
        .append("escrow_payment_ids.0", BsonNull.VALUE);
    MongoAvroPayload expected3 = makeUpdatePayload("1", 3000L, new BsonDocument("$set", patch3));
    assertEqualsPayload(expected3, payload5.preCombine(payload4, schema));
  }

  @Test
  public void testCombineAndGetUpdateValue() throws IOException {
    String alan = "{\"name\": {\"first\": \"Alan\", \"last\": \"Turing\"}}";
    String james = "{\"name\": {\"first\": \"James\", \"last\": \"Bond\"}}";
    GenericRecord record1 = makeGenericRecord(Operation.READ, "1", 1000L, "N/A", 300.0d,
        new String[]{alan, james}, new Float[]{1.0f, 3.0f});
    GenericRecord record2 = makeGenericRecord(Operation.CREATE, "1", 1500L, "", 500.0d,
        new String[]{james, alan}, new Float[]{3.0f, 5.0f});
    assertEqualsRecord(record2, toPayload(record2).combineAndGetUpdateValue(record1, schema));

    GenericRecord record3 = makeDeleteRecord("1", 2000L);
    assertEqualsRecord(null, toPayload(record3).combineAndGetUpdateValue(record1, schema));

    BsonDocument patch = new BsonDocument("past_tracking_info", toDoc("zone", "us-east"))
        .append("escrow_payment_ids.0.age", toBsonInt32(50))
        .append("escrow_payment_ids.1.age", toBsonInt32(55))
        .append("escrow_payment_amounts.1", toBsonDouble(10.0f));
    GenericRecord record4 = makeUpdateRecord("1", 2500L, new BsonDocument("$set", patch));

    String alan2 = "{\"name\": {\"first\": \"Alan\", \"last\": \"Turing\"}, \"age\": 50}";
    String james2 = "{\"name\": {\"first\": \"James\", \"last\": \"Bond\"}, \"age\": 55}";
    GenericRecord expected = makeGenericRecord(Operation.READ, "1", 2500L,
        "{\"zone\": \"us-east\"}", 300.0d, new String[]{alan2, james2}, new Float[]{1.0f, 10.0f});
    assertEqualsRecord(expected, toPayload(record4).combineAndGetUpdateValue(record1, schema));

    BsonDocument patch2 = new BsonDocument("past_tracking_info", BsonBoolean.TRUE)
        .append("escrow_payment_ids.0", BsonBoolean.TRUE);
    GenericRecord record5 = makeUpdateRecord("1", 3000L, new BsonDocument("$unset", patch2));

    GenericRecord expected2 = makeGenericRecord(Operation.READ, "1", 3000L,
        null, 300.0d, new String[]{null, james}, new Float[]{1.0f, 3.0f});
    assertEqualsRecord(expected2, toPayload(record5).combineAndGetUpdateValue(record1, schema));

    BsonDocument patch3 = new BsonDocument("past_tracking_info", BsonNull.VALUE)
        .append("escrow_payment_ids.0.age", toBsonInt32(50))
        .append("escrow_payment_ids.1.age", toBsonInt32(55))
        .append("escrow_payment_amounts.1", toBsonDouble(10.0f))
        .append("escrow_payment_ids.0", BsonNull.VALUE);
    GenericRecord expected3 = makeUpdateRecord("1", 3000L, new BsonDocument("$set", patch3));
    assertEqualsRecord(expected3, toPayload(record5).combineAndGetUpdateValue(record4, schema));
  }
}
