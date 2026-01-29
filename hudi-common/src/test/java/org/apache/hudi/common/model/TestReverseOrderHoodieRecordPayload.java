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

package org.apache.hudi.common.model;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.testutils.OrderingFieldsTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import static org.apache.hudi.common.model.HoodieRecord.SENTINEL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Unit tests {@link ReverseOrderHoodieRecordPayload}.
 */
public class TestReverseOrderHoodieRecordPayload {

  private Schema schema;
  private Properties props;

  @BeforeEach
  public void setUp() throws Exception {
    schema = Schema.createRecord(Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), "", null),
        new Schema.Field("partition", Schema.create(Schema.Type.STRING), "", null),
        new Schema.Field("ts", Schema.create(Schema.Type.LONG), "", null),
        new Schema.Field("_hoodie_is_deleted", Schema.create(Schema.Type.BOOLEAN), "", false)
    ));
    props = new Properties();
    props.setProperty(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY, "ts");
    props.setProperty(HoodiePayloadProps.PAYLOAD_EVENT_TIME_FIELD_PROP_KEY, "ts");
  }

  @ParameterizedTest
  @MethodSource("org.apache.hudi.common.testutils.OrderingFieldsTestUtils#configureOrderingFields")
  public void testPrecombineAndCombineAndGetUpdateValueMethods(String key) throws IOException {
    OrderingFieldsTestUtils.setOrderingFieldsConfig(props, key, "ts");
    GenericRecord record1 = new GenericData.Record(schema);
    record1.put("id", "1");
    record1.put("partition", "partition0");
    record1.put("ts", 0L);
    record1.put("_hoodie_is_deleted", false);

    GenericRecord record2 = new GenericData.Record(schema);
    record2.put("id", "2");
    record2.put("partition", "partition1");
    record2.put("ts", 1L);
    record2.put("_hoodie_is_deleted", false);

    ReverseOrderHoodieRecordPayload payload1 = new ReverseOrderHoodieRecordPayload(record1, 1);
    ReverseOrderHoodieRecordPayload payload2 = new ReverseOrderHoodieRecordPayload(record2, 2);
    assertEquals(payload1.preCombine(payload2, props), payload1);
    assertEquals(payload2.preCombine(payload1, props), payload1);

    assertEquals(record1, payload1.getInsertValue(schema, props).get());
    assertEquals(record2, payload2.getInsertValue(schema, props).get());

    // Here payload1 with lower orderingVal trying to combine with
    // record2 with higher orderingVal this will return payload1
    assertEquals(payload1.combineAndGetUpdateValue(record2, schema, props).get(), record1);

    // Here payload2 with higher orderingVal trying to combine with
    // record1 with lower orderingVal this will return IgnoreRecord.
    assertEquals(SENTINEL, payload2.combineAndGetUpdateValue(record1, schema, props).get());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testDeleteRecord(boolean deleteRecordWithHigherOrderingVal) throws IOException {
    long record1PrecombineVal = 1;
    GenericRecord record1 = new GenericData.Record(schema);
    record1.put("id", "1");
    record1.put("partition", "partition0");
    record1.put("ts", record1PrecombineVal);
    record1.put("_hoodie_is_deleted", false);

    GenericRecord delete2 = new GenericData.Record(schema);
    delete2.put("id", "2");
    delete2.put("partition", "partition1");
    delete2.put("ts", deleteRecordWithHigherOrderingVal ? record1PrecombineVal + 1 : record1PrecombineVal - 1);
    delete2.put("_hoodie_is_deleted", true);

    ReverseOrderHoodieRecordPayload payload1 = new ReverseOrderHoodieRecordPayload(record1, 1);
    ReverseOrderHoodieRecordPayload payload2 = new ReverseOrderHoodieRecordPayload(delete2, 2);

    assertEquals(payload1, payload1.preCombine(payload2, props));
    assertEquals(payload1, payload2.preCombine(payload1, props));

    assertEquals(record1, payload1.getInsertValue(schema, props).get());
    assertFalse(payload2.getInsertValue(schema, props).isPresent());

    if (deleteRecordWithHigherOrderingVal) {
      // Here delete record has higherOrderingVal, so either record1 will be returned
      // or an instance of Ignore record will be returned if the persisted entry's ordering value is lower.
      assertEquals(record1, payload1.combineAndGetUpdateValue(delete2, schema, props).get());
      assertEquals(SENTINEL, payload2.combineAndGetUpdateValue(record1, schema, props).get());
    } else {
      // Here delete record has lowerOrderingVal, so either empty record signifying lower be returned
      // or an instance of Ignore record will be returned if the persisted entry's ordering value is lower.
      assertEquals(SENTINEL, payload1.combineAndGetUpdateValue(delete2, schema, props).get());
      assertFalse(payload2.combineAndGetUpdateValue(record1, schema, props).isPresent());
    }
  }
}