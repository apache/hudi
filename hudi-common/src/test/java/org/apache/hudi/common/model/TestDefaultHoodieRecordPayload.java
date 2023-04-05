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

import org.apache.hudi.common.testutils.PreCombineTestUtils;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_KEY;
import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_MARKER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Unit tests {@link DefaultHoodieRecordPayload}.
 */
public class TestDefaultHoodieRecordPayload {

  private Schema schema;
  private Properties props;

  @BeforeEach
  public void setUp() throws Exception {
    schema = Schema.createRecord(Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), "", null),
        new Schema.Field("partition", Schema.create(Schema.Type.STRING), "", null),
        new Schema.Field("ts", Schema.create(Schema.Type.LONG), "", null),
        new Schema.Field("_hoodie_is_deleted", Schema.create(Type.BOOLEAN), "", false)
    ));
    props = new Properties();
    props.setProperty(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY, "ts");
    props.setProperty(HoodiePayloadProps.PAYLOAD_EVENT_TIME_FIELD_PROP_KEY, "ts");
  }

  @ParameterizedTest
  @MethodSource("org.apache.hudi.common.testutils.PreCombineTestUtils#configurePreCombine")
  public void testActiveRecords(String key) throws IOException {
    PreCombineTestUtils.setPreCombineConfig(props, key, "ts");
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

    DefaultHoodieRecordPayload payload1 = new DefaultHoodieRecordPayload(record1, 1);
    DefaultHoodieRecordPayload payload2 = new DefaultHoodieRecordPayload(record2, 2);
    assertEquals(payload1.preCombine(payload2, props), payload2);
    assertEquals(payload2.preCombine(payload1, props), payload2);

    assertEquals(record1, payload1.getInsertValue(schema, props).get());
    assertEquals(record2, payload2.getInsertValue(schema, props).get());

    assertEquals(payload1.combineAndGetUpdateValue(record2, schema, props).get(), record2);
    assertEquals(payload2.combineAndGetUpdateValue(record1, schema, props).get(), record2);
  }

  @ParameterizedTest
  @MethodSource("org.apache.hudi.common.testutils.PreCombineTestUtils#configurePreCombine")
  public void testDeletedRecord(String key) throws IOException {
    PreCombineTestUtils.setPreCombineConfig(props, key, "ts");
    GenericRecord record1 = new GenericData.Record(schema);
    record1.put("id", "1");
    record1.put("partition", "partition0");
    record1.put("ts", 0L);
    record1.put("_hoodie_is_deleted", false);

    GenericRecord delRecord1 = new GenericData.Record(schema);
    delRecord1.put("id", "2");
    delRecord1.put("partition", "partition1");
    delRecord1.put("ts", 1L);
    delRecord1.put("_hoodie_is_deleted", true);

    DefaultHoodieRecordPayload payload1 = new DefaultHoodieRecordPayload(record1, 1);
    DefaultHoodieRecordPayload payload2 = new DefaultHoodieRecordPayload(delRecord1, 2);
    assertEquals(payload1.preCombine(payload2, props), payload2);
    assertEquals(payload2.preCombine(payload1, props), payload2);

    assertEquals(record1, payload1.getInsertValue(schema, props).get());
    assertFalse(payload2.getInsertValue(schema, props).isPresent());

    assertEquals(payload1.combineAndGetUpdateValue(delRecord1, schema, props).get(), delRecord1);
    assertFalse(payload2.combineAndGetUpdateValue(record1, schema, props).isPresent());
  }

  @Test
  public void testDeleteKey() throws IOException {
    props.setProperty(DELETE_KEY, "ts");
    props.setProperty(DELETE_MARKER, String.valueOf(1L));
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", "1");
    record.put("partition", "partition0");
    record.put("ts", 0L);
    record.put("_hoodie_is_deleted", false);

    GenericRecord delRecord = new GenericData.Record(schema);
    delRecord.put("id", "2");
    delRecord.put("partition", "partition1");
    delRecord.put("ts", 1L);
    delRecord.put("_hoodie_is_deleted", false);

    GenericRecord defaultDeleteRecord = new GenericData.Record(schema);
    defaultDeleteRecord.put("id", "2");
    defaultDeleteRecord.put("partition", "partition1");
    defaultDeleteRecord.put("ts", 2L);
    defaultDeleteRecord.put("_hoodie_is_deleted", true);

    DefaultHoodieRecordPayload payload = new DefaultHoodieRecordPayload(record, 1);
    DefaultHoodieRecordPayload deletePayload = new DefaultHoodieRecordPayload(delRecord, 2);
    DefaultHoodieRecordPayload defaultDeletePayload = new DefaultHoodieRecordPayload(defaultDeleteRecord, 2);

    assertEquals(record, payload.getInsertValue(schema, props).get());
    assertEquals(defaultDeleteRecord, defaultDeletePayload.getInsertValue(schema, props).get());
    assertFalse(deletePayload.getInsertValue(schema, props).isPresent());

    assertEquals(delRecord, payload.combineAndGetUpdateValue(delRecord, schema, props).get());
    assertEquals(defaultDeleteRecord, payload.combineAndGetUpdateValue(defaultDeleteRecord, schema, props).get());
    assertFalse(deletePayload.combineAndGetUpdateValue(record, schema, props).isPresent());
  }

  @Test
  public void testDeleteKeyConfiguration() throws IOException {
    props.setProperty(DELETE_KEY, "ts");
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", "1");
    record.put("partition", "partition0");
    record.put("ts", 0L);
    record.put("_hoodie_is_deleted", false);

    DefaultHoodieRecordPayload payload = new DefaultHoodieRecordPayload(record, 1);

    // Verify failure when DELETE_MARKER is not configured along with DELETE_KEY
    try {
      payload.getInsertValue(schema, props).get();
      fail("Should fail");
    } catch (IllegalArgumentException e) {
      // Ignore
    }

    try {
      payload.combineAndGetUpdateValue(record, schema, props).get();
      fail("Should fail");
    } catch (IllegalArgumentException e) {
      // Ignore
    }
  }

  @Test
  public void testGetEmptyMetadata() {
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", "1");
    record.put("partition", "partition0");
    record.put("ts", 0L);
    record.put("_hoodie_is_deleted", false);
    DefaultHoodieRecordPayload payload = new DefaultHoodieRecordPayload(Option.of(record));
    assertFalse(payload.getMetadata().isPresent());
  }

  @ParameterizedTest
  @ValueSource(longs = {1L, 1612542030000L})
  public void testGetEventTimeInMetadata(long eventTime) throws IOException {
    GenericRecord record1 = new GenericData.Record(schema);
    record1.put("id", "1");
    record1.put("partition", "partition0");
    record1.put("ts", 0L);
    record1.put("_hoodie_is_deleted", false);

    GenericRecord record2 = new GenericData.Record(schema);
    record2.put("id", "1");
    record2.put("partition", "partition0");
    record2.put("ts", eventTime);
    record2.put("_hoodie_is_deleted", false);

    DefaultHoodieRecordPayload payload2 = new DefaultHoodieRecordPayload(record2, eventTime);
    payload2.combineAndGetUpdateValue(record1, schema, props);
    assertTrue(payload2.getMetadata().isPresent());
    assertEquals(eventTime,
        Long.parseLong(payload2.getMetadata().get().get(DefaultHoodieRecordPayload.METADATA_EVENT_TIME_KEY)));
  }

  @Test
  public void testEmptyProperty() throws IOException {
    GenericRecord record1 = new GenericData.Record(schema);
    record1.put("id", "1");
    record1.put("partition", "partition0");
    record1.put("ts", 0L);
    record1.put("_hoodie_is_deleted", false);

    GenericRecord record2 = new GenericData.Record(schema);
    record2.put("id", "1");
    record2.put("partition", "partition0");
    record2.put("ts", 1L);
    record2.put("_hoodie_is_deleted", false);

    DefaultHoodieRecordPayload payload = new DefaultHoodieRecordPayload(Option.of(record1));
    Properties properties = new Properties();
    payload.getInsertValue(schema, properties);
    payload.combineAndGetUpdateValue(record2, schema, properties);
  }

  @ParameterizedTest
  @ValueSource(longs = {1L, 1612542030000L})
  public void testGetEventTimeInMetadataForInserts(long eventTime) throws IOException {
    GenericRecord record = new GenericData.Record(schema);

    record.put("id", "1");
    record.put("partition", "partition0");
    record.put("ts", eventTime);
    record.put("_hoodie_is_deleted", false);
    DefaultHoodieRecordPayload payload = new DefaultHoodieRecordPayload(record, eventTime);
    payload.getInsertValue(schema, props);
    assertTrue(payload.getMetadata().isPresent());
    assertEquals(eventTime,
        Long.parseLong(payload.getMetadata().get().get(DefaultHoodieRecordPayload.METADATA_EVENT_TIME_KEY)));
  }
}
