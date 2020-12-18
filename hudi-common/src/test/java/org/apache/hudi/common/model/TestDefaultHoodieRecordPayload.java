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
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

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
    props.setProperty(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP, "ts");
  }

  @Test
  public void testActiveRecords() throws IOException {
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

    assertEquals(record1, payload1.getInsertValue(schema).get());
    assertEquals(record2, payload2.getInsertValue(schema).get());

    assertEquals(payload1.combineAndGetUpdateValue(record2, schema, props).get(), record2);
    assertEquals(payload2.combineAndGetUpdateValue(record1, schema, props).get(), record2);
  }

  @Test
  public void testDeletedRecord() throws IOException {
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

    assertEquals(record1, payload1.getInsertValue(schema).get());
    assertFalse(payload2.getInsertValue(schema).isPresent());

    assertEquals(payload1.combineAndGetUpdateValue(delRecord1, schema, props).get(), delRecord1);
    assertFalse(payload2.combineAndGetUpdateValue(record1, schema, props).isPresent());
  }

}
