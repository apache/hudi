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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.util.Option;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;


/**
 * Unit tests {@link TestRecordCountAvroPayload}.
 */
public class TestRecordCountAvroPayload {
  private Schema schema;

  private Properties properties = new Properties();
  String jsonSchema = "{\n"
          + "  \"type\": \"record\",\n"
          + "  \"name\": \"partialRecord\", \"namespace\":\"org.apache.hudi\",\n"
          + "  \"fields\": [\n"
          + "    {\"name\": \"id\", \"type\": [\"null\", \"string\"]},\n"
          + "    {\"name\": \"partition\", \"type\": [\"null\", \"string\"]},\n"
          + "    {\"name\": \"ts\", \"type\": [\"null\", \"long\"]},\n"
          + "    {\"name\": \"_hoodie_is_deleted\", \"type\": [\"null\", \"boolean\"]},\n"
          + "    {\"name\": \"hoodie_record_count\", \"type\": [\"null\", \"long\"]},\n"
          + "    {\"name\": \"city\", \"type\": [\"null\", \"string\"]},\n"
          + "    {\"name\": \"child\", \"type\": [\"null\", {\"type\": \"array\", \"items\": \"string\"}]}\n"
          + "  ]\n"
          + "}";

  @BeforeEach
  public void setUp() throws Exception {
    schema = new Schema.Parser().parse(jsonSchema);
    properties = new Properties();
    properties.put(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY, "ts");
  }

  @Test
  public void testActiveRecords() throws IOException {
    GenericRecord record1 = new GenericData.Record(schema);
    record1.put("id", "1");
    record1.put("partition", "partition1");
    record1.put("ts", 1L);
    record1.put("_hoodie_is_deleted", false);
    record1.put("hoodie_record_count", null);
    record1.put("city", "NY0");
    record1.put("child", Arrays.asList("A"));

    GenericRecord record2 = new GenericData.Record(schema);
    record2.put("id", "1");
    record2.put("partition", "partition1");
    record2.put("ts", 2L);
    record2.put("_hoodie_is_deleted", false);
    record2.put("hoodie_record_count", 3L);
    record2.put("city", null);
    record2.put("child", Arrays.asList("B"));

    GenericRecord record3 = new GenericData.Record(schema);
    record3.put("id", "1");
    record3.put("partition", "partition1");
    record3.put("ts", 2L);
    record3.put("_hoodie_is_deleted", false);
    record3.put("hoodie_record_count", 4L);
    record3.put("city", "NY0");
    record3.put("child", Arrays.asList("A"));

    GenericRecord record4 = new GenericData.Record(schema);
    record4.put("id", "1");
    record4.put("partition", "partition1");
    record4.put("ts", 2L);
    record4.put("_hoodie_is_deleted", false);
    record4.put("hoodie_record_count", 4L);
    record4.put("city", null);
    record4.put("child", Arrays.asList("B"));

    // Test preCombine: payload2's ordering val is larger
    RecordCountAvroPayload payload1 = new RecordCountAvroPayload(record1, 1L);
    RecordCountAvroPayload payload2 = new RecordCountAvroPayload(record2, 2L);
    assertArrayEquals(payload1.preCombine(payload2, schema, properties).recordBytes, new RecordCountAvroPayload(record4, 2).recordBytes);
    assertArrayEquals(payload2.preCombine(payload1, schema, properties).recordBytes, new RecordCountAvroPayload(record4, 2).recordBytes);

    // 'hoodie_record_count' field value is 'null' and represents 1
    record1.put("hoodie_record_count", 1L);
    assertEquals(record1, payload1.getInsertValue(schema).get());
    assertEquals(record2, payload2.getInsertValue(schema).get());

    // Test combineAndGetUpdateValue: payload2's ordering val is larger
    assertEquals(payload1.combineAndGetUpdateValue(record2, schema, properties).get(), record4);
    assertEquals(payload2.combineAndGetUpdateValue(record1, schema, properties).get(), record4);

    // Test preCombine again: let payload1's ordering val larger than payload2
    record1.put("ts", 2L);
    record2.put("ts", 1L);
    // reset value
    record2.put("hoodie_record_count", 3L);
    payload1 = new RecordCountAvroPayload(record1, 2);
    payload2 = new RecordCountAvroPayload(record2, 1);
    assertArrayEquals(payload1.preCombine(payload2, schema, properties).recordBytes, new RecordCountAvroPayload(record3, 2).recordBytes);
    assertArrayEquals(payload2.preCombine(payload1, schema, properties).recordBytes, new RecordCountAvroPayload(record3, 2).recordBytes);
  }

  @Test
  public void testDeletedRecord() throws IOException {
    GenericRecord record1 = new GenericData.Record(schema);
    record1.put("id", "1");
    record1.put("partition", "partition0");
    record1.put("ts", 1L);
    record1.put("_hoodie_is_deleted", false);
    record1.put("hoodie_record_count", null);
    record1.put("city", "NY0");
    record1.put("child", Collections.emptyList());

    GenericRecord delRecord1 = new GenericData.Record(schema);
    delRecord1.put("id", "2");
    delRecord1.put("partition", "partition1");
    delRecord1.put("ts", 2L);
    delRecord1.put("_hoodie_is_deleted", true);
    delRecord1.put("hoodie_record_count", 1L);
    delRecord1.put("city", "NY0");
    delRecord1.put("child", Collections.emptyList());

    RecordCountAvroPayload payload1 = new RecordCountAvroPayload(record1, 1L);
    RecordCountAvroPayload payload2 = new RecordCountAvroPayload(delRecord1, 2L);

    assertEquals(payload1.preCombine(payload2, schema, properties), payload1);
    assertEquals(payload2.preCombine(payload1, schema, properties), payload2);

    // 'hoodie_record_count' field value is 'null' and represents 1
    record1.put("hoodie_record_count", 1L);
    assertEquals(record1, payload1.getInsertValue(schema).get());
    assertFalse(payload2.getInsertValue(schema).isPresent());

    assertEquals(payload1.combineAndGetUpdateValue(delRecord1, schema, properties), Option.empty());
    assertFalse(payload2.combineAndGetUpdateValue(record1, schema, properties).isPresent());
  }
}
