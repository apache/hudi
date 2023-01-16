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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests {@link TestHoodieAvroRecordMerger}.
 */
public class TestHoodieAvroRecordMerger {
  private Schema schema;

  String jsonSchema = "{\n"
      + "  \"type\": \"record\",\n"
      + "  \"name\": \"testMerge\", \"namespace\":\"org.apache.hudi\",\n"
      + "  \"fields\": [\n"
      + "    {\"name\": \"id\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"ts\", \"type\": [\"null\", \"long\"]},\n"
      + "    {\"name\": \"name\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"price\", \"type\": [\"null\", \"int\"]},\n"
      + "    {\"name\": \"partition\", \"type\": [\"null\", \"string\"]}\n"
      + "  ]\n"
      + "}";

  @BeforeEach
  public void setUp() throws Exception {
    schema = new Schema.Parser().parse(jsonSchema);
  }

  @Test
  public void testMerge() throws IOException {
    HoodieKey key = new HoodieKey("1", "partition1");

    GenericRecord record1 = new GenericData.Record(schema);
    record1.put("id", "1");
    record1.put("ts", 0L);
    record1.put("name", "name1");
    record1.put("price", null);
    record1.put("partition", "partition1");

    GenericRecord record2 = new GenericData.Record(schema);
    record2.put("id", "1");
    record2.put("ts", 1L);
    record2.put("name", null);
    record2.put("price", 1);
    record2.put("partition", "partition1");

    GenericRecord mergedRecord = new GenericData.Record(schema);
    mergedRecord.put("id", "1");
    mergedRecord.put("ts", 1L);
    mergedRecord.put("name", "name1");
    mergedRecord.put("price", 1);
    mergedRecord.put("partition", "partition1");

    //test PartialUpdateAvroPayload
    PartialUpdateAvroPayload payload1 = new PartialUpdateAvroPayload(record1, 0L);
    PartialUpdateAvroPayload payload2 = new PartialUpdateAvroPayload(record2, 1L);

    HoodieAvroRecordMerger merger = new HoodieAvroRecordMerger();

    Option<Pair<HoodieRecord, Schema>> newRecord =
        merger.merge(new HoodieAvroRecord<>(key, payload1), schema, new HoodieAvroRecord<>(key, payload2), schema, new TypedProperties());

    assertTrue(newRecord.isPresent());
    assertEquals(mergedRecord, newRecord.get().getKey().getData());

    // test OverwriteWithLatestAvroPayload
    OverwriteWithLatestAvroPayload payload3 = new OverwriteWithLatestAvroPayload(record1, 0L);
    OverwriteWithLatestAvroPayload payload4 = new OverwriteWithLatestAvroPayload(record2, 1L);

    Option<Pair<HoodieRecord, Schema>> latestRecord =
        merger.merge(new HoodieAvroRecord<>(key, payload3), schema, new HoodieAvroRecord<>(key, payload4), schema, new TypedProperties());

    assertTrue(latestRecord.isPresent());
    assertEquals(record2, latestRecord.get().getKey().getData());
  }

}
