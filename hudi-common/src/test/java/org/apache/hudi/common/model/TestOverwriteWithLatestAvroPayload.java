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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Unit tests {@link OverwriteWithLatestAvroPayload}.
 */
public class TestOverwriteWithLatestAvroPayload {

  private Schema schema;
  String defaultDeleteMarkerField = "_hoodie_is_deleted";
  String deleteMarkerField = "delete_marker_field";

  @BeforeEach
  public void setUp() throws Exception {
    schema = Schema.createRecord(Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), "", null),
        new Schema.Field("partition", Schema.create(Schema.Type.STRING), "", null),
        new Schema.Field("ts", Schema.create(Schema.Type.LONG), "", null),
        new Schema.Field(defaultDeleteMarkerField, Schema.create(Type.BOOLEAN), "", false),
        new Schema.Field(deleteMarkerField, Schema.create(Type.BOOLEAN), "", false)
    ));
  }

  @Test
  public void testOverwriteWithLatestAvroPayload() throws IOException {
    GenericRecord record1 = new GenericData.Record(schema);
    record1.put("id", "1");
    record1.put("partition", "partition0");
    record1.put("ts", 0L);
    record1.put(defaultDeleteMarkerField, false);
    record1.put(deleteMarkerField, false);

    // test1: set default marker field value to true and user defined to false
    GenericRecord record2 = new GenericData.Record(schema);
    record2.put("id", "2");
    record2.put("partition", "partition1");
    record2.put("ts", 1L);
    record2.put(defaultDeleteMarkerField, true);
    record2.put(deleteMarkerField, false);

    // set to user defined marker field with false, the record should be considered active.
    assertActiveRecord(record1, record2, deleteMarkerField);

    // set to default marker field with true, the record should be considered delete.
    assertDeletedRecord(record1, record2, defaultDeleteMarkerField);

    // test2: set default marker field value to false and user defined to true
    GenericRecord record3 = new GenericData.Record(schema);
    record3.put("id", "2");
    record3.put("partition", "partition1");
    record3.put("ts", 1L);
    record3.put(defaultDeleteMarkerField, false);
    record3.put(deleteMarkerField, true);

    // set to user defined marker field with true, the record should be considered delete.
    assertDeletedRecord(record1, record3, deleteMarkerField);

    // set to default marker field with false, the record should be considered active.
    assertActiveRecord(record1, record3, defaultDeleteMarkerField);
  }

  private void assertActiveRecord(GenericRecord record1,
                                  GenericRecord record2, String field) throws IOException {
    OverwriteWithLatestAvroPayload payload1 = new OverwriteWithLatestAvroPayload(
        record1, 1, field);
    OverwriteWithLatestAvroPayload payload2 = new OverwriteWithLatestAvroPayload(
        record2, 2, field);

    assertEquals(payload1.preCombine(payload2), payload2);
    assertEquals(payload2.preCombine(payload1), payload2);

    assertEquals(record1, payload1.getInsertValue(schema).get());
    assertEquals(record2, payload2.getInsertValue(schema).get());

    assertEquals(payload1.combineAndGetUpdateValue(record2, schema).get(), record1);
    assertEquals(payload2.combineAndGetUpdateValue(record1, schema).get(), record2);
  }

  private void assertDeletedRecord(GenericRecord record1,
                                   GenericRecord delRecord1, String field) throws IOException {
    OverwriteWithLatestAvroPayload payload1 = new OverwriteWithLatestAvroPayload(
        record1, 1, field);
    OverwriteWithLatestAvroPayload payload2 = new OverwriteWithLatestAvroPayload(
        delRecord1, 2, field);
    assertEquals(payload1.preCombine(payload2), payload2);
    assertEquals(payload2.preCombine(payload1), payload2);

    assertEquals(record1, payload1.getInsertValue(schema).get());
    assertFalse(payload2.getInsertValue(schema).isPresent());

    assertEquals(payload1.combineAndGetUpdateValue(delRecord1, schema).get(), record1);
    assertFalse(payload2.combineAndGetUpdateValue(record1, schema).isPresent());
  }
}
