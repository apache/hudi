/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.model;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestPartialUpdateAvroMerger {
  private PartialUpdateAvroMerger merger;
  private HoodieRecord oldRecord;
  private HoodieRecord newRecord;
  private TypedProperties props;

  @BeforeEach
  void setUp() {
    merger = PartialUpdateAvroMerger.INSTANCE;
    oldRecord = mock(HoodieRecord.class);
    newRecord = mock(HoodieRecord.class);
    props = new TypedProperties();
  }

  @Test
  void testMergingStrategy() {
    assertEquals(HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID, merger.getMergingStrategy());
  }

  @Test
  void testMergeWhenNewRecordIsDelete() throws IOException {
    Schema schema = createHighSchema();
    when(oldRecord.getOrderingValue(schema, props)).thenReturn(100L);
    when(newRecord.getOrderingValue(schema, props)).thenReturn(200L);
    when(oldRecord.isDelete(schema, props)).thenReturn(false);
    when(newRecord.isDelete(schema, props)).thenReturn(true);

    Option<Pair<HoodieRecord, Schema>> result = merger.merge(oldRecord, schema, newRecord, schema, props);
    assertTrue(result.isPresent());
    assertEquals(newRecord, result.get().getLeft());
  }

  @Test
  void testMergeWhenOldRecordIsDelete() throws IOException {
    Schema schema = createHighSchema();
    when(oldRecord.getOrderingValue(schema, props)).thenReturn(200L);
    when(newRecord.getOrderingValue(schema, props)).thenReturn(100L);
    when(oldRecord.isDelete(schema, props)).thenReturn(true);
    when(newRecord.isDelete(schema, props)).thenReturn(false);

    Option<Pair<HoodieRecord, Schema>> result = merger.merge(oldRecord, schema, newRecord, schema, props);
    assertTrue(result.isPresent());
    assertEquals(oldRecord, result.get().getLeft());
  }

  @Test
  void testMergeIndexedRecordOverridesNullFields() {
    Schema lowSchema = createLowSchema();
    GenericRecord low = new GenericData.Record(lowSchema);
    low.put("ts", "1");
    low.put("id", "123");
    low.put("val", "old");

    Schema highSchema = createHighSchema();
    GenericRecord high = new GenericData.Record(highSchema);
    high.put("id", "123");
    high.put("val", "new");

    IndexedRecord result = merger.mergeIndexedRecord(low, high, lowSchema, highSchema);
    assertEquals("1", result.get(0).toString());
    assertEquals("123", result.get(1).toString());
    assertEquals("new", result.get(2).toString());
  }

  @Test
  void testMergeIndexedRecordRetainsLowValueWhenHighIsNull() {
    Schema lowSchema = createLowSchema();
    GenericRecord low = new GenericData.Record(lowSchema);
    low.put("ts", "1");
    low.put("id", "123");
    low.put("val", "old");

    Schema highSchema = createHighSchema();
    GenericRecord high = new GenericData.Record(highSchema);
    high.put("id", "123");
    high.put("val", null);

    IndexedRecord result = merger.mergeIndexedRecord(low, high, lowSchema, highSchema);
    assertEquals("1", result.get(0).toString());
    assertEquals("123", result.get(1).toString());
    assertEquals("old", result.get(2).toString());
  }

  private Schema createLowSchema() {
    String schemaStr = "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":["
        + "{\"name\":\"ts\",\"type\":\"string\"},"
        + "{\"name\":\"id\",\"type\":\"string\"},"
        + "{\"name\":\"val\",\"type\":[\"null\", \"string\"],\"default\":null}"
        + "]}";
    return new Schema.Parser().parse(schemaStr);
  }

  private Schema createHighSchema() {
    String schemaStr = "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":["
        + "{\"name\":\"id\",\"type\":\"string\"},"
        + "{\"name\":\"val\",\"type\":[\"null\", \"string\"],\"default\":null}"
        + "]}";
    return new Schema.Parser().parse(schemaStr);
  }
}
