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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestPartialUpdateWithNonDefaultValueAvroMerger {
  private final PartialUpdateWithNonDefaultValueAvroMerger merger =
      PartialUpdateWithNonDefaultValueAvroMerger.INSTANCE;

  @Test
  void testMergingStrategy() {
    assertEquals(HoodieRecordMerger.PARTIAL_UPDATE_WITH_NON_DEFAULT_VALUE_MERGE_STRATEGY_UUID,
        merger.getMergingStrategy());
  }

  @Test
  void testMergeIndexedRecordOverridesWithNonDefault() {
    Schema schema = createTestSchemaWithDefaults();
    GenericRecord low = new GenericData.Record(schema);
    low.put("id", "123");
    low.put("val", "low");

    GenericRecord high = new GenericData.Record(schema);
    high.put("id", "456"); // overrides default
    high.put("val", "high"); // overrides default

    IndexedRecord result = merger.mergeIndexedRecord(low, high, schema, schema);
    assertEquals("456", result.get(0).toString());
    assertEquals("high", result.get(1).toString());
  }

  @Test
  void testMergeIndexedRecordKeepsLowIfHighIsDefault() {
    Schema schema = createTestSchemaWithDefaults();
    GenericRecord low = new GenericData.Record(schema);
    low.put("id", "123");
    low.put("val", "low");

    GenericRecord high = new GenericData.Record(schema);
    high.put("id", ""); // default
    high.put("val", null); // default

    IndexedRecord result = merger.mergeIndexedRecord(low, high, schema, schema);
    assertEquals("123", result.get(0).toString()); // kept low
    assertEquals("low", result.get(1).toString()); // kept low
  }

  private Schema createTestSchemaWithDefaults() {
    String schemaStr = "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":["
        + "{\"name\":\"id\",\"type\":\"string\", \"default\": \"\"},"
        + "{\"name\":\"val\",\"type\":[\"null\", \"string\"],\"default\": null}"
        + "]}";
    return new Schema.Parser().parse(schemaStr);
  }
}
