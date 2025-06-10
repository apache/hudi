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

package org.apache.hudi.avro;

import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.SizeEstimator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

public class TestSizeEstimator {
  private Schema schema;

  String jsonSchema = "{\n"
      + "  \"type\": \"record\",\n"
      + "  \"name\": \"Record\", \"namespace\":\"org.apache.hudi\",\n"
      + "  \"fields\": [\n"
      + "    {\"name\": \"id\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"ts\", \"type\": [\"null\", \"long\"]},\n"
      + "    {\"name\": \"city\", \"type\": [\"null\", \"string\"]}\n"
      + "  ]\n"
      + "}";

  @BeforeEach
  public void setUp() throws Exception {
    schema = new Schema.Parser().parse(jsonSchema);
  }

  @Test
  void testEstimatingRecord() {
    // testing Avro builtin IndexedRecord
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", "id1");
    record.put("ts", 100);
    record.put("city", "SH");
    SizeEstimator<BufferedRecord<IndexedRecord>> estimator = new AvroRecordSizeEstimator(schema);
    BufferedRecord<IndexedRecord> bufferedRecord = new BufferedRecord<>("id", 100, record, 1, false);
    Assertions.assertEquals(256, estimator.sizeEstimate(bufferedRecord));

    // testing generated IndexedRecord
    HoodieMetadataRecord metadataRecord = new HoodieMetadataRecord("__all_partitions__", 1, new HashMap<>(), null, null, null, null);
    bufferedRecord = new BufferedRecord<>("__all_partitions__", 0, metadataRecord, 1, false);
    Assertions.assertEquals(232, estimator.sizeEstimate(bufferedRecord));
  }
}
