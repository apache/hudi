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

package org.apache.hudi.common.serialization;

import org.apache.hudi.avro.AvroRecordSerializer;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecordSerializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;

public class TestBufferedRecordSerializer {
  private Schema schema;

  String jsonSchema = "{\n"
      + "  \"type\": \"record\",\n"
      + "  \"name\": \"Record\", \"namespace\":\"org.apache.hudi\",\n"
      + "  \"fields\": [\n"
      + "    {\"name\": \"id\", \"type\": \"string\"},\n"
      + "    {\"name\": \"ts\", \"type\": \"long\"},\n"
      + "    {\"name\": \"city\", \"type\": \"string\"}\n"
      + "  ]\n"
      + "}";

  @BeforeEach
  public void setUp() throws Exception {
    schema = new Schema.Parser().parse(jsonSchema);
  }

  @Test
  void testAvroRecordSerAndDe() {
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", "id1");
    record.put("ts", 100L);
    record.put("city", "SH");
    AvroRecordSerializer avroRecordSerializer = new AvroRecordSerializer(integer -> schema);
    byte[] avroBytes = avroRecordSerializer.serialize(record);
    IndexedRecord result = avroRecordSerializer.deserialize(avroBytes, 1);
    Assertions.assertEquals(record, result);

    avroRecordSerializer = new AvroRecordSerializer(integer -> HoodieMetadataRecord.SCHEMA$);
    HoodieMetadataRecord metadataRecord = new HoodieMetadataRecord("__all_partitions__", 1, new HashMap<>(), null, null, null, null);
    avroBytes = avroRecordSerializer.serialize(metadataRecord);
    result = avroRecordSerializer.deserialize(avroBytes, 1);
    for (int i = 0; i < metadataRecord.getSchema().getFields().size(); i++) {
      Assertions.assertEquals(metadataRecord.get(i), result.get(i));
    }
  }

  @Test
  void testBufferedRecordSerAndDe() throws IOException {
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", "id1");
    record.put("ts", 100L);
    record.put("city", "SH");
    BufferedRecord<IndexedRecord> bufferedRecord = new BufferedRecord<>("id", 100, record, 1, false);

    AvroRecordSerializer avroRecordSerializer = new AvroRecordSerializer(integer -> schema);
    BufferedRecordSerializer<IndexedRecord> bufferedRecordSerializer = new BufferedRecordSerializer<>(avroRecordSerializer);

    byte[] bytes = bufferedRecordSerializer.serialize(bufferedRecord);
    BufferedRecord<IndexedRecord> result = bufferedRecordSerializer.deserialize(bytes);
    Assertions.assertEquals(bufferedRecord, result);

    avroRecordSerializer = new AvroRecordSerializer(integer -> HoodieMetadataRecord.SCHEMA$);
    bufferedRecordSerializer = new BufferedRecordSerializer<>(avroRecordSerializer);
    HoodieMetadataRecord metadataRecord = new HoodieMetadataRecord("__all_partitions__", 1, new HashMap<>(), null, null, null, null);
    bufferedRecord = new BufferedRecord<>("__all_partitions__", 0, metadataRecord, 1, false);
    bytes = bufferedRecordSerializer.serialize(bufferedRecord);
    result = bufferedRecordSerializer.deserialize(bytes);

    Assertions.assertEquals(bufferedRecord.getRecordKey(), result.getRecordKey());
    Assertions.assertEquals(bufferedRecord.getOrderingValue(), result.getOrderingValue());
    Assertions.assertEquals(bufferedRecord.getSchemaId(), result.getSchemaId());
    Assertions.assertEquals(bufferedRecord.isDelete(), result.isDelete());
    for (int i = 0; i < metadataRecord.getSchema().getFields().size(); i++) {
      Assertions.assertEquals(metadataRecord.get(i), result.getRecord().get(i));
    }
  }
}
