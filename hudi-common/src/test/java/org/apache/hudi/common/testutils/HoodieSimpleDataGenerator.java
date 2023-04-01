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

package org.apache.hudi.common.testutils;

import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HoodieSimpleDataGenerator {

  /*
  {
    "type": "record",
    "name": "ExampleRecord",
    "namespace": "com.example.avro",
    "fields": [
      {"name": "id", "type": "int"},
      {"name": "pt", "type": "string"},
      {"name": "ts", "type": "long"}
    ]
   }
   */
  public static final String SCHEMA_STR =
      "{\"type\":\"record\",\"name\":\"ExampleRecord\",\"namespace\":\"com.example.avro\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"pt\",\"type\":\"string\"},{\"name\":\"ts\",\"type\":\"long\"}]}";
  public static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_STR);

  public List<HoodieRecord<DefaultHoodieRecordPayload>> getInserts(int n, int numPartitions, long ts) {
    return IntStream.range(0, n).mapToObj(id -> {
      String pt = numPartitions == 0 ? "" : "p" + id % numPartitions;
      return getNewRecord(id, pt, ts);
    }).collect(Collectors.toList());
  }

  public HoodieRecord<DefaultHoodieRecordPayload> getNewRecord(int id, String pt, long ts) {
    GenericRecord r = new GenericData.Record(SCHEMA);
    r.put("id", id);
    r.put("pt", pt);
    r.put("ts", ts);
    DefaultHoodieRecordPayload payload = new DefaultHoodieRecordPayload(r, ts);
    return new HoodieAvroRecord<>(new HoodieKey(String.valueOf(id), pt), payload);
  }
}
