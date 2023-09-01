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

package org.apache.hudi.model;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class TestUtil {
  public static final Schema SCHEMA = SchemaBuilder.record("MetadataValue")
      .fields()
      .name("valueId").type().stringType().noDefault()
      .name("extraInfo").type().stringType().noDefault()
      .endRecord();
  public static final Random RANDOM = new Random(TestHoodieAvroRecordMerger.class.hashCode());

  public static String generateRandomString(int length) {
    byte[] array = new byte[length];
    RANDOM.nextBytes(array);
    return new String(array, StandardCharsets.UTF_8);
  }

  public static List<HoodieAvroRecord> generateData(int numRecord) throws IOException {
    List<HoodieAvroRecord> data = new ArrayList<>();
    for (int i = 0; i < numRecord; i++) {
      UUID uuid = UUID.randomUUID();
      GenericRecord record = new GenericData.Record(SCHEMA);
      record.put("valueId", uuid.toString());
      record.put("extraInfo", new Utf8(generateRandomString(7)));
      data.add(new HoodieAvroRecord<>(
          new HoodieKey(String.valueOf(i), String.valueOf(i)),
          new HoodieAvroPayload(record, 0)));
    }
    return data;
  }

  public static String getFieldFromAvroRecord(HoodieAvroRecord record,
                                               Schema schema,
                                               String fieldName) throws IOException {
    return ((Utf8) HoodieAvroUtils.bytesToAvro(((HoodieAvroPayload) record.getData()).getRecordBytes(), schema)
        .get(fieldName)).toString();
  }

  public static String getFieldFromIndexedRecord(HoodieRecord record, int fieldIndex) {
    return ((Utf8) ((IndexedRecord) record.getData()).get(fieldIndex)).toString();
  }
}
