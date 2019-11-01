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

package org.apache.hudi.bench.generator;

import static junit.framework.TestCase.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.util.HoodieAvroUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestGenericRecordPayloadGenerator {

  @Test
  public void testSimplePayload() throws Exception {
    Schema schema = new Schema.Parser().parse(getClass().getClassLoader()
        .getResourceAsStream("hudi-bench-config/source.avsc"));
    GenericRecordFullPayloadGenerator payloadGenerator = new GenericRecordFullPayloadGenerator(schema);
    GenericRecord record = payloadGenerator.getNewPayload();
    // The generated payload should validate with the provided schema
    payloadGenerator.validate(record);
  }

  @Test
  public void testComplexPayload() throws IOException {
    Schema schema = new Schema.Parser().parse(getClass().getClassLoader()
        .getResourceAsStream("hudi-bench-config/complex-source.avsc"));
    GenericRecordFullPayloadGenerator payloadGenerator = new GenericRecordFullPayloadGenerator(schema);
    GenericRecord record = payloadGenerator.getNewPayload();
    // The generated payload should validate with the provided schema
    Assert.assertTrue(payloadGenerator.validate(record));
  }

  @Test
  public void testComplexPartialPayload() throws IOException {
    Schema schema = new Schema.Parser().parse(getClass().getClassLoader()
        .getResourceAsStream("hudi-bench-config/complex-source.avsc"));
    GenericRecordPartialPayloadGenerator payloadGenerator = new GenericRecordPartialPayloadGenerator(schema);
    IntStream.range(0, 10).forEach(a -> {
      GenericRecord record = payloadGenerator.getNewPayload();
      // The generated payload should validate with the provided schema
      Assert.assertTrue(payloadGenerator.validate(record));
    });
  }

  @Test
  public void testUpdatePayloadGenerator() throws IOException {
    Schema schema = new Schema.Parser().parse(getClass().getClassLoader()
        .getResourceAsStream("hudi-bench-config/source.avsc"));
    GenericRecordFullPayloadGenerator payloadGenerator = new GenericRecordFullPayloadGenerator(schema);
    List<String> insertRowKeys = new ArrayList<>();
    List<String> updateRowKeys = new ArrayList<>();
    List<Long> insertTimeStamps = new ArrayList<>();
    List<Long> updateTimeStamps = new ArrayList<>();
    List<GenericRecord> records = new ArrayList<>();
    // Generate 10 new records
    IntStream.range(0, 10).forEach(a -> {
      GenericRecord record = payloadGenerator.getNewPayload();
      records.add(record);
      insertRowKeys.add(record.get("_row_key").toString());
      insertTimeStamps.add((Long) record.get("timestamp"));
    });
    List<String> blacklistFields = Arrays.asList("_row_key");
    records.stream().forEach(a -> {
      // Generate 10 updated records
      GenericRecord record = payloadGenerator.getUpdatePayload(a, blacklistFields);
      updateRowKeys.add(record.get("_row_key").toString());
      updateTimeStamps.add((Long) record.get("timestamp"));
    });
    // The row keys from insert payloads should match all the row keys from the update payloads
    Assert.assertTrue(insertRowKeys.containsAll(updateRowKeys));
    // The timestamp field for the insert payloads should not all match with the update payloads
    Assert.assertFalse(insertTimeStamps.containsAll(updateTimeStamps));
  }

  @Test
  public void testSimplePayloadWithLargeMinSize() throws Exception {
    Schema schema = new Schema.Parser().parse(getClass().getClassLoader()
        .getResourceAsStream("hudi-bench-config/source.avsc"));
    int minPayloadSize = 1000;
    GenericRecordFullPayloadGenerator payloadGenerator = new GenericRecordFullPayloadGenerator(schema,
        minPayloadSize);
    GenericRecord record = payloadGenerator.getNewPayload();
    // The payload generated is less than minPayloadSize due to no collections present
    assertTrue(HoodieAvroUtils.avroToBytes(record).length < minPayloadSize);
  }

  @Test
  public void testComplexPayloadWithLargeMinSize() throws Exception {
    Schema schema = new Schema.Parser().parse(getClass().getClassLoader()
        .getResourceAsStream("hudi-bench-config/complex-source.avsc"));
    int minPayloadSize = 10000;
    GenericRecordFullPayloadGenerator payloadGenerator = new GenericRecordFullPayloadGenerator(
        schema, minPayloadSize);
    GenericRecord record = payloadGenerator.getNewPayload();
    // The payload generated should be within 10% extra of the minPayloadSize
    assertTrue(HoodieAvroUtils.avroToBytes(record).length < minPayloadSize + 0.1 * minPayloadSize);
  }

}
