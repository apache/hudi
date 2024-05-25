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

package org.apache.hudi.integ.testsuite.generator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link GenericRecordFullPayloadGenerator} and {@link GenericRecordPartialPayloadGenerator}.
 */
public class TestGenericRecordPayloadGenerator {

  private static final String SOURCE_SCHEMA_DOCKER_DEMO_RELATIVE_PATH = "/docker/demo/config/test-suite/source.avsc";
  private static final String COMPLEX_SOURCE_SCHEMA_DOCKER_DEMO_RELATIVE_PATH =
      "/docker/demo/config/test-suite/complex-source.avsc";

  @Test
  public void testSimplePayload() throws Exception {
    Schema schema = new Schema.Parser().parse(UtilitiesTestBase.Helpers
        .readFileFromAbsolutePath(System.getProperty("user.dir") + "/.." + SOURCE_SCHEMA_DOCKER_DEMO_RELATIVE_PATH));
    GenericRecordFullPayloadGenerator payloadGenerator = new GenericRecordFullPayloadGenerator(schema);
    GenericRecord record = payloadGenerator.getNewPayload();
    // The generated payload should validate with the provided schema
    payloadGenerator.validate(record);
  }

  @Test
  public void testComplexPayload() throws IOException {
    Schema schema = new Schema.Parser().parse(UtilitiesTestBase.Helpers
        .readFileFromAbsolutePath(System.getProperty("user.dir") + "/.."
            + COMPLEX_SOURCE_SCHEMA_DOCKER_DEMO_RELATIVE_PATH));
    GenericRecordFullPayloadGenerator payloadGenerator = new GenericRecordFullPayloadGenerator(schema);
    GenericRecord record = payloadGenerator.getNewPayload();
    // The generated payload should validate with the provided schema
    assertTrue(payloadGenerator.validate(record));
  }

  @Test
  public void testComplexPartialPayload() throws IOException {
    Schema schema = new Schema.Parser().parse(UtilitiesTestBase.Helpers
        .readFileFromAbsolutePath(System.getProperty("user.dir") + "/.."
            + COMPLEX_SOURCE_SCHEMA_DOCKER_DEMO_RELATIVE_PATH));
    GenericRecordPartialPayloadGenerator payloadGenerator = new GenericRecordPartialPayloadGenerator(schema);
    IntStream.range(0, 10).forEach(a -> {
      GenericRecord record = payloadGenerator.getNewPayload();
      // The generated payload should validate with the provided schema
      assertTrue(payloadGenerator.validate(record));
    });
  }

  @Test
  public void testUpdatePayloadGenerator() throws IOException {
    Schema schema = new Schema.Parser().parse(UtilitiesTestBase.Helpers
        .readFileFromAbsolutePath(System.getProperty("user.dir") + "/.." + SOURCE_SCHEMA_DOCKER_DEMO_RELATIVE_PATH));
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
    Set<String> blacklistFields = new HashSet<>();
    blacklistFields.add("_row_key");
    records.stream().forEach(a -> {
      // Generate 10 updated records
      GenericRecord record = payloadGenerator.getUpdatePayload(a, blacklistFields);
      updateRowKeys.add(record.get("_row_key").toString());
      updateTimeStamps.add((Long) record.get("timestamp"));
    });
    // The row keys from insert payloads should match all the row keys from the update payloads
    assertTrue(insertRowKeys.containsAll(updateRowKeys));
    // The timestamp field for the insert payloads should not all match with the update payloads
    assertFalse(insertTimeStamps.containsAll(updateTimeStamps));
  }

  @Test
  public void testSimplePayloadWithLargeMinSize() throws Exception {
    Schema schema = new Schema.Parser().parse(UtilitiesTestBase.Helpers
        .readFileFromAbsolutePath(System.getProperty("user.dir") + "/.." + SOURCE_SCHEMA_DOCKER_DEMO_RELATIVE_PATH));
    int minPayloadSize = 1000;
    GenericRecordFullPayloadGenerator payloadGenerator = new GenericRecordFullPayloadGenerator(schema,
        minPayloadSize);
    GenericRecord record = payloadGenerator.getNewPayload();
    // The payload generated is less than minPayloadSize due to no collections present
    assertTrue(HoodieAvroUtils.avroToBytes(record).length < minPayloadSize);
  }

  @Test
  public void testComplexPayloadWithLargeMinSize() throws Exception {
    Schema schema = new Schema.Parser().parse(UtilitiesTestBase.Helpers
        .readFileFromAbsolutePath(System.getProperty("user.dir") + "/.."
            + COMPLEX_SOURCE_SCHEMA_DOCKER_DEMO_RELATIVE_PATH));
    int minPayloadSize = 10000;
    GenericRecordFullPayloadGenerator payloadGenerator = new GenericRecordFullPayloadGenerator(
        schema, minPayloadSize);
    GenericRecord record = payloadGenerator.getNewPayload();
    // The payload generated should be within 10% extra of the minPayloadSize
    assertTrue(HoodieAvroUtils.avroToBytes(record).length < minPayloadSize + 0.1 * minPayloadSize);
  }

  @Test
  public void testUpdatePayloadGeneratorWithTimestamp() throws IOException {
    Schema schema = new Schema.Parser().parse(UtilitiesTestBase.Helpers
        .readFileFromAbsolutePath(System.getProperty("user.dir") + "/.." + SOURCE_SCHEMA_DOCKER_DEMO_RELATIVE_PATH));
    GenericRecordFullPayloadGenerator payloadGenerator = new GenericRecordFullPayloadGenerator(schema);
    List<String> insertRowKeys = new ArrayList<>();
    List<String> updateRowKeys = new ArrayList<>();
    List<Long> insertTimeStamps = new ArrayList<>();
    List<Long> updateTimeStamps = new ArrayList<>();
    List<GenericRecord> records = new ArrayList<>();
    Long startSeconds = 0L;
    Long endSeconds = TimeUnit.SECONDS.convert(10, TimeUnit.DAYS);
    // Generate 10 new records
    IntStream.range(0, 10).forEach(a -> {
      GenericRecord record = payloadGenerator.getNewPayloadWithTimestamp("timestamp");
      records.add(record);
      insertRowKeys.add(record.get("_row_key").toString());
      insertTimeStamps.add((Long) record.get("timestamp"));
    });
    Set<String> blacklistFields = new HashSet<>(Arrays.asList("_row_key"));
    records.stream().forEach(a -> {
      // Generate 10 updated records
      GenericRecord record = payloadGenerator.getUpdatePayloadWithTimestamp(a, blacklistFields, "timestamp");
      updateRowKeys.add(record.get("_row_key").toString());
      updateTimeStamps.add((Long) record.get("timestamp"));
    });
    // The row keys from insert payloads should match all the row keys from the update payloads
    assertTrue(insertRowKeys.containsAll(updateRowKeys));
    // The timestamp field for the insert payloads should not all match with the update payloads
    assertFalse(insertTimeStamps.containsAll(updateTimeStamps));
    assertTrue(insertTimeStamps.stream().allMatch(t -> t >= startSeconds  && t <= endSeconds));
  }
}
