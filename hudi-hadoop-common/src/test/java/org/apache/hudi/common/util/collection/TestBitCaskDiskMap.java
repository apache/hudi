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

package org.apache.hudi.common.util.collection;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.serialization.DefaultSerializer;
import org.apache.hudi.common.testutils.AvroBinaryTestPayload;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.InProcessTimeGenerator;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.testutils.SpillableMapTestUtils;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SpillableMapUtils;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.SchemaTestUtil.getSimpleSchema;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests dis based map {@link BitCaskDiskMap}.
 */
public class TestBitCaskDiskMap extends HoodieCommonTestHarness {

  @BeforeEach
  public void setup() {
    initPath();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testSimpleInsert(boolean isCompressionEnabled) throws IOException, URISyntaxException {
    try (BitCaskDiskMap records = new BitCaskDiskMap<>(basePath, new DefaultSerializer<>(), isCompressionEnabled)) {
      SchemaTestUtil testUtil = new SchemaTestUtil();
      List<IndexedRecord> iRecords = testUtil.generateHoodieTestRecords(0, 100);
      List<String> recordKeys = SpillableMapTestUtils.upsertRecords(iRecords, records);

      Map<String, IndexedRecord> originalRecords = iRecords.stream()
          .collect(Collectors.toMap(k -> ((GenericRecord) k).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString(), v -> v));

      // make sure records have spilled to disk
      assertTrue(records.sizeOfFileOnDiskInBytes() > 0);
      Iterator<HoodieRecord<? extends HoodieRecordPayload>> itr = records.iterator();
      while (itr.hasNext()) {
        HoodieRecord<? extends HoodieRecordPayload> rec = itr.next();
        assert recordKeys.contains(rec.getRecordKey());
        IndexedRecord originalRecord = originalRecords.get(rec.getRecordKey());
        HoodieAvroPayload payload = (HoodieAvroPayload) rec.getData();
        Option<IndexedRecord> value = payload.getInsertValue(HoodieAvroUtils.addMetadataFields(getSimpleSchema().toAvroSchema()));
        assertEquals(originalRecord, value.get());
      }

      verifyCleanup(records);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testSimpleInsertWithoutHoodieMetadata(boolean isCompressionEnabled) throws IOException, URISyntaxException {
    try (BitCaskDiskMap records = new BitCaskDiskMap<>(basePath, new DefaultSerializer<>(), isCompressionEnabled)) {
      SchemaTestUtil testUtil = new SchemaTestUtil();
      List<HoodieRecord> hoodieRecords = testUtil.generateHoodieTestRecordsWithoutHoodieMetadata(0, 1000);
      Set<String> recordKeys = new HashSet<>();
      // insert generated records into the map
      hoodieRecords.forEach(r -> {
        records.put(r.getRecordKey(), r);
        recordKeys.add(r.getRecordKey());
      });
      // make sure records have spilled to disk
      assertTrue(records.sizeOfFileOnDiskInBytes() > 0);
      Iterator<HoodieRecord<? extends HoodieRecordPayload>> itr = records.iterator();
      List<HoodieRecord> oRecords = new ArrayList<>();
      while (itr.hasNext()) {
        HoodieRecord<? extends HoodieRecordPayload> rec = itr.next();
        oRecords.add(rec);
        assert recordKeys.contains(rec.getRecordKey());
      }


      // test iterator with predicate
      String firstKey = recordKeys.stream().findFirst().get();
      recordKeys.remove(firstKey);
      itr = records.iterator(key -> !key.equals(firstKey));
      int cntSize = 0;
      while (itr.hasNext()) {
        HoodieRecord<? extends HoodieRecordPayload> rec = itr.next();
        cntSize++;
        assert recordKeys.contains(rec.getRecordKey());
      }
      assertEquals(recordKeys.size(), cntSize);

      verifyCleanup(records);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testSimpleUpsert(boolean isCompressionEnabled) throws IOException, URISyntaxException {
    HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(getSimpleSchema());

    try (BitCaskDiskMap records = new BitCaskDiskMap<>(basePath, new DefaultSerializer<>(), isCompressionEnabled)) {
      SchemaTestUtil testUtil = new SchemaTestUtil();
      List<IndexedRecord> iRecords = testUtil.generateHoodieTestRecords(0, 100);

      // perform some inserts
      List<String> recordKeys = SpillableMapTestUtils.upsertRecords(iRecords, records);

      long fileSize = records.sizeOfFileOnDiskInBytes();
      // make sure records have spilled to disk
      assertTrue(fileSize > 0);

      // generate updates from inserts
      List<IndexedRecord> updatedRecords = SchemaTestUtil.updateHoodieTestRecords(recordKeys,
          testUtil.generateHoodieTestRecords(0, 100), InProcessTimeGenerator.createNewInstantTime());
      String newCommitTime =
          ((GenericRecord) updatedRecords.get(0)).get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString();

      // perform upserts
      recordKeys = SpillableMapTestUtils.upsertRecords(updatedRecords, records);

      // upserts should be appended to the existing file, hence increasing the sizeOfFile on disk
      assertTrue(records.sizeOfFileOnDiskInBytes() > fileSize);

      // Upserted records (on disk) should have the latest commit time
      Iterator<HoodieRecord<? extends HoodieRecordPayload>> itr = records.iterator();
      while (itr.hasNext()) {
        HoodieRecord<? extends HoodieRecordPayload> rec = itr.next();
        assert recordKeys.contains(rec.getRecordKey());
        try {
          IndexedRecord indexedRecord = (IndexedRecord) rec.getData().getInsertValue(schema.toAvroSchema()).get();
          String latestCommitTime =
              ((GenericRecord) indexedRecord).get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString();
          assertEquals(latestCommitTime, newCommitTime);
        } catch (IOException io) {
          throw new UncheckedIOException(io);
        }
      }
      verifyCleanup(records);
    }
  }

  @Test
  public void testSizeEstimator() throws IOException, URISyntaxException {
    HoodieSchema schema = SchemaTestUtil.getSimpleSchema();

    // Test sizeEstimator without hoodie metadata fields
    SchemaTestUtil testUtil = new SchemaTestUtil();
    List<HoodieRecord> hoodieRecords = testUtil.generateHoodieTestRecords(0, 1, schema);

    long payloadSize =
        SpillableMapUtils.computePayloadSize(hoodieRecords.remove(0), new HoodieRecordSizeEstimator(schema.toAvroSchema()));
    assertTrue(payloadSize > 0);

    // Test sizeEstimator with hoodie metadata fields
    schema = HoodieSchemaUtils.addMetadataFields(schema);
    hoodieRecords = testUtil.generateHoodieTestRecords(0, 1, schema);
    payloadSize = SpillableMapUtils.computePayloadSize(hoodieRecords.remove(0), new HoodieRecordSizeEstimator(schema.toAvroSchema()));
    assertTrue(payloadSize > 0);

    // Following tests payloads without an Avro Schema in the Record

    // Test sizeEstimator without hoodie metadata fields and without schema object in the payload
    schema = SchemaTestUtil.getSimpleSchema();
    List<IndexedRecord> indexedRecords = testUtil.generateHoodieTestRecords(0, 1);
    hoodieRecords =
        indexedRecords.stream().map(r -> new HoodieAvroRecord<>(new HoodieKey(UUID.randomUUID().toString(), "0000/00/00"),
            new AvroBinaryTestPayload(Option.of((GenericRecord) r)))).collect(Collectors.toList());
    payloadSize = SpillableMapUtils.computePayloadSize(hoodieRecords.remove(0), new HoodieRecordSizeEstimator(schema.toAvroSchema()));
    assertTrue(payloadSize > 0);

    // Test sizeEstimator with hoodie metadata fields and without schema object in the payload
    final HoodieSchema simpleSchemaWithMetadata = HoodieSchemaUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema());
    indexedRecords = testUtil.generateHoodieTestRecords(0, 1);
    hoodieRecords = indexedRecords.stream()
        .map(r -> new HoodieAvroRecord<>(new HoodieKey(UUID.randomUUID().toString(), "0000/00/00"),
            new AvroBinaryTestPayload(
                Option.of(HoodieAvroUtils.rewriteRecord((GenericRecord) r, simpleSchemaWithMetadata.toAvroSchema())))))
        .collect(Collectors.toList());
    payloadSize = SpillableMapUtils.computePayloadSize(hoodieRecords.remove(0), new HoodieRecordSizeEstimator(schema.toAvroSchema()));
    assertTrue(payloadSize > 0);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testPutAll(boolean isCompressionEnabled) throws IOException, URISyntaxException {
    try (BitCaskDiskMap<String, HoodieRecord> records = new BitCaskDiskMap<>(basePath, new DefaultSerializer<>(), isCompressionEnabled)) {
      SchemaTestUtil testUtil = new SchemaTestUtil();
      List<IndexedRecord> iRecords = testUtil.generateHoodieTestRecords(0, 100);
      Map<String, HoodieRecord> recordMap = new HashMap<>();
      iRecords.forEach(r -> {
        String key = ((GenericRecord) r).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
        String partitionPath = ((GenericRecord) r).get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString();
        HoodieRecord value = new HoodieAvroRecord<>(new HoodieKey(key, partitionPath), new HoodieAvroPayload(Option.of((GenericRecord) r)));
        recordMap.put(key, value);
      });

      records.putAll(recordMap);
      // make sure records have spilled to disk
      assertTrue(records.sizeOfFileOnDiskInBytes() > 0);

      // make sure all added records are present
      for (Map.Entry<String, HoodieRecord> entry : records.entrySet()) {
        assertTrue(recordMap.containsKey(entry.getKey()));
      }
    }
  }

  /**
   * @na: Leaving this test here for a quick performance test
   */
  @Disabled
  @Test
  public void testSizeEstimatorPerformance() throws IOException, URISyntaxException {
    // Test sizeEstimatorPerformance with simpleSchema
    HoodieSchema schema = SchemaTestUtil.getSimpleSchema();
    SchemaTestUtil testUtil = new SchemaTestUtil();
    List<HoodieRecord> hoodieRecords = testUtil.generateHoodieTestRecords(0, 1, schema);
    HoodieRecordSizeEstimator sizeEstimator = new HoodieRecordSizeEstimator<>(schema.toAvroSchema());
    HoodieRecord record = hoodieRecords.remove(0);
    long startTime = System.currentTimeMillis();
    SpillableMapUtils.computePayloadSize(record, sizeEstimator);
    long timeTaken = System.currentTimeMillis() - startTime;
    assertTrue(timeTaken < 100, "Expected execution time under 100ms but was " + timeTaken);
  }

  private void verifyCleanup(BitCaskDiskMap<String, HoodieRecord> records) {
    File basePathDir = new File(basePath);
    assert Objects.requireNonNull(basePathDir.list()).length > 0;
    records.close();
    assertEquals(Objects.requireNonNull(basePathDir.list()).length, 0);
  }
}
