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

import org.apache.hudi.common.HoodieCommonTestHarness;
import org.apache.hudi.common.model.AvroBinaryTestPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.HoodieAvroUtils;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SchemaTestUtil;
import org.apache.hudi.common.util.SpillableMapTestUtils;
import org.apache.hudi.common.util.SpillableMapUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.SchemaTestUtil.getSimpleSchema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests dis based map {@link DiskBasedMap}.
 */
public class TestDiskBasedMap extends HoodieCommonTestHarness {

  @Before
  public void setup() {
    initPath();
  }

  @Test
  public void testSimpleInsert() throws IOException, URISyntaxException {
    DiskBasedMap records = new DiskBasedMap<>(basePath);
    List<IndexedRecord> iRecords = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    ((GenericRecord) iRecords.get(0)).get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString();
    List<String> recordKeys = SpillableMapTestUtils.upsertRecords(iRecords, records);

    // make sure records have spilled to disk
    assertTrue(records.sizeOfFileOnDiskInBytes() > 0);
    Iterator<HoodieRecord<? extends HoodieRecordPayload>> itr = records.iterator();
    List<HoodieRecord> oRecords = new ArrayList<>();
    while (itr.hasNext()) {
      HoodieRecord<? extends HoodieRecordPayload> rec = itr.next();
      oRecords.add(rec);
      assert recordKeys.contains(rec.getRecordKey());
    }
  }

  @Test
  public void testSimpleInsertWithoutHoodieMetadata() throws IOException, URISyntaxException {
    DiskBasedMap records = new DiskBasedMap<>(basePath);
    List<HoodieRecord> hoodieRecords = SchemaTestUtil.generateHoodieTestRecordsWithoutHoodieMetadata(0, 1000);
    Set<String> recordKeys = new HashSet<>();
    // insert generated records into the map
    hoodieRecords.stream().forEach(r -> {
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
  }

  @Test
  public void testSimpleUpsert() throws IOException, URISyntaxException {
    Schema schema = HoodieAvroUtils.addMetadataFields(getSimpleSchema());

    DiskBasedMap records = new DiskBasedMap<>(basePath);
    List<IndexedRecord> iRecords = SchemaTestUtil.generateHoodieTestRecords(0, 100);

    // perform some inserts
    List<String> recordKeys = SpillableMapTestUtils.upsertRecords(iRecords, records);

    long fileSize = records.sizeOfFileOnDiskInBytes();
    // make sure records have spilled to disk
    assertTrue(fileSize > 0);

    // generate updates from inserts
    List<IndexedRecord> updatedRecords = SchemaTestUtil.updateHoodieTestRecords(recordKeys,
        SchemaTestUtil.generateHoodieTestRecords(0, 100), HoodieActiveTimeline.createNewCommitTime());
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
        IndexedRecord indexedRecord = (IndexedRecord) rec.getData().getInsertValue(schema).get();
        String latestCommitTime =
            ((GenericRecord) indexedRecord).get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString();
        assertEquals(latestCommitTime, newCommitTime);
      } catch (IOException io) {
        throw new UncheckedIOException(io);
      }
    }
  }

  @Test
  public void testSizeEstimator() throws IOException, URISyntaxException {
    Schema schema = SchemaTestUtil.getSimpleSchema();

    // Test sizeEstimator without hoodie metadata fields
    List<HoodieRecord> hoodieRecords = SchemaTestUtil.generateHoodieTestRecords(0, 1, schema);

    long payloadSize =
        SpillableMapUtils.computePayloadSize(hoodieRecords.remove(0), new HoodieRecordSizeEstimator(schema));
    assertTrue(payloadSize > 0);

    // Test sizeEstimator with hoodie metadata fields
    schema = HoodieAvroUtils.addMetadataFields(schema);
    hoodieRecords = SchemaTestUtil.generateHoodieTestRecords(0, 1, schema);
    payloadSize = SpillableMapUtils.computePayloadSize(hoodieRecords.remove(0), new HoodieRecordSizeEstimator(schema));
    assertTrue(payloadSize > 0);

    // Following tests payloads without an Avro Schema in the Record

    // Test sizeEstimator without hoodie metadata fields and without schema object in the payload
    schema = SchemaTestUtil.getSimpleSchema();
    List<IndexedRecord> indexedRecords = SchemaTestUtil.generateHoodieTestRecords(0, 1);
    hoodieRecords =
        indexedRecords.stream().map(r -> new HoodieRecord(new HoodieKey(UUID.randomUUID().toString(), "0000/00/00"),
            new AvroBinaryTestPayload(Option.of((GenericRecord) r)))).collect(Collectors.toList());
    payloadSize = SpillableMapUtils.computePayloadSize(hoodieRecords.remove(0), new HoodieRecordSizeEstimator(schema));
    assertTrue(payloadSize > 0);

    // Test sizeEstimator with hoodie metadata fields and without schema object in the payload
    final Schema simpleSchemaWithMetadata = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema());
    indexedRecords = SchemaTestUtil.generateHoodieTestRecords(0, 1);
    hoodieRecords = indexedRecords.stream()
        .map(r -> new HoodieRecord(new HoodieKey(UUID.randomUUID().toString(), "0000/00/00"),
            new AvroBinaryTestPayload(
                Option.of(HoodieAvroUtils.rewriteRecord((GenericRecord) r, simpleSchemaWithMetadata)))))
        .collect(Collectors.toList());
    payloadSize = SpillableMapUtils.computePayloadSize(hoodieRecords.remove(0), new HoodieRecordSizeEstimator(schema));
    assertTrue(payloadSize > 0);
  }

  /**
   * @na: Leaving this test here for a quick performance test
   */
  @Ignore
  @Test
  public void testSizeEstimatorPerformance() throws IOException, URISyntaxException {
    // Test sizeEstimatorPerformance with simpleSchema
    Schema schema = SchemaTestUtil.getSimpleSchema();
    List<HoodieRecord> hoodieRecords = SchemaTestUtil.generateHoodieTestRecords(0, 1, schema);
    HoodieRecordSizeEstimator sizeEstimator = new HoodieRecordSizeEstimator(schema);
    HoodieRecord record = hoodieRecords.remove(0);
    long startTime = System.currentTimeMillis();
    SpillableMapUtils.computePayloadSize(record, sizeEstimator);
    long timeTaken = System.currentTimeMillis() - startTime;
    System.out.println("Time taken :" + timeTaken);
    assertTrue(timeTaken < 100);
  }
}
