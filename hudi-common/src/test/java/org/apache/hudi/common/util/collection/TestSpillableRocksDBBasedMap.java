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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.testutils.SpillableMapTestUtils;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.apache.hudi.common.testutils.SchemaTestUtil.getSimpleSchema;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test the rocksDb based Map {@link SpillableRocksDBBasedMap}
 * that is used by {@link ExternalSpillableMap}.
 */
public class TestSpillableRocksDBBasedMap extends HoodieCommonTestHarness {

  @BeforeEach
  public void setUp() {
    initPath();
  }

  @Test
  public void testSimpleInsertSequential() throws IOException, URISyntaxException {
    SpillableRocksDBBasedMap rocksDBBasedMap = new SpillableRocksDBBasedMap(basePath);
    List<IndexedRecord> iRecords = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    ((GenericRecord) iRecords.get(0)).get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString();
    List<String> recordKeys = SpillableMapTestUtils.upsertRecords(iRecords, rocksDBBasedMap);

    // Ensure the number of records is correct
    assertEquals(rocksDBBasedMap.size(), recordKeys.size());
    // make sure records have spilled to disk
    assertTrue(rocksDBBasedMap.sizeOfFileOnDiskInBytes() > 0);
    Iterator<HoodieRecord<? extends HoodieRecordPayload>> itr = rocksDBBasedMap.iterator();
    List<HoodieRecord> oRecords = new ArrayList<>();
    while (itr.hasNext()) {
      HoodieRecord<? extends HoodieRecordPayload> rec = itr.next();
      oRecords.add(rec);
      assert recordKeys.contains(rec.getRecordKey());
    }
    assertEquals(recordKeys.size(), oRecords.size());
  }

  @Test
  public void testSimpleInsertRandomAccess() throws IOException, URISyntaxException {
    SpillableRocksDBBasedMap rocksDBBasedMap = new SpillableRocksDBBasedMap(basePath);
    List<IndexedRecord> iRecords = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    ((GenericRecord) iRecords.get(0)).get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString();
    List<String> recordKeys = SpillableMapTestUtils.upsertRecords(iRecords, rocksDBBasedMap);

    // make sure records have spilled to disk
    assertTrue(rocksDBBasedMap.sizeOfFileOnDiskInBytes() > 0);

    Random random = new Random();
    for (int i = 0; i < recordKeys.size(); i++) {
      String key = recordKeys.get(random.nextInt(recordKeys.size()));
      assert rocksDBBasedMap.get(key) != null;
    }
  }

  @Test
  public void testSimpleInsertWithoutHoodieMetadata() throws IOException, URISyntaxException {
    SpillableRocksDBBasedMap rocksDBBasedMap = new SpillableRocksDBBasedMap<>(basePath);
    List<HoodieRecord> hoodieRecords = SchemaTestUtil.generateHoodieTestRecordsWithoutHoodieMetadata(0, 1000);
    Set<String> recordKeys = new HashSet<>();
    // insert generated records into the map
    hoodieRecords.forEach(r -> {
      rocksDBBasedMap.put(r.getRecordKey(), r);
      recordKeys.add(r.getRecordKey());
    });
    // make sure records have spilled to disk
    assertTrue(rocksDBBasedMap.sizeOfFileOnDiskInBytes() > 0);
    Iterator<HoodieRecord<? extends HoodieRecordPayload>> itr = rocksDBBasedMap.iterator();
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

    SpillableRocksDBBasedMap rocksDBBasedMap = new SpillableRocksDBBasedMap<>(basePath);
    List<IndexedRecord> iRecords = SchemaTestUtil.generateHoodieTestRecords(0, 100);

    // perform some inserts
    List<String> recordKeys = SpillableMapTestUtils.upsertRecords(iRecords, rocksDBBasedMap);

    long fileSize = rocksDBBasedMap.sizeOfFileOnDiskInBytes();
    // make sure records have spilled to disk
    assertTrue(fileSize > 0);

    // generate updates from inserts
    List<IndexedRecord> updatedRecords = SchemaTestUtil.updateHoodieTestRecords(recordKeys,
        SchemaTestUtil.generateHoodieTestRecords(0, 100), HoodieActiveTimeline.createNewInstantTime());
    String newCommitTime =
        ((GenericRecord) updatedRecords.get(0)).get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString();

    // perform upserts
    recordKeys = SpillableMapTestUtils.upsertRecords(updatedRecords, rocksDBBasedMap);

    // On disk (rocksDb), upserts should actual replace the original records, and the size
    // should not increase. But due to the way the bytes are accounted,
    // sizeOfFileOnDiskInBytes only maintains the total
    // bytes written, and will increase with upserts.
    assertTrue(rocksDBBasedMap.sizeOfFileOnDiskInBytes() > fileSize);

    // Upserted records (on disk) should have the latest commit time
    Iterator<HoodieRecord<? extends HoodieRecordPayload>> itr = rocksDBBasedMap.iterator();
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
  public void testPutAll() throws IOException, URISyntaxException {
    SpillableRocksDBBasedMap<String, HoodieRecord> rocksDBBasedMap = new SpillableRocksDBBasedMap<>(basePath);
    List<IndexedRecord> iRecords = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    Map<String, HoodieRecord> recordMap = new HashMap<>();
    iRecords.forEach(r -> {
      String key = ((GenericRecord) r).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
      String partitionPath = ((GenericRecord) r).get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString();
      HoodieRecord value = new HoodieRecord<>(new HoodieKey(key, partitionPath), new HoodieAvroPayload(Option.of((GenericRecord) r)));
      recordMap.put(key, value);
    });

    rocksDBBasedMap.putAll(recordMap);
    // make sure records have spilled to disk
    assertTrue(rocksDBBasedMap.sizeOfFileOnDiskInBytes() > 0);

    // make sure all added records are present
    for (Map.Entry<String, HoodieRecord> entry : rocksDBBasedMap.entrySet()) {
      assertTrue(recordMap.containsKey(entry.getKey()));
    }
  }

  @Test
  public void testSimpleRemove() throws IOException, URISyntaxException {
    SpillableRocksDBBasedMap rocksDBBasedMap = new SpillableRocksDBBasedMap(basePath);
    List<IndexedRecord> iRecords = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    ((GenericRecord) iRecords.get(0)).get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString();
    List<String> recordKeys = SpillableMapTestUtils.upsertRecords(iRecords, rocksDBBasedMap);

    // make sure records have spilled to disk
    assertTrue(rocksDBBasedMap.sizeOfFileOnDiskInBytes() > 0);

    List<String> deleteKeys = recordKeys.subList(0, 10);
    for (String deleteKey : deleteKeys) {
      assert rocksDBBasedMap.remove(deleteKey) != null;
      assert rocksDBBasedMap.get(deleteKey) == null;
    }
  }
}