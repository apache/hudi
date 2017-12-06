/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.util.collection;

import com.uber.hoodie.common.model.HoodieAvroPayload;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import com.uber.hoodie.common.util.SchemaTestUtil;
import com.uber.hoodie.common.util.SpillableMapTestUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestExternalSpillableMap {

  private static final String FAILURE_OUTPUT_PATH = "/tmp/test_fail";

  @Test
  public void simpleInsertTest() throws IOException, URISyntaxException {
    ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> records =
        new ExternalSpillableMap<>
            (16L, HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema()),
                HoodieAvroPayload.class.getName(), Optional.empty()); //16B

    List<IndexedRecord> iRecords = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    List<String> recordKeys = SpillableMapTestUtils.upsertRecords(iRecords, records);
    assert (recordKeys.size() == 100);
    Iterator<HoodieRecord<? extends HoodieRecordPayload>> itr = records.iterator();
    List<HoodieRecord> oRecords = new ArrayList<>();
    while(itr.hasNext()) {
      HoodieRecord<? extends HoodieRecordPayload> rec = itr.next();
      oRecords.add(rec);
      assert recordKeys.contains(rec.getRecordKey());
    }
  }

  @Test
  public void testSimpleUpsert() throws IOException, URISyntaxException {

    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema());
    ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> records =
        new ExternalSpillableMap<>
            (16L, schema,
                HoodieAvroPayload.class.getName(), Optional.of(FAILURE_OUTPUT_PATH)); //16B

    List<IndexedRecord> iRecords = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    List<String> recordKeys = SpillableMapTestUtils.upsertRecords(iRecords, records);
    assert (recordKeys.size() == 100);
    Iterator<HoodieRecord<? extends HoodieRecordPayload>> itr = records.iterator();
    while(itr.hasNext()) {
      HoodieRecord<? extends HoodieRecordPayload> rec = itr.next();
      assert recordKeys.contains(rec.getRecordKey());
    }
    List<IndexedRecord> updatedRecords =
        SchemaTestUtil.updateHoodieTestRecords(recordKeys, SchemaTestUtil.generateHoodieTestRecords(0, 100),
            HoodieActiveTimeline.createNewCommitTime());

    // update records already inserted
    SpillableMapTestUtils.upsertRecords(updatedRecords, records);

    // make sure we have records spilled to disk
    assertTrue(records.getDiskBasedMapNumEntries() > 0);

    // iterate over the updated records and compare the value from Map
    updatedRecords.stream().forEach(record -> {
      HoodieRecord rec = records.get(((GenericRecord) record).get(HoodieRecord.RECORD_KEY_METADATA_FIELD));
      try {
        assertEquals(rec.getData().getInsertValue(schema).get(),record);
      } catch(IOException io) {
        throw new UncheckedIOException(io);
      }
    });
  }

  @Test
  public void testAllMapOperations() throws IOException, URISyntaxException {

    ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> records =
        new ExternalSpillableMap<>
            (16L, HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema()),
                HoodieAvroPayload.class.getName(), Optional.empty()); //16B

    List<IndexedRecord> iRecords = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    // insert a bunch of records so that values spill to disk too
    List<String> recordKeys = SpillableMapTestUtils.upsertRecords(iRecords, records);
    IndexedRecord inMemoryRecord = iRecords.get(0);
    String ikey = ((GenericRecord)inMemoryRecord).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
    String iPartitionPath = ((GenericRecord)inMemoryRecord).get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString();
    HoodieRecord inMemoryHoodieRecord = new HoodieRecord<>(new HoodieKey(ikey, iPartitionPath),
        new HoodieAvroPayload(Optional.of((GenericRecord)inMemoryRecord)));

    IndexedRecord onDiskRecord = iRecords.get(99);
    String dkey = ((GenericRecord)onDiskRecord).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
    String dPartitionPath = ((GenericRecord)onDiskRecord).get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString();
    HoodieRecord onDiskHoodieRecord = new HoodieRecord<>(new HoodieKey(dkey, dPartitionPath),
        new HoodieAvroPayload(Optional.of((GenericRecord)onDiskRecord)));
    // assert size
    assert records.size() == 100;
    // get should return the same HoodieKey and same value
    assert inMemoryHoodieRecord.getKey().equals(records.get(ikey).getKey());
    assert onDiskHoodieRecord.getKey().equals(records.get(dkey).getKey());
    //assert inMemoryHoodieRecord.equals(records.get(ikey));
    //assert onDiskHoodieRecord.equals(records.get(dkey));

    // test contains
    assertTrue(records.containsKey(ikey));
    assertTrue(records.containsKey(dkey));

    // test isEmpty
    assertFalse(records.isEmpty());

    // test containsAll
    assertTrue(records.keySet().containsAll(recordKeys));

    // remove (from inMemory and onDisk)
    HoodieRecord removedRecord = records.remove(ikey);
    assertTrue(removedRecord != null);
    assertFalse(records.containsKey(ikey));

    removedRecord = records.remove(dkey);
    assertTrue(removedRecord != null);
    assertFalse(records.containsKey(dkey));

    // test clear
    records.clear();
    assertTrue(records.size() == 0);
  }

  @Test(expected = IOException.class)
  public void simpleTestWithException() throws IOException, URISyntaxException {
    ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> records =
        new ExternalSpillableMap<>
            (16L, HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema()),
                HoodieAvroPayload.class.getName(), Optional.of(FAILURE_OUTPUT_PATH)); //16B

    List<IndexedRecord> iRecords = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    List<String> recordKeys = SpillableMapTestUtils.upsertRecords(iRecords, records);
    assert (recordKeys.size() == 100);
    Iterator<HoodieRecord<? extends HoodieRecordPayload>> itr = records.iterator();
    while(itr.hasNext()) {
      throw new IOException("Testing failures...");
    }
  }

  @Test
  public void simpleTestWithExceptionValidateFileIsRemoved() throws Exception {
    File file = new File(FAILURE_OUTPUT_PATH);
    assertFalse(file.exists());
  }

  @Test
  public void testDataCorrectnessInMapAndDisk() throws IOException, URISyntaxException {

    Schema schema = SchemaTestUtil.getSimpleSchema();
    ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> records =
        new ExternalSpillableMap<>
            (16L, HoodieAvroUtils.addMetadataFields(schema),
                HoodieAvroPayload.class.getName(), Optional.of(FAILURE_OUTPUT_PATH)); //16B

    List<String> recordKeys = new ArrayList<>();
    // Ensure we spill to disk
    while(records.getDiskBasedMapNumEntries() < 1) {
      List<IndexedRecord> iRecords = SchemaTestUtil.generateHoodieTestRecords(0, 100);
      recordKeys.addAll(SpillableMapTestUtils.upsertRecords(iRecords, records));
    }

    // Get a record from the in-Memory map
    String key = recordKeys.get(0);
    HoodieRecord record = records.get(key);
    List<IndexedRecord> recordsToUpdate = new ArrayList<>();
    schema = HoodieAvroUtils.addMetadataFields(schema);
    recordsToUpdate.add((IndexedRecord) record.getData().getInsertValue(schema).get());

    String newCommitTime = HoodieActiveTimeline.createNewCommitTime();
    List<String> keysToBeUpdated = new ArrayList<>();
    keysToBeUpdated.add(key);
    // Update the commitTime for this record
    List<IndexedRecord> updatedRecords =
        SchemaTestUtil.updateHoodieTestRecords(keysToBeUpdated, recordsToUpdate, newCommitTime);
    // Upsert this updated record
    SpillableMapTestUtils.upsertRecords(updatedRecords, records);
    GenericRecord gRecord = (GenericRecord) records.get(key).getData().getInsertValue(schema).get();
    // The record returned for this key should have the updated commitTime
    assert newCommitTime.contentEquals(gRecord.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString());


    // Get a record from the disk based map
    key = recordKeys.get(recordKeys.size() - 1);
    record = records.get(key);
    recordsToUpdate = new ArrayList<>();
    recordsToUpdate.add((IndexedRecord) record.getData().getInsertValue(schema).get());

    newCommitTime = HoodieActiveTimeline.createNewCommitTime();
    keysToBeUpdated = new ArrayList<>();
    keysToBeUpdated.add(key);
    // Update the commitTime for this record
    updatedRecords =
        SchemaTestUtil.updateHoodieTestRecords(keysToBeUpdated, recordsToUpdate, newCommitTime);
    // Upsert this updated record
    SpillableMapTestUtils.upsertRecords(updatedRecords, records);
    gRecord = (GenericRecord) records.get(key).getData().getInsertValue(schema).get();
    // The record returned for this key should have the updated commitTime
    assert newCommitTime.contentEquals(gRecord.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString());

  }

  // TODO : come up with a performance eval test for spillableMap
  @Test
  public void testLargeInsertUpsert() {
  }
}
