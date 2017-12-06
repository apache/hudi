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
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import com.uber.hoodie.common.util.SchemaTestUtil;
import com.uber.hoodie.common.util.SpillableMapTestUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class TestDiskBasedMap {

  @Test
  public void testSimpleInsert() throws IOException, URISyntaxException {
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema());
    DiskBasedMap records = new DiskBasedMap<>(schema, HoodieAvroPayload.class.getName(),Optional.empty());
    List<IndexedRecord> iRecords = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    String commitTime = ((GenericRecord)iRecords.get(0)).get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString();
    List<String> recordKeys = SpillableMapTestUtils.upsertRecords(iRecords, records);

    // make sure records have spilled to disk
    assertTrue(records.sizeOfFileOnDiskInBytes() > 0);
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
    DiskBasedMap records = new DiskBasedMap<>(schema, HoodieAvroPayload.class.getName(),Optional.empty());
    List<IndexedRecord> iRecords = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    String commitTime = ((GenericRecord)iRecords.get(0)).get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString();
    // perform some inserts
    List<String> recordKeys = SpillableMapTestUtils.upsertRecords(iRecords, records);

    long fileSize = records.sizeOfFileOnDiskInBytes();
    // make sure records have spilled to disk
    assertTrue(fileSize > 0);

    // generate updates from inserts
    List<IndexedRecord> updatedRecords =
        SchemaTestUtil.updateHoodieTestRecords(recordKeys, SchemaTestUtil.generateHoodieTestRecords(0, 100),
            HoodieActiveTimeline.createNewCommitTime());
    String newCommitTime = ((GenericRecord)updatedRecords.get(0)).get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString();

    // new commit time should be different
    assertEquals(commitTime, newCommitTime);

    // perform upserts
    recordKeys = SpillableMapTestUtils.upsertRecords(updatedRecords, records);

    // upserts should be appended to the existing file, hence increasing the sizeOfFile on disk
    assertTrue(records.sizeOfFileOnDiskInBytes() > fileSize);

    // Upserted records (on disk) should have the latest commit time
    Iterator<HoodieRecord<? extends HoodieRecordPayload>> itr = records.iterator();
    while(itr.hasNext()) {
      HoodieRecord<? extends HoodieRecordPayload> rec = itr.next();
      assert recordKeys.contains(rec.getRecordKey());
      try {
        IndexedRecord indexedRecord = (IndexedRecord)rec.getData().getInsertValue(schema).get();
        String latestCommitTime = ((GenericRecord)indexedRecord).get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString();
        assertEquals(latestCommitTime, newCommitTime);
      } catch(IOException io) {
        throw new UncheckedIOException(io);
      }
    }
  }
}
