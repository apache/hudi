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

package org.apache.hudi.metadata;

import org.apache.hudi.avro.model.HoodieRecordIndexInfo;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.InProcessTimeGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieMetadataException;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMetadataIndexMapper extends HoodieCommonTestHarness {

  @Test
  public void testRLIIndexMapperWithInsertsAndUpserts() throws Exception {
    // Generate write status with inserts and upserts
    WriteStatus writeStatus = new WriteStatus(true, 0.0d);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    List<HoodieRecord> records = dataGenerator.generateInserts("001", 10);
    for (int i = 0; i < records.size(); i++) {
      HoodieRecord record = records.get(i);
      String fileId = UUID.randomUUID().toString();
      if (i < 5) {
        // 5 updates
        record.setCurrentLocation(new HoodieRecordLocation(InProcessTimeGenerator.createNewInstantTime(), fileId));
      }
      // 5 inserts
      record.setNewLocation(new HoodieRecordLocation(InProcessTimeGenerator.createNewInstantTime(), fileId));
      writeStatus.markSuccess(record, Option.empty());
    }

    // Generate RLI records
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("random")
        .build();
    RecordIndexMapper recordIndexMapper = new RecordIndexMapper(writeConfig);
    Iterator<HoodieRecord> rliRecords = recordIndexMapper.apply(writeStatus);
    AtomicInteger totalRLIRecords = new AtomicInteger();
    rliRecords.forEachRemaining(rliRecord -> {
      totalRLIRecords.getAndIncrement();

      // verify RLI metadata
      HoodieRecord<HoodieMetadataPayload> record = rliRecord;
      assertEquals(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), record.getKey().getPartitionPath());
      HoodieRecordIndexInfo recordIndexInfo = record.getData().recordIndexMetadata;
      assertNotNull(recordIndexInfo);
      assertTrue(StringUtils.nonEmpty(recordIndexInfo.getPartitionName()));
      assertNotNull(recordIndexInfo.getFileIdHighBits());
      assertNotNull(recordIndexInfo.getFileIdLowBits());
    });
    // RLI is only updated for inserts
    assertEquals(5, totalRLIRecords.get());
  }

  @Test
  public void testRLIIndexGeneratorWithDeletes() throws Exception {
    // Generate write status with deletes
    WriteStatus writeStatus = new WriteStatus(true, 0.0d);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    List<HoodieRecord> records = dataGenerator.generateInserts("001", 10);
    for (int i = 0; i < records.size(); i++) {
      HoodieRecord record = records.get(i);
      String fileId = UUID.randomUUID().toString();
      record.setCurrentLocation(new HoodieRecordLocation(InProcessTimeGenerator.createNewInstantTime(), fileId));
      writeStatus.markSuccess(record, Option.empty());
    }

    // Generate RLI records
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("random")
        .build();
    RecordIndexMapper recordIndexMapper = new RecordIndexMapper(writeConfig);
    Iterator<HoodieRecord> rliRecords = recordIndexMapper.apply(writeStatus);
    AtomicInteger totalRLIRecords = new AtomicInteger();
    rliRecords.forEachRemaining(rliRecord -> {
      totalRLIRecords.getAndIncrement();

      // verify RLI record payload is EmptyHoodieRecordPayload
      HoodieRecord<EmptyHoodieRecordPayload> record = rliRecord;
      assertEquals(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), record.getKey().getPartitionPath());
      assertTrue(record.getData() instanceof EmptyHoodieRecordPayload);
    });
    // RLI is only updated for inserts
    assertEquals(10, totalRLIRecords.get());
  }

  @Test
  void testRLIIndexMapperRejectsKeyInsertedTwice() throws Exception {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("random").build();
    RecordIndexMapper recordIndexMapper = new RecordIndexMapper(writeConfig);
    String instantTime = InProcessTimeGenerator.createNewInstantTime();
    List<HoodieRecord> rliRecords = new ArrayList<>();
    rliRecords.addAll(generateRecords(recordIndexMapper, insertStatus(instantTime, "key1", "key2")));
    rliRecords.addAll(generateRecords(recordIndexMapper, insertStatus(instantTime, "key2", "key3")));
    assertEquals(4, rliRecords.size());

    HoodieMetadataException e = assertThrows(HoodieMetadataException.class,
        () -> recordIndexMapper.postProcess(HoodieListData.lazy(rliRecords)).collectAsList());
    assertTrue(e.getMessage().startsWith("Key key2 appears more than once in this write for metadata partition record_index"),
        e.getMessage());
    assertTrue(e.getMessage().contains("hoodie.combine.before.insert=true"), e.getMessage());
  }

  @Test
  void testRLIIndexMapperKeepsInsertOverDeleteOfSameKey() throws Exception {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("random").build();
    RecordIndexMapper recordIndexMapper = new RecordIndexMapper(writeConfig);
    String instantTime = InProcessTimeGenerator.createNewInstantTime();
    // a delete of key1 from a p1 file group and an insert of key1 into a p2 file group in the same write
    WriteStatus deleteStatus = new WriteStatus(true, 0.0d);
    HoodieRecord deleted = new HoodieAvroRecord(new HoodieKey("key1", "p1"), new EmptyHoodieRecordPayload());
    deleted.setCurrentLocation(new HoodieRecordLocation(instantTime, UUID.randomUUID().toString()));
    deleteStatus.markSuccess(deleted, Option.empty());
    List<HoodieRecord> rliRecords = new ArrayList<>();
    rliRecords.addAll(generateRecords(recordIndexMapper, deleteStatus));
    rliRecords.addAll(generateRecords(recordIndexMapper, insertStatus(instantTime, "key1", "key2")));
    assertEquals(3, rliRecords.size());

    List<HoodieRecord> reduced = recordIndexMapper.postProcess(HoodieListData.lazy(rliRecords)).collectAsList();
    assertEquals(2, reduced.size());
    for (HoodieRecord record : reduced) {
      HoodieRecordIndexInfo recordIndexInfo = ((HoodieMetadataPayload) record.getData()).recordIndexMetadata;
      assertNotNull(recordIndexInfo, "the insert, not the delete, is kept for " + record.getRecordKey());
      assertEquals("p2", recordIndexInfo.getPartitionName());
    }
  }

  private static WriteStatus insertStatus(String instantTime, String... recordKeys) {
    WriteStatus writeStatus = new WriteStatus(true, 0.0d);
    String fileId = UUID.randomUUID().toString();
    for (String recordKey : recordKeys) {
      HoodieRecord record = new HoodieAvroRecord(new HoodieKey(recordKey, "p2"), new EmptyHoodieRecordPayload());
      record.setNewLocation(new HoodieRecordLocation(instantTime, fileId));
      writeStatus.markSuccess(record, Option.empty());
    }
    return writeStatus;
  }

  private static List<HoodieRecord> generateRecords(RecordIndexMapper recordIndexMapper, WriteStatus writeStatus) throws Exception {
    List<HoodieRecord> records = new ArrayList<>();
    recordIndexMapper.apply(writeStatus).forEachRemaining(records::add);
    return records;
  }
}
