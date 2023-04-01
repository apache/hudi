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

package org.apache.hudi.index.simple;

import org.apache.hudi.common.data.HoodieListPairData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.testutils.HoodieSimpleDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestHoodieGlobalSimpleIndex {

  @Mock
  HoodieWriteConfig mockWriteConfig;

  @Test
  public void testTagRecordsWithPartitionUpdates() {
    HoodieSimpleDataGenerator dataGen = new HoodieSimpleDataGenerator();
    HoodieRecordLocation dummyLoc = new HoodieRecordLocation("dummy-instant-time", "dummy-file-id");
    HoodiePairData<HoodieKey, HoodieRecordLocation> existingRecords = HoodieListPairData.eager(Stream.of(
            dataGen.getNewRecord(0, "p0", 0),
            dataGen.getNewRecord(0, "p1", 1))
        .map(r -> Pair.of(r.getKey(), dummyLoc))
        .collect(Collectors.toList()));
    HoodiePairData<String, HoodieRecord<DefaultHoodieRecordPayload>> incomingRecords = HoodieListPairData.eager(Stream.of(
        Pair.of("0", dataGen.getNewRecord(0, "p2", 2))).collect(Collectors.toList()));

    when(mockWriteConfig.getGlobalSimpleIndexUpdatePartitionPath()).thenReturn(true);
    HoodieGlobalSimpleIndex index = new HoodieGlobalSimpleIndex(mockWriteConfig, Option.empty());
    List<HoodieRecord<DefaultHoodieRecordPayload>> taggedRecords = index.getTaggedRecords(incomingRecords, existingRecords)
        .collectAsList().stream().sorted(Comparator.comparing(HoodieRecord::getPartitionPath)).collect(Collectors.toList());
    assertEquals(3, taggedRecords.size(), "expect 2 delete records in p0 and p1, and 1 tagged record in p2");
    HoodieRecord<DefaultHoodieRecordPayload> deleteRecordPt0 = taggedRecords.get(0);
    assertEquals("0", deleteRecordPt0.getRecordKey());
    assertEquals("p0", deleteRecordPt0.getPartitionPath());
    assertEquals(dummyLoc, deleteRecordPt0.getCurrentLocation());
    HoodieRecord<DefaultHoodieRecordPayload> deleteRecordPt1 = taggedRecords.get(1);
    assertEquals("0", deleteRecordPt1.getRecordKey());
    assertEquals("p1", deleteRecordPt1.getPartitionPath());
    assertEquals(dummyLoc, deleteRecordPt1.getCurrentLocation());
    HoodieRecord<DefaultHoodieRecordPayload> insertRecord = taggedRecords.get(2);
    assertEquals("0", insertRecord.getRecordKey());
    assertEquals("p2", insertRecord.getPartitionPath());
    assertNull(insertRecord.getCurrentLocation());
  }
}
