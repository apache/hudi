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
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
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
  public void testTagRecordsWithPartitionUpdates() throws IOException {
    final String[] partitions = new String[] {"pt1", "pt2", "pt3"};
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEED, partitions, new HashMap<>());
    List<HoodieRecord> recordsIn1stPartition = dataGen.generateInsertsForPartition("001", 1, "pt1");
    final String recordKey = recordsIn1stPartition.get(0).getRecordKey();
    List<HoodieRecord> recordsIn2ndPartition = dataGen.generateUpdatesForDifferentPartition("002", recordsIn1stPartition, "pt2");
    List<HoodieRecord> recordsIn3rdPartition = dataGen.generateUpdatesForDifferentPartition("003", recordsIn2ndPartition, "pt3");
    HoodieRecordLocation dummyLoc = new HoodieRecordLocation("dummy-instant-time", "dummy-file-id");
    HoodiePairData<HoodieKey, HoodieRecordLocation> existingRecords = HoodieListPairData.eager(Stream.of(
            recordsIn1stPartition.get(0),
            recordsIn2ndPartition.get(0))
        .map(r -> Pair.of(r.getKey(), dummyLoc))
        .collect(Collectors.toList()));
    HoodiePairData<String, HoodieRecord<RawTripTestPayload>> incomingRecords = HoodieListPairData.eager(Stream.of(
        Pair.of(recordKey, (HoodieRecord<RawTripTestPayload>) recordsIn3rdPartition.get(0))).collect(Collectors.toList()));

    when(mockWriteConfig.getGlobalSimpleIndexUpdatePartitionPath()).thenReturn(true);
    HoodieGlobalSimpleIndex index = new HoodieGlobalSimpleIndex(mockWriteConfig, Option.empty());
    List<HoodieRecord<RawTripTestPayload>> taggedRecords = index.getTaggedRecords(incomingRecords, existingRecords)
        .collectAsList().stream().sorted(Comparator.comparing(HoodieRecord::getPartitionPath)).collect(Collectors.toList());
    assertEquals(3, taggedRecords.size(), "expect 2 delete records in pt1 and pt2, and 1 tagged record in pt3");
    HoodieRecord<RawTripTestPayload> deleteRecordPt1 = taggedRecords.get(0);
    assertEquals(recordKey, deleteRecordPt1.getRecordKey());
    assertEquals("pt1", deleteRecordPt1.getPartitionPath());
    assertEquals(dummyLoc, deleteRecordPt1.getCurrentLocation());
    HoodieRecord<RawTripTestPayload> deleteRecordPt2 = taggedRecords.get(1);
    assertEquals(recordKey, deleteRecordPt2.getRecordKey());
    assertEquals("pt2", deleteRecordPt2.getPartitionPath());
    assertEquals(dummyLoc, deleteRecordPt2.getCurrentLocation());
    HoodieRecord<RawTripTestPayload> insertRecord = taggedRecords.get(2);
    assertEquals(recordKey, insertRecord.getRecordKey());
    assertEquals("pt3", insertRecord.getPartitionPath());
    assertNull(insertRecord.getCurrentLocation());
  }
}
