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

package org.apache.hudi.index;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.data.HoodieListPairData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.collection.Pair;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HoodieGlobalIndexTestHarness {

  protected void verifyTagRecordsWithPartitionUpdates(
      BiFunction<HoodieData<HoodieRecord<RawTripTestPayload>>, HoodiePairData<HoodieKey, HoodieRecordLocation>, List<HoodieRecord<RawTripTestPayload>>> indexTagging)
      throws IOException {
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEED, new String[] {"pt1", "pt2", "pt3"}, new HashMap<>());
    List<HoodieRecord> recordsPt1 = dataGen.generateInsertsForPartition("001", 1, "pt1");
    List<HoodieRecord> recordsPt2 = dataGen.generateUpdatesForDifferentPartition("002", recordsPt1, "pt2");
    final HoodieRecordLocation loc1 = new HoodieRecordLocation("001", "file-in-pt1");
    final HoodieRecordLocation loc2 = new HoodieRecordLocation("002", "file-in-pt2");
    final HoodiePairData<HoodieKey, HoodieRecordLocation> existingRecords = HoodieListPairData.eager(Stream.of(
            Pair.of(recordsPt1.get(0).getKey(), loc1),
            Pair.of(recordsPt2.get(0).getKey(), loc2))
        .collect(Collectors.toList()));
    final String expectedRecordKey = recordsPt1.get(0).getRecordKey();

    // case 1: the incoming updating record is going to pt3
    HoodieData<HoodieRecord<RawTripTestPayload>> incomingPt3 = HoodieListData.eager(Collections.singletonList(
        (HoodieRecord<RawTripTestPayload>) dataGen.generateUpdatesForDifferentPartition("003", recordsPt2, "pt3").get(0)));

    List<HoodieRecord> taggedRecords1 = indexTagging.apply(incomingPt3, existingRecords)
        .stream().sorted(Comparator.comparing(HoodieRecord::getPartitionPath)).collect(Collectors.toList());
    assertEquals(3, taggedRecords1.size(), "expect 2 deletes in pt1 and pt2, and 1 insert in pt3");

    HoodieRecord tagged1First = taggedRecords1.get(0);
    assertEquals(expectedRecordKey, tagged1First.getRecordKey());
    assertEquals("pt1", tagged1First.getPartitionPath());
    assertEquals(loc1, tagged1First.getCurrentLocation());
    assertTrue(tagged1First.getData() instanceof EmptyHoodieRecordPayload);

    HoodieRecord tagged1Second = taggedRecords1.get(1);
    assertEquals(expectedRecordKey, tagged1Second.getRecordKey());
    assertEquals("pt2", tagged1Second.getPartitionPath());
    assertEquals(loc2, tagged1Second.getCurrentLocation());
    assertTrue(tagged1Second.getData() instanceof EmptyHoodieRecordPayload);

    HoodieRecord tagged1Third = taggedRecords1.get(2);
    assertEquals(expectedRecordKey, tagged1Third.getRecordKey());
    assertEquals("pt3", tagged1Third.getPartitionPath());
    assertNull(tagged1Third.getCurrentLocation());
    assertTrue(tagged1Third.getData() instanceof RawTripTestPayload);

    // case 2: the incoming updating record is going to pt1
    HoodieData<HoodieRecord<RawTripTestPayload>> incomingPt1 = HoodieListData.eager(Collections.singletonList(
        (HoodieRecord<RawTripTestPayload>) dataGen.generateUpdatesForDifferentPartition("003", recordsPt2, "pt1").get(0)));

    List<HoodieRecord> case2Tagged = indexTagging.apply(incomingPt1, existingRecords)
        .stream().sorted(Comparator.comparing(HoodieRecord::getPartitionPath)).collect(Collectors.toList());
    assertEquals(2, case2Tagged.size(), "expect 1 update to pt1 and 1 delete to pt2");

    HoodieRecord case2First = case2Tagged.get(0);
    assertEquals(expectedRecordKey, case2First.getRecordKey());
    assertEquals("pt1", case2First.getPartitionPath());
    assertEquals(loc1, case2First.getCurrentLocation());
    assertTrue(case2First.getData() instanceof RawTripTestPayload);

    HoodieRecord case2Second = case2Tagged.get(1);
    assertEquals(expectedRecordKey, case2Second.getRecordKey());
    assertEquals("pt2", case2Second.getPartitionPath());
    assertEquals(loc2, case2Second.getCurrentLocation());
    assertTrue(case2Second.getData() instanceof EmptyHoodieRecordPayload);
  }
}
