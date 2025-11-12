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

package org.apache.hudi.table.action.rollback;

import org.apache.hudi.avro.model.HoodieRollbackRequest;
import org.apache.hudi.common.util.collection.Triple;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.table.action.rollback.RollbackUtils.groupSerializableRollbackRequestsBasedOnFileGroup;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRollbackUtils {
  @ParameterizedTest
  @CsvSource(value = {"true,true", "true,false", "false,false"})
  void testGroupRollbackRequestsBasedOnFileGroup(boolean nonPartitioned, boolean useNullPartitionPath) {
    // The file names generated here are not compliant to the Hudi format
    // However, that's irrelevant to grouping the rollback requests
    List<HoodieRollbackRequest> inputList = new ArrayList<>();
    String partition1 = nonPartitioned ? (useNullPartitionPath ? null : "") : "partition1";
    String partition2 = nonPartitioned ? (useNullPartitionPath ? null : "") : "partition2";
    String expectedPartition2 = nonPartitioned ? "" : "partition2";
    String baseFileName1 = "basefile1";
    String baseFileName2 = "basefile2";
    String baseFileName3 = "basefile3";
    String baseFileName4 = "basefile4";
    String baseInstantTime1 = "003";
    String baseFileId1 = UUID.randomUUID().toString();
    String baseFileId2 = UUID.randomUUID().toString();
    String baseFileId3 = UUID.randomUUID().toString();
    String logFileId1 = UUID.randomUUID().toString();
    int numLogFiles1 = 10;
    String baseInstantTime2 = "002";
    String logFileId2 = UUID.randomUUID().toString();
    int numLogFiles2 = 5;
    String baseInstantTime3 = "001";

    // Empty
    inputList.add(new HoodieRollbackRequest(
        partition1, baseFileId1, baseInstantTime1, Collections.emptyList(),
        Collections.emptyMap()));
    inputList.add(new HoodieRollbackRequest(
        partition2, baseFileId2, baseInstantTime1, Collections.emptyList(),
        Collections.emptyMap()));
    // Base Files
    inputList.add(new HoodieRollbackRequest(
        partition1, baseFileId1, baseInstantTime1, Collections.singletonList(baseFileName1),
        Collections.emptyMap()));
    inputList.add(new HoodieRollbackRequest(
        partition2, baseFileId2, baseInstantTime1, Collections.singletonList(baseFileName2),
        Collections.emptyMap()));
    inputList.add(new HoodieRollbackRequest(
        partition2, baseFileId3, baseInstantTime1, Collections.singletonList(baseFileName3),
        Collections.emptyMap()));
    List<HoodieRollbackRequest> expected = new ArrayList<>(inputList);
    Map<String, Long> logFileMap1 = new HashMap<>();
    Map<String, Long> logFileMap2 = new HashMap<>();
    // Log Files
    IntStream.rangeClosed(2, numLogFiles1 + 1).forEach(i -> {
      inputList.add(new HoodieRollbackRequest(
          partition2, logFileId1, baseInstantTime2, Collections.emptyList(),
          Collections.singletonMap(logFileId1 + "." + i, i * 2L)));
      logFileMap1.put(logFileId1 + "." + i, i * 2L);
    });
    IntStream.rangeClosed(2, numLogFiles2).forEach(i -> {
      inputList.add(new HoodieRollbackRequest(
          partition2, logFileId2, baseInstantTime3, Collections.emptyList(),
          Collections.singletonMap(logFileId2 + "." + i, i * 3L)));
      logFileMap2.put(logFileId2 + "." + i, i * 3L);
    });
    // Base + Log files which should not happen, but should not fail the grouping
    inputList.add(new HoodieRollbackRequest(
        partition2, logFileId2, baseInstantTime3, Collections.singletonList(baseFileName4),
        Collections.singletonMap(logFileId2 + "." + (numLogFiles2 + 1), (numLogFiles2 + 1) * 3L)));
    logFileMap2.put(logFileId2 + "." + (numLogFiles2 + 1), (numLogFiles2 + 1) * 3L);
    expected.add(new HoodieRollbackRequest(
        expectedPartition2, logFileId1, baseInstantTime2, Collections.emptyList(), logFileMap1));
    expected.add(new HoodieRollbackRequest(
        expectedPartition2, logFileId2, baseInstantTime3, Collections.emptyList(), logFileMap2));
    expected.add(new HoodieRollbackRequest(
        expectedPartition2, logFileId2, baseInstantTime3, Collections.singletonList(baseFileName4), Collections.emptyMap()));
    assertEquals(5 + numLogFiles1 + numLogFiles2, inputList.size());

    List<HoodieRollbackRequest> actual = RollbackUtils.groupRollbackRequestsBasedOnFileGroup(inputList);
    List<SerializableHoodieRollbackRequest> actualSerializable = groupSerializableRollbackRequestsBasedOnFileGroup(
        inputList.stream().map(SerializableHoodieRollbackRequest::new)
            .collect(Collectors.toList()));

    assertRollbackRequestListEquals(expected, actual);
    assertSerializableRollbackRequestListEquals(
        expected.stream().map(SerializableHoodieRollbackRequest::new)
            .collect(Collectors.toList()),
        actualSerializable);
  }

  public static void assertRollbackRequestListEquals(List<HoodieRollbackRequest> expected,
                                                     List<HoodieRollbackRequest> actual) {
    assertEquals(expected.size(), actual.size());
    assertSerializableRollbackRequestListEquals(
        expected.stream().map(SerializableHoodieRollbackRequest::new).collect(Collectors.toList()),
        actual.stream().map(SerializableHoodieRollbackRequest::new).collect(Collectors.toList()));
  }

  public static void assertSerializableRollbackRequestListEquals(List<SerializableHoodieRollbackRequest> expected,
                                                                 List<SerializableHoodieRollbackRequest> actual) {
    assertEquals(expected.size(), actual.size());
    List<SerializableHoodieRollbackRequest> sortedExpected = getSortedRollbackRequests(expected);
    List<SerializableHoodieRollbackRequest> sortedActual = getSortedRollbackRequests(actual);

    for (int i = 0; i < sortedExpected.size(); i++) {
      SerializableHoodieRollbackRequest expectedRequest = sortedExpected.get(i);
      SerializableHoodieRollbackRequest actualRequest = sortedActual.get(i);
      assertEquals(expectedRequest.getPartitionPath(), actualRequest.getPartitionPath());
      assertEquals(expectedRequest.getFileId(), actualRequest.getFileId());
      assertEquals(expectedRequest.getLatestBaseInstant(), actualRequest.getLatestBaseInstant());
      assertEquals(
          expectedRequest.getFilesToBeDeleted().stream().sorted().collect(Collectors.toList()),
          actualRequest.getFilesToBeDeleted().stream().sorted().collect(Collectors.toList()));

      Map<String, Long> expectedLogFileMap = expectedRequest.getLogBlocksToBeDeleted();
      Map<String, Long> actualLogFileMap = actualRequest.getLogBlocksToBeDeleted();
      assertEquals(expectedLogFileMap.size(), actualLogFileMap.size());
      for (Map.Entry<String, Long> entry : expectedLogFileMap.entrySet()) {
        assertTrue(actualLogFileMap.containsKey(entry.getKey()));
        assertEquals(entry.getValue(), actualLogFileMap.get(entry.getKey()));
      }
    }
  }

  private static List<SerializableHoodieRollbackRequest> getSortedRollbackRequests(
      List<SerializableHoodieRollbackRequest> rollbackRequestList) {
    return rollbackRequestList.stream()
        .sorted(Comparator.comparing(
            e -> Triple.of(
                e.getFileId(),
                e.getLogBlocksToBeDeleted().size(),
                !e.getFilesToBeDeleted().isEmpty()
                    ? e.getFilesToBeDeleted().get(0)
                    : !e.getLogBlocksToBeDeleted().isEmpty()
                    ? e.getLogBlocksToBeDeleted().keySet().stream().findFirst().get()
                    : ""),
            Comparator.naturalOrder()))
        .collect(Collectors.toList());
  }
}

