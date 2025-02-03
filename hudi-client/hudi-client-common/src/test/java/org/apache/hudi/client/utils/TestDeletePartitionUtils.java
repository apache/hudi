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

package org.apache.hudi.client.utils;

import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieDeletePartitionException;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestDeletePartitionUtils {

  private static final String PARTITION_IN_PENDING_SERVICE_ACTION = "partition_with_pending_table_service_action";
  private static final String HARDCODED_INSTANT_TIME = "0";

  private final HoodieTable table = Mockito.mock(HoodieTable.class);

  private final SyncableFileSystemView fileSystemView = Mockito.mock(SyncableFileSystemView.class);

  public static Stream<Arguments> generateTruthValues() {
    int noOfVariables = 3;
    int noOfRows = 1 << noOfVariables;
    Object[][] truthValues = new Object[noOfRows][noOfVariables];
    for (int i = 0; i < noOfRows; i++) {
      for (int j = noOfVariables - 1; j >= 0; j--) {
        boolean out = (i / (1 << j)) % 2 != 0;
        truthValues[i][j] = out;
      }
    }
    return Stream.of(truthValues).map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("generateTruthValues")
  public void testDeletePartitionUtils(
      boolean hasPendingCompactionOperations,
      boolean hasPendingLogCompactionOperations,
      boolean hasFileGroupsInPendingClustering) {
    System.out.printf("hasPendingCompactionOperations: %s, hasPendingLogCompactionOperations: %s, hasFileGroupsInPendingClustering: %s%n",
        hasPendingCompactionOperations, hasPendingLogCompactionOperations, hasFileGroupsInPendingClustering);
    Mockito.when(table.getSliceView()).thenReturn(fileSystemView);
    Mockito.when(fileSystemView.getPendingCompactionOperations()).thenReturn(createPendingCompactionOperations(hasPendingCompactionOperations));
    Mockito.when(fileSystemView.getPendingLogCompactionOperations()).thenReturn(createPendingCompactionOperations(hasPendingLogCompactionOperations));
    Mockito.when(fileSystemView.getFileGroupsInPendingClustering()).thenReturn(createFileGroupsInPendingClustering(hasFileGroupsInPendingClustering));

    boolean shouldThrowException = hasPendingCompactionOperations || hasPendingLogCompactionOperations || hasFileGroupsInPendingClustering;

    if (shouldThrowException) {
      assertThrows(HoodieDeletePartitionException.class,
          () -> DeletePartitionUtils.checkForPendingTableServiceActions(table,
              Collections.singletonList(PARTITION_IN_PENDING_SERVICE_ACTION)));
    } else {
      assertDoesNotThrow(() -> DeletePartitionUtils.checkForPendingTableServiceActions(table,
          Collections.singletonList(PARTITION_IN_PENDING_SERVICE_ACTION)));
    }
  }

  private static Stream<Pair<String, CompactionOperation>> createPendingCompactionOperations(boolean hasPendingCompactionOperations) {
    return Stream.of(Pair.of(HARDCODED_INSTANT_TIME, getCompactionOperation(hasPendingCompactionOperations)));
  }

  private static CompactionOperation getCompactionOperation(boolean hasPendingJobInPartition) {
    return new CompactionOperation(
        "fileId", getPartitionName(hasPendingJobInPartition), HARDCODED_INSTANT_TIME, Option.empty(),
        new ArrayList<>(), Option.empty(), Option.empty(), new HashMap<>());
  }

  private static Stream<Pair<HoodieFileGroupId, HoodieInstant>> createFileGroupsInPendingClustering(boolean hasFileGroupsInPendingClustering) {
    HoodieFileGroupId hoodieFileGroupId = new HoodieFileGroupId(getPartitionName(hasFileGroupsInPendingClustering), "fileId");
    HoodieInstant hoodieInstant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, "replacecommit", HARDCODED_INSTANT_TIME);
    return Stream.of(Pair.of(hoodieFileGroupId, hoodieInstant));
  }

  private static String getPartitionName(boolean hasPendingTableServiceAction) {
    return hasPendingTableServiceAction ? PARTITION_IN_PENDING_SERVICE_ACTION : "unaffected_partition";
  }

}
