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

package org.apache.hudi.sink.clustering.update.strategy;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.ConsistentHashingNode;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieConsistentHashingMetadata;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.index.bucket.ConsistentBucketIdentifier;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.action.cluster.util.ConsistentHashingUpdateStrategyUtils;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

/**
 * Tests {@link FlinkConsistentBucketUpdateStrategy}.
 */
class TestFlinkConsistentBucketUpdateStrategy {

  private static final String PARTITION = "partition";
  private static final String OLD_FILE_ID = "old-file-id";
  private static final String CLUSTERING_INSTANT = "002";
  private static final String NEW_FILE_ID_PREFIX = "new-file";

  @Test
  void testInitializationIsRequiredAndRegularUpdatesAreNotDuplicated() {
    TestContext context = new TestContext();
    FlinkConsistentBucketUpdateStrategy<HoodieRecordPayload> strategy =
        new FlinkConsistentBucketUpdateStrategy<>(context.writeClient, Collections.emptyList());
    List<Pair<List<HoodieRecord>, String>> records = records("003");

    assertThrows(IllegalArgumentException.class, () -> strategy.handleUpdate(records));

    try (MockedStatic<ClusteringUtils> clusteringUtils = mockStatic(ClusteringUtils.class)) {
      clusteringUtils.when(() -> ClusteringUtils.getPendingClusteringInstantTimes(context.metaClient))
          .thenReturn(Collections.emptyList());

      strategy.initialize(context.writeClient);
      strategy.initialize(context.writeClient);
    }

    Pair<List<Pair<List<HoodieRecord>, String>>, Set<HoodieFileGroupId>> result =
        strategy.handleUpdate(records);
    assertSame(records, result.getLeft());
    assertEquals(Collections.singleton(new HoodieFileGroupId(PARTITION, OLD_FILE_ID)), result.getRight());
    assertFalse(strategy.needDualWrite(new HoodieFileGroupId(PARTITION, OLD_FILE_ID)));

    assertThrows(IllegalArgumentException.class, () -> strategy.handleUpdate(Collections.emptyList()));
    strategy.reset();
    assertThrows(IllegalArgumentException.class, () -> strategy.handleUpdate(records));
  }

  @Test
  void testUpdatesToPendingFileGroupAreRoutedToOldAndNewBuckets() {
    TestContext context = new TestContext();
    HoodieFileGroupId oldFileGroup = new HoodieFileGroupId(PARTITION, OLD_FILE_ID);
    HoodieInstant pendingInstant = mock(HoodieInstant.class);
    when(pendingInstant.requestedTime()).thenReturn(CLUSTERING_INSTANT);
    when(context.fileSystemView.getFileGroupsInPendingClustering())
        .thenReturn(Stream.of(Pair.of(oldFileGroup, pendingInstant)));

    ConsistentHashingNode newNode = new ConsistentHashingNode(Integer.MAX_VALUE, NEW_FILE_ID_PREFIX);
    HoodieConsistentHashingMetadata hashingMetadata = new HoodieConsistentHashingMetadata(
        (short) 0, PARTITION, CLUSTERING_INSTANT, 1, 0, Collections.singletonList(newNode));
    ConsistentBucketIdentifier identifier = new ConsistentBucketIdentifier(hashingMetadata);
    Map<String, Pair<String, ConsistentBucketIdentifier>> identifiers =
        Collections.singletonMap(PARTITION, Pair.of(CLUSTERING_INSTANT, identifier));

    FlinkConsistentBucketUpdateStrategy<HoodieRecordPayload> strategy =
        new FlinkConsistentBucketUpdateStrategy<>(context.writeClient, Collections.emptyList());
    try (MockedStatic<ClusteringUtils> clusteringUtils = mockStatic(ClusteringUtils.class);
         MockedStatic<ConsistentHashingUpdateStrategyUtils> updateStrategyUtils =
             mockStatic(ConsistentHashingUpdateStrategyUtils.class)) {
      clusteringUtils.when(() -> ClusteringUtils.getPendingClusteringInstantTimes(context.metaClient))
          .thenReturn(Collections.singletonList(pendingInstant));
      updateStrategyUtils.when(() -> ConsistentHashingUpdateStrategyUtils.constructPartitionToIdentifier(
          anySet(), same(context.table))).thenReturn(identifiers);

      strategy.initialize(context.writeClient);
      assertTrue(strategy.needDualWrite(oldFileGroup));

      List<Pair<List<HoodieRecord>, String>> originalRecords = records("003");
      Pair<List<Pair<List<HoodieRecord>, String>>, Set<HoodieFileGroupId>> result =
          strategy.handleUpdate(originalRecords);
      strategy.handleUpdate(records("004"));

      assertEquals(2, result.getLeft().size());
      Pair<List<HoodieRecord>, String> duplicateRecords = result.getLeft().get(0);
      assertEquals(CLUSTERING_INSTANT, duplicateRecords.getRight());
      assertEquals(FSUtils.createNewFileId(NEW_FILE_ID_PREFIX, 0),
          duplicateRecords.getLeft().get(0).getCurrentLocation().getFileId());
      assertSame(originalRecords.get(0), result.getLeft().get(1));
      assertEquals(2, result.getRight().size());
      assertTrue(result.getRight().contains(oldFileGroup));
      assertTrue(result.getRight().contains(new HoodieFileGroupId(
          PARTITION, FSUtils.createNewFileId(NEW_FILE_ID_PREFIX, 0))));

      updateStrategyUtils.verify(() -> ConsistentHashingUpdateStrategyUtils.constructPartitionToIdentifier(
          anySet(), same(context.table)), times(1));
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static List<Pair<List<HoodieRecord>, String>> records(String instant) {
    HoodieRecordPayload payload = mock(HoodieRecordPayload.class);
    HoodieRecord record = new HoodieAvroRecord<>(new HoodieKey("record-key", PARTITION), payload);
    record.setCurrentLocation(new HoodieRecordLocation("001", OLD_FILE_ID));
    return Collections.singletonList(Pair.of(Collections.singletonList(record), instant));
  }

  private static class TestContext {
    private final HoodieFlinkWriteClient<HoodieRecordPayload> writeClient;
    private final HoodieFlinkTable<HoodieRecordPayload> table;
    private final HoodieTableMetaClient metaClient;
    private final TableFileSystemView fileSystemView;

    @SuppressWarnings("unchecked")
    private TestContext() {
      writeClient = mock(HoodieFlinkWriteClient.class);
      table = mock(HoodieFlinkTable.class);
      metaClient = mock(HoodieTableMetaClient.class);
      fileSystemView = mock(TableFileSystemView.class);
      when(writeClient.getEngineContext()).thenReturn(mock(HoodieEngineContext.class));
      when(writeClient.getHoodieTable()).thenReturn(table);
      when(table.getMetaClient()).thenReturn(metaClient);
      when(table.getFileSystemView()).thenReturn(fileSystemView);
    }
  }
}
