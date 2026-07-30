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

package org.apache.hudi.sink.clustering;

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.utils.MockStreamingRuntimeContext;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.util.ClusteringUtil;
import org.apache.hudi.util.FlinkWriteClients;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests {@link ClusteringCommitSink}.
 */
class TestClusteringCommitSink {

  private static final String INSTANT = "20240101000000000";

  private Configuration conf;
  private HoodieFlinkWriteClient writeClient;
  private HoodieFlinkTable table;
  private HoodieTableMetaClient metaClient;
  private HoodieActiveTimeline activeTimeline;
  private InstantGenerator instantGenerator;
  private HoodieInstant inflightInstant;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    conf = new Configuration();
    conf.set(FlinkOptions.CLEAN_ASYNC_ENABLED, false);
    writeClient = mock(HoodieFlinkWriteClient.class);
    table = mock(HoodieFlinkTable.class);
    metaClient = mock(HoodieTableMetaClient.class);
    activeTimeline = mock(HoodieActiveTimeline.class);
    instantGenerator = mock(InstantGenerator.class);
    inflightInstant = mock(HoodieInstant.class);

    when(writeClient.getHoodieTable()).thenReturn(table);
    when(table.getMetaClient()).thenReturn(metaClient);
    when(table.getInstantGenerator()).thenReturn(instantGenerator);
    when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
  }

  @Test
  void testIgnoreEventsWhoseClusteringPlanNoLongerExists() throws Exception {
    ClusteringCommitSink sink = openSink();
    try (MockedStatic<ClusteringUtils> clusteringUtils = mockStatic(ClusteringUtils.class);
         MockedStatic<ClusteringUtil> clusteringUtil = mockStatic(ClusteringUtil.class)) {
      clusteringUtils.when(() -> ClusteringUtils.getInflightClusteringInstant(
          INSTANT, activeTimeline, instantGenerator)).thenReturn(Option.empty());

      sink.invoke(new ClusteringCommitEvent(INSTANT, "file-1", 0), null);

      clusteringUtil.verifyNoInteractions();
      verify(writeClient, never()).completeTableService(
          any(), any(), any(), any());
    }
  }

  @Test
  void testWaitForEveryGroupThenRollbackFailedClustering() throws Exception {
    ClusteringCommitSink sink = openSink();
    HoodieClusteringPlan plan = planWithGroups(2);
    try (MockedStatic<ClusteringUtils> clusteringUtils = mockStatic(ClusteringUtils.class);
         MockedStatic<ClusteringUtil> clusteringUtil = mockStatic(ClusteringUtil.class)) {
      stubClusteringPlan(clusteringUtils, plan);

      sink.invoke(successEvent("file-1", new WriteStatus()), null);
      clusteringUtil.verifyNoInteractions();

      sink.invoke(new ClusteringCommitEvent(INSTANT, "file-2", 1), null);
      clusteringUtil.verify(
          () -> ClusteringUtil.rollbackClustering(table, writeClient, INSTANT), times(1));

      // A reset sink starts buffering from scratch and reloads the plan.
      sink.invoke(successEvent("file-1", new WriteStatus()), null);
      clusteringUtils.verify(
          () -> ClusteringUtils.getClusteringPlan(metaClient, inflightInstant), times(2));
    }
  }

  @Test
  void testRollbackWriteStatusesWithErrorsUnlessConfiguredToIgnore() throws Exception {
    ClusteringCommitSink sink = openSink();
    HoodieClusteringPlan plan = planWithGroups(1);
    WriteStatus failedStatus = new WriteStatus();
    failedStatus.setTotalErrorRecords(3);

    try (MockedStatic<ClusteringUtils> clusteringUtils = mockStatic(ClusteringUtils.class);
         MockedStatic<ClusteringUtil> clusteringUtil = mockStatic(ClusteringUtil.class)) {
      stubClusteringPlan(clusteringUtils, plan);

      sink.invoke(successEvent("file-1", failedStatus), null);

      clusteringUtil.verify(
          () -> ClusteringUtil.rollbackClustering(table, writeClient, INSTANT));
      verify(writeClient, never()).completeTableService(
          any(), any(), any(), any());
    }
  }

  @Test
  void testCommitWriteStatusesWithErrorsWhenConfiguredToIgnore() throws Exception {
    conf.set(FlinkOptions.IGNORE_FAILED, true);
    ClusteringCommitSink sink = openSink();
    HoodieClusteringPlan plan = planWithGroups(1);
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setPartitionPath("partition");
    writeStat.setFileId("new-file");
    writeStat.setNumWrites(1);
    WriteStatus failedStatus = new WriteStatus();
    failedStatus.setStat(writeStat);
    failedStatus.setTotalErrorRecords(3);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    when(writeConfig.getSchema()).thenReturn("{}");
    when(writeClient.getConfig()).thenReturn(writeConfig);

    try (MockedStatic<ClusteringUtils> clusteringUtils = mockStatic(ClusteringUtils.class);
         MockedStatic<ClusteringUtil> clusteringUtil = mockStatic(ClusteringUtil.class)) {
      stubClusteringPlan(clusteringUtils, plan);
      clusteringUtils.when(() -> ClusteringUtils.getFileGroupsFromClusteringPlan(plan))
          .thenReturn(Stream.of(new HoodieFileGroupId("partition", "old-file")));

      sink.invoke(successEvent("file-1", failedStatus), null);

      clusteringUtil.verifyNoInteractions();
      verify(writeClient).completeTableService(
          eq(TableServiceType.CLUSTER),
          any(HoodieCommitMetadata.class),
          same(table),
          eq(INSTANT));
    }
  }

  @Test
  void testCommitSuccessfulClusteringAndCleanInline() throws Exception {
    ClusteringCommitSink sink = openSink();
    HoodieClusteringPlan plan = planWithGroups(1);
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setPartitionPath("partition");
    writeStat.setFileId("new-file");
    writeStat.setNumWrites(1);
    WriteStatus status = new WriteStatus();
    status.setStat(writeStat);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    when(writeConfig.getSchema()).thenReturn("{}");
    when(writeClient.getConfig()).thenReturn(writeConfig);

    try (MockedStatic<ClusteringUtils> clusteringUtils = mockStatic(ClusteringUtils.class)) {
      stubClusteringPlan(clusteringUtils, plan);
      clusteringUtils.when(() -> ClusteringUtils.getFileGroupsFromClusteringPlan(plan))
          .thenReturn(Stream.of(
              new HoodieFileGroupId("partition", "old-file"),
              new HoodieFileGroupId("partition", "new-file")));

      sink.invoke(successEvent("file-1", status), null);
    }

    verify(metaClient).reloadActiveTimeline();
    verify(writeClient).completeTableService(
        eq(TableServiceType.CLUSTER),
        any(HoodieCommitMetadata.class),
        same(table),
        eq(INSTANT));
    verify(writeClient).clean();
  }

  private ClusteringCommitSink openSink() throws Exception {
    ClusteringCommitSink sink = new ClusteringCommitSink(conf);
    MockStreamingRuntimeContext runtimeContext = new MockStreamingRuntimeContext(false, 1, 0);
    sink.setRuntimeContext(runtimeContext);
    try (MockedStatic<FlinkWriteClients> writeClients = mockStatic(FlinkWriteClients.class)) {
      writeClients.when(() -> FlinkWriteClients.createWriteClient(conf, runtimeContext))
          .thenReturn(writeClient);
      sink.open(conf);
    }
    return sink;
  }

  private void stubClusteringPlan(
      MockedStatic<ClusteringUtils> clusteringUtils,
      HoodieClusteringPlan plan) {
    clusteringUtils.when(() -> ClusteringUtils.getInflightClusteringInstant(
        INSTANT, activeTimeline, instantGenerator)).thenReturn(Option.of(inflightInstant));
    clusteringUtils.when(() -> ClusteringUtils.getClusteringPlan(metaClient, inflightInstant))
        .thenReturn(Option.of(Pair.of(inflightInstant, plan)));
  }

  private HoodieClusteringPlan planWithGroups(int numGroups) {
    HoodieClusteringPlan plan = mock(HoodieClusteringPlan.class);
    HoodieClusteringGroup[] groups = new HoodieClusteringGroup[numGroups];
    Arrays.setAll(groups, ignored -> mock(HoodieClusteringGroup.class));
    when(plan.getInputGroups()).thenReturn(Arrays.asList(groups));
    return plan;
  }

  private ClusteringCommitEvent successEvent(String fileId, WriteStatus... statuses) {
    List<WriteStatus> statusList = Arrays.asList(statuses);
    return new ClusteringCommitEvent(INSTANT, fileId, statusList, 0);
  }
}
