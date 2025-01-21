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

package org.apache.hudi.client;

import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.HoodiePendingRollbackInfo;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.InProcessTimeGenerator;
import org.apache.hudi.common.testutils.MockHoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestBaseHoodieTableServiceClient extends HoodieCommonTestHarness {

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void cleanRollsBackFailedWritesWithLazyPolicy(boolean rollbackOccurred) throws IOException {
    String cleanInstantTime = "001";
    initMetaClient();
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
            .build())
        .build();
    HoodieTable<String, String, String, String> firstTable = mock(HoodieTable.class);
    HoodieTable<String, String, String, String> secondTable = mock(HoodieTable.class);
    HoodieActiveTimeline timeline = mock(HoodieActiveTimeline.class, RETURNS_DEEP_STUBS);
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class, RETURNS_DEEP_STUBS);
    when(firstTable.getMetaClient()).thenReturn(mockMetaClient);
    Map<String, Option<HoodiePendingRollbackInfo>> expectedRollbackInfo;
    if (rollbackOccurred) {
      // mock rollback setup
      String newInstantTime = InProcessTimeGenerator.createNewInstantTime();
      HoodieTimeline pendingTimeline = new MockHoodieTimeline(Stream.empty(), Stream.of(newInstantTime));
      when(mockMetaClient.getCommitsTimeline().filterPendingExcludingCompaction()).thenReturn(pendingTimeline);
      when(mockMetaClient.getActiveTimeline().filterPendingRollbackTimeline().getInstants()).thenReturn(Collections.emptyList());
      expectedRollbackInfo = Collections.singletonMap(newInstantTime, Option.empty());
      when(secondTable.getActiveTimeline()).thenReturn(timeline);
    } else {
      HoodieTimeline pendingTimeline = new MockHoodieTimeline(Stream.empty(), Stream.empty());
      when(mockMetaClient.getCommitsTimeline().filterPendingExcludingCompaction()).thenReturn(pendingTimeline);
      when(mockMetaClient.getActiveTimeline().filterPendingRollbackTimeline().getInstants()).thenReturn(Collections.emptyList());
      expectedRollbackInfo = Collections.emptyMap();
      when(firstTable.getActiveTimeline()).thenReturn(timeline);
    }

    // mock no inflight cleaning
    when(timeline.getCleanerTimeline().filterInflightsAndRequested().firstInstant()).thenReturn(Option.empty());

    // create empty clean plan
    if (rollbackOccurred) {
      when(secondTable.clean(any(), eq(cleanInstantTime))).thenReturn(null);
    } else {
      when(firstTable.clean(any(), eq(cleanInstantTime))).thenReturn(null);
    }

    TestTableServiceClient tableServiceClient = new TestTableServiceClient(writeConfig, Arrays.asList(firstTable, secondTable).iterator(), Option.empty(), expectedRollbackInfo);
    tableServiceClient.clean(cleanInstantTime, false);
  }

  private static class TestTableServiceClient extends BaseHoodieTableServiceClient<String, String, String> {
    private final Iterator<HoodieTable<String, String, String, String>> tables;
    // specify the expected rollback map
    private final Map<String, Option<HoodiePendingRollbackInfo>> expectedRollbackInfo;

    public TestTableServiceClient(HoodieWriteConfig writeConfig, Iterator<HoodieTable<String, String, String, String>> tables,
                                  Option<EmbeddedTimelineService> timelineService, Map<String, Option<HoodiePendingRollbackInfo>> expectedRollbackInfo) {
      super(new HoodieLocalEngineContext(getDefaultStorageConf()), writeConfig, timelineService);
      this.tables = tables;
      this.expectedRollbackInfo = expectedRollbackInfo;
    }

    @Override
    protected void validateClusteringCommit(HoodieWriteMetadata<String> clusteringMetadata, String clusteringCommitTime, HoodieTable table) {

    }

    @Override
    protected HoodieWriteMetadata<String> convertToOutputMetadata(HoodieWriteMetadata<String> writeMetadata) {
      return null;
    }

    @Override
    protected HoodieTable<?, String, ?, String> createTable(HoodieWriteConfig config, StorageConfiguration<?> storageConf, boolean skipValidation) {
      return tables.next();
    }

    @Override
    protected void rollbackFailedWrites(Map<String, Option<HoodiePendingRollbackInfo>> instantsToRollback) {
      assertEquals(expectedRollbackInfo, instantsToRollback);
    }
  }
}
