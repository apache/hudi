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

import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.v2.ActiveTimelineV2;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestHoodieBackedTableMetadataWriter {
  @ParameterizedTest
  @CsvSource(value = {
      "true,true,false,true",
      "false,true,false,true",
      "true,false,false,true",
      "false,false,false,false",
      "false,false,true,false",
  })
  void runPendingTableServicesOperations(boolean hasPendingCompaction, boolean hasPendingLogCompaction, boolean requiresRefresh, boolean ranService) {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline initialTimeline = mock(HoodieActiveTimeline.class, RETURNS_DEEP_STUBS);
    BaseHoodieWriteClient writeClient = mock(BaseHoodieWriteClient.class);
    if (requiresRefresh) {
      when(metaClient.reloadActiveTimeline()).thenReturn(initialTimeline);
    } else {
      when(metaClient.getActiveTimeline()).thenReturn(initialTimeline);
    }
    if (hasPendingCompaction) {
      when(initialTimeline.filterPendingCompactionTimeline().countInstants()).thenReturn(1);
    }
    if (hasPendingLogCompaction) {
      when(initialTimeline.filterPendingLogCompactionTimeline().countInstants()).thenReturn(1);
    }
    HoodieActiveTimeline expectedResult;
    if (ranService) {
      HoodieActiveTimeline timelineReloadedAfterServicesRun = mock(HoodieActiveTimeline.class);
      when(metaClient.reloadActiveTimeline()).thenReturn(timelineReloadedAfterServicesRun);
      expectedResult = timelineReloadedAfterServicesRun;
    } else {
      expectedResult = initialTimeline;
    }
    assertSame(expectedResult, HoodieBackedTableMetadataWriter.runPendingTableServicesOperationsAndRefreshTimeline(metaClient, writeClient, requiresRefresh));

    verify(writeClient, times(hasPendingCompaction ? 1 : 0)).runAnyPendingCompactions();
    verify(writeClient, times(hasPendingLogCompaction ? 1 : 0)).runAnyPendingLogCompactions();
    int expectedTimelineReloads = (requiresRefresh ? 1 : 0) + (ranService ? 1 : 0);
    verify(metaClient, times(expectedTimelineReloads)).reloadActiveTimeline();
  }

  @Test
  void rollbackFailedWrites_reloadsTimelineOnWritesRolledBack() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("file://tmp/")
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.EAGER).build())
        .build();
    BaseHoodieWriteClient mockWriteClient = mock(BaseHoodieWriteClient.class);
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    when(mockWriteClient.rollbackFailedWrites(mockMetaClient)).thenReturn(true);
    try (MockedStatic<HoodieTableMetaClient> mockedStatic = mockStatic(HoodieTableMetaClient.class)) {
      HoodieTableMetaClient reloadedClient = mock(HoodieTableMetaClient.class);
      mockedStatic.when(() -> HoodieTableMetaClient.reload(mockMetaClient)).thenReturn(reloadedClient);
      assertSame(reloadedClient, HoodieBackedTableMetadataWriter.rollbackFailedWrites(writeConfig, mockWriteClient, mockMetaClient));
    }
  }

  @Test
  void rollbackFailedWrites_avoidsTimelineReload() {
    HoodieWriteConfig eagerWriteConfig = HoodieWriteConfig.newBuilder().withPath("file://tmp/")
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.EAGER).build())
        .build();
    BaseHoodieWriteClient mockWriteClient = mock(BaseHoodieWriteClient.class);
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    when(mockWriteClient.rollbackFailedWrites(mockMetaClient)).thenReturn(false);
    assertSame(mockMetaClient, HoodieBackedTableMetadataWriter.rollbackFailedWrites(eagerWriteConfig, mockWriteClient, mockMetaClient));

    HoodieWriteConfig lazyWriteConfig = HoodieWriteConfig.newBuilder().withPath("file://tmp/")
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.EAGER).build())
        .build();
    assertSame(mockMetaClient, HoodieBackedTableMetadataWriter.rollbackFailedWrites(lazyWriteConfig, mockWriteClient, mockMetaClient));
  }

  @Test
  void testValidateRollbackForMDT() throws Exception {
    List<HoodieInstant> instants = new ArrayList<>();

    instants.add(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.DELTA_COMMIT_ACTION, "20250925012123905"));

    instants.add(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "20250925012447357", "20250925013432341"));
    instants.add(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "20250925012518125", "20250925012831379"));
    instants.add(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "20250925012851950", "20250925013157886"));

    instants.add(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "20250925012523368", "20250925015000000"));

    instants.add(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "20250925013447468", "20250925014634434"));

    HoodieActiveTimeline timeline = createMockTimeline(instants);

    HoodieBackedTableMetadataWriter<?, ?> writer = mock(HoodieBackedTableMetadataWriter.class);

    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    when(mockMetaClient.getActiveTimeline()).thenReturn(timeline);

    java.lang.reflect.Field metadataMetaClientField = HoodieBackedTableMetadataWriter.class.getDeclaredField("metadataMetaClient");
    metadataMetaClientField.setAccessible(true);
    metadataMetaClientField.set(writer, mockMetaClient);

    String rollbackCommitTime = "20250925012123905";

    java.lang.reflect.Method validateRollbackMethod = HoodieBackedTableMetadataWriter.class.getDeclaredMethod("validateRollback", String.class);
    validateRollbackMethod.setAccessible(true);

    assertDoesNotThrow(() -> validateRollbackMethod.invoke(writer, rollbackCommitTime));
  }

  @SuppressWarnings("deprecation")
  private HoodieActiveTimeline createMockTimeline(List<HoodieInstant> instants) {
    ActiveTimelineV2 timeline = new ActiveTimelineV2();
    timeline.setInstants(instants);
    return timeline;
  }
}
