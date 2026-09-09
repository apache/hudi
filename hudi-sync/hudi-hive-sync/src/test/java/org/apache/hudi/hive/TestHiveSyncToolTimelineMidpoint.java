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

package org.apache.hudi.hive;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.MockHoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.sync.common.HoodieSyncClient;

import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link HiveSyncTool#isLastCommitTimeSyncedBehindTimelineMidpoint}, which decides
 * whether a no-change conditional sync should still advance last commit time synced. The helper
 * reads the completed commits timeline, so these mock {@code getMetaClient().getCommitsTimeline()};
 * a call to any other timeline would hit an unstubbed mock and fail.
 */
class TestHiveSyncToolTimelineMidpoint {

  private static final String TABLE_NAME = "table";

  @Test
  void midpointIsComputedFromCompletedCommitsOnly() {
    // Completed commits [100, 102, 104], midpoint 102; the later inflight 106 must not shift it to 104.
    HoodieSyncClient syncClient = mockSyncClient(new MockHoodieTimeline(Stream.of("100", "102", "104"), Stream.of("106")));
    HiveSyncTool tool = toolWith(syncClient);

    // Not synced yet: nothing to advance.
    stubLastCommitTimeSynced(syncClient, Option.empty());
    assertFalse(tool.isLastCommitTimeSyncedBehindTimelineMidpoint(TABLE_NAME));

    // Trails the midpoint.
    stubLastCommitTimeSynced(syncClient, Option.of("101"));
    assertTrue(tool.isLastCommitTimeSyncedBehindTimelineMidpoint(TABLE_NAME));

    // At the midpoint is not behind it.
    stubLastCommitTimeSynced(syncClient, Option.of("102"));
    assertFalse(tool.isLastCommitTimeSyncedBehindTimelineMidpoint(TABLE_NAME));

    // Past 102 but below 104: behind only if the inflight 106 is wrongly counted.
    stubLastCommitTimeSynced(syncClient, Option.of("103"));
    assertFalse(tool.isLastCommitTimeSyncedBehindTimelineMidpoint(TABLE_NAME));
  }

  @Test
  void midpointHandlesSmallTimelines() {
    // Size 1: the sole commit is the midpoint.
    HoodieSyncClient syncClient = mockSyncClient(new MockHoodieTimeline(Stream.of("101"), Stream.empty()));
    HiveSyncTool tool = toolWith(syncClient);
    stubLastCommitTimeSynced(syncClient, Option.of("100"));
    assertTrue(tool.isLastCommitTimeSyncedBehindTimelineMidpoint(TABLE_NAME));
    stubLastCommitTimeSynced(syncClient, Option.of("101"));
    assertFalse(tool.isLastCommitTimeSyncedBehindTimelineMidpoint(TABLE_NAME));

    // Size 2: the midpoint (index 1) is the newer commit.
    syncClient = mockSyncClient(new MockHoodieTimeline(Stream.of("100", "102"), Stream.empty()));
    tool = toolWith(syncClient);
    stubLastCommitTimeSynced(syncClient, Option.of("101"));
    assertTrue(tool.isLastCommitTimeSyncedBehindTimelineMidpoint(TABLE_NAME));
    stubLastCommitTimeSynced(syncClient, Option.of("102"));
    assertFalse(tool.isLastCommitTimeSyncedBehindTimelineMidpoint(TABLE_NAME));
  }

  @Test
  void emptyCommitsTimelineIsNotBehind() {
    HoodieSyncClient syncClient = mockSyncClient(new MockHoodieTimeline(Stream.empty(), Stream.empty()));
    HiveSyncTool tool = toolWith(syncClient);
    stubLastCommitTimeSynced(syncClient, Option.of("103"));
    assertFalse(tool.isLastCommitTimeSyncedBehindTimelineMidpoint(TABLE_NAME));
  }

  private static HoodieSyncClient mockSyncClient(HoodieTimeline commitsTimeline) {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getCommitsTimeline()).thenReturn(commitsTimeline);
    HoodieSyncClient syncClient = mock(HoodieSyncClient.class);
    when(syncClient.getMetaClient()).thenReturn(metaClient);
    return syncClient;
  }

  private static HiveSyncTool toolWith(HoodieSyncClient syncClient) {
    HiveSyncTool tool = mock(HiveSyncTool.class, CALLS_REAL_METHODS);
    tool.syncClient = syncClient;
    return tool;
  }

  private static void stubLastCommitTimeSynced(HoodieSyncClient syncClient, Option<String> value) {
    when(syncClient.getLastCommitTimeSynced(TABLE_NAME)).thenReturn(value);
  }
}
