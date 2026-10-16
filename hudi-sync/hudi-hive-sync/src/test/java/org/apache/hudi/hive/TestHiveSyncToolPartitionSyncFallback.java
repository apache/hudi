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
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.testutils.MockHoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.sync.common.HoodieSyncClient;

import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.stream.Stream;

import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_INCREMENTAL;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_FIELDS;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests the incremental partition sync falling back to a full listing, within the same sync, when a timeline read
 * fails because the archiver removed instants the sync still needed.
 */
class TestHiveSyncToolPartitionSyncFallback {

  private static final String TABLE_NAME = "table";
  private static final String LAST_COMMIT_TIME_SYNCED = "102";

  @Test
  void syncsAllPartitionsWhenLastCommitTimeSyncedIsArchivedMidSync() {
    HoodieActiveTimeline timeline = mock(HoodieActiveTimeline.class);
    HoodieSyncClient syncClient = mockSyncClient(timeline);
    // The sync starts with 102 inside the active timeline; the fresh listing after the failed read no longer has it.
    when(syncClient.getActiveTimeline()).thenReturn(new MockHoodieTimeline(Stream.of("100", "102", "104"), Stream.empty()));
    when(timeline.reload()).thenReturn(new MockHoodieTimeline(Stream.of("104", "106"), Stream.empty()));
    when(syncClient.getWrittenPartitionsSince(any(), any())).thenThrow(new HoodieIOException("Could not read commit details"));
    HiveSyncTool tool = toolWith(syncClient);

    assertFalse(tool.validateAndSyncPartitions(TABLE_NAME, true));

    verify(syncClient).getAllPartitionPathsOnStorage();
    verify(syncClient, never()).getDroppedPartitionsSince(any(), any());
    // The timeline this sync works from is left alone, so the last commit time synced recorded later matches it.
    verify(syncClient.getMetaClient(), never()).reloadActiveTimeline();
  }

  @Test
  void syncsAllPartitionsWhenAnInstantInTheWindowIsArchived() {
    HoodieActiveTimeline timeline = mock(HoodieActiveTimeline.class);
    HoodieSyncClient syncClient = mockSyncClient(timeline);
    when(syncClient.getActiveTimeline()).thenReturn(new MockHoodieTimeline(Stream.of("100", "102", "104"), Stream.empty()));
    when(syncClient.getWrittenPartitionsSince(any(), any())).thenReturn(Collections.singletonList("p"));
    // The shape of a lazy S3 read of a deleted instant: the FileNotFoundException is two causes deep.
    when(syncClient.getDroppedPartitionsSince(any(), any())).thenThrow(new HoodieIOException("Failed to get partitions cleaned at 103",
        new IOException("Not an Avro data file.", new FileNotFoundException("103.clean"))));
    HiveSyncTool tool = toolWith(syncClient);

    assertFalse(tool.validateAndSyncPartitions(TABLE_NAME, true));

    verify(syncClient).getAllPartitionPathsOnStorage();
    verify(timeline, never()).reload();
  }

  @Test
  void rethrowsWhenLastCommitTimeSyncedIsStillActive() {
    HoodieActiveTimeline timeline = mock(HoodieActiveTimeline.class);
    HoodieSyncClient syncClient = mockSyncClient(timeline);
    when(syncClient.getActiveTimeline()).thenReturn(new MockHoodieTimeline(Stream.of("100", "102", "104"), Stream.empty()));
    when(timeline.reload()).thenReturn(new MockHoodieTimeline(Stream.of("100", "102", "104", "106"), Stream.empty()));
    when(syncClient.getWrittenPartitionsSince(any(), any())).thenReturn(Collections.singletonList("p"));
    HoodieIOException readFailure = new HoodieIOException("Could not read commit details");
    when(syncClient.getDroppedPartitionsSince(any(), any())).thenThrow(readFailure);
    HiveSyncTool tool = toolWith(syncClient);

    assertSame(readFailure, assertThrows(HoodieIOException.class, () -> tool.validateAndSyncPartitions(TABLE_NAME, true)));

    verify(timeline).reload();
    verify(syncClient, never()).getAllPartitionPathsOnStorage();
  }

  private static HoodieSyncClient mockSyncClient(HoodieActiveTimeline timeline) {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getActiveTimeline()).thenReturn(timeline);
    HoodieSyncClient syncClient = mock(HoodieSyncClient.class);
    when(syncClient.getMetaClient()).thenReturn(metaClient);
    when(syncClient.getLastCommitTimeSynced(TABLE_NAME)).thenReturn(Option.of(LAST_COMMIT_TIME_SYNCED));
    when(syncClient.getLastCommitCompletionTimeSynced(TABLE_NAME)).thenReturn(Option.empty());
    return syncClient;
  }

  private static HiveSyncTool toolWith(HoodieSyncClient syncClient) {
    HiveSyncConfig config = mock(HiveSyncConfig.class);
    when(config.getBoolean(META_SYNC_INCREMENTAL)).thenReturn(true);
    when(config.getSplitStrings(META_SYNC_PARTITION_FIELDS)).thenReturn(Collections.singletonList("p"));
    HiveSyncTool tool = mock(HiveSyncTool.class, CALLS_REAL_METHODS);
    tool.config = config;
    tool.syncClient = syncClient;
    return tool;
  }
}
