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

package org.apache.hudi.utilities;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMetadataSyncUtils {

  @Test
  public void testGetTableSyncExtraMetadata_NoPreviousInstant() {
    Option<HoodieInstant> targetLastInstant = Option.empty();
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);

    String sourceId = "sourceA";
    String sourceInstantSynced = "100";
    List<String> pendingInstants = Arrays.asList("200", "201");

    SyncMetadata result = MetadataSyncUtils.getTableSyncExtraMetadata(
        targetLastInstant,
        metaClient,
        sourceId,
        sourceInstantSynced,
        pendingInstants
    );

    assertEquals(1, result.tableCheckpointInfos.size());
    TableCheckpointInfo info = result.tableCheckpointInfos.get(0);

    assertEquals(sourceInstantSynced, info.lastInstantSynced);
    assertEquals(pendingInstants, info.instantsToConsiderForNextSync);
    assertEquals(sourceId, info.sourceIdentifier);
  }

  @Test
  public void testGetTableSyncExtraMetadata_AddNewSourceCheckpoint() throws Exception {
    // Instant
    HoodieInstant instant =
        new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "001");

    Option<HoodieInstant> targetInstantOpt = Option.of(instant);

    // --- SETUP METACLIENT + TIMELINE MOCKS ---
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline activeTimeline = Mockito.mock(HoodieActiveTimeline.class);
    HoodieTimeline completedTimeline = Mockito.mock(HoodieTimeline.class);
    HoodieTimeline commitsTimeline = Mockito.mock(HoodieTimeline.class);

    Mockito.when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
    Mockito.when(activeTimeline.filterCompletedInstants()).thenReturn(completedTimeline);
    Mockito.when(completedTimeline.filter(Mockito.any())).thenReturn(completedTimeline);
    Mockito.when(completedTimeline.firstInstant()).thenReturn(Option.of(instant));
    Mockito.when(metaClient.getCommitsTimeline()).thenReturn(commitsTimeline);

    // OLD metadata contains one entry from *another* source
    TableCheckpointInfo oldInfo =
        TableCheckpointInfo.of("050", Arrays.asList("060"), "otherSource");
    SyncMetadata oldSync = SyncMetadata.of(Instant.now(), Arrays.asList(oldInfo));

    byte[] metadataBytes = buildCommitMetadataBytes(oldSync);

    Mockito.when(commitsTimeline.getInstantDetails(instant))
        .thenReturn(Option.of(metadataBytes));

    // New source info
    String newSourceId = "sourceA";
    String newSyncedInst = "100";
    List<String> pending = Arrays.asList("200", "201");

    // --- CALL METHOD ---
    SyncMetadata updated =
        MetadataSyncUtils.getTableSyncExtraMetadata(
            targetInstantOpt,
            metaClient,
            newSourceId,
            newSyncedInst,
            pending
        );

    // --- VERIFY ---
    assertEquals(2, updated.tableCheckpointInfos.size());

    boolean newSourceFound = updated.tableCheckpointInfos
        .stream()
        .anyMatch(i -> i.sourceIdentifier.equals(newSourceId));

    assertTrue(newSourceFound);

    Optional<TableCheckpointInfo> newSourceCheckpointInfo = updated.tableCheckpointInfos.stream()
        .filter(tableCheckpointInfo -> tableCheckpointInfo.sourceIdentifier.equals(newSourceId)).findFirst();
    assertTrue(newSourceCheckpointInfo.isPresent());
    assertEquals(newSourceId, newSourceCheckpointInfo.get().getSourceIdentifier());
    assertEquals(newSyncedInst, newSourceCheckpointInfo.get().getLastInstantSynced());
    assertEquals(pending, newSourceCheckpointInfo.get().getInstantsToConsiderForNextSync());
  }

  @Test
  public void testGetTableSyncExtraMetadata_UpdateExistingSourceCheckpoint() throws Exception {
    HoodieInstant instant =
        new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "010");

    Option<HoodieInstant> targetInstantOpt = Option.of(instant);

    // --- Mock setup ---
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline activeTimeline = Mockito.mock(HoodieActiveTimeline.class);
    HoodieTimeline completedTimeline = Mockito.mock(HoodieTimeline.class);
    HoodieTimeline commitsTimeline = Mockito.mock(HoodieTimeline.class);

    Mockito.when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
    Mockito.when(activeTimeline.filterCompletedInstants()).thenReturn(completedTimeline);
    Mockito.when(completedTimeline.filter(Mockito.any())).thenReturn(completedTimeline);
    Mockito.when(completedTimeline.firstInstant()).thenReturn(Option.of(instant));
    Mockito.when(metaClient.getCommitsTimeline()).thenReturn(commitsTimeline);

    // Old metadata with same source
    TableCheckpointInfo oldInfo =
        TableCheckpointInfo.of("080", Arrays.asList("090"), "sourceA");
    SyncMetadata oldSync = SyncMetadata.of(Instant.now(), Arrays.asList(oldInfo));

    byte[] metadataBytes = buildCommitMetadataBytes(oldSync);
    Mockito.when(commitsTimeline.getInstantDetails(instant))
        .thenReturn(Option.of(metadataBytes));

    // New details for that same source
    String sourceId = "sourceA";
    String newSynced = "100";
    List<String> pending = Arrays.asList("200");

    // --- Call ---
    SyncMetadata updated =
        MetadataSyncUtils.getTableSyncExtraMetadata(
            targetInstantOpt, metaClient, sourceId, newSynced, pending);

    // --- Verify ---
    assertEquals(1, updated.tableCheckpointInfos.size());
    TableCheckpointInfo info = updated.tableCheckpointInfos.get(0);

    assertEquals(newSynced, info.lastInstantSynced);
    assertEquals(pending, info.instantsToConsiderForNextSync);
    assertEquals(sourceId, info.sourceIdentifier);
  }


  private byte[] buildCommitMetadataBytes(SyncMetadata metadata) throws Exception {
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addMetadata(SyncMetadata.TABLE_SYNC_METADATA, metadata.toJson());
    return commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8);
  }
}
