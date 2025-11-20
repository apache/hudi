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

package org.apache.hudi.utilities;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class MetadataSyncUtils {

  public static List<HoodieInstant> getPendingInstants(
      HoodieActiveTimeline activeTimeline,
      Option<HoodieInstant> latestCommitOpt) {
    List<HoodieInstant> pendingHoodieInstants;
    if (latestCommitOpt.isPresent()) {
      pendingHoodieInstants =
          activeTimeline
              .filterInflightsAndRequested().findInstantsBefore(latestCommitOpt.get().getTimestamp())
              .getInstants();
    } else {
      pendingHoodieInstants =
          activeTimeline
              .filterInflightsAndRequested()
              .getInstants();
    }
    return pendingHoodieInstants;
  }

  public static SyncMetadata getTableSyncExtraMetadata(Option<HoodieInstant> targetTableLastInstant, HoodieTableMetaClient targetTableMetaClient, String sourceIdentifier,
                                                       String sourceInstantSynced, TreeMap<HoodieInstant, Boolean> instantStatusMap, Option<String> lastSyncCheckpointOpt, HoodieInstant currentInstantToSync) {
    Pair<String, List<String>> pendingInstantsAndCheckpointPair = getPendingInstantsAndCheckpointForNextSync(instantStatusMap, lastSyncCheckpointOpt, currentInstantToSync);
    return targetTableLastInstant.map(instant -> {
      SyncMetadata lastSyncMetadata;
      try {
        lastSyncMetadata = getTableSyncMetadataFromCommitMetadata(instant, targetTableMetaClient);
      } catch (IOException e) {
        throw new HoodieException("Failed to get sync metadata from instant " + instant.getTimestamp());
      }

      TableCheckpointInfo checkpointInfo = TableCheckpointInfo.of(pendingInstantsAndCheckpointPair.getLeft(), pendingInstantsAndCheckpointPair.getRight(), sourceIdentifier);
      List<TableCheckpointInfo> updatedCheckpointInfos = lastSyncMetadata.getTableCheckpointInfos().stream()
          .filter(metadata -> !metadata.getSourceIdentifier().equals(sourceIdentifier)).collect(Collectors.toList());
      updatedCheckpointInfos.add(checkpointInfo);
      return SyncMetadata.of(Instant.now(), updatedCheckpointInfos);
    }).orElseGet(() -> {
      List<TableCheckpointInfo> checkpointInfos = Collections.singletonList(TableCheckpointInfo.of(sourceInstantSynced, pendingInstantsAndCheckpointPair.getRight(), sourceIdentifier));
      return SyncMetadata.of(Instant.now(), checkpointInfos);
    });
  }

  public static SyncMetadata getTableSyncExtraMetadata(Option<HoodieInstant> targetTableLastInstant, HoodieTableMetaClient targetTableMetaClient, String sourceIdentifier,
                                                       String sourceInstantSynced, List<String> pendingInstants) {
    return targetTableLastInstant.map(instant -> {
      SyncMetadata lastSyncMetadata = null;
      try {
        lastSyncMetadata = getTableSyncMetadataFromCommitMetadata(instant, targetTableMetaClient);
      } catch (IOException e) {
        throw new HoodieException("Failed to get sync metadata");
      }

      TableCheckpointInfo checkpointInfo = TableCheckpointInfo.of(sourceInstantSynced, pendingInstants, sourceIdentifier);
      List<TableCheckpointInfo> updatedCheckpointInfos = lastSyncMetadata.getTableCheckpointInfos().stream()
          .filter(metadata -> !metadata.getSourceIdentifier().equals(sourceIdentifier)).collect(Collectors.toList());
      updatedCheckpointInfos.add(checkpointInfo);
      return SyncMetadata.of(Instant.now(), updatedCheckpointInfos);
    }).orElseGet(() -> {
      List<TableCheckpointInfo> checkpointInfos = Collections.singletonList(TableCheckpointInfo.of(sourceInstantSynced, pendingInstants, sourceIdentifier));
      return SyncMetadata.of(Instant.now(), checkpointInfos);
    });
  }

  private static Pair<String, List<String>> getPendingInstantsAndCheckpointForNextSync(TreeMap<HoodieInstant, Boolean> instantStatusMap, Option<String> lastSyncCheckpoint, HoodieInstant currentInstantToSync) {
    if(!lastSyncCheckpoint.isPresent()) {
      return Pair.of(currentInstantToSync.getTimestamp(), Collections.emptyList());
    }

    List<String> pendingInstantsForNextSync = new ArrayList<>();
    HoodieInstant lastSyncCheckpointInstant = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, lastSyncCheckpoint.get());
    for (Map.Entry<HoodieInstant, Boolean> entry : instantStatusMap.entrySet()) {
      HoodieInstant instant = entry.getKey();
      boolean isCompleted = entry.getValue();

      if(instant.compareTo(currentInstantToSync) >= 0) {
        break;
      }

      if(!isCompleted) {
        pendingInstantsForNextSync.add(instant.getTimestamp());
      }
    }

    String checkpointForNextSync;
    if (currentInstantToSync.compareTo(lastSyncCheckpointInstant) <= 0) {
      NavigableMap<HoodieInstant, Boolean> subMap = instantStatusMap.subMap(currentInstantToSync, false, lastSyncCheckpointInstant, false);
      pendingInstantsForNextSync.addAll(subMap.keySet().stream().map(HoodieInstant::getTimestamp).collect(Collectors.toList()));
      checkpointForNextSync = lastSyncCheckpoint.get();
    } else {
      checkpointForNextSync = currentInstantToSync.getTimestamp();
    }

    return Pair.of(checkpointForNextSync, pendingInstantsForNextSync);
  }

  public static SyncMetadata getTableSyncMetadataFromCommitMetadata(HoodieInstant instant, HoodieTableMetaClient metaClient) throws IOException {
    HoodieCommitMetadata commitMetadata = getHoodieCommitMetadata(instant.getTimestamp(), metaClient);
    Option<String> tableSyncMetadataJson = Option.ofNullable(commitMetadata.getMetadata(SyncMetadata.TABLE_SYNC_METADATA));
    if (!tableSyncMetadataJson.isPresent()) {
      // if table sync metadata is not present, sync all commits from source table
      throw new HoodieException("Table sync metadata is missing in the target table commit metadata");
    }
    Option<SyncMetadata> tableSyncMetadataListOpt = SyncMetadata.fromJson(tableSyncMetadataJson.get());
    if (!tableSyncMetadataListOpt.isPresent()) {
      throw new HoodieException("Table Sync metadata is empty in the target table commit metadata");
    }

    return tableSyncMetadataListOpt.get();
  }

  public static HoodieCommitMetadata getHoodieCommitMetadata(String instantTime, HoodieTableMetaClient metaClient) throws IOException {
    Option<HoodieInstant> instantOpt = metaClient.getActiveTimeline().filterCompletedInstants().filter(instant -> instant.getTimestamp().equalsIgnoreCase(instantTime)).firstInstant();
    if (instantOpt.isPresent()) {
      HoodieInstant instant = instantOpt.get();
      return HoodieCommitMetadata.fromBytes(
          metaClient.getCommitsTimeline().getInstantDetails(instant).get(), HoodieCommitMetadata.class);
    } else {
      throw new HoodieException("Could not find " + instantTime + " in source table timeline ");
    }
  }

  public static Pair<TreeMap<HoodieInstant, Boolean>, Option<String>>
  getInstantsToSyncAndLastSyncCheckpoint(
      String sourceIdentifier,
      HoodieTableMetaClient targetTableMetaClient,
      HoodieTableMetaClient sourceTableMetaClient
  ) throws IOException {

    Option<HoodieInstant> lastInstant =
        targetTableMetaClient.reloadActiveTimeline()
            .getWriteTimeline()
            .filterCompletedInstants()
            .lastInstant();

    HoodieActiveTimeline sourceTimeline = sourceTableMetaClient.getActiveTimeline();
    TreeMap<HoodieInstant, Boolean> instantSyncMap = new TreeMap<>();
    Option<String> lastSyncedCheckpoint = Option.empty();

    // --- Case 1: No previous sync done ---
    if (!lastInstant.isPresent()) {
      fillAllInstants(sourceTimeline, instantSyncMap);
      return Pair.of(instantSyncMap, lastSyncedCheckpoint);
    }

    // --- Case 2: Last sync exists ---
    SyncMetadata syncMetadata =
        getTableSyncMetadataFromCommitMetadata(lastInstant.get(), targetTableMetaClient);

    Optional<TableCheckpointInfo> maybeSourceSync =
        syncMetadata.getTableCheckpointInfos().stream()
            .filter(info -> info.getSourceIdentifier().equals(sourceIdentifier))
            .findFirst();

    // --- Case 2A: No sync metadata for this source (fresh sync) ---
    if (!maybeSourceSync.isPresent()) {
      fillAllInstants(sourceTimeline, instantSyncMap);
      return Pair.of(instantSyncMap, lastSyncedCheckpoint);
    }

    // --- Case 2B: There is previous sync metadata ---
    TableCheckpointInfo checkpointInfo = maybeSourceSync.get();
    String lastInstantSynced = checkpointInfo.getLastInstantSynced();

    List<HoodieInstant> newInstants =
        sourceTimeline.filterCompletedInstants()
            .findInstantsAfter(lastInstantSynced)
            .getInstants();

    List<HoodieInstant> currentPending = getPendingInstants(sourceTimeline, Option.empty());

    List<HoodieInstant> pendingFromLastSync =
        checkpointInfo.getInstantsToConsiderForNextSync().stream()
            .map(instant ->
                sourceTimeline.findInstantsAfterOrEquals(instant, 1)
                    .getInstants().stream().findFirst().orElse(null))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    // pending carried-over
    pendingFromLastSync.stream()
        .filter(currentPending::contains)
        .forEach(i -> instantSyncMap.put(i, false));

    // all current pending
    currentPending.forEach(i -> instantSyncMap.put(i, false));

    // new completed instants after last sync
    newInstants.forEach(i -> instantSyncMap.put(i, true));

    lastSyncedCheckpoint = Option.of(lastInstantSynced);

    return Pair.of(instantSyncMap, lastSyncedCheckpoint);
  }

  private static void fillAllInstants(
      HoodieActiveTimeline timeline,
      TreeMap<HoodieInstant, Boolean> map
  ) {
    List<HoodieInstant> completed =
        timeline.filterCompletedInstants().getInstants();

    List<HoodieInstant> pending =
        getPendingInstants(timeline, Option.empty());

    completed.forEach(i -> map.put(i, true));
    pending.forEach(i -> map.put(i, false));
  }
}
