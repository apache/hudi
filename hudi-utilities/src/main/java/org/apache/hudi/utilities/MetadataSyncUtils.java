package org.apache.hudi.utilities;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class MetadataSyncUtils {

  public static List<String> getPendingInstants(
      HoodieActiveTimeline activeTimeline,
      HoodieInstant latestCommit) {
    List<HoodieInstant> pendingHoodieInstants =
        activeTimeline
            .filterInflightsAndRequested()
            .findInstantsBefore(latestCommit.getTimestamp())
            .getInstants();
    return pendingHoodieInstants.stream()
        .map(HoodieInstant::getTimestamp)
        .collect(Collectors.toList());
  }

  public static SyncMetadata getTableSyncExtraMetadata(Option<HoodieInstant> targetTableLastInstant, HoodieTableMetaClient targetTableMetaClient, String sourceIdentifier,
                                                       String sourceInstantSynced, List<String> pendingInstantsToSync) {
    return targetTableLastInstant.map(instant -> {
      SyncMetadata lastSyncMetadata = null;
      try {
        lastSyncMetadata = getTableSyncMetadataFromCommitMetadata(instant, targetTableMetaClient);
      } catch (IOException e) {
        throw new HoodieException("Failed to get sync metadata");
      }

      TableCheckpointInfo checkpointInfo = TableCheckpointInfo.of(sourceInstantSynced, pendingInstantsToSync, sourceIdentifier);
      List<TableCheckpointInfo> updatedCheckpointInfos = lastSyncMetadata.getTableCheckpointInfos().stream()
          .filter(metadata -> !metadata.getSourceIdentifier().equals(sourceIdentifier)).collect(Collectors.toList());
      updatedCheckpointInfos.add(checkpointInfo);
      return SyncMetadata.of(Instant.now(), updatedCheckpointInfos);
    }).orElseGet(() -> {
      List<TableCheckpointInfo> checkpointInfos = Collections.singletonList(TableCheckpointInfo.of(sourceInstantSynced, pendingInstantsToSync, sourceIdentifier));
      return SyncMetadata.of(Instant.now(), checkpointInfos);
    });
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
}
