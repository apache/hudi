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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.SAVEPOINT_ACTION;

/**
 * TimelineUtils provides a common way to query incremental meta-data changes for a hoodie table.
 *
 * This is useful in multiple places including:
 * 1) HiveSync - this can be used to query partitions that changed since previous sync.
 * 2) Incremental reads - InputFormats can use this API to query
 */
public class TimelineUtils {
  private static final Logger LOG = LogManager.getLogger(TimelineUtils.class);

  /**
   * Returns partitions that have new data strictly after commitTime.
   * Does not include internal operations such as clean in the timeline.
   */
  public static List<String> getWrittenPartitions(HoodieTimeline timeline) {
    HoodieTimeline timelineToSync = timeline.getWriteTimeline();
    return getAffectedPartitions(timelineToSync);
  }

  /**
   * Returns partitions that have been deleted or marked for deletion in the given timeline.
   * Does not include internal operations such as clean in the timeline.
   */
  public static List<String> getDroppedPartitions(HoodieTimeline timeline) {
    HoodieTimeline replaceCommitTimeline = timeline.getWriteTimeline().filterCompletedInstants().getCompletedReplaceTimeline();

    return replaceCommitTimeline.getInstantsAsStream().flatMap(instant -> {
      try {
        HoodieReplaceCommitMetadata commitMetadata = HoodieReplaceCommitMetadata.fromBytes(
            replaceCommitTimeline.getInstantDetails(instant).get(), HoodieReplaceCommitMetadata.class);
        if (WriteOperationType.DELETE_PARTITION.equals(commitMetadata.getOperationType())) {
          Map<String, List<String>> partitionToReplaceFileIds = commitMetadata.getPartitionToReplaceFileIds();
          return partitionToReplaceFileIds.keySet().stream();
        } else {
          return Stream.empty();
        }
      } catch (IOException e) {
        throw new HoodieIOException("Failed to get partitions modified at " + instant, e);
      }
    }).distinct().filter(partition -> !partition.isEmpty()).collect(Collectors.toList());
  }

  /**
   * Returns partitions that have been modified including internal operations such as clean in the passed timeline.
   */
  public static List<String> getAffectedPartitions(HoodieTimeline timeline) {
    return timeline.filterCompletedInstants().getInstantsAsStream().flatMap(s -> {
      switch (s.getAction()) {
        case COMMIT_ACTION:
        case DELTA_COMMIT_ACTION:
          try {
            HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(s).get(), HoodieCommitMetadata.class);
            return commitMetadata.getPartitionToWriteStats().keySet().stream();
          } catch (IOException e) {
            throw new HoodieIOException("Failed to get partitions written at " + s, e);
          }
        case REPLACE_COMMIT_ACTION:
          try {
            HoodieReplaceCommitMetadata commitMetadata = HoodieReplaceCommitMetadata.fromBytes(
                timeline.getInstantDetails(s).get(), HoodieReplaceCommitMetadata.class);
            Set<String> partitions = new HashSet<>();
            partitions.addAll(commitMetadata.getPartitionToReplaceFileIds().keySet());
            partitions.addAll(commitMetadata.getPartitionToWriteStats().keySet());
            return partitions.stream();
          } catch (IOException e) {
            throw new HoodieIOException("Failed to get partitions modified at " + s, e);
          }
        case HoodieTimeline.CLEAN_ACTION:
          try {
            HoodieCleanMetadata cleanMetadata = TimelineMetadataUtils.deserializeHoodieCleanMetadata(timeline.getInstantDetails(s).get());
            return cleanMetadata.getPartitionMetadata().keySet().stream();
          } catch (IOException e) {
            throw new HoodieIOException("Failed to get partitions cleaned at " + s, e);
          }
        case HoodieTimeline.ROLLBACK_ACTION:
          try {
            return TimelineMetadataUtils.deserializeHoodieRollbackMetadata(timeline.getInstantDetails(s).get()).getPartitionMetadata().keySet().stream();
          } catch (IOException e) {
            throw new HoodieIOException("Failed to get partitions rolledback at " + s, e);
          }
        case HoodieTimeline.RESTORE_ACTION:
          try {
            HoodieRestoreMetadata restoreMetadata = TimelineMetadataUtils.deserializeAvroMetadata(timeline.getInstantDetails(s).get(), HoodieRestoreMetadata.class);
            return restoreMetadata.getHoodieRestoreMetadata().values().stream()
                .flatMap(Collection::stream)
                .flatMap(rollbackMetadata -> rollbackMetadata.getPartitionMetadata().keySet().stream());
          } catch (IOException e) {
            throw new HoodieIOException("Failed to get partitions restored at " + s, e);
          }
        case HoodieTimeline.SAVEPOINT_ACTION:
          try {
            return TimelineMetadataUtils.deserializeHoodieSavepointMetadata(timeline.getInstantDetails(s).get()).getPartitionMetadata().keySet().stream();
          } catch (IOException e) {
            throw new HoodieIOException("Failed to get partitions savepoint at " + s, e);
          }
        case HoodieTimeline.COMPACTION_ACTION:
          // compaction is not a completed instant.  So no need to consider this action.
          return Stream.empty();
        default:
          throw new HoodieIOException("unknown action in timeline " + s.getAction());
      }

    }).distinct().filter(s -> !s.isEmpty()).collect(Collectors.toList());
  }

  /**
   * Get extra metadata for specified key from latest commit/deltacommit/replacecommit(eg. insert_overwrite) instant.
   */
  public static Option<String> getExtraMetadataFromLatest(HoodieTableMetaClient metaClient, String extraMetadataKey) {
    return metaClient.getCommitsTimeline().filterCompletedInstants().getReverseOrderedInstants()
        // exclude clustering commits for returning user stored extra metadata 
        .filter(instant -> !isClusteringCommit(metaClient, instant))
        .findFirst().map(instant ->
            getMetadataValue(metaClient, extraMetadataKey, instant)).orElse(Option.empty());
  }

  /**
   * Get extra metadata for specified key from latest commit/deltacommit/replacecommit instant including internal commits
   * such as clustering.
   */
  public static Option<String> getExtraMetadataFromLatestIncludeClustering(HoodieTableMetaClient metaClient, String extraMetadataKey) {
    return metaClient.getCommitsTimeline().filterCompletedInstants().getReverseOrderedInstants()
        .findFirst().map(instant ->
            getMetadataValue(metaClient, extraMetadataKey, instant)).orElse(Option.empty());
  }

  /**
   * Get extra metadata for specified key from all active commit/deltacommit instants.
   */
  public static Map<String, Option<String>> getAllExtraMetadataForKey(HoodieTableMetaClient metaClient, String extraMetadataKey) {
    return metaClient.getCommitsTimeline().filterCompletedInstants().getReverseOrderedInstants().collect(Collectors.toMap(
        HoodieInstant::getTimestamp, instant -> getMetadataValue(metaClient, extraMetadataKey, instant)));
  }

  private static Option<String> getMetadataValue(HoodieTableMetaClient metaClient, String extraMetadataKey, HoodieInstant instant) {
    try {
      LOG.info("reading checkpoint info for:"  + instant + " key: " + extraMetadataKey);
      HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
          metaClient.getCommitsTimeline().getInstantDetails(instant).get(), HoodieCommitMetadata.class);

      return Option.ofNullable(commitMetadata.getExtraMetadata().get(extraMetadataKey));
    } catch (IOException e) {
      throw new HoodieIOException("Unable to parse instant metadata " + instant, e);
    }
  }

  public static boolean isClusteringCommit(HoodieTableMetaClient metaClient, HoodieInstant instant) {
    try {
      if (REPLACE_COMMIT_ACTION.equals(instant.getAction())) {
        // replacecommit is used for multiple operations: insert_overwrite/cluster etc. 
        // Check operation type to see if this instant is related to clustering.
        HoodieReplaceCommitMetadata replaceMetadata = HoodieReplaceCommitMetadata.fromBytes(
            metaClient.getActiveTimeline().getInstantDetails(instant).get(), HoodieReplaceCommitMetadata.class);
        return WriteOperationType.CLUSTER.equals(replaceMetadata.getOperationType());
      }

      return false;
    } catch (IOException e) {
      throw new HoodieIOException("Unable to read instant information: " + instant + " for " + metaClient.getBasePath(), e);
    }
  }

  public static HoodieDefaultTimeline getTimeline(HoodieTableMetaClient metaClient, boolean includeArchivedTimeline) {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    if (includeArchivedTimeline) {
      HoodieArchivedTimeline archivedTimeline = metaClient.getArchivedTimeline();
      return archivedTimeline.mergeTimeline(activeTimeline);
    }
    return activeTimeline;
  }

  /**
   * Returns a Hudi timeline with commits after the given instant time (exclusive).
   *
   * @param metaClient                {@link HoodieTableMetaClient} instance.
   * @param exclusiveStartInstantTime Start instant time (exclusive).
   * @return Hudi timeline.
   */
  public static HoodieTimeline getCommitsTimelineAfter(
      HoodieTableMetaClient metaClient, String exclusiveStartInstantTime) {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    HoodieDefaultTimeline timeline =
        activeTimeline.isBeforeTimelineStarts(exclusiveStartInstantTime)
            ? metaClient.getArchivedTimeline(exclusiveStartInstantTime)
            .mergeTimeline(activeTimeline)
            : activeTimeline;
    return timeline.getCommitsTimeline()
        .findInstantsAfter(exclusiveStartInstantTime, Integer.MAX_VALUE);
  }
  
  /**
   * Returns the commit metadata of the given instant.
   *
   * @param instant  The hoodie instant
   * @param timeline The timeline
   * @return the commit metadata
   */
  public static HoodieCommitMetadata getCommitMetadata(
      HoodieInstant instant,
      HoodieTimeline timeline) throws IOException {
    byte[] data = timeline.getInstantDetails(instant).get();
    if (instant.getAction().equals(REPLACE_COMMIT_ACTION)) {
      return HoodieReplaceCommitMetadata.fromBytes(data, HoodieReplaceCommitMetadata.class);
    } else {
      return HoodieCommitMetadata.fromBytes(data, HoodieCommitMetadata.class);
    }
  }

  /**
   * Gets the qualified earliest instant from the active timeline of the data table
   * for the archival in metadata table.
   * <p>
   * the qualified earliest instant is chosen as the earlier one between the earliest
   * commit (COMMIT, DELTA_COMMIT, and REPLACE_COMMIT only, considering non-savepoint
   * commit only if enabling archive beyond savepoint) and the earliest inflight
   * instant (all actions).
   *
   * @param dataTableActiveTimeline      the active timeline of the data table.
   * @param shouldArchiveBeyondSavepoint whether to archive beyond savepoint.
   * @return the instant meeting the requirement.
   */
  public static Option<HoodieInstant> getEarliestInstantForMetadataArchival(
      HoodieActiveTimeline dataTableActiveTimeline, boolean shouldArchiveBeyondSavepoint) {
    // This is for commits only, not including CLEAN, ROLLBACK, etc.
    // When archive beyond savepoint is enabled, there are chances that there could be holes
    // in the timeline due to archival and savepoint interplay.  So, the first non-savepoint
    // commit in the data timeline is considered as beginning of the active timeline.
    Option<HoodieInstant> earliestCommit = shouldArchiveBeyondSavepoint
        ? dataTableActiveTimeline.getTimelineOfActions(
            CollectionUtils.createSet(
                COMMIT_ACTION, DELTA_COMMIT_ACTION, REPLACE_COMMIT_ACTION, SAVEPOINT_ACTION))
        .getFirstNonSavepointCommit()
        : dataTableActiveTimeline.getCommitsTimeline().firstInstant();
    // This is for all instants which are in-flight
    Option<HoodieInstant> earliestInflight =
        dataTableActiveTimeline.filterInflightsAndRequested().firstInstant();

    if (earliestCommit.isPresent() && earliestInflight.isPresent()) {
      if (earliestCommit.get().compareTo(earliestInflight.get()) < 0) {
        return earliestCommit;
      }
      return earliestInflight;
    } else if (earliestCommit.isPresent()) {
      return earliestCommit;
    } else if (earliestInflight.isPresent()) {
      return earliestInflight;
    } else {
      return Option.empty();
    }
  }
}
