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
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieTimeTravelException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.config.HoodieCommonConfig.INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.GREATER_THAN;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.GREATER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.LESSER_THAN;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.LESSER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.SAVEPOINT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.compareTimestamps;

/**
 * TimelineUtils provides a common way to query incremental meta-data changes for a hoodie table.
 * <p>
 * This is useful in multiple places including:
 * 1) HiveSync - this can be used to query partitions that changed since previous sync.
 * 2) Incremental reads - InputFormats can use this API to query
 */
public class TimelineUtils {
  private static final Logger LOG = LoggerFactory.getLogger(TimelineUtils.class);

  /**
   * Returns partitions that have new data strictly after commitTime.
   * Does not include internal operations such as clean in the timeline.
   */
  public static List<String> getWrittenPartitions(HoodieTimeline timeline) {
    HoodieTimeline timelineToSync = timeline.getWriteTimeline();
    return getAffectedPartitions(timelineToSync);
  }

  /**
   * Returns partitions that have been deleted or marked for deletion in the timeline between given commit time range.
   * Does not include internal operations such as clean in the timeline.
   */
  public static List<String> getDroppedPartitions(HoodieTableMetaClient metaClient, Option<String> lastCommitTimeSynced, Option<String> lastCommitCompletionTimeSynced) {
    HoodieTimeline timeline = lastCommitTimeSynced.isPresent()
        ? TimelineUtils.getCommitsTimelineAfter(metaClient, lastCommitTimeSynced.get(), lastCommitCompletionTimeSynced)
        : metaClient.getActiveTimeline();
    HoodieTimeline completedTimeline = timeline.getWriteTimeline().filterCompletedInstants();
    HoodieTimeline replaceCommitTimeline = completedTimeline.getCompletedReplaceTimeline();
    Map<String, String> partitionToLatestDeleteTimestamp = replaceCommitTimeline.getInstantsAsStream()
        .map(instant -> {
          try {
            HoodieReplaceCommitMetadata commitMetadata = HoodieReplaceCommitMetadata.fromBytes(
                replaceCommitTimeline.getInstantDetails(instant).get(), HoodieReplaceCommitMetadata.class);
            return Pair.of(instant, commitMetadata);
          } catch (IOException e) {
            throw new HoodieIOException("Failed to get partitions modified at " + instant, e);
          }
        })
        .filter(pair -> isDeletePartition(pair.getRight().getOperationType()))
        .flatMap(pair -> pair.getRight().getPartitionToReplaceFileIds().keySet().stream()
            .map(partition -> new AbstractMap.SimpleEntry<>(partition, pair.getLeft().getTimestamp()))
        ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (existing, replace) -> replace));
    // cleaner could delete a partition when there are no active filegroups in the partition
    HoodieTimeline cleanerTimeline = metaClient.getActiveTimeline().getCleanerTimeline().filterCompletedInstants();
    cleanerTimeline.getInstantsAsStream()
        .forEach(instant -> {
          try {
            HoodieCleanMetadata cleanMetadata = TimelineMetadataUtils.deserializeHoodieCleanMetadata(cleanerTimeline.getInstantDetails(instant).get());
            cleanMetadata.getPartitionMetadata().forEach((partition, partitionMetadata) -> {
              if (Boolean.TRUE.equals(partitionMetadata.getIsPartitionDeleted())) {
                partitionToLatestDeleteTimestamp.put(partition, instant.getTimestamp());
              }
            });
          } catch (IOException e) {
            throw new HoodieIOException("Failed to get partitions cleaned at " + instant, e);
          }
        });

    if (partitionToLatestDeleteTimestamp.isEmpty()) {
      // There is no dropped partitions
      return Collections.emptyList();
    }
    String earliestDeleteTimestamp = partitionToLatestDeleteTimestamp.values().stream()
        .reduce((left, right) -> compareTimestamps(left, LESSER_THAN, right) ? left : right)
        .get();
    Map<String, String> partitionToLatestWriteTimestamp = completedTimeline.getInstantsAsStream()
        .filter(instant -> compareTimestamps(instant.getTimestamp(), GREATER_THAN_OR_EQUALS, earliestDeleteTimestamp))
        .flatMap(instant -> {
          try {
            HoodieCommitMetadata commitMetadata = getCommitMetadata(instant, completedTimeline);
            return commitMetadata.getWritePartitionPaths().stream()
                .map(partition -> new AbstractMap.SimpleEntry<>(partition, instant.getTimestamp()));
          } catch (IOException e) {
            throw new HoodieIOException("Failed to get partitions writes at " + instant, e);
          }
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (existing, replace) -> replace));

    return partitionToLatestDeleteTimestamp.entrySet().stream()
        .filter(entry -> !partitionToLatestWriteTimestamp.containsKey(entry.getKey())
            || compareTimestamps(entry.getValue(), GREATER_THAN, partitionToLatestWriteTimestamp.get(entry.getKey()))
        ).map(Map.Entry::getKey).filter(partition -> !partition.isEmpty()).collect(Collectors.toList());
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
      LOG.info("reading checkpoint info for:" + instant + " key: " + extraMetadataKey);
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
   * @param lastMaxCompletionTime     Last commit max completion time synced
   * @return Hudi timeline.
   */
  public static HoodieTimeline getCommitsTimelineAfter(
      HoodieTableMetaClient metaClient, String exclusiveStartInstantTime, Option<String> lastMaxCompletionTime) {
    HoodieDefaultTimeline writeTimeline = metaClient.getActiveTimeline().getWriteTimeline();

    HoodieDefaultTimeline timeline = writeTimeline.isBeforeTimelineStarts(exclusiveStartInstantTime)
        ? metaClient.getArchivedTimeline(exclusiveStartInstantTime).mergeTimeline(writeTimeline)
        : writeTimeline;

    HoodieDefaultTimeline timelineSinceLastSync = (HoodieDefaultTimeline) timeline.getCommitsTimeline()
        .findInstantsAfter(exclusiveStartInstantTime, Integer.MAX_VALUE);

    if (lastMaxCompletionTime.isPresent()) {
      // Get 'hollow' instants that have less instant time than exclusiveStartInstantTime but with greater commit completion time
      HoodieDefaultTimeline hollowInstantsTimeline = (HoodieDefaultTimeline) timeline.getCommitsTimeline()
          .filter(s -> compareTimestamps(s.getTimestamp(), LESSER_THAN, exclusiveStartInstantTime))
          .filter(s -> compareTimestamps(s.getStateTransitionTime(), GREATER_THAN, lastMaxCompletionTime.get()));
      if (!hollowInstantsTimeline.empty()) {
        return timelineSinceLastSync.mergeTimeline(hollowInstantsTimeline);
      }
    }

    return timelineSinceLastSync;
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

  /**
   * Validate user-specified timestamp of time travel query against incomplete commit's timestamp.
   *
   * @throws HoodieException when time travel query's timestamp >= incomplete commit's timestamp
   */
  public static void validateTimestampAsOf(HoodieTableMetaClient metaClient, String timestampAsOf) {
    Option<HoodieInstant> firstIncompleteCommit = metaClient.getCommitsTimeline()
        .filterInflightsAndRequested()
        .filter(instant ->
            !HoodieTimeline.REPLACE_COMMIT_ACTION.equals(instant.getAction())
                || !ClusteringUtils.getClusteringPlan(metaClient, instant).isPresent())
        .firstInstant();

    if (firstIncompleteCommit.isPresent()) {
      String incompleteCommitTime = firstIncompleteCommit.get().getTimestamp();
      if (compareTimestamps(timestampAsOf, GREATER_THAN_OR_EQUALS, incompleteCommitTime)) {
        throw new HoodieTimeTravelException(String.format(
            "Time travel's timestamp '%s' must be earlier than the first incomplete commit timestamp '%s'.",
            timestampAsOf, incompleteCommitTime));
      }
    }

    // also timestamp as of cannot query cleaned up data.
    Option<HoodieInstant> latestCleanOpt = metaClient.getActiveTimeline().getCleanerTimeline().filterCompletedInstants().lastInstant();
    if (latestCleanOpt.isPresent()) {
      // Ensure timestamp as of is > than the earliest commit to retain and
      try {
        HoodieCleanMetadata cleanMetadata = CleanerUtils.getCleanerMetadata(metaClient, latestCleanOpt.get());
        String earliestCommitToRetain = cleanMetadata.getEarliestCommitToRetain();
        if (!StringUtils.isNullOrEmpty(earliestCommitToRetain)) {
          ValidationUtils.checkArgument(HoodieTimeline.compareTimestamps(earliestCommitToRetain, LESSER_THAN_OR_EQUALS, timestampAsOf),
              "Cleaner cleaned up the timestamp of interest. Please ensure sufficient commits are retained with cleaner "
                  + "for Timestamp as of query to work");
        } else {
          // when cleaner is based on file versions, we may not find value for earliestCommitToRetain.
          // so, lets check if timestamp of interest is archived based on first entry in active timeline
          Option<HoodieInstant> firstCompletedInstant = metaClient.getActiveTimeline().getWriteTimeline().filterCompletedInstants().firstInstant();
          if (firstCompletedInstant.isPresent()) {
            ValidationUtils.checkArgument(HoodieTimeline.compareTimestamps(firstCompletedInstant.get().getTimestamp(), LESSER_THAN_OR_EQUALS, timestampAsOf),
                "Please ensure sufficient commits are retained (uncleaned and un-archived) for timestamp as of query to work.");
          }
        }
      } catch (IOException e) {
        throw new HoodieTimeTravelException("Cleaner cleaned up the timestamp of interest. "
            + "Please ensure sufficient commits are retained with cleaner for Timestamp as of query to work ");
      }
    }
  }

  /**
   * Handles hollow commit as per {@link HoodieCommonConfig#INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT}
   * and return filtered or non-filtered timeline for incremental query to run against.
   */
  public static HoodieTimeline handleHollowCommitIfNeeded(HoodieTimeline completedCommitTimeline,
      HoodieTableMetaClient metaClient, HollowCommitHandling handlingMode) {
    if (handlingMode == HollowCommitHandling.USE_TRANSITION_TIME) {
      return completedCommitTimeline;
    }

    Option<HoodieInstant> firstIncompleteCommit = metaClient.getCommitsTimeline()
        .filterInflightsAndRequested()
        .filter(instant ->
            !HoodieTimeline.REPLACE_COMMIT_ACTION.equals(instant.getAction())
                || !ClusteringUtils.getClusteringPlan(metaClient, instant).isPresent())
        .firstInstant();

    boolean noHollowCommit = firstIncompleteCommit
        .map(i -> completedCommitTimeline.findInstantsAfter(i.getTimestamp()).empty())
        .orElse(true);
    if (noHollowCommit) {
      return completedCommitTimeline;
    }

    String hollowCommitTimestamp = firstIncompleteCommit.get().getTimestamp();
    switch (handlingMode) {
      case FAIL:
        throw new HoodieException(String.format(
            "Found hollow commit: '%s'. Adjust config `%s` accordingly if to avoid throwing this exception.",
            hollowCommitTimestamp, INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT.key()));
      case BLOCK:
        LOG.warn(String.format(
            "Found hollow commit '%s'. Config `%s` was set to `%s`: no data will be returned beyond '%s' until it's completed.",
            hollowCommitTimestamp, INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT.key(), handlingMode, hollowCommitTimestamp));
        return completedCommitTimeline.findInstantsBefore(hollowCommitTimestamp);
      default:
        throw new HoodieException("Unexpected handling mode: " + handlingMode);
    }
  }

  public enum HollowCommitHandling {
    FAIL, BLOCK, USE_TRANSITION_TIME
  }

  public static boolean isDeletePartition(WriteOperationType operation) {
    return operation == WriteOperationType.DELETE_PARTITION
        || operation == WriteOperationType.INSERT_OVERWRITE_TABLE
        || operation == WriteOperationType.INSERT_OVERWRITE;
  }
}
