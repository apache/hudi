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

import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantComparatorV1;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.storage.StorageConfiguration;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.HoodieTableConfig.ARCHIVELOG_FOLDER;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;

/**
 * HoodieBackedTableMetadataWriter for tables with version 6. The class derives most of the functionality from HoodieBackedTableMetadataWriter
 * and overrides some behaviour to make it compatible for version 6.
 */
public abstract class HoodieBackedTableMetadataWriterTableVersionSix<I, O> extends HoodieBackedTableMetadataWriter<I, O> {

  private static final int PARTITION_INITIALIZATION_TIME_SUFFIX = 10;

  /**
   * Hudi backed table metadata writer.
   *
   * Timestamps are generated in format {@link org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator#MILLIS_INSTANT_TIMESTAMP_FORMAT}.
   * For operations like compaction, log compaction, clean, index and rollback - suffix are used which are appended to the timestamp. In some cases
   * it is possible for suffix to be added twice. For instance, there is an index commit in metadata table leading to an instant with 010 suffix in
   * the timeline. If compaction triggers now, the compaction would use the index instant timestamp and add 001 suffix to it, creating a timestamp
   * value with suffix 010001. Both indexing and compaction suffix would be present in the compaction timestamp in such a case.
   *
   * @param storageConf                Storage configuration to use for the metadata writer
   * @param writeConfig                Writer config
   * @param failedWritesCleaningPolicy Cleaning policy on failed writes
   * @param engineContext              Engine context
   * @param inflightInstantTimestamp   Timestamp of any instant in progress
   */
  protected HoodieBackedTableMetadataWriterTableVersionSix(StorageConfiguration<?> storageConf,
                                                           HoodieWriteConfig writeConfig,
                                                           HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                                           HoodieEngineContext engineContext,
                                                           Option<String> inflightInstantTimestamp) {
    super(storageConf, writeConfig, failedWritesCleaningPolicy, engineContext, inflightInstantTimestamp);
  }

  @Override
  List<MetadataPartitionType> getEnabledPartitions(HoodieMetadataConfig metadataConfig, HoodieTableMetaClient metaClient) {
    return MetadataPartitionType.getEnabledPartitions(metadataConfig, metaClient).stream()
        .filter(partition -> !partition.equals(MetadataPartitionType.SECONDARY_INDEX))
        .filter(partition -> !partition.equals(MetadataPartitionType.EXPRESSION_INDEX))
        .filter(partition -> !partition.equals(MetadataPartitionType.PARTITION_STATS))
        .collect(Collectors.toList());
  }

  @Override
  boolean shouldInitializeFromFilesystem(Set<String> pendingDataInstants, Option<String> inflightInstantTimestamp) {
    if (pendingDataInstants.stream()
        .anyMatch(i -> !inflightInstantTimestamp.isPresent() || !i.equals(inflightInstantTimestamp.get()))) {
      metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.BOOTSTRAP_ERR_STR, 1));
      LOG.warn("Cannot initialize metadata table as operation(s) are in progress on the dataset: {}",
          Arrays.toString(pendingDataInstants.toArray()));
      return false;
    } else {
      return true;
    }
  }

  @Override
  String generateUniqueInstantTime(String initializationTime) {
    // if its initialized via Async indexer, we don't need to alter the init time
    HoodieTimeline dataIndexTimeline = dataMetaClient.getActiveTimeline().filter(instant -> instant.getAction().equals(HoodieTimeline.INDEXING_ACTION));
    if (HoodieTableMetadataUtil.isIndexingCommit(dataIndexTimeline, initializationTime)) {
      return initializationTime;
    }
    // Add suffix to initializationTime to find an unused instant time for the next index initialization.
    // This function would be called multiple times in a single application if multiple indexes are being
    // initialized one after the other.
    for (int offset = 0; ; ++offset) {
      final String commitInstantTime = createIndexInitTimestamp(initializationTime, offset);
      if (!metadataMetaClient.getCommitsTimeline().containsInstant(commitInstantTime)) {
        return commitInstantTime;
      }
    }
  }

  /**
   * Create the timestamp for an index initialization operation on the metadata table.
   * <p>
   * Since many MDT partitions can be initialized one after other the offset parameter controls generating a
   * unique timestamp.
   */
  private String createIndexInitTimestamp(String timestamp, int offset) {
    return String.format("%s%03d", timestamp, PARTITION_INITIALIZATION_TIME_SUFFIX + offset);
  }

  @Override
  String getTimelineHistoryPath() {
    return ARCHIVELOG_FOLDER.defaultValue();
  }

  /**
   * Validates the timeline for both main and metadata tables to ensure compaction on MDT can be scheduled.
   */
  @Override
  boolean validateCompactionScheduling(Option<String> inFlightInstantTimestamp, String latestDeltaCommitTimeInMetadataTable) {
    // we need to find if there are any inflights in data table timeline before or equal to the latest delta commit in metadata table.
    // Whenever you want to change this logic, please ensure all below scenarios are considered.
    // a. There could be a chance that latest delta commit in MDT is committed in MDT, but failed in DT. And so findInstantsBeforeOrEquals() should be employed
    // b. There could be DT inflights after latest delta commit in MDT and we are ok with it. bcoz, the contract is, the latest compaction instant time in MDT represents
    // any instants before that is already synced with metadata table.
    // c. Do consider out of order commits. For eg, c4 from DT could complete before c3. and we can't trigger compaction in MDT with c4 as base instant time, until every
    // instant before c4 is synced with metadata table.
    List<HoodieInstant> pendingInstants = dataMetaClient.reloadActiveTimeline().filterInflightsAndRequested()
        .findInstantsBeforeOrEquals(latestDeltaCommitTimeInMetadataTable).getInstants();

    if (!pendingInstants.isEmpty()) {
      checkNumDeltaCommits(metadataMetaClient, dataWriteConfig.getMetadataConfig().getMaxNumDeltacommitsWhenPending());
      LOG.info(
          "Cannot compact metadata table as there are {} inflight instants in data table before latest deltacommit in metadata table: {}. Inflight instants in data table: {}",
          pendingInstants.size(), latestDeltaCommitTimeInMetadataTable, Arrays.toString(pendingInstants.toArray()));
      return false;
    }

    // Check if there are any pending compaction or log compaction instants in the timeline.
    // If pending compact/logCompaction operations are found abort scheduling new compaction/logCompaction operations.
    Option<HoodieInstant> pendingLogCompactionInstant =
        metadataMetaClient.getActiveTimeline().filterPendingLogCompactionTimeline().firstInstant();
    Option<HoodieInstant> pendingCompactionInstant =
        metadataMetaClient.getActiveTimeline().filterPendingCompactionTimeline().firstInstant();
    if (pendingLogCompactionInstant.isPresent() || pendingCompactionInstant.isPresent()) {
      LOG.warn(String.format("Not scheduling compaction or logCompaction, since a pending compaction instant %s or logCompaction %s instant is present",
          pendingCompactionInstant, pendingLogCompactionInstant));
      return false;
    }
    return true;
  }

  private void checkNumDeltaCommits(HoodieTableMetaClient metaClient, int maxNumDeltaCommitsWhenPending) {
    final HoodieActiveTimeline activeTimeline = metaClient.reloadActiveTimeline();
    Option<HoodieInstant> lastCompaction = activeTimeline.filterCompletedInstants()
        .filter(s -> s.getAction().equals(COMMIT_ACTION)).lastInstant();
    int numDeltaCommits = lastCompaction.isPresent()
        ? activeTimeline.getDeltaCommitTimeline().findInstantsAfter(lastCompaction.get().requestedTime()).countInstants()
        : activeTimeline.getDeltaCommitTimeline().countInstants();
    if (numDeltaCommits > maxNumDeltaCommitsWhenPending) {
      throw new HoodieMetadataException(String.format("Metadata table's deltacommits exceeded %d: "
              + "this is likely caused by a pending instant in the data table. Resolve the pending instant "
              + "or adjust `%s`, then restart the pipeline.",
          maxNumDeltaCommitsWhenPending, HoodieMetadataConfig.METADATA_MAX_NUM_DELTACOMMITS_WHEN_PENDING.key()));
    }
  }

  /**
   * Update from {@code HoodieRollbackMetadata}.
   *
   * @param rollbackMetadata {@code HoodieRollbackMetadata}
   * @param instantTime      Timestamp at which the rollback was performed
   */
  @Override
  public void update(HoodieRollbackMetadata rollbackMetadata, String instantTime) {
    if (initialized && metadata != null) {
      // The commit which is being rolled back on the dataset
      final String commitToRollbackInstantTime = rollbackMetadata.getCommitsRollback().get(0);
      // Find the deltacommits since the last compaction
      Option<Pair<HoodieTimeline, HoodieInstant>> deltaCommitsInfo =
          CompactionUtils.getDeltaCommitsSinceLatestCompaction(metadataMetaClient.getActiveTimeline());

      // This could be a compaction or deltacommit instant (See CompactionUtils.getDeltaCommitsSinceLatestCompaction)
      HoodieInstant compactionInstant = deltaCommitsInfo.get().getValue();
      HoodieTimeline deltacommitsSinceCompaction = deltaCommitsInfo.get().getKey();

      // The deltacommit that will be rolled back
      HoodieInstant deltaCommitInstant = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, commitToRollbackInstantTime,
          InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);

      validateRollbackVersionSix(commitToRollbackInstantTime, compactionInstant, deltacommitsSinceCompaction);

      // lets apply a delta commit with DT's rb instant containing following records:
      // a. any log files as part of RB commit metadata that was added
      // b. log files added by the commit in DT being rolled back. By rolled back, we mean, a rollback block will be added and does not mean it will be deleted.
      // both above list should only be added to FILES partition.
      processAndCommit(instantTime, () -> HoodieTableMetadataUtil.convertMetadataToRecords(engineContext, dataMetaClient, rollbackMetadata, instantTime));

      String rollbackInstantTime = createRollbackTimestamp(instantTime);
      if (deltacommitsSinceCompaction.containsInstant(deltaCommitInstant)) {
        LOG.info("Rolling back MDT deltacommit " + commitToRollbackInstantTime);
        if (!getWriteClient().rollback(commitToRollbackInstantTime, rollbackInstantTime)) {
          throw new HoodieMetadataException("Failed to rollback deltacommit at " + commitToRollbackInstantTime);
        }
      } else {
        LOG.info("Ignoring rollback of instant {} at {}. The commit to rollback is not found in MDT",
            commitToRollbackInstantTime, instantTime);
      }
      closeInternal();
    }
  }

  private void validateRollbackVersionSix(
      String commitToRollbackInstantTime,
      HoodieInstant compactionInstant,
      HoodieTimeline deltacommitsSinceCompaction) {
    // The commit being rolled back should not be earlier than the latest compaction on the MDT. Compaction on MDT only occurs when all actions
    // are completed on the dataset. Hence, this case implies a rollback of completed commit which should actually be handled using restore.
    if (compactionInstant.getAction().equals(HoodieTimeline.COMMIT_ACTION)) {
      final String compactionInstantTime = compactionInstant.requestedTime();
      if (compareTimestamps(commitToRollbackInstantTime, LESSER_THAN_OR_EQUALS, compactionInstantTime)) {
        throw new HoodieMetadataException(String.format("Commit being rolled back %s is earlier than the latest compaction %s. "
                + "There are %d deltacommits after this compaction: %s", commitToRollbackInstantTime, compactionInstantTime,
            deltacommitsSinceCompaction.countInstants(), deltacommitsSinceCompaction.getInstants()));
      }
    }
  }

  /**
   * Perform a compaction on the Metadata Table.
   * <p>
   * Cases to be handled:
   * 1. We cannot perform compaction if there are previous inflight operations on the dataset. This is because
   * a compacted metadata base file at time Tx should represent all the actions on the dataset till time Tx.
   * <p>
   * 2. In multi-writer scenario, a parallel operation with a greater instantTime may have completed creating a
   * deltacommit.
   */
  @Override
  void compactIfNecessary(BaseHoodieWriteClient<?,I,?,O> writeClient, Option<String> latestDeltaCommitTimeOpt) {
    // Trigger compaction with suffixes based on the same instant time. This ensures that any future
    // delta commits synced over will not have an instant time lesser than the last completed instant on the
    // metadata table.
    final String compactionInstantTime = createCompactionTimestamp(latestDeltaCommitTimeOpt.get());

    // we need to avoid checking compaction w/ same instant again.
    // let's say we trigger compaction after C5 in MDT and so compaction completes with C4001. but C5 crashed before completing in MDT.
    // and again w/ C6, we will re-attempt compaction at which point latest delta commit is C4 in MDT.
    // and so we try compaction w/ instant C4001. So, we can avoid compaction if we already have compaction w/ same instant time.
    if (metadataMetaClient.getActiveTimeline().filterCompletedInstants().containsInstant(compactionInstantTime)) {
      LOG.info("Compaction with same {} time is already present in the timeline.", compactionInstantTime);
    } else if (writeClient.scheduleCompactionAtInstant(compactionInstantTime, Option.empty())) {
      LOG.info("Compaction is scheduled for timestamp {}", compactionInstantTime);
      writeClient.compact(compactionInstantTime, true);
    } else if (metadataWriteConfig.isLogCompactionEnabled()) {
      // Schedule and execute log compaction with suffixes based on the same instant time. This ensures that any future
      // delta commits synced over will not have an instant time lesser than the last completed instant on the
      // metadata table.
      final String logCompactionInstantTime = createLogCompactionTimestamp(latestDeltaCommitTimeOpt.get());
      if (metadataMetaClient.getActiveTimeline().filterCompletedInstants().containsInstant(logCompactionInstantTime)) {
        LOG.info("Log compaction with same {} time is already present in the timeline.", logCompactionInstantTime);
      } else if (writeClient.scheduleLogCompactionAtInstant(logCompactionInstantTime, Option.empty())) {
        LOG.info("Log compaction is scheduled for timestamp {}", logCompactionInstantTime);
        writeClient.logCompact(logCompactionInstantTime, true);
      }
    }
  }

  @Override
  protected void executeClean(BaseHoodieWriteClient writeClient, String instantTime) {
    writeClient.clean(createCleanTimestamp(instantTime));
  }

  @Override
  String createRestoreInstantTime() {
    return createRestoreTimestamp(getWriteClient().createNewInstantTime(false));
  }

  /**
   * Create the timestamp for a compaction operation on the metadata table.
   */
  private String createCompactionTimestamp(String timestamp) {
    return timestamp + getCompactionOperationSuffix();
  }

  /**
   * Create the timestamp for a compaction operation on the metadata table.
   */
  private String createLogCompactionTimestamp(String timestamp) {
    return timestamp + getLogCompactionOperationSuffix();
  }

  private String createRollbackTimestamp(String timestamp) {
    return timestamp + getRollbackOperationSuffix();
  }

  private String createCleanTimestamp(String timestamp) {
    return timestamp + getCleanOperationSuffix();
  }

  private String createRestoreTimestamp(String timestamp) {
    return timestamp + getRestoreOperationSuffix();
  }

  private String getCompactionOperationSuffix() {
    return "001";
  }

  private String getLogCompactionOperationSuffix() {
    return "005";
  }

  private String getRollbackOperationSuffix() {
    return "006";
  }

  private String getCleanOperationSuffix() {
    return "002";
  }

  private String getRestoreOperationSuffix() {
    return "003";
  }
}