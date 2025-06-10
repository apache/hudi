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

package org.apache.hudi.table.action.rollback;

import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.client.heartbeat.HoodieHeartbeatClient;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.bootstrap.index.BootstrapIndex;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.PartitionBucketIndexHashingConfig;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.InstantComparison.EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;

public abstract class BaseRollbackActionExecutor<T, I, K, O> extends BaseActionExecutor<T, I, K, O, HoodieRollbackMetadata> {

  private static final Logger LOG = LoggerFactory.getLogger(BaseRollbackActionExecutor.class);

  protected final HoodieInstant instantToRollback;
  protected final boolean deleteInstants;
  protected final boolean skipTimelinePublish;
  private final TransactionManager txnManager;
  private final boolean skipLocking;

  protected HoodieInstant resolvedInstant;

  public BaseRollbackActionExecutor(HoodieEngineContext context,
                                    HoodieWriteConfig config,
                                    HoodieTable<T, I, K, O> table,
                                    String instantTime,
                                    HoodieInstant instantToRollback,
                                    boolean deleteInstants,
                                    boolean skipLocking,
                                    boolean isRestore) {
    this(context, config, table, instantTime, instantToRollback, deleteInstants, false, skipLocking, isRestore);
  }

  public BaseRollbackActionExecutor(HoodieEngineContext context,
      HoodieWriteConfig config,
      HoodieTable<T, I, K, O> table,
      String instantTime,
      HoodieInstant instantToRollback,
      boolean deleteInstants,
      boolean skipTimelinePublish,
      boolean skipLocking, boolean isRestore) {
    super(context, config, table, instantTime);
    this.instantToRollback = instantToRollback;
    this.resolvedInstant = instantToRollback;
    this.deleteInstants = deleteInstants;
    this.skipTimelinePublish = skipTimelinePublish;
    this.skipLocking = skipLocking;
    this.txnManager = new TransactionManager(config, table.getStorage());
  }

  /**
   * Execute actual rollback and fetch list of RollbackStats.
   * @param hoodieRollbackPlan instance of {@link HoodieRollbackPlan} that needs to be executed.
   * @return a list of {@link HoodieRollbackStat}s.
   * @throws IOException
   */
  protected abstract List<HoodieRollbackStat> executeRollback(HoodieRollbackPlan hoodieRollbackPlan) throws IOException;

  private HoodieRollbackMetadata runRollback(HoodieTable<T, I, K, O> table, HoodieInstant rollbackInstant, HoodieRollbackPlan rollbackPlan) {
    ValidationUtils.checkArgument(rollbackInstant.getState().equals(HoodieInstant.State.REQUESTED)
        || rollbackInstant.getState().equals(HoodieInstant.State.INFLIGHT));
    final HoodieInstant inflightInstant = rollbackInstant.isRequested()
        ? table.getActiveTimeline().transitionRollbackRequestedToInflight(rollbackInstant)
        : rollbackInstant;

    HoodieTimer rollbackTimer = HoodieTimer.start();
    List<HoodieRollbackStat> stats = doRollbackAndGetStats(rollbackPlan);
    HoodieRollbackMetadata rollbackMetadata = TimelineMetadataUtils.convertRollbackMetadata(
        instantTime,
        Option.of(rollbackTimer.endTimer()),
        Collections.singletonList(instantToRollback),
        stats);
    finishRollback(inflightInstant, rollbackMetadata);

    // Finally, remove the markers post rollback.
    WriteMarkersFactory.get(config.getMarkersType(), table, instantToRollback.requestedTime())
        .quietDeleteMarkerDir(context, config.getMarkersDeleteParallelism());
    if (table.getMetaClient().getTableConfig().getTableVersion().lesserThan(HoodieTableVersion.EIGHT)
        && table.getMetaClient().getTableConfig().getTableType() == HoodieTableType.MERGE_ON_READ) {
      // For MOR table rollbacks in table version 6 and below, rollback command blocks might
      // generate markers under rollback instant. So, lets clean up the markers if any.
      WriteMarkersFactory.get(config.getMarkersType(), table, rollbackInstant.requestedTime())
          .quietDeleteMarkerDir(context, config.getMarkersDeleteParallelism());
    }

    return rollbackMetadata;
  }

  @Override
  public HoodieRollbackMetadata execute() {
    table.getMetaClient().reloadActiveTimeline();
    Option<HoodieInstant> rollbackInstant = table.getRollbackTimeline()
        .filterInflightsAndRequested()
        .filter(instant -> instant.requestedTime().equals(instantTime))
        .firstInstant();
    if (!rollbackInstant.isPresent()) {
      throw new HoodieRollbackException("No pending rollback instants found to execute rollback");
    }
    try {
      HoodieRollbackPlan rollbackPlan = RollbackUtils.getRollbackPlan(table.getMetaClient(), rollbackInstant.get());
      return runRollback(table, rollbackInstant.get(), rollbackPlan);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to fetch rollback plan for commit " + instantTime, e);
    }
  }

  private void validateSavepointRollbacks() {
    // Check if any of the commits is a savepoint - do not allow rollback on those commits
    List<String> savepoints = table.getCompletedSavepointTimeline().getInstantsAsStream()
        .map(HoodieInstant::requestedTime)
        .collect(Collectors.toList());
    savepoints.forEach(s -> {
      if (s.contains(instantToRollback.requestedTime())) {
        throw new HoodieRollbackException(
            "Could not rollback a savepointed commit. Delete savepoint first before rolling back" + s);
      }
    });
  }

  /**
   * Validate commit sequence for rollback commits.
   */
  private void validateRollbackCommitSequence() {
    // Continue to provide the same behavior if policy is EAGER (similar to pendingRollback logic). This is required
    // since with LAZY rollback we support parallel writing which can allow a new inflight while rollback is ongoing
    // Remove this once we support LAZY rollback of failed writes by default as parallel writing becomes the default
    // writer mode.
    if (config.getFailedWritesCleanPolicy().isEager()  && !HoodieTableMetadata.isMetadataTable(config.getBasePath())) {
      final String instantTimeToRollback = instantToRollback.requestedTime();
      HoodieTimeline commitTimeline = table.getCompletedCommitsTimeline();
      HoodieTimeline pendingCommitsTimeline = table.getPendingCommitsTimeline();
      // Check validity of completed commit timeline.
      // Make sure only the last n commits are being rolled back
      // If there is a commit in-between or after that is not rolled back, then abort
      // this condition may not hold good for metadata table. since the order of commits applied to MDT in data table commits and the ordering could be different.
      if ((instantTimeToRollback != null) && !commitTimeline.empty()
          && !commitTimeline.findInstantsAfter(instantTimeToRollback, Integer.MAX_VALUE).empty()) {
        // check if remnants are from a previous LAZY rollback config, if yes, let out of order rollback continue
        try {
          if (!HoodieHeartbeatClient.heartbeatExists(table.getStorage(),
              config.getBasePath(), instantTimeToRollback)) {
            throw new HoodieRollbackException(
                "Found commits after time :" + instantTimeToRollback + ", please rollback greater commits first");
          }
        } catch (IOException io) {
          throw new HoodieRollbackException("Unable to rollback commits ", io);
        }
      }

      List<String> inflights = pendingCommitsTimeline.getInstantsAsStream()
          .filter(instant -> !ClusteringUtils.isClusteringInstant(table.getActiveTimeline(), instant, instantGenerator))
          .map(HoodieInstant::requestedTime)
          .collect(Collectors.toList());
      if ((instantTimeToRollback != null) && !inflights.isEmpty()
          && (inflights.indexOf(instantTimeToRollback) != inflights.size() - 1)) {
        throw new HoodieRollbackException(
            "Found in-flight commits after time :" + instantTimeToRollback + ", please rollback greater commits first");
      }
    }
  }

  private void rollBackIndex() {
    if (!table.getIndex().rollbackCommit(instantToRollback.requestedTime())) {
      throw new HoodieRollbackException("Rollback index changes failed, for time :" + instantToRollback);
    }
    LOG.info("Index rolled back for commits " + instantToRollback);
  }

  public List<HoodieRollbackStat> doRollbackAndGetStats(HoodieRollbackPlan hoodieRollbackPlan) {
    final String instantTimeToRollback = instantToRollback.requestedTime();
    final boolean isPendingCompaction = Objects.equals(HoodieTimeline.COMPACTION_ACTION, instantToRollback.getAction())
        && !instantToRollback.isCompleted();

    final boolean isPendingClustering = !instantToRollback.isCompleted()
        && ClusteringUtils.isClusteringInstant(
            table.getMetaClient().getActiveTimeline(), instantToRollback, instantGenerator);
    validateSavepointRollbacks();
    if (!isPendingCompaction && !isPendingClustering) {
      validateRollbackCommitSequence();
    }

    backupRollbackInstantsIfNeeded();
    rollbackHashingConfigIfNecessary(instantToRollback);

    try {
      List<HoodieRollbackStat> stats = executeRollback(hoodieRollbackPlan);
      LOG.info("Rolled back inflight instant " + instantTimeToRollback);
      if (!isPendingCompaction) {
        rollBackIndex();
      }
      return stats;
    } catch (IOException e) {
      throw new HoodieIOException("Unable to execute rollback ", e);
    }
  }

  /**
   * try to delete hashing config during rollback if using partition level bucket index.
   * @param instantToRollback
   */
  private void rollbackHashingConfigIfNecessary(HoodieInstant instantToRollback) {
    if (StringUtils.nonEmpty(config.getBucketIndexPartitionExpression()) && instantToRollback.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION)) {
      PartitionBucketIndexHashingConfig.rollbackHashingConfig(instantToRollback, table.getMetaClient());
    }
  }

  /**
   * Execute rollback and fetch rollback stats.
   * @param instantToRollback instant to be rolled back.
   * @param rollbackPlan instance of {@link HoodieRollbackPlan} for which rollback needs to be executed.
   * @return list of {@link HoodieRollbackStat}s.
   */
  protected List<HoodieRollbackStat> executeRollback(HoodieInstant instantToRollback, HoodieRollbackPlan rollbackPlan) {
    return RollbackHelperFactory.getRollBackHelper(table, config).performRollback(context, instantTime, instantToRollback, rollbackPlan.getRollbackRequests());
  }

  protected void finishRollback(HoodieInstant inflightInstant, HoodieRollbackMetadata rollbackMetadata) throws HoodieIOException {
    boolean enableLocking = (!skipLocking && !skipTimelinePublish);
    try {
      if (enableLocking) {
        this.txnManager.beginStateChange(Option.of(inflightInstant), Option.empty());
      }

      // If publish the rollback to the timeline, we first write the rollback metadata to metadata table
      // Then transition the inflight rollback to completed state.
      if (!skipTimelinePublish) {
        writeTableMetadata(rollbackMetadata);
      }

      // Then we delete the inflight instant in the data table timeline if enabled
      deleteInflightAndRequestedInstant(deleteInstants, table.getActiveTimeline(), resolvedInstant);

      // If publish the rollback to the timeline, we finally transition the inflight rollback
      // to complete in the data table timeline
      if (!skipTimelinePublish) {
        // NOTE: no need to lock here, since !skipTimelinePublish is always true,
        // when skipLocking is false, txnManager above-mentioned should lock it.
        // when skipLocking is true, the caller should have already held the lock.
        table.getActiveTimeline().transitionRollbackInflightToComplete(false, inflightInstant, rollbackMetadata);
        LOG.info("Rollback of Commits " + rollbackMetadata.getCommitsRollback() + " is complete");
      }
    } finally {
      if (enableLocking) {
        this.txnManager.endStateChange(Option.of(inflightInstant));
      }
    }
  }

  /**
   * Delete Inflight instant if enabled.
   * @param deleteInstant Enable Deletion of Inflight instant
   * @param activeTimeline Hoodie active timeline
   * @param instantToBeDeleted Instant to be deleted
   */
  protected void deleteInflightAndRequestedInstant(boolean deleteInstant,
                                                   HoodieActiveTimeline activeTimeline,
                                                   HoodieInstant instantToBeDeleted) {
    // Remove the rolled back inflight commits
    if (deleteInstant) {
      LOG.info("Deleting instant=" + instantToBeDeleted);
      activeTimeline.deletePending(instantToBeDeleted);
      if (instantToBeDeleted.isInflight() && !table.getMetaClient().getTimelineLayoutVersion().isNullVersion()) {
        // Delete corresponding requested instant
        instantToBeDeleted = instantGenerator.createNewInstant(HoodieInstant.State.REQUESTED, instantToBeDeleted.getAction(),
            instantToBeDeleted.requestedTime());
        activeTimeline.deletePending(instantToBeDeleted);
      }
      LOG.info("Deleted pending commit " + instantToBeDeleted);
    } else {
      LOG.warn("Rollback finished without deleting inflight instant file. Instant=" + instantToBeDeleted);
    }
  }

  protected void dropBootstrapIndexIfNeeded(HoodieInstant instantToRollback) {
    if (compareTimestamps(instantToRollback.requestedTime(), EQUALS, HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS)) {
      LOG.info("Dropping bootstrap index as metadata bootstrap commit is getting rolled back !!");
      BootstrapIndex.getBootstrapIndex(table.getMetaClient()).dropIndex();
    }
  }

  private void backupRollbackInstantsIfNeeded() {
    if (!config.shouldBackupRollbacks()) {
      // Backup not required
      return;
    }
    StoragePath backupDir = new StoragePath(config.getRollbackBackupDirectory());
    if (!backupDir.isAbsolute()) {
      // Path specified is relative to the meta directory
      backupDir = new StoragePath(table.getMetaClient().getMetaPath(), config.getRollbackBackupDirectory());
    }

    // Determine the instants to back up
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    List<HoodieInstant> instantsToBackup = new ArrayList<>(3);
    instantsToBackup.add(instantToRollback);
    if (instantToRollback.isCompleted()) {
      instantsToBackup.add(TimelineUtils.getInflightInstant(instantToRollback, table.getMetaClient()));
      instantsToBackup.add(instantGenerator.getRequestedInstant(instantToRollback));
    }
    if (instantToRollback.isInflight()) {
      instantsToBackup.add(instantGenerator.getRequestedInstant(instantToRollback));
    }

    for (HoodieInstant instant : instantsToBackup) {
      try {
        activeTimeline.copyInstant(instant, backupDir);
        LOG.info("Copied instant {} to backup dir {} during rollback at {}", instant, backupDir, instantTime);
      } catch (HoodieIOException e) {
        // Ignoring error in backing up
        LOG.warn("Failed to backup rollback instant", e);
      }
    }
  }
}
