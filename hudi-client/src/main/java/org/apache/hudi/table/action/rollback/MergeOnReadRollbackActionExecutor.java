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

import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class MergeOnReadRollbackActionExecutor extends BaseRollbackActionExecutor {

  private static final Logger LOG = LogManager.getLogger(MergeOnReadRollbackActionExecutor.class);

  public MergeOnReadRollbackActionExecutor(JavaSparkContext jsc,
                                           HoodieWriteConfig config,
                                           HoodieTable<?> table,
                                           String instantTime,
                                           HoodieInstant commitInstant,
                                           boolean deleteInstants) {
    super(jsc, config, table, instantTime, commitInstant, deleteInstants);
  }

  public MergeOnReadRollbackActionExecutor(JavaSparkContext jsc,
                                           HoodieWriteConfig config,
                                           HoodieTable<?> table,
                                           String instantTime,
                                           HoodieInstant commitInstant,
                                           boolean deleteInstants,
                                           boolean skipTimelinePublish) {
    super(jsc, config, table, instantTime, commitInstant, deleteInstants, skipTimelinePublish);
  }

  @Override
  protected List<HoodieRollbackStat> executeRollback() throws IOException {
    long startTime = System.currentTimeMillis();
    LOG.info("Rolling back instant " + instantToRollback.getTimestamp());

    HoodieInstant resolvedInstant = instantToRollback;
    // Atomically un-publish all non-inflight commits
    if (instantToRollback.isCompleted()) {
      LOG.info("Un-publishing instant " + instantToRollback + ", deleteInstants=" + deleteInstants);
      resolvedInstant = table.getActiveTimeline().revertToInflight(instantToRollback);
      // reload meta-client to reflect latest timeline status
      table.getMetaClient().reloadActiveTimeline();
    }

    List<HoodieRollbackStat> allRollbackStats = new ArrayList<>();

    // At the moment, MOR table type does not support bulk nested rollbacks. Nested rollbacks is an experimental
    // feature that is expensive. To perform nested rollbacks, initiate multiple requests of client.rollback
    // (commitToRollback).
    // NOTE {@link HoodieCompactionConfig#withCompactionLazyBlockReadEnabled} needs to be set to TRUE. This is
    // required to avoid OOM when merging multiple LogBlocks performed during nested rollbacks.
    // Atomically un-publish all non-inflight commits
    // For Requested State (like failure during index lookup), there is nothing to do rollback other than
    // deleting the timeline file
    if (!resolvedInstant.isRequested()) {
      LOG.info("Un-published " + resolvedInstant);
      List<RollbackRequest> rollbackRequests = generateRollbackRequests(jsc, resolvedInstant);
      // TODO: We need to persist this as rollback workload and use it in case of partial failures
      allRollbackStats = new RollbackHelper(table.getMetaClient(), config).performRollback(jsc, resolvedInstant, rollbackRequests);
    }

    // Delete Inflight instants if enabled
    deleteInflightAndRequestedInstant(deleteInstants, table.getActiveTimeline(), resolvedInstant);

    LOG.info("Time(in ms) taken to finish rollback " + (System.currentTimeMillis() - startTime));

    return allRollbackStats;
  }

  /**
   * Generate all rollback requests that we need to perform for rolling back this action without actually performing
   * rolling back.
   *
   * @param jsc JavaSparkContext
   * @param instantToRollback Instant to Rollback
   * @return list of rollback requests
   * @throws IOException
   */
  private List<RollbackRequest> generateRollbackRequests(JavaSparkContext jsc, HoodieInstant instantToRollback)
      throws IOException {
    String commit = instantToRollback.getTimestamp();
    List<String> partitions = FSUtils.getAllPartitionPaths(table.getMetaClient().getFs(), table.getMetaClient().getBasePath(),
        config.shouldAssumeDatePartitioning());
    int sparkPartitions = Math.max(Math.min(partitions.size(), config.getRollbackParallelism()), 1);
    jsc.setJobGroup(this.getClass().getSimpleName(), "Generate all rollback requests");
    return jsc.parallelize(partitions, Math.min(partitions.size(), sparkPartitions)).flatMap(partitionPath -> {
      HoodieActiveTimeline activeTimeline = table.getMetaClient().reloadActiveTimeline();
      List<RollbackRequest> partitionRollbackRequests = new ArrayList<>();
      switch (instantToRollback.getAction()) {
        case HoodieTimeline.COMMIT_ACTION:
          LOG.info("Rolling back commit action. There are higher delta commits. So only rolling back this instant");
          partitionRollbackRequests.add(
              RollbackRequest.createRollbackRequestWithDeleteDataAndLogFilesAction(partitionPath, instantToRollback));
          break;
        case HoodieTimeline.COMPACTION_ACTION:
          // If there is no delta commit present after the current commit (if compaction), no action, else we
          // need to make sure that a compaction commit rollback also deletes any log files written as part of the
          // succeeding deltacommit.
          boolean higherDeltaCommits =
              !activeTimeline.getDeltaCommitTimeline().filterCompletedInstants().findInstantsAfter(commit, 1).empty();
          if (higherDeltaCommits) {
            // Rollback of a compaction action with no higher deltacommit means that the compaction is scheduled
            // and has not yet finished. In this scenario we should delete only the newly created parquet files
            // and not corresponding base commit log files created with this as baseCommit since updates would
            // have been written to the log files.
            LOG.info("Rolling back compaction. There are higher delta commits. So only deleting data files");
            partitionRollbackRequests.add(
                RollbackRequest.createRollbackRequestWithDeleteDataFilesOnlyAction(partitionPath, instantToRollback));
          } else {
            // No deltacommits present after this compaction commit (inflight or requested). In this case, we
            // can also delete any log files that were created with this compaction commit as base
            // commit.
            LOG.info("Rolling back compaction plan. There are NO higher delta commits. So deleting both data and"
                + " log files");
            partitionRollbackRequests.add(
                RollbackRequest.createRollbackRequestWithDeleteDataAndLogFilesAction(partitionPath, instantToRollback));
          }
          break;
        case HoodieTimeline.DELTA_COMMIT_ACTION:
          // --------------------------------------------------------------------------------------------------
          // (A) The following cases are possible if index.canIndexLogFiles and/or index.isGlobal
          // --------------------------------------------------------------------------------------------------
          // (A.1) Failed first commit - Inserts were written to log files and HoodieWriteStat has no entries. In
          // this scenario we would want to delete these log files.
          // (A.2) Failed recurring commit - Inserts/Updates written to log files. In this scenario,
          // HoodieWriteStat will have the baseCommitTime for the first log file written, add rollback blocks.
          // (A.3) Rollback triggered for first commit - Inserts were written to the log files but the commit is
          // being reverted. In this scenario, HoodieWriteStat will be `null` for the attribute prevCommitTime and
          // and hence will end up deleting these log files. This is done so there are no orphan log files
          // lying around.
          // (A.4) Rollback triggered for recurring commits - Inserts/Updates are being rolled back, the actions
          // taken in this scenario is a combination of (A.2) and (A.3)
          // ---------------------------------------------------------------------------------------------------
          // (B) The following cases are possible if !index.canIndexLogFiles and/or !index.isGlobal
          // ---------------------------------------------------------------------------------------------------
          // (B.1) Failed first commit - Inserts were written to parquet files and HoodieWriteStat has no entries.
          // In this scenario, we delete all the parquet files written for the failed commit.
          // (B.2) Failed recurring commits - Inserts were written to parquet files and updates to log files. In
          // this scenario, perform (A.1) and for updates written to log files, write rollback blocks.
          // (B.3) Rollback triggered for first commit - Same as (B.1)
          // (B.4) Rollback triggered for recurring commits - Same as (B.2) plus we need to delete the log files
          // as well if the base parquet file gets deleted.
          try {
            HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
                table.getMetaClient().getCommitTimeline()
                    .getInstantDetails(
                        new HoodieInstant(true, instantToRollback.getAction(), instantToRollback.getTimestamp()))
                    .get(),
                HoodieCommitMetadata.class);

            // In case all data was inserts and the commit failed, delete the file belonging to that commit
            // We do not know fileIds for inserts (first inserts are either log files or parquet files),
            // delete all files for the corresponding failed commit, if present (same as COW)
            partitionRollbackRequests.add(
                RollbackRequest.createRollbackRequestWithDeleteDataAndLogFilesAction(partitionPath, instantToRollback));

            // append rollback blocks for updates
            if (commitMetadata.getPartitionToWriteStats().containsKey(partitionPath)) {
              partitionRollbackRequests
                  .addAll(generateAppendRollbackBlocksAction(partitionPath, instantToRollback, commitMetadata));
            }
            break;
          } catch (IOException io) {
            throw new UncheckedIOException("Failed to collect rollback actions for commit " + commit, io);
          }
        default:
          break;
      }
      return partitionRollbackRequests.iterator();
    }).filter(Objects::nonNull).collect();
  }

  private List<RollbackRequest> generateAppendRollbackBlocksAction(String partitionPath, HoodieInstant rollbackInstant,
                                                                   HoodieCommitMetadata commitMetadata) {
    ValidationUtils.checkArgument(rollbackInstant.getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION));

    // wStat.getPrevCommit() might not give the right commit time in the following
    // scenario : If a compaction was scheduled, the new commitTime associated with the requested compaction will be
    // used to write the new log files. In this case, the commit time for the log file is the compaction requested time.
    // But the index (global) might store the baseCommit of the parquet and not the requested, hence get the
    // baseCommit always by listing the file slice
    Map<String, String> fileIdToBaseCommitTimeForLogMap = table.getSliceView().getLatestFileSlices(partitionPath)
        .collect(Collectors.toMap(FileSlice::getFileId, FileSlice::getBaseInstantTime));
    return commitMetadata.getPartitionToWriteStats().get(partitionPath).stream().filter(wStat -> {

      // Filter out stats without prevCommit since they are all inserts
      boolean validForRollback = (wStat != null) && (!wStat.getPrevCommit().equals(HoodieWriteStat.NULL_COMMIT))
          && (wStat.getPrevCommit() != null) && fileIdToBaseCommitTimeForLogMap.containsKey(wStat.getFileId());

      if (validForRollback) {
        // For sanity, log instant time can never be less than base-commit on which we are rolling back
        ValidationUtils
            .checkArgument(HoodieTimeline.compareTimestamps(fileIdToBaseCommitTimeForLogMap.get(wStat.getFileId()),
                HoodieTimeline.LESSER_THAN_OR_EQUALS, rollbackInstant.getTimestamp()));
      }

      return validForRollback && HoodieTimeline.compareTimestamps(fileIdToBaseCommitTimeForLogMap.get(
          // Base Ts should be strictly less. If equal (for inserts-to-logs), the caller employs another option
          // to delete and we should not step on it
          wStat.getFileId()), HoodieTimeline.LESSER_THAN, rollbackInstant.getTimestamp());
    }).map(wStat -> {
      String baseCommitTime = fileIdToBaseCommitTimeForLogMap.get(wStat.getFileId());
      return RollbackRequest.createRollbackRequestWithAppendRollbackBlockAction(partitionPath, wStat.getFileId(),
          baseCommitTime, rollbackInstant);
    }).collect(Collectors.toList());
  }
}
