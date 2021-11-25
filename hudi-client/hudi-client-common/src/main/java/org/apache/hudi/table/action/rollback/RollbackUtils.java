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

import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.fs.FileStatus;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class RollbackUtils {

  private static final Logger LOG = LogManager.getLogger(RollbackUtils.class);

  /**
   * Get Latest version of Rollback plan corresponding to a clean instant.
   *
   * @param metaClient      Hoodie Table Meta Client
   * @param rollbackInstant Instant referring to rollback action
   * @return Rollback plan corresponding to rollback instant
   * @throws IOException
   */
  public static HoodieRollbackPlan getRollbackPlan(HoodieTableMetaClient metaClient, HoodieInstant rollbackInstant)
      throws IOException {
    // TODO: add upgrade step if required.
    return TimelineMetadataUtils.deserializeAvroMetadata(
        metaClient.getActiveTimeline().readRollbackInfoAsBytes(rollbackInstant).get(), HoodieRollbackPlan.class);
  }

  static Map<HoodieLogBlock.HeaderMetadataType, String> generateHeader(String instantToRollback, String rollbackInstantTime) {
    // generate metadata
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>(3);
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, rollbackInstantTime);
    header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, instantToRollback);
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_PREVIOUS_BLOCK.ordinal()));
    return header;
  }

  /**
   * Helper to merge 2 rollback-stats for a given partition.
   *
   * @param stat1 HoodieRollbackStat
   * @param stat2 HoodieRollbackStat
   * @return Merged HoodieRollbackStat
   */
  static HoodieRollbackStat mergeRollbackStat(HoodieRollbackStat stat1, HoodieRollbackStat stat2) {
    ValidationUtils.checkArgument(stat1.getPartitionPath().equals(stat2.getPartitionPath()));
    final List<String> successDeleteFiles = new ArrayList<>();
    final List<String> failedDeleteFiles = new ArrayList<>();
    final Map<FileStatus, Long> commandBlocksCount = new HashMap<>();
    final Map<FileStatus, Long> writtenLogFileSizeMap = new HashMap<>();
    Option.ofNullable(stat1.getSuccessDeleteFiles()).ifPresent(successDeleteFiles::addAll);
    Option.ofNullable(stat2.getSuccessDeleteFiles()).ifPresent(successDeleteFiles::addAll);
    Option.ofNullable(stat1.getFailedDeleteFiles()).ifPresent(failedDeleteFiles::addAll);
    Option.ofNullable(stat2.getFailedDeleteFiles()).ifPresent(failedDeleteFiles::addAll);
    Option.ofNullable(stat1.getCommandBlocksCount()).ifPresent(commandBlocksCount::putAll);
    Option.ofNullable(stat2.getCommandBlocksCount()).ifPresent(commandBlocksCount::putAll);
    Option.ofNullable(stat1.getWrittenLogFileSizeMap()).ifPresent(writtenLogFileSizeMap::putAll);
    Option.ofNullable(stat2.getWrittenLogFileSizeMap()).ifPresent(writtenLogFileSizeMap::putAll);
    return new HoodieRollbackStat(stat1.getPartitionPath(), successDeleteFiles, failedDeleteFiles, commandBlocksCount, writtenLogFileSizeMap);
  }

  /**
   * Generate all rollback requests that needs rolling back this action without actually performing rollback for COW table type.
   * @param engineContext instance of {@link HoodieEngineContext} to use.
   * @param basePath base path of interest.
   * @return {@link List} of {@link ListingBasedRollbackRequest}s thus collected.
   */
  public static List<ListingBasedRollbackRequest> generateRollbackRequestsByListingCOW(HoodieEngineContext engineContext, String basePath) {
    return FSUtils.getAllPartitionPaths(engineContext, basePath, false, false).stream()
        .map(ListingBasedRollbackRequest::createRollbackRequestWithDeleteDataAndLogFilesAction)
        .collect(Collectors.toList());
  }

  /**
   * Generate all rollback requests that we need to perform for rolling back this action without actually performing rolling back for MOR table type.
   *
   * @param instantToRollback Instant to Rollback
   * @param table instance of {@link HoodieTable} to use.
   * @param context instance of {@link HoodieEngineContext} to use.
   * @return list of rollback requests
   */
  public static List<ListingBasedRollbackRequest> generateRollbackRequestsUsingFileListingMOR(HoodieInstant instantToRollback, HoodieTable table, HoodieEngineContext context) throws IOException {
    String commit = instantToRollback.getTimestamp();
    HoodieWriteConfig config = table.getConfig();
    List<String> partitions = FSUtils.getAllPartitionPaths(context, table.getMetaClient().getBasePath(), false, false);
    if (partitions.isEmpty()) {
      return new ArrayList<>();
    }
    int sparkPartitions = Math.max(Math.min(partitions.size(), config.getRollbackParallelism()), 1);
    context.setJobStatus(RollbackUtils.class.getSimpleName(), "Generate all rollback requests");
    return context.flatMap(partitions, partitionPath -> {
      HoodieActiveTimeline activeTimeline = table.getMetaClient().reloadActiveTimeline();
      List<ListingBasedRollbackRequest> partitionRollbackRequests = new ArrayList<>();
      switch (instantToRollback.getAction()) {
        case HoodieTimeline.COMMIT_ACTION:
        case HoodieTimeline.REPLACE_COMMIT_ACTION:
          LOG.info("Rolling back commit action.");
          partitionRollbackRequests.add(
              ListingBasedRollbackRequest.createRollbackRequestWithDeleteDataAndLogFilesAction(partitionPath));
          break;
        case HoodieTimeline.COMPACTION_ACTION:
          // If there is no delta commit present after the current commit (if compaction), no action, else we
          // need to make sure that a compaction commit rollback also deletes any log files written as part of the
          // succeeding deltacommit.
          boolean higherDeltaCommits =
              !activeTimeline.getDeltaCommitTimeline().filterCompletedInstants().findInstantsAfter(commit, 1).empty();
          if (higherDeltaCommits) {
            // Rollback of a compaction action with no higher deltacommit means that the compaction is scheduled
            // and has not yet finished. In this scenario we should delete only the newly created base files
            // and not corresponding base commit log files created with this as baseCommit since updates would
            // have been written to the log files.
            LOG.info("Rolling back compaction. There are higher delta commits. So only deleting data files");
            partitionRollbackRequests.add(
                ListingBasedRollbackRequest.createRollbackRequestWithDeleteDataFilesOnlyAction(partitionPath));
          } else {
            // No deltacommits present after this compaction commit (inflight or requested). In this case, we
            // can also delete any log files that were created with this compaction commit as base
            // commit.
            LOG.info("Rolling back compaction plan. There are NO higher delta commits. So deleting both data and"
                + " log files");
            partitionRollbackRequests.add(
                ListingBasedRollbackRequest.createRollbackRequestWithDeleteDataAndLogFilesAction(partitionPath));
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
          // (B.1) Failed first commit - Inserts were written to base files and HoodieWriteStat has no entries.
          // In this scenario, we delete all the base files written for the failed commit.
          // (B.2) Failed recurring commits - Inserts were written to base files and updates to log files. In
          // this scenario, perform (A.1) and for updates written to log files, write rollback blocks.
          // (B.3) Rollback triggered for first commit - Same as (B.1)
          // (B.4) Rollback triggered for recurring commits - Same as (B.2) plus we need to delete the log files
          // as well if the base base file gets deleted.
          try {
            HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
                table.getMetaClient().getCommitTimeline()
                    .getInstantDetails(new HoodieInstant(true, instantToRollback.getAction(), instantToRollback.getTimestamp()))
                    .get(),
                HoodieCommitMetadata.class);

            // In case all data was inserts and the commit failed, delete the file belonging to that commit
            // We do not know fileIds for inserts (first inserts are either log files or base files),
            // delete all files for the corresponding failed commit, if present (same as COW)
            partitionRollbackRequests.add(
                ListingBasedRollbackRequest.createRollbackRequestWithDeleteDataAndLogFilesAction(partitionPath));

            // append rollback blocks for updates
            if (commitMetadata.getPartitionToWriteStats().containsKey(partitionPath)) {
              partitionRollbackRequests
                  .addAll(generateAppendRollbackBlocksAction(partitionPath, instantToRollback, commitMetadata, table));
            }
            break;
          } catch (IOException io) {
            throw new HoodieIOException("Failed to collect rollback actions for commit " + commit, io);
          }
        default:
          break;
      }
      return partitionRollbackRequests.stream();
    }, Math.min(partitions.size(), sparkPartitions)).stream().filter(Objects::nonNull).collect(Collectors.toList());
  }

  private static List<ListingBasedRollbackRequest> generateAppendRollbackBlocksAction(String partitionPath, HoodieInstant rollbackInstant,
      HoodieCommitMetadata commitMetadata, HoodieTable table) {
    ValidationUtils.checkArgument(rollbackInstant.getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION));

    // wStat.getPrevCommit() might not give the right commit time in the following
    // scenario : If a compaction was scheduled, the new commitTime associated with the requested compaction will be
    // used to write the new log files. In this case, the commit time for the log file is the compaction requested time.
    // But the index (global) might store the baseCommit of the base and not the requested, hence get the
    // baseCommit always by listing the file slice
    // With multi writers, rollbacks could be lazy. and so we need to use getLatestFileSlicesBeforeOrOn() instead of getLatestFileSlices()
    Map<String, String> fileIdToBaseCommitTimeForLogMap = table.getSliceView().getLatestFileSlicesBeforeOrOn(partitionPath, rollbackInstant.getTimestamp(),
        true).collect(Collectors.toMap(FileSlice::getFileId, FileSlice::getBaseInstantTime));

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
      return ListingBasedRollbackRequest.createRollbackRequestWithAppendRollbackBlockAction(partitionPath, wStat.getFileId(),
          baseCommitTime);
    }).collect(Collectors.toList());
  }
}
