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

package org.apache.hudi.table;

import org.apache.hudi.WriteStatus;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.SyncableFileSystemView;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCompactionException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.func.MergeOnReadLazyInsertIterable;
import org.apache.hudi.io.HoodieAppendHandle;
import org.apache.hudi.io.compact.HoodieRealtimeTableCompactor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Implementation of a more real-time read-optimized Hoodie Table where
 * <p>
 * INSERTS - Same as HoodieCopyOnWriteTable - Produce new files, block aligned to desired size (or) Merge with the
 * smallest existing file, to expand it
 * </p>
 * <p>
 * UPDATES - Appends the changes to a rolling log file maintained per file Id. Compaction merges the log file into the
 * base file.
 * </p>
 * <p>
 * WARNING - MOR table type does not support nested rollbacks, every rollback must be followed by an attempted commit
 * action
 * </p>
 */
public class HoodieMergeOnReadTable<T extends HoodieRecordPayload> extends HoodieCopyOnWriteTable<T> {

  private static Logger logger = LogManager.getLogger(HoodieMergeOnReadTable.class);

  // UpsertPartitioner for MergeOnRead table type
  private MergeOnReadUpsertPartitioner mergeOnReadUpsertPartitioner;

  public HoodieMergeOnReadTable(HoodieWriteConfig config, JavaSparkContext jsc) {
    super(config, jsc);
  }

  @Override
  public Partitioner getUpsertPartitioner(WorkloadProfile profile) {
    if (profile == null) {
      throw new HoodieUpsertException("Need workload profile to construct the upsert partitioner.");
    }
    mergeOnReadUpsertPartitioner = new MergeOnReadUpsertPartitioner(profile);
    return mergeOnReadUpsertPartitioner;
  }

  @Override
  public Iterator<List<WriteStatus>> handleUpdate(String commitTime, String fileId, Iterator<HoodieRecord<T>> recordItr)
      throws IOException {
    logger.info("Merging updates for commit " + commitTime + " for file " + fileId);

    if (!index.canIndexLogFiles() && mergeOnReadUpsertPartitioner.getSmallFileIds().contains(fileId)) {
      logger.info("Small file corrections for updates for commit " + commitTime + " for file " + fileId);
      return super.handleUpdate(commitTime, fileId, recordItr);
    } else {
      HoodieAppendHandle<T> appendHandle = new HoodieAppendHandle<>(config, commitTime, this, fileId, recordItr);
      appendHandle.doAppend();
      appendHandle.close();
      return Collections.singletonList(Collections.singletonList(appendHandle.getWriteStatus())).iterator();
    }
  }

  @Override
  public Iterator<List<WriteStatus>> handleInsert(String commitTime, String idPfx, Iterator<HoodieRecord<T>> recordItr)
      throws Exception {
    // If canIndexLogFiles, write inserts to log files else write inserts to parquet files
    if (index.canIndexLogFiles()) {
      return new MergeOnReadLazyInsertIterable<>(recordItr, config, commitTime, this, idPfx);
    } else {
      return super.handleInsert(commitTime, idPfx, recordItr);
    }
  }

  @Override
  public HoodieCompactionPlan scheduleCompaction(JavaSparkContext jsc, String instantTime) {
    logger.info("Checking if compaction needs to be run on " + config.getBasePath());
    Option<HoodieInstant> lastCompaction =
        getActiveTimeline().getCommitTimeline().filterCompletedInstants().lastInstant();
    String deltaCommitsSinceTs = "0";
    if (lastCompaction.isPresent()) {
      deltaCommitsSinceTs = lastCompaction.get().getTimestamp();
    }

    int deltaCommitsSinceLastCompaction = getActiveTimeline().getDeltaCommitTimeline()
        .findInstantsAfter(deltaCommitsSinceTs, Integer.MAX_VALUE).countInstants();
    if (config.getInlineCompactDeltaCommitMax() > deltaCommitsSinceLastCompaction) {
      logger.info("Not running compaction as only " + deltaCommitsSinceLastCompaction
          + " delta commits was found since last compaction " + deltaCommitsSinceTs + ". Waiting for "
          + config.getInlineCompactDeltaCommitMax());
      return new HoodieCompactionPlan();
    }

    logger.info("Compacting merge on read table " + config.getBasePath());
    HoodieRealtimeTableCompactor compactor = new HoodieRealtimeTableCompactor();
    try {
      return compactor.generateCompactionPlan(jsc, this, config, instantTime,
          ((SyncableFileSystemView) getRTFileSystemView()).getPendingCompactionOperations()
              .map(instantTimeCompactionopPair -> instantTimeCompactionopPair.getValue().getFileGroupId())
              .collect(Collectors.toSet()));

    } catch (IOException e) {
      throw new HoodieCompactionException("Could not schedule compaction " + config.getBasePath(), e);
    }
  }

  @Override
  public JavaRDD<WriteStatus> compact(JavaSparkContext jsc, String compactionInstantTime,
      HoodieCompactionPlan compactionPlan) {
    HoodieRealtimeTableCompactor compactor = new HoodieRealtimeTableCompactor();
    try {
      return compactor.compact(jsc, compactionPlan, this, config, compactionInstantTime);
    } catch (IOException e) {
      throw new HoodieCompactionException("Could not compact " + config.getBasePath(), e);
    }
  }

  @Override
  public List<HoodieRollbackStat> rollback(JavaSparkContext jsc, String commit, boolean deleteInstants)
      throws IOException {
    // At the moment, MOR table type does not support bulk nested rollbacks. Nested rollbacks is an experimental
    // feature that is expensive. To perform nested rollbacks, initiate multiple requests of client.rollback
    // (commitToRollback).
    // NOTE {@link HoodieCompactionConfig#withCompactionLazyBlockReadEnabled} needs to be set to TRUE. This is
    // required to avoid OOM when merging multiple LogBlocks performed during nested rollbacks.
    // Atomically un-publish all non-inflight commits
    Option<HoodieInstant> commitOrCompactionOption = Option.fromJavaOptional(this.getActiveTimeline()
        .getTimelineOfActions(Sets.newHashSet(HoodieActiveTimeline.COMMIT_ACTION,
            HoodieActiveTimeline.DELTA_COMMIT_ACTION, HoodieActiveTimeline.COMPACTION_ACTION))
        .getInstants().filter(i -> commit.equals(i.getTimestamp())).findFirst());
    HoodieInstant instantToRollback = commitOrCompactionOption.get();
    // Atomically un-publish all non-inflight commits
    if (!instantToRollback.isInflight()) {
      this.getActiveTimeline().revertToInflight(instantToRollback);
    }
    logger.info("Unpublished " + commit);
    Long startTime = System.currentTimeMillis();
    List<RollbackRequest> rollbackRequests = generateRollbackRequests(jsc, instantToRollback);
    // TODO: We need to persist this as rollback workload and use it in case of partial failures
    List<HoodieRollbackStat> allRollbackStats =
        new RollbackExecutor(metaClient, config).performRollback(jsc, instantToRollback, rollbackRequests);
    // Delete Inflight instants if enabled
    deleteInflightInstant(deleteInstants, this.getActiveTimeline(),
        new HoodieInstant(true, instantToRollback.getAction(), instantToRollback.getTimestamp()));

    logger.info("Time(in ms) taken to finish rollback " + (System.currentTimeMillis() - startTime));

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
    List<String> partitions = FSUtils.getAllPartitionPaths(this.metaClient.getFs(), this.getMetaClient().getBasePath(),
        config.shouldAssumeDatePartitioning());
    int sparkPartitions = Math.max(Math.min(partitions.size(), config.getRollbackParallelism()), 1);
    return jsc.parallelize(partitions, Math.min(partitions.size(), sparkPartitions)).flatMap(partitionPath -> {
      HoodieActiveTimeline activeTimeline = this.getActiveTimeline().reload();
      List<RollbackRequest> partitionRollbackRequests = new ArrayList<>();
      switch (instantToRollback.getAction()) {
        case HoodieTimeline.COMMIT_ACTION:
          logger.info(
              "Rolling back commit action. There are higher delta commits. So only rolling back this " + "instant");
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
            logger.info("Rolling back compaction. There are higher delta commits. So only deleting data files");
            partitionRollbackRequests.add(
                RollbackRequest.createRollbackRequestWithDeleteDataFilesOnlyAction(partitionPath, instantToRollback));
          } else {
            // No deltacommits present after this compaction commit (inflight or requested). In this case, we
            // can also delete any log files that were created with this compaction commit as base
            // commit.
            logger.info("Rolling back compaction plan. There are NO higher delta commits. So deleting both data and"
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
                metaClient.getCommitTimeline()
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

  @Override
  public void finalizeWrite(JavaSparkContext jsc, String instantTs, List<HoodieWriteStat> stats)
      throws HoodieIOException {
    // delegate to base class for MOR tables
    super.finalizeWrite(jsc, instantTs, stats);
  }

  /**
   * UpsertPartitioner for MergeOnRead table type, this allows auto correction of small parquet files to larger ones
   * without the need for an index in the logFile.
   */
  class MergeOnReadUpsertPartitioner extends HoodieCopyOnWriteTable.UpsertPartitioner {

    MergeOnReadUpsertPartitioner(WorkloadProfile profile) {
      super(profile);
    }

    @Override
    protected List<SmallFile> getSmallFiles(String partitionPath) {

      // smallFiles only for partitionPath
      List<SmallFile> smallFileLocations = new ArrayList<>();

      // Init here since this class (and member variables) might not have been initialized
      HoodieTimeline commitTimeline = getCompletedCommitsTimeline();

      // Find out all eligible small file slices
      if (!commitTimeline.empty()) {
        HoodieInstant latestCommitTime = commitTimeline.lastInstant().get();
        // find smallest file in partition and append to it
        List<FileSlice> allSmallFileSlices = new ArrayList<>();
        // If we cannot index log files, then we choose the smallest parquet file in the partition and add inserts to
        // it. Doing this overtime for a partition, we ensure that we handle small file issues
        if (!index.canIndexLogFiles()) {
          // TODO : choose last N small files since there can be multiple small files written to a single partition
          // by different spark partitions in a single batch
          Option<FileSlice> smallFileSlice = Option.fromJavaOptional(getRTFileSystemView()
              .getLatestFileSlicesBeforeOrOn(partitionPath, latestCommitTime.getTimestamp(), false)
              .filter(fileSlice -> fileSlice.getLogFiles().count() < 1
                  && fileSlice.getDataFile().get().getFileSize() < config.getParquetSmallFileLimit())
              .sorted((FileSlice left,
                  FileSlice right) -> left.getDataFile().get().getFileSize() < right.getDataFile().get().getFileSize()
                      ? -1
                      : 1)
              .findFirst());
          if (smallFileSlice.isPresent()) {
            allSmallFileSlices.add(smallFileSlice.get());
          }
        } else {
          // If we can index log files, we can add more inserts to log files for fileIds including those under
          // pending compaction.
          List<FileSlice> allFileSlices =
              getRTFileSystemView().getLatestFileSlicesBeforeOrOn(partitionPath, latestCommitTime.getTimestamp(), true)
                  .collect(Collectors.toList());
          for (FileSlice fileSlice : allFileSlices) {
            if (isSmallFile(partitionPath, fileSlice)) {
              allSmallFileSlices.add(fileSlice);
            }
          }
        }
        // Create SmallFiles from the eligible file slices
        for (FileSlice smallFileSlice : allSmallFileSlices) {
          SmallFile sf = new SmallFile();
          if (smallFileSlice.getDataFile().isPresent()) {
            // TODO : Move logic of file name, file id, base commit time handling inside file slice
            String filename = smallFileSlice.getDataFile().get().getFileName();
            sf.location = new HoodieRecordLocation(FSUtils.getCommitTime(filename), FSUtils.getFileId(filename));
            sf.sizeBytes = getTotalFileSize(partitionPath, smallFileSlice);
            smallFileLocations.add(sf);
            // Update the global small files list
            smallFiles.add(sf);
          } else {
            HoodieLogFile logFile = smallFileSlice.getLogFiles().findFirst().get();
            sf.location = new HoodieRecordLocation(FSUtils.getBaseCommitTimeFromLogPath(logFile.getPath()),
                FSUtils.getFileIdFromLogPath(logFile.getPath()));
            sf.sizeBytes = getTotalFileSize(partitionPath, smallFileSlice);
            smallFileLocations.add(sf);
            // Update the global small files list
            smallFiles.add(sf);
          }
        }
      }
      return smallFileLocations;
    }

    public List<String> getSmallFileIds() {
      return (List<String>) smallFiles.stream().map(smallFile -> ((SmallFile) smallFile).location.getFileId())
          .collect(Collectors.toList());
    }

    private long getTotalFileSize(String partitionPath, FileSlice fileSlice) {
      if (!fileSlice.getDataFile().isPresent()) {
        return convertLogFilesSizeToExpectedParquetSize(fileSlice.getLogFiles().collect(Collectors.toList()));
      } else {
        return fileSlice.getDataFile().get().getFileSize()
            + convertLogFilesSizeToExpectedParquetSize(fileSlice.getLogFiles().collect(Collectors.toList()));
      }
    }

    private boolean isSmallFile(String partitionPath, FileSlice fileSlice) {
      long totalSize = getTotalFileSize(partitionPath, fileSlice);
      return totalSize < config.getParquetMaxFileSize();
    }

    // TODO (NA) : Make this static part of utility
    @VisibleForTesting
    public long convertLogFilesSizeToExpectedParquetSize(List<HoodieLogFile> hoodieLogFiles) {
      long totalSizeOfLogFiles = hoodieLogFiles.stream().map(hoodieLogFile -> hoodieLogFile.getFileSize())
          .filter(size -> size > 0).reduce((a, b) -> (a + b)).orElse(0L);
      // Here we assume that if there is no base parquet file, all log files contain only inserts.
      // We can then just get the parquet equivalent size of these log files, compare that with
      // {@link config.getParquetMaxFileSize()} and decide if there is scope to insert more rows
      long logFilesEquivalentParquetFileSize =
          (long) (totalSizeOfLogFiles * config.getLogFileToParquetCompressionRatio());
      return logFilesEquivalentParquetFileSize;
    }
  }

  private List<RollbackRequest> generateAppendRollbackBlocksAction(String partitionPath, HoodieInstant rollbackInstant,
      HoodieCommitMetadata commitMetadata) {
    Preconditions.checkArgument(rollbackInstant.getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION));

    // wStat.getPrevCommit() might not give the right commit time in the following
    // scenario : If a compaction was scheduled, the new commitTime associated with the requested compaction will be
    // used to write the new log files. In this case, the commit time for the log file is the compaction requested time.
    // But the index (global) might store the baseCommit of the parquet and not the requested, hence get the
    // baseCommit always by listing the file slice
    Map<String, String> fileIdToBaseCommitTimeForLogMap = this.getRTFileSystemView().getLatestFileSlices(partitionPath)
        .collect(Collectors.toMap(FileSlice::getFileId, FileSlice::getBaseInstantTime));
    return commitMetadata.getPartitionToWriteStats().get(partitionPath).stream().filter(wStat -> {

      // Filter out stats without prevCommit since they are all inserts
      boolean validForRollback = (wStat != null) && (wStat.getPrevCommit() != HoodieWriteStat.NULL_COMMIT)
          && (wStat.getPrevCommit() != null) && fileIdToBaseCommitTimeForLogMap.containsKey(wStat.getFileId());

      if (validForRollback) {
        // For sanity, log instant time can never be less than base-commit on which we are rolling back
        Preconditions
            .checkArgument(HoodieTimeline.compareTimestamps(fileIdToBaseCommitTimeForLogMap.get(wStat.getFileId()),
                rollbackInstant.getTimestamp(), HoodieTimeline.LESSER_OR_EQUAL));
      }

      return validForRollback && HoodieTimeline.compareTimestamps(fileIdToBaseCommitTimeForLogMap.get(
          // Base Ts should be strictly less. If equal (for inserts-to-logs), the caller employs another option
          // to delete and we should not step on it
          wStat.getFileId()), rollbackInstant.getTimestamp(), HoodieTimeline.LESSER);
    }).map(wStat -> {
      String baseCommitTime = fileIdToBaseCommitTimeForLogMap.get(wStat.getFileId());
      return RollbackRequest.createRollbackRequestWithAppendRollbackBlockAction(partitionPath, wStat.getFileId(),
          baseCommitTime, rollbackInstant);
    }).collect(Collectors.toList());
  }
}
