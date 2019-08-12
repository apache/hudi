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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
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
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Writer;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock.HoodieCommandBlockTypeEnum;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCompactionException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.func.MergeOnReadLazyInsertIterable;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.io.HoodieAppendHandle;
import org.apache.hudi.io.compact.HoodieRealtimeTableCompactor;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Implementation of a more real-time read-optimized Hoodie Table where <p> INSERTS - Same as
 * HoodieCopyOnWriteTable - Produce new files, block aligned to desired size (or) Merge with the
 * smallest existing file, to expand it </p> <p> UPDATES - Appends the changes to a rolling log file
 * maintained per file Id. Compaction merges the log file into the base file. </p> <p> WARNING - MOR
 * table type does not support nested rollbacks, every rollback must be followed by an attempted
 * commit action </p>
 */
public class HoodieMergeOnReadTable<T extends HoodieRecordPayload> extends
    HoodieCopyOnWriteTable<T> {

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
  public Iterator<List<WriteStatus>> handleUpdate(String commitTime, String fileId,
      Iterator<HoodieRecord<T>> recordItr) throws IOException {
    logger.info("Merging updates for commit " + commitTime + " for file " + fileId);

    if (!index.canIndexLogFiles() && mergeOnReadUpsertPartitioner.getSmallFileIds().contains(fileId)) {
      logger.info(
          "Small file corrections for updates for commit " + commitTime + " for file " + fileId);
      return super.handleUpdate(commitTime, fileId, recordItr);
    } else {
      HoodieAppendHandle<T> appendHandle = new HoodieAppendHandle<>(config, commitTime, this,
          fileId, recordItr);
      appendHandle.doAppend();
      appendHandle.close();
      return Collections.singletonList(Collections.singletonList(appendHandle.getWriteStatus()))
          .iterator();
    }
  }

  @Override
  public Iterator<List<WriteStatus>> handleInsert(String commitTime, String idPfx,
      Iterator<HoodieRecord<T>> recordItr) throws Exception {
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
    Option<HoodieInstant> lastCompaction = getActiveTimeline().getCommitTimeline()
        .filterCompletedInstants().lastInstant();
    String deltaCommitsSinceTs = "0";
    if (lastCompaction.isPresent()) {
      deltaCommitsSinceTs = lastCompaction.get().getTimestamp();
    }

    int deltaCommitsSinceLastCompaction = getActiveTimeline().getDeltaCommitTimeline()
        .findInstantsAfter(deltaCommitsSinceTs, Integer.MAX_VALUE).countInstants();
    if (config.getInlineCompactDeltaCommitMax() > deltaCommitsSinceLastCompaction) {
      logger.info("Not running compaction as only " + deltaCommitsSinceLastCompaction
          + " delta commits was found since last compaction " + deltaCommitsSinceTs
          + ". Waiting for " + config.getInlineCompactDeltaCommitMax());
      return new HoodieCompactionPlan();
    }

    logger.info("Compacting merge on read table " + config.getBasePath());
    HoodieRealtimeTableCompactor compactor = new HoodieRealtimeTableCompactor();
    try {
      return compactor.generateCompactionPlan(jsc, this, config, instantTime,
          ((SyncableFileSystemView)getRTFileSystemView()).getPendingCompactionOperations()
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
            HoodieActiveTimeline.DELTA_COMMIT_ACTION, HoodieActiveTimeline.COMPACTION_ACTION)).getInstants()
        .filter(i -> commit.equals(i.getTimestamp()))
        .findFirst());
    HoodieInstant instantToRollback = commitOrCompactionOption.get();
    // Atomically un-publish all non-inflight commits
    if (!instantToRollback.isInflight()) {
      this.getActiveTimeline().revertToInflight(instantToRollback);
    }
    logger.info("Unpublished " + commit);
    Long startTime = System.currentTimeMillis();
    List<HoodieRollbackStat> allRollbackStats = jsc.parallelize(FSUtils
        .getAllPartitionPaths(this.metaClient.getFs(), this.getMetaClient().getBasePath(),
            config.shouldAssumeDatePartitioning()))
        .map((Function<String, HoodieRollbackStat>) partitionPath -> {
          HoodieActiveTimeline activeTimeline = this.getActiveTimeline().reload();
          HoodieRollbackStat hoodieRollbackStats = null;
          // Need to put the path filter here since Filter is not serializable
          // PathFilter to get all parquet files and log files that need to be deleted
          PathFilter filter = (path) -> {
            if (path.toString().contains(".parquet")) {
              String fileCommitTime = FSUtils.getCommitTime(path.getName());
              return commit.equals(fileCommitTime);
            } else if (path.toString().contains(".log")) {
              // Since the baseCommitTime is the only commit for new log files, it's okay here
              String fileCommitTime = FSUtils.getBaseCommitTimeFromLogPath(path);
              return commit.equals(fileCommitTime);
            }
            return false;
          };

          final Map<FileStatus, Boolean> filesToDeletedStatus = new HashMap<>();

          switch (instantToRollback.getAction()) {
            case HoodieTimeline.COMMIT_ACTION:
              try {
                // Rollback of a commit should delete the newly created parquet files along with any log
                // files created with this as baseCommit. This is required to support multi-rollbacks in a MOR table.
                super.deleteCleanedFiles(filesToDeletedStatus, partitionPath, filter);
                hoodieRollbackStats = HoodieRollbackStat.newBuilder()
                    .withPartitionPath(partitionPath).withDeletedFileResults(filesToDeletedStatus).build();
                break;
              } catch (IOException io) {
                throw new UncheckedIOException("Failed to rollback for commit " + commit, io);
              }
            case HoodieTimeline.COMPACTION_ACTION:
              try {
                // If there is no delta commit present after the current commit (if compaction), no action, else we
                // need to make sure that a compaction commit rollback also deletes any log files written as part of the
                // succeeding deltacommit.
                boolean higherDeltaCommits = !activeTimeline.getDeltaCommitTimeline()
                    .filterCompletedInstants().findInstantsAfter(commit, 1).empty();
                if (higherDeltaCommits) {
                  // Rollback of a compaction action with no higher deltacommit means that the compaction is scheduled
                  // and has not yet finished. In this scenario we should delete only the newly created parquet files
                  // and not corresponding base commit log files created with this as baseCommit since updates would
                  // have been written to the log files.
                  super.deleteCleanedFiles(filesToDeletedStatus, commit, partitionPath);
                  hoodieRollbackStats = HoodieRollbackStat.newBuilder()
                      .withPartitionPath(partitionPath).withDeletedFileResults(filesToDeletedStatus).build();
                } else {
                  // No deltacommits present after this compaction commit (inflight or requested). In this case, we
                  // can also delete any log files that were created with this compaction commit as base
                  // commit.
                  super.deleteCleanedFiles(filesToDeletedStatus, partitionPath, filter);
                  hoodieRollbackStats = HoodieRollbackStat.newBuilder()
                      .withPartitionPath(partitionPath).withDeletedFileResults(filesToDeletedStatus).build();
                }
                break;
              } catch (IOException io) {
                throw new UncheckedIOException("Failed to rollback for commit " + commit, io);
              }
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
                    metaClient.getCommitTimeline().getInstantDetails(
                        new HoodieInstant(true, instantToRollback.getAction(), instantToRollback.getTimestamp()))
                        .get(), HoodieCommitMetadata.class);

                // read commit file and (either append delete blocks or delete file)
                Map<FileStatus, Long> filesToNumBlocksRollback = new HashMap<>();

                // In case all data was inserts and the commit failed, delete the file belonging to that commit
                // We do not know fileIds for inserts (first inserts are either log files or parquet files),
                // delete all files for the corresponding failed commit, if present (same as COW)
                super.deleteCleanedFiles(filesToDeletedStatus, partitionPath, filter);
                final Set<String> deletedFiles = filesToDeletedStatus.entrySet().stream()
                    .map(entry -> {
                      Path filePath = entry.getKey().getPath();
                      return FSUtils.getFileIdFromFilePath(filePath);
                    }).collect(Collectors.toSet());

                // append rollback blocks for updates
                if (commitMetadata.getPartitionToWriteStats().containsKey(partitionPath)) {
                  hoodieRollbackStats = rollback(index, partitionPath, commit, commitMetadata, filesToDeletedStatus,
                      filesToNumBlocksRollback, deletedFiles);
                }
                break;
              } catch (IOException io) {
                throw new UncheckedIOException("Failed to rollback for commit " + commit, io);
              }
            default:
              break;
          }
          return hoodieRollbackStats;
        }).filter(Objects::nonNull).collect();

    // Delete Inflight instants if enabled
    deleteInflightInstant(deleteInstants, this.getActiveTimeline(), new HoodieInstant(true, instantToRollback
        .getAction(), instantToRollback.getTimestamp()));

    logger.debug("Time(in ms) taken to finish rollback " + (System.currentTimeMillis() - startTime));

    return allRollbackStats;
  }

  @Override
  public void finalizeWrite(JavaSparkContext jsc, String instantTs, List<HoodieWriteStat> stats)
      throws HoodieIOException {
    // delegate to base class for MOR tables
    super.finalizeWrite(jsc, instantTs, stats);
  }

  /**
   * UpsertPartitioner for MergeOnRead table type, this allows auto correction of small parquet
   * files to larger ones without the need for an index in the logFile.
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
              .getLatestFileSlicesBeforeOrOn(partitionPath, latestCommitTime.getTimestamp(), false).filter(
                  fileSlice -> fileSlice.getLogFiles().count() < 1
                      && fileSlice.getDataFile().get().getFileSize() < config
                      .getParquetSmallFileLimit()).sorted((FileSlice left, FileSlice right) ->
                  left.getDataFile().get().getFileSize() < right.getDataFile().get().getFileSize()
                      ? -1 : 1).findFirst());
          if (smallFileSlice.isPresent()) {
            allSmallFileSlices.add(smallFileSlice.get());
          }
        } else {
          // If we can index log files, we can add more inserts to log files for fileIds including those under
          // pending compaction.
          List<FileSlice> allFileSlices = getRTFileSystemView()
              .getLatestFileSlicesBeforeOrOn(partitionPath, latestCommitTime.getTimestamp(), true)
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
      return (List<String>) smallFiles.stream()
          .map(smallFile -> ((SmallFile) smallFile).location.getFileId())
          .collect(Collectors.toList());
    }

    private long getTotalFileSize(String partitionPath, FileSlice fileSlice) {
      if (!fileSlice.getDataFile().isPresent()) {
        return convertLogFilesSizeToExpectedParquetSize(fileSlice.getLogFiles().collect(Collectors.toList()));
      } else {
        return fileSlice.getDataFile().get().getFileSize() + convertLogFilesSizeToExpectedParquetSize(fileSlice
            .getLogFiles().collect(Collectors.toList()));
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
          .filter(size -> size > 0)
          .reduce((a, b) -> (a + b)).orElse(0L);
      // Here we assume that if there is no base parquet file, all log files contain only inserts.
      // We can then just get the parquet equivalent size of these log files, compare that with
      // {@link config.getParquetMaxFileSize()} and decide if there is scope to insert more rows
      long logFilesEquivalentParquetFileSize = (long) (totalSizeOfLogFiles * config
          .getLogFileToParquetCompressionRatio());
      return logFilesEquivalentParquetFileSize;
    }
  }

  private Map<HeaderMetadataType, String> generateHeader(String commit) {
    // generate metadata
    Map<HeaderMetadataType, String> header = Maps.newHashMap();
    header.put(HeaderMetadataType.INSTANT_TIME, metaClient.getActiveTimeline().lastInstant().get().getTimestamp());
    header.put(HeaderMetadataType.TARGET_INSTANT_TIME, commit);
    header.put(HeaderMetadataType.COMMAND_BLOCK_TYPE, String.valueOf(HoodieCommandBlockTypeEnum.ROLLBACK_PREVIOUS_BLOCK
        .ordinal()));
    return header;
  }

  private HoodieRollbackStat rollback(HoodieIndex hoodieIndex, String partitionPath, String commit,
      HoodieCommitMetadata commitMetadata, final Map<FileStatus, Boolean> filesToDeletedStatus,
      Map<FileStatus, Long> filesToNumBlocksRollback, Set<String> deletedFiles) {
    // wStat.getPrevCommit() might not give the right commit time in the following
    // scenario : If a compaction was scheduled, the new commitTime associated with the requested compaction will be
    // used to write the new log files. In this case, the commit time for the log file is the compaction requested time.
    // But the index (global) might store the baseCommit of the parquet and not the requested, hence get the
    // baseCommit always by listing the file slice
    Map<String, String> fileIdToBaseCommitTimeForLogMap = this.getRTFileSystemView().getLatestFileSlices(partitionPath)
            .collect(Collectors.toMap(FileSlice::getFileId, FileSlice::getBaseInstantTime));
    commitMetadata.getPartitionToWriteStats().get(partitionPath).stream()
        .filter(wStat -> {
          // Filter out stats without prevCommit since they are all inserts
          return wStat != null && wStat.getPrevCommit() != HoodieWriteStat.NULL_COMMIT && wStat.getPrevCommit() != null
              && !deletedFiles.contains(wStat.getFileId());
        }).forEach(wStat -> {
          Writer writer = null;
          String baseCommitTime = fileIdToBaseCommitTimeForLogMap.get(wStat.getFileId());
          if (null != baseCommitTime) {
            boolean success = false;
            try {
              writer = HoodieLogFormat.newWriterBuilder().onParentPath(
                  FSUtils.getPartitionPath(this.getMetaClient().getBasePath(), partitionPath))
                  .withFileId(wStat.getFileId()).overBaseCommit(baseCommitTime)
                  .withFs(this.metaClient.getFs())
                  .withFileExtension(HoodieLogFile.DELTA_EXTENSION).build();
              // generate metadata
              Map<HeaderMetadataType, String> header = generateHeader(commit);
              // if update belongs to an existing log file
              writer = writer.appendBlock(new HoodieCommandBlock(header));
              success = true;
            } catch (IOException | InterruptedException io) {
              throw new HoodieRollbackException(
                  "Failed to rollback for commit " + commit, io);
            } finally {
              try {
                if (writer != null) {
                  writer.close();
                }
                if (success) {
                  // This step is intentionally done after writer is closed. Guarantees that
                  // getFileStatus would reflect correct stats and FileNotFoundException is not thrown in
                  // cloud-storage : HUDI-168
                  filesToNumBlocksRollback.put(this.getMetaClient().getFs()
                      .getFileStatus(writer.getLogFile().getPath()), 1L);
                }
              } catch (IOException io) {
                throw new UncheckedIOException(io);
              }
            }
          }
        });
    return HoodieRollbackStat.newBuilder()
        .withPartitionPath(partitionPath)
        .withDeletedFileResults(filesToDeletedStatus)
        .withRollbackBlockAppendResults(filesToNumBlocksRollback).build();
  }

}
