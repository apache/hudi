/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.table;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.avro.model.HoodieCompactionPlan;
import com.uber.hoodie.common.HoodieRollbackStat;
import com.uber.hoodie.common.model.FileSlice;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordLocation;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.model.HoodieRollingStat;
import com.uber.hoodie.common.model.HoodieRollingStatMetadata;
import com.uber.hoodie.common.model.HoodieWriteStat;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.log.HoodieLogFormat;
import com.uber.hoodie.common.table.log.block.HoodieCommandBlock;
import com.uber.hoodie.common.table.log.block.HoodieCommandBlock.HoodieCommandBlockTypeEnum;
import com.uber.hoodie.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.table.view.HoodieTableFileSystemView;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieCompactionException;
import com.uber.hoodie.exception.HoodieException;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.exception.HoodieRollbackException;
import com.uber.hoodie.exception.HoodieUpsertException;
import com.uber.hoodie.func.MergeOnReadLazyInsertIterable;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.io.HoodieAppendHandle;
import com.uber.hoodie.io.compact.HoodieRealtimeTableCompactor;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
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
  public Iterator<List<WriteStatus>> handleInsert(String commitTime,
      Iterator<HoodieRecord<T>> recordItr) throws Exception {
    // If canIndexLogFiles, write inserts to log files else write inserts to parquet files
    if (index.canIndexLogFiles()) {
      return new MergeOnReadLazyInsertIterable<>(recordItr, config, commitTime, this);
    } else {
      return super.handleInsert(commitTime, recordItr);
    }
  }

  @Override
  public HoodieCompactionPlan scheduleCompaction(JavaSparkContext jsc, String instantTime) {
    logger.info("Checking if compaction needs to be run on " + config.getBasePath());
    Optional<HoodieInstant> lastCompaction = getActiveTimeline().getCommitTimeline()
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
          new HashSet<>(((HoodieTableFileSystemView)getRTFileSystemView())
              .getFileIdToPendingCompaction().keySet()));
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
  public List<HoodieRollbackStat> rollback(JavaSparkContext jsc, List<String> commits)
      throws IOException {

    //At the moment, MOR table type does not support nested rollbacks
    if (commits.size() > 1) {
      throw new UnsupportedOperationException("Nested Rollbacks are not supported");
    }
    Map<String, HoodieInstant> commitsAndCompactions = this.getActiveTimeline()
        .getTimelineOfActions(Sets.newHashSet(HoodieActiveTimeline.COMMIT_ACTION,
            HoodieActiveTimeline.DELTA_COMMIT_ACTION, HoodieActiveTimeline.COMPACTION_ACTION)).getInstants()
        .filter(i -> commits.contains(i.getTimestamp()))
        .collect(Collectors.toMap(i -> i.getTimestamp(), i -> i));

    // Atomically un-publish all non-inflight commits
    commitsAndCompactions.entrySet().stream().map(entry -> entry.getValue())
        .filter(i -> !i.isInflight()).forEach(this.getActiveTimeline()::revertToInflight);
    logger.info("Unpublished " + commits);
    Long startTime = System.currentTimeMillis();
    List<HoodieRollbackStat> allRollbackStats = jsc.parallelize(FSUtils
        .getAllPartitionPaths(this.metaClient.getFs(), this.getMetaClient().getBasePath(),
            config.shouldAssumeDatePartitioning()))
        .map((Function<String, List<HoodieRollbackStat>>) partitionPath -> {
          return commits.stream().map(commit -> {
            HoodieInstant instant = commitsAndCompactions.get(commit);
            HoodieRollbackStat hoodieRollbackStats = null;
            // Need to put the path filter here since Filter is not serializable
            // PathFilter to get all parquet files and log files that need to be deleted
            PathFilter filter = (path) -> {
              if (path.toString().contains(".parquet")) {
                String fileCommitTime = FSUtils.getCommitTime(path.getName());
                return commits.contains(fileCommitTime);
              } else if (path.toString().contains(".log")) {
                // Since the baseCommitTime is the only commit for new log files, it's okay here
                String fileCommitTime = FSUtils.getBaseCommitTimeFromLogPath(path);
                return commits.contains(fileCommitTime);
              }
              return false;
            };

            switch (instant.getAction()) {
              case HoodieTimeline.COMMIT_ACTION:
              case HoodieTimeline.COMPACTION_ACTION:
                try {
                  Map<FileStatus, Boolean> results = super
                      .deleteCleanedFiles(partitionPath, Arrays.asList(commit));
                  hoodieRollbackStats = HoodieRollbackStat.newBuilder()
                      .withPartitionPath(partitionPath).withDeletedFileResults(results).build();
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
                          new HoodieInstant(true, instant.getAction(), instant.getTimestamp()))
                          .get(), HoodieCommitMetadata.class);

                  // read commit file and (either append delete blocks or delete file)
                  final Map<FileStatus, Boolean> filesToDeletedStatus = new HashMap<>();
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
          }).collect(Collectors.toList());
        }).flatMap(x -> x.iterator()).filter(x -> x != null).collect();

    commitsAndCompactions.entrySet().stream().map(
        entry -> new HoodieInstant(true, entry.getValue().getAction(),
            entry.getValue().getTimestamp())).forEach(this.getActiveTimeline()::deleteInflight);
    logger
        .debug("Time(in ms) taken to finish rollback " + (System.currentTimeMillis() - startTime));

    return allRollbackStats;
  }

  @Override
  public void finalizeWrite(JavaSparkContext jsc, List<WriteStatus> writeStatuses)
      throws HoodieIOException {
    // delegate to base class for MOR tables
    super.finalizeWrite(jsc, writeStatuses);
  }

  @Override
  protected HoodieRollingStatMetadata getRollingStats() {
    try {
      Optional<HoodieInstant> lastInstant = this.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants()
          .lastInstant();
      if (lastInstant.isPresent()) {
        HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
            this.getActiveTimeline().getInstantDetails(lastInstant.get()).get(), HoodieCommitMetadata.class);
        Optional<String> lastRollingStat = Optional.ofNullable(commitMetadata.getExtraMetadata()
            .get(HoodieRollingStatMetadata.ROLLING_STAT_METADATA_KEY));
        if (lastRollingStat.isPresent()) {
          HoodieRollingStatMetadata rollingStatMetadata = HoodieCommitMetadata
              .fromBytes(lastRollingStat.get().getBytes(), HoodieRollingStatMetadata.class);
          return rollingStatMetadata;
        }
      }
      return null;
    } catch (IOException e) {
      throw new HoodieException();
    }
  }

  /**
   * UpsertPartitioner for MergeOnRead table type, this allows auto correction of small parquet
   * files to larger ones without the need for an index in the logFile.
   */
  class MergeOnReadUpsertPartitioner extends HoodieCopyOnWriteTable.UpsertPartitioner {

    MergeOnReadUpsertPartitioner(WorkloadProfile profile) {
      super(profile);
    }

    protected List<SmallFile> getSmallFiles(String partitionPath) {

      // smallFiles only for partitionPath
      List<SmallFile> smallFileLocations = new ArrayList<>();

      // Init here since this class (and member variables) might not have been initialized
      HoodieTimeline commitTimeline = getCompletedCommitTimeline();

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
          Optional<FileSlice> smallFileSlice = getRTFileSystemView()
              .getLatestFileSlicesBeforeOrOn(partitionPath, latestCommitTime.getTimestamp()).filter(
                  fileSlice -> fileSlice.getLogFiles().count() < 1
                      && fileSlice.getDataFile().get().getFileSize() < config
                      .getParquetSmallFileLimit()).sorted((FileSlice left, FileSlice right) ->
                  left.getDataFile().get().getFileSize() < right.getDataFile().get().getFileSize()
                      ? -1 : 1).findFirst();
          if (smallFileSlice.isPresent()) {
            allSmallFileSlices.add(smallFileSlice.get());
          }
        } else {
          // If we can index log files, we can add more inserts to log files.
          List<FileSlice> allFileSlices = getRTFileSystemView()
              .getLatestFileSlicesBeforeOrOn(partitionPath, latestCommitTime.getTimestamp())
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
      if (rollingStatMetadata != null) {
        Map<String, HoodieRollingStat> partitionRollingStats =
            rollingStatMetadata.getPartitionToRollingStats().get(partitionPath);
        if (partitionRollingStats != null) {
          HoodieRollingStat rollingStatForFile = partitionRollingStats.get(fileSlice.getFileId());
          if (rollingStatForFile != null) {
            long inserts = rollingStatForFile.getInserts();
            long totalSize = averageRecordSize * inserts;
            return totalSize;
          }
        }
      }
      // In case Rolling Stats is not present, fall back to sizing log files based on heuristics
      if (!fileSlice.getDataFile().isPresent()) {
        return convertLogFilesSizeToExpectedParquetSize(fileSlice.getLogFiles().collect(Collectors.toList()));
      } else {
        return fileSlice.getDataFile().get().getFileSize() + convertLogFilesSizeToExpectedParquetSize(fileSlice
            .getLogFiles().collect(Collectors.toList()));
      }
    }

    private boolean isSmallFile(String partitionPath, FileSlice fileSlice) {
      long totalSize = getTotalFileSize(partitionPath, fileSlice);
      if (totalSize < config.getParquetMaxFileSize()) {
        return true;
      }
      return false;
    }

    // TODO (NA) : Make this static part of utility
    @VisibleForTesting
    public long convertLogFilesSizeToExpectedParquetSize(List<HoodieLogFile> hoodieLogFiles) {
      long totalSizeOfLogFiles = hoodieLogFiles.stream().map(hoodieLogFile -> hoodieLogFile.getFileSize().get())
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
    // The following needs to be done since GlobalIndex at the moment does not store the latest commit time.
    // Also, wStat.getPrevCommit() might not give the right commit time in the following
    // scenario : If a compaction was scheduled, the new commitTime associated with the requested compaction will be
    // used to write the new log files. In this case, the commit time for the log file is the compaction requested time.
    Map<String, String> fileIdToBaseCommitTimeForLogMap =
        hoodieIndex.isGlobal() ? this.getRTFileSystemView().getLatestFileSlices(partitionPath)
            .collect(Collectors.toMap(FileSlice::getFileId, FileSlice::getBaseInstantTime)) : null;
    commitMetadata.getPartitionToWriteStats().get(partitionPath).stream()
        .filter(wStat -> {
          // Filter out stats without prevCommit since they are all inserts
          if (wStat != null && wStat.getPrevCommit() != HoodieWriteStat.NULL_COMMIT && wStat.getPrevCommit() != null
              && !deletedFiles.contains(wStat.getFileId())) {
            return true;
          }
          return false;
        }).forEach(wStat -> {
          HoodieLogFormat.Writer writer = null;
          String baseCommitTime = wStat.getPrevCommit();
          if (hoodieIndex.isGlobal()) {
            baseCommitTime = fileIdToBaseCommitTimeForLogMap.get(wStat.getFileId());
          }
          try {
            writer = HoodieLogFormat.newWriterBuilder().onParentPath(
                new Path(this.getMetaClient().getBasePath(), partitionPath))
                .withFileId(wStat.getFileId()).overBaseCommit(baseCommitTime)
                .withFs(this.metaClient.getFs())
                .withFileExtension(HoodieLogFile.DELTA_EXTENSION).build();
            Long numRollbackBlocks = 0L;
            // generate metadata
            Map<HeaderMetadataType, String> header = generateHeader(commit);
            // if update belongs to an existing log file
            writer = writer.appendBlock(new HoodieCommandBlock(header));
            numRollbackBlocks++;
            filesToNumBlocksRollback.put(this.getMetaClient().getFs()
                .getFileStatus(writer.getLogFile().getPath()), numRollbackBlocks);
          } catch (IOException | InterruptedException io) {
            throw new HoodieRollbackException(
                "Failed to rollback for commit " + commit, io);
          } finally {
            try {
              if (writer != null) {
                writer.close();
              }
            } catch (IOException io) {
              throw new UncheckedIOException(io);
            }
          }
        });
    return HoodieRollbackStat.newBuilder()
        .withPartitionPath(partitionPath)
        .withDeletedFileResults(filesToDeletedStatus)
        .withRollbackBlockAppendResults(filesToNumBlocksRollback).build();
  }

}
