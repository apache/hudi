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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.HoodieRollbackStat;
import com.uber.hoodie.common.model.FileSlice;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordLocation;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.model.HoodieWriteStat;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.log.HoodieLogFormat;
import com.uber.hoodie.common.table.log.block.HoodieCommandBlock;
import com.uber.hoodie.common.table.log.block.HoodieLogBlock;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieCompactionException;
import com.uber.hoodie.exception.HoodieRollbackException;
import com.uber.hoodie.exception.HoodieUpsertException;
import com.uber.hoodie.io.HoodieAppendHandle;
import com.uber.hoodie.io.compact.HoodieRealtimeTableCompactor;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
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

  public HoodieMergeOnReadTable(HoodieWriteConfig config, HoodieTableMetaClient metaClient) {
    super(config, metaClient);
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

    if (mergeOnReadUpsertPartitioner.getSmallFileIds().contains(fileId)) {
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
  public JavaRDD<WriteStatus> compact(JavaSparkContext jsc, String compactionCommitTime) {
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
      return jsc.emptyRDD();
    }

    logger.info("Compacting merge on read table " + config.getBasePath());
    HoodieRealtimeTableCompactor compactor = new HoodieRealtimeTableCompactor();
    try {
      return compactor.compact(jsc, config, this, compactionCommitTime);
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
            HoodieActiveTimeline.DELTA_COMMIT_ACTION)).getInstants()
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
            switch (instant.getAction()) {
              case HoodieTimeline.COMMIT_ACTION:
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
                try {
                  HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
                      this.getCommitTimeline().getInstantDetails(
                          new HoodieInstant(true, instant.getAction(), instant.getTimestamp()))
                          .get());

                  // read commit file and (either append delete blocks or delete file)
                  Map<FileStatus, Boolean> filesToDeletedStatus = new HashMap<>();
                  Map<FileStatus, Long> filesToNumBlocksRollback = new HashMap<>();

                  // we do not know fileIds for inserts (first inserts are parquet files), delete
                  // all parquet files for the corresponding failed commit, if present (same as COW)
                  filesToDeletedStatus = super
                      .deleteCleanedFiles(partitionPath, Arrays.asList(commit));

                  // append rollback blocks for updates
                  if (commitMetadata.getPartitionToWriteStats().containsKey(partitionPath)) {
                    commitMetadata.getPartitionToWriteStats().get(partitionPath).stream()
                        .filter(wStat -> {
                          return wStat != null
                              && wStat.getPrevCommit() != HoodieWriteStat.NULL_COMMIT
                              && wStat.getPrevCommit() != null;
                        }).forEach(wStat -> {
                          HoodieLogFormat.Writer writer = null;
                          try {
                            writer = HoodieLogFormat.newWriterBuilder().onParentPath(
                                new Path(this.getMetaClient().getBasePath(), partitionPath))
                                .withFileId(wStat.getFileId()).overBaseCommit(wStat.getPrevCommit())
                                .withFs(this.metaClient.getFs())
                                .withFileExtension(HoodieLogFile.DELTA_EXTENSION).build();
                            Long numRollbackBlocks = 0L;
                            // generate metadata
                            Map<HoodieLogBlock.HeaderMetadataType, String> header =
                                Maps.newHashMap();
                            header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME,
                                metaClient.getActiveTimeline().lastInstant().get().getTimestamp());
                            header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME,
                                commit);
                            header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE, String
                                .valueOf(
                                    HoodieCommandBlock.HoodieCommandBlockTypeEnum
                                        .ROLLBACK_PREVIOUS_BLOCK
                                        .ordinal()));
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
                              writer.close();
                            } catch (IOException io) {
                              throw new UncheckedIOException(io);
                            }
                          }
                        });
                    hoodieRollbackStats = HoodieRollbackStat.newBuilder()
                        .withPartitionPath(partitionPath)
                        .withDeletedFileResults(filesToDeletedStatus)
                        .withRollbackBlockAppendResults(filesToNumBlocksRollback).build();
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
  public Optional<Integer> finalizeWrite(JavaSparkContext jsc, List writeStatuses) {
    // do nothing for MOR tables
    return Optional.empty();
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
      HoodieTimeline commitTimeline = getCompletedCommitTimeline();

      if (!commitTimeline.empty()) {
        HoodieInstant latestCommitTime = commitTimeline.lastInstant().get();
        // find smallest file in partition and append to it
        Optional<FileSlice> smallFileSlice = getRTFileSystemView()
            .getLatestFileSlicesBeforeOrOn(partitionPath, latestCommitTime.getTimestamp()).filter(
                fileSlice -> fileSlice.getLogFiles().count() < 1
                    && fileSlice.getDataFile().get().getFileSize() < config
                    .getParquetSmallFileLimit()).sorted((FileSlice left, FileSlice right) ->
                left.getDataFile().get().getFileSize() < right.getDataFile().get().getFileSize()
                    ? -1 : 1).findFirst();

        if (smallFileSlice.isPresent()) {
          String filename = smallFileSlice.get().getDataFile().get().getFileName();
          SmallFile sf = new SmallFile();
          sf.location = new HoodieRecordLocation(FSUtils.getCommitTime(filename),
              FSUtils.getFileId(filename));
          sf.sizeBytes = smallFileSlice.get().getDataFile().get().getFileSize();
          smallFileLocations.add(sf);
          // Update the global small files list
          smallFiles.add(sf);
        }
      }

      return smallFileLocations;
    }

    public List<String> getSmallFileIds() {
      return (List<String>) smallFiles.stream()
          .map(smallFile -> ((SmallFile) smallFile).location.getFileId())
          .collect(Collectors.toList());
    }
  }
}
