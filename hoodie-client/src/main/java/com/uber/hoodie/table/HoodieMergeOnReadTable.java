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
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.model.HoodieWriteStat;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.log.HoodieLogFormat;
import com.uber.hoodie.common.table.log.block.HoodieCommandBlock;
import com.uber.hoodie.common.table.log.block.HoodieLogBlock;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieCompactionException;
import com.uber.hoodie.exception.HoodieRollbackException;
import com.uber.hoodie.io.HoodieAppendHandle;
import com.uber.hoodie.io.HoodieIOHandle;
import com.uber.hoodie.io.HoodieMergeHandle;
import com.uber.hoodie.io.compact.HoodieRealtimeTableCompactor;
import java.io.IOException;
import java.io.UncheckedIOException;
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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


/**
 * Implementation of a more real-time read-optimized Hoodie Table where
 *
 * INSERTS - Same as HoodieCopyOnWriteTable - Produce new files, block aligned to desired size (or)
 * Merge with the smallest existing file, to expand it
 *
 * UPDATES - Appends the changes to a rolling log file maintained per file Id. Compaction merges the
 * log file into the base file.
 *
 * WARNING - MOR table type does not support nested rollbacks, every rollback must be followed by an
 * attempted commit action
 */
public class HoodieMergeOnReadTable<T extends HoodieRecordPayload> extends
    HoodieCopyOnWriteTable<T> {

  private static Logger logger = LogManager.getLogger(HoodieMergeOnReadTable.class);

  public HoodieMergeOnReadTable(HoodieWriteConfig config, HoodieTableMetaClient metaClient) {
    super(config, metaClient);
  }

  @Override
  protected HoodieIOHandle<T> getIOHandle(String commitTime, String fileLoc,
                                          Iterator<HoodieRecord<T>> recordItr) {
    return new HoodieAppendHandle<>(config, commitTime, this, recordItr, fileLoc);
  }

  @Override
  public Iterator<List<WriteStatus>> handleUpdate(HoodieIOHandle<T> ioHandle) throws IOException {
    HoodieAppendHandle<T> appendHandle = (HoodieAppendHandle<T>) ioHandle;
    logger.info("Merging updates for commit " + appendHandle.getCommitTime() + " for file " + appendHandle.getFileId());
    appendHandle.doAppend();
    appendHandle.close();
    return Collections.singletonList(Collections.singletonList(appendHandle.getWriteStatus()))
        .iterator();
  }

  @Override
  public Optional<HoodieCommitMetadata> compact(JavaSparkContext jsc, String compactionCommitTime) {
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
      return Optional.empty();
    }

    logger.info("Compacting merge on read table " + config.getBasePath());
    HoodieRealtimeTableCompactor compactor = new HoodieRealtimeTableCompactor();
    try {
      return Optional.of(compactor.compact(jsc, config, this, compactionCommitTime));
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
    Map<String, HoodieInstant> commitsAndCompactions =
        this.getActiveTimeline()
            .getTimelineOfActions(Sets.newHashSet(HoodieActiveTimeline.COMMIT_ACTION, HoodieActiveTimeline.DELTA_COMMIT_ACTION))
            .getInstants()
            .filter(i -> commits.contains(i.getTimestamp()))
            .collect(Collectors.toMap(i -> i.getTimestamp(), i -> i));

    // Atomically un-publish all non-inflight commits
    commitsAndCompactions.entrySet().stream().map(entry -> entry.getValue())
        .filter(i -> !i.isInflight()).forEach(this.getActiveTimeline()::revertToInflight);

    logger.info("Unpublished " + commits);

    Long startTime = System.currentTimeMillis();

    List<HoodieRollbackStat> allRollbackStats = commits.stream().map(commit -> {
      HoodieInstant instant = commitsAndCompactions.get(commit);
      List<HoodieRollbackStat> stats = null;
      switch (instant.getAction()) {
        case HoodieTimeline.COMMIT_ACTION:
          try {
            logger.info("Starting to rollback Commit/Compaction " + instant);
            HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
                .fromBytes(this.getCommitsTimeline().getInstantDetails(
                    new HoodieInstant(true, instant.getAction(), instant.getTimestamp())).get());

            stats = jsc.parallelize(commitMetadata.getPartitionToWriteStats().keySet().stream()
                .collect(Collectors.toList()))
                .map((Function<String, HoodieRollbackStat>) partitionPath -> {
                  Map<FileStatus, Boolean> results = super
                      .deleteCleanedFiles(partitionPath, Arrays.asList(commit));
                  return HoodieRollbackStat.newBuilder().withPartitionPath(partitionPath)
                      .withDeletedFileResults(results).build();
                }).collect();
            logger.info("Finished rollback of Commit/Compaction " + instant);
            break;
          } catch (IOException io) {
            throw new UncheckedIOException("Failed to rollback for commit " + commit, io);
          }
        case HoodieTimeline.DELTA_COMMIT_ACTION:
          try {
            logger.info("Starting to rollback delta commit " + instant);

            HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
                .fromBytes(this.getCommitsTimeline().getInstantDetails(
                    new HoodieInstant(true, instant.getAction(), instant.getTimestamp())).get());

            stats = jsc.parallelize(commitMetadata.getPartitionToWriteStats().keySet().stream()
                .collect(Collectors.toList()))
                .map((Function<String, HoodieRollbackStat>) partitionPath -> {
                  // read commit file and (either append delete blocks or delete file)
                  Map<FileStatus, Boolean> filesToDeletedStatus = new HashMap<>();
                  Map<FileStatus, Long> filesToNumBlocksRollback = new HashMap<>();

                  // we do not know fileIds for inserts (first inserts are parquet files), delete all parquet files for the corresponding failed commit, if present (same as COW)
                  filesToDeletedStatus = super
                      .deleteCleanedFiles(partitionPath, Arrays.asList(commit));

                  // append rollback blocks for updates
                  commitMetadata.getPartitionToWriteStats().get(partitionPath).stream()
                      .filter(wStat -> wStat.getPrevCommit() != HoodieWriteStat.NULL_COMMIT)
                      .forEach(wStat -> {
                        HoodieLogFormat.Writer writer = null;
                        try {
                          writer = HoodieLogFormat.newWriterBuilder()
                              .onParentPath(
                                  new Path(this.getMetaClient().getBasePath(), partitionPath))
                              .withFileId(wStat.getFileId()).overBaseCommit(wStat.getPrevCommit())
                              .withFs(getMetaClient().getFs())
                              .withFileExtension(HoodieLogFile.DELTA_EXTENSION).build();
                          Long numRollbackBlocks = 0L;
                          // generate metadata
                          Map<HoodieLogBlock.LogMetadataType, String> metadata = Maps.newHashMap();
                          metadata.put(HoodieLogBlock.LogMetadataType.INSTANT_TIME,
                              metaClient.getActiveTimeline().lastInstant().get().getTimestamp());
                          metadata.put(HoodieLogBlock.LogMetadataType.TARGET_INSTANT_TIME, commit);
                          // if update belongs to an existing log file
                          writer.appendBlock(new HoodieCommandBlock(
                              HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_PREVIOUS_BLOCK,
                              metadata));
                          numRollbackBlocks++;
                          if (wStat.getNumDeletes() > 0) {
                            writer.appendBlock(new HoodieCommandBlock(
                                HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_PREVIOUS_BLOCK,
                                metadata));
                            numRollbackBlocks++;
                          }
                          filesToNumBlocksRollback
                              .put(getMetaClient().getFs()
                                      .getFileStatus(writer.getLogFile().getPath()),
                                  numRollbackBlocks);
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
                  return HoodieRollbackStat.newBuilder().withPartitionPath(partitionPath)
                      .withDeletedFileResults(filesToDeletedStatus)
                      .withRollbackBlockAppendResults(filesToNumBlocksRollback).build();
                }).collect();
            logger.info("Fnished rollback of delta commit " + instant);
            break;
          } catch (IOException io) {
            throw new UncheckedIOException("Failed to rollback for commit " + commit, io);
          }
      }
      return stats;
    }).flatMap(x -> x.stream()).collect(Collectors.toList());

    commitsAndCompactions.entrySet().stream()
        .map(entry -> new HoodieInstant(true, entry.getValue().getAction(),
            entry.getValue().getTimestamp()))
        .forEach(this.getActiveTimeline()::deleteInflight);

    logger
        .debug("Time(in ms) taken to finish rollback " + (System.currentTimeMillis() - startTime));

    return allRollbackStats;
  }

}
