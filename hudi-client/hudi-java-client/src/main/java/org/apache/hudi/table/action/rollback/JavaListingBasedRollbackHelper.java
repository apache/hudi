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
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieRollbackException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Performs Rollback of Hoodie Tables.
 */
public class JavaListingBasedRollbackHelper implements Serializable {

  private static final Logger LOG = LogManager.getLogger(JavaListingBasedRollbackHelper.class);

  private final HoodieTableMetaClient metaClient;
  private final HoodieWriteConfig config;

  public JavaListingBasedRollbackHelper(HoodieTableMetaClient metaClient, HoodieWriteConfig config) {
    this.metaClient = metaClient;
    this.config = config;
  }

  /**
   * Performs all rollback actions that we have collected in parallel.
   */
  public List<HoodieRollbackStat> performRollback(HoodieEngineContext context, HoodieInstant instantToRollback, List<ListingBasedRollbackRequest> rollbackRequests) {
    Map<String, HoodieRollbackStat> partitionPathRollbackStatsPairs = maybeDeleteAndCollectStats(context, instantToRollback, rollbackRequests, true);

    Map<String, List<Pair<String, HoodieRollbackStat>>> collect = partitionPathRollbackStatsPairs.entrySet()
        .stream()
        .map(x -> Pair.of(x.getKey(), x.getValue())).collect(Collectors.groupingBy(Pair::getLeft));
    return collect.values().stream()
        .map(pairs -> pairs.stream().map(Pair::getRight).reduce(RollbackUtils::mergeRollbackStat).orElse(null))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  /**
   * Collect all file info that needs to be rollbacked.
   */
  public List<HoodieRollbackStat> collectRollbackStats(HoodieEngineContext context, HoodieInstant instantToRollback, List<ListingBasedRollbackRequest> rollbackRequests) {
    Map<String, HoodieRollbackStat> partitionPathRollbackStatsPairs = maybeDeleteAndCollectStats(context, instantToRollback, rollbackRequests, false);
    return new ArrayList<>(partitionPathRollbackStatsPairs.values());
  }

  /**
   * May be delete interested files and collect stats or collect stats only.
   *
   * @param context           instance of {@link HoodieEngineContext} to use.
   * @param instantToRollback {@link HoodieInstant} of interest for which deletion or collect stats is requested.
   * @param rollbackRequests  List of {@link ListingBasedRollbackRequest} to be operated on.
   * @param doDelete          {@code true} if deletion has to be done. {@code false} if only stats are to be collected w/o performing any deletes.
   * @return stats collected with or w/o actual deletions.
   */
  Map<String, HoodieRollbackStat> maybeDeleteAndCollectStats(HoodieEngineContext context,
                                                             HoodieInstant instantToRollback,
                                                             List<ListingBasedRollbackRequest> rollbackRequests,
                                                             boolean doDelete) {
    return context.mapToPair(rollbackRequests, rollbackRequest -> {
      switch (rollbackRequest.getType()) {
        case DELETE_DATA_FILES_ONLY: {
          final Map<FileStatus, Boolean> filesToDeletedStatus = deleteBaseFiles(metaClient, config, instantToRollback.getTimestamp(),
              rollbackRequest.getPartitionPath(), doDelete);
          return new ImmutablePair<>(rollbackRequest.getPartitionPath(),
              HoodieRollbackStat.newBuilder().withPartitionPath(rollbackRequest.getPartitionPath())
                  .withDeletedFileResults(filesToDeletedStatus).build());
        }
        case DELETE_DATA_AND_LOG_FILES: {
          final Map<FileStatus, Boolean> filesToDeletedStatus = deleteBaseAndLogFiles(metaClient, config, instantToRollback.getTimestamp(), rollbackRequest.getPartitionPath(), doDelete);
          return new ImmutablePair<>(rollbackRequest.getPartitionPath(),
              HoodieRollbackStat.newBuilder().withPartitionPath(rollbackRequest.getPartitionPath())
                  .withDeletedFileResults(filesToDeletedStatus).build());
        }
        case APPEND_ROLLBACK_BLOCK: {
          HoodieLogFormat.Writer writer = null;
          try {
            writer = HoodieLogFormat.newWriterBuilder()
                .onParentPath(FSUtils.getPartitionPath(metaClient.getBasePath(), rollbackRequest.getPartitionPath()))
                .withFileId(rollbackRequest.getFileId().get())
                .overBaseCommit(rollbackRequest.getLatestBaseInstant().get()).withFs(metaClient.getFs())
                .withFileExtension(HoodieLogFile.DELTA_EXTENSION).build();

            // generate metadata
            if (doDelete) {
              Map<HoodieLogBlock.HeaderMetadataType, String> header = generateHeader(instantToRollback.getTimestamp());
              // if update belongs to an existing log file
              writer.appendBlock(new HoodieCommandBlock(header));
            }
          } catch (IOException | InterruptedException io) {
            throw new HoodieRollbackException("Failed to rollback for instant " + instantToRollback, io);
          } finally {
            try {
              if (writer != null) {
                writer.close();
              }
            } catch (IOException io) {
              throw new HoodieIOException("Error appending rollback block..", io);
            }
          }

          // This step is intentionally done after writer is closed. Guarantees that
          // getFileStatus would reflect correct stats and FileNotFoundException is not thrown in
          // cloud-storage : HUDI-168
          Map<FileStatus, Long> filesToNumBlocksRollback = Collections.singletonMap(
              metaClient.getFs().getFileStatus(Objects.requireNonNull(writer).getLogFile().getPath()), 1L
          );
          return new ImmutablePair<>(rollbackRequest.getPartitionPath(),
              HoodieRollbackStat.newBuilder().withPartitionPath(rollbackRequest.getPartitionPath())
                  .withRollbackBlockAppendResults(filesToNumBlocksRollback).build());
        }
        default:
          throw new IllegalStateException("Unknown Rollback action " + rollbackRequest);
      }
    }, 0);
  }

  /**
   * Common method used for cleaning out base files under a partition path during rollback of a set of commits.
   */
  private Map<FileStatus, Boolean> deleteBaseAndLogFiles(HoodieTableMetaClient metaClient, HoodieWriteConfig config,
                                                         String commit, String partitionPath, boolean doDelete) throws IOException {
    LOG.info("Cleaning path " + partitionPath);
    String basefileExtension = metaClient.getTableConfig().getBaseFileFormat().getFileExtension();
    SerializablePathFilter filter = (path) -> {
      if (path.toString().endsWith(basefileExtension)) {
        String fileCommitTime = FSUtils.getCommitTime(path.getName());
        return commit.equals(fileCommitTime);
      } else if (FSUtils.isLogFile(path)) {
        // Since the baseCommitTime is the only commit for new log files, it's okay here
        String fileCommitTime = FSUtils.getBaseCommitTimeFromLogPath(path);
        return commit.equals(fileCommitTime);
      }
      return false;
    };

    final Map<FileStatus, Boolean> results = new HashMap<>();
    FileSystem fs = metaClient.getFs();
    FileStatus[] toBeDeleted = fs.listStatus(FSUtils.getPartitionPath(config.getBasePath(), partitionPath), filter);
    for (FileStatus file : toBeDeleted) {
      if (doDelete) {
        boolean success = fs.delete(file.getPath(), false);
        results.put(file, success);
        LOG.info("Delete file " + file.getPath() + "\t" + success);
      } else {
        results.put(file, true);
      }
    }
    return results;
  }

  /**
   * Common method used for cleaning out base files under a partition path during rollback of a set of commits.
   */
  private Map<FileStatus, Boolean> deleteBaseFiles(HoodieTableMetaClient metaClient, HoodieWriteConfig config,
                                                   String commit, String partitionPath, boolean doDelete) throws IOException {
    final Map<FileStatus, Boolean> results = new HashMap<>();
    LOG.info("Cleaning path " + partitionPath);
    FileSystem fs = metaClient.getFs();
    String basefileExtension = metaClient.getTableConfig().getBaseFileFormat().getFileExtension();
    PathFilter filter = (path) -> {
      if (path.toString().contains(basefileExtension)) {
        String fileCommitTime = FSUtils.getCommitTime(path.getName());
        return commit.equals(fileCommitTime);
      }
      return false;
    };
    FileStatus[] toBeDeleted = fs.listStatus(FSUtils.getPartitionPath(config.getBasePath(), partitionPath), filter);
    for (FileStatus file : toBeDeleted) {
      if (doDelete) {
        boolean success = fs.delete(file.getPath(), false);
        results.put(file, success);
        LOG.info("Delete file " + file.getPath() + "\t" + success);
      } else {
        results.put(file, true);
      }
    }
    return results;
  }

  private Map<HoodieLogBlock.HeaderMetadataType, String> generateHeader(String commit) {
    // generate metadata
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>(3);
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, metaClient.getActiveTimeline().lastInstant().get().getTimestamp());
    header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, commit);
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_PREVIOUS_BLOCK.ordinal()));
    return header;
  }

  public interface SerializablePathFilter extends PathFilter, Serializable {

  }
}
