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

import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Writer;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock.HoodieCommandBlockTypeEnum;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieRollbackException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

/**
 * Performs Rollback of Hoodie Tables.
 */
public class RollbackExecutor implements Serializable {

  private static Logger logger = LogManager.getLogger(RollbackExecutor.class);

  private final HoodieTableMetaClient metaClient;
  private final HoodieWriteConfig config;

  public RollbackExecutor(HoodieTableMetaClient metaClient, HoodieWriteConfig config) {
    this.metaClient = metaClient;
    this.config = config;
  }

  /**
   * Performs all rollback actions that we have collected in parallel.
   */
  public List<HoodieRollbackStat> performRollback(JavaSparkContext jsc, HoodieInstant instantToRollback,
      List<RollbackRequest> rollbackRequests) {

    SerializablePathFilter filter = (path) -> {
      if (path.toString().contains(".parquet")) {
        String fileCommitTime = FSUtils.getCommitTime(path.getName());
        return instantToRollback.getTimestamp().equals(fileCommitTime);
      } else if (path.toString().contains(".log")) {
        // Since the baseCommitTime is the only commit for new log files, it's okay here
        String fileCommitTime = FSUtils.getBaseCommitTimeFromLogPath(path);
        return instantToRollback.getTimestamp().equals(fileCommitTime);
      }
      return false;
    };

    int sparkPartitions = Math.max(Math.min(rollbackRequests.size(), config.getRollbackParallelism()), 1);
    return jsc.parallelize(rollbackRequests, sparkPartitions).mapToPair(rollbackRequest -> {
      final Map<FileStatus, Boolean> filesToDeletedStatus = new HashMap<>();
      switch (rollbackRequest.getRollbackAction()) {
        case DELETE_DATA_FILES_ONLY: {
          deleteCleanedFiles(metaClient, config, filesToDeletedStatus, instantToRollback.getTimestamp(),
              rollbackRequest.getPartitionPath());
          return new Tuple2<String, HoodieRollbackStat>(rollbackRequest.getPartitionPath(),
              HoodieRollbackStat.newBuilder().withPartitionPath(rollbackRequest.getPartitionPath())
                  .withDeletedFileResults(filesToDeletedStatus).build());
        }
        case DELETE_DATA_AND_LOG_FILES: {
          deleteCleanedFiles(metaClient, config, filesToDeletedStatus, rollbackRequest.getPartitionPath(), filter);
          return new Tuple2<String, HoodieRollbackStat>(rollbackRequest.getPartitionPath(),
              HoodieRollbackStat.newBuilder().withPartitionPath(rollbackRequest.getPartitionPath())
                  .withDeletedFileResults(filesToDeletedStatus).build());
        }
        case APPEND_ROLLBACK_BLOCK: {
          Writer writer = null;
          boolean success = false;
          try {
            writer = HoodieLogFormat.newWriterBuilder()
                .onParentPath(FSUtils.getPartitionPath(metaClient.getBasePath(), rollbackRequest.getPartitionPath()))
                .withFileId(rollbackRequest.getFileId().get())
                .overBaseCommit(rollbackRequest.getLatestBaseInstant().get()).withFs(metaClient.getFs())
                .withFileExtension(HoodieLogFile.DELTA_EXTENSION).build();

            // generate metadata
            Map<HeaderMetadataType, String> header = generateHeader(instantToRollback.getTimestamp());
            // if update belongs to an existing log file
            writer = writer.appendBlock(new HoodieCommandBlock(header));
            success = true;
          } catch (IOException | InterruptedException io) {
            throw new HoodieRollbackException("Failed to rollback for instant " + instantToRollback, io);
          } finally {
            try {
              if (writer != null) {
                writer.close();
              }
            } catch (IOException io) {
              throw new UncheckedIOException(io);
            }
          }

          // This step is intentionally done after writer is closed. Guarantees that
          // getFileStatus would reflect correct stats and FileNotFoundException is not thrown in
          // cloud-storage : HUDI-168
          Map<FileStatus, Long> filesToNumBlocksRollback = new HashMap<>();
          filesToNumBlocksRollback.put(metaClient.getFs().getFileStatus(writer.getLogFile().getPath()), 1L);
          return new Tuple2<String, HoodieRollbackStat>(rollbackRequest.getPartitionPath(),
              HoodieRollbackStat.newBuilder().withPartitionPath(rollbackRequest.getPartitionPath())
                  .withRollbackBlockAppendResults(filesToNumBlocksRollback).build());
        }
        default:
          throw new IllegalStateException("Unknown Rollback action " + rollbackRequest);
      }
    }).reduceByKey(this::mergeRollbackStat).map(Tuple2::_2).collect();
  }

  /**
   * Helper to merge 2 rollback-stats for a given partition.
   *
   * @param stat1 HoodieRollbackStat
   * @param stat2 HoodieRollbackStat
   * @return Merged HoodieRollbackStat
   */
  private HoodieRollbackStat mergeRollbackStat(HoodieRollbackStat stat1, HoodieRollbackStat stat2) {
    Preconditions.checkArgument(stat1.getPartitionPath().equals(stat2.getPartitionPath()));
    final List<String> successDeleteFiles = new ArrayList<>();
    final List<String> failedDeleteFiles = new ArrayList<>();
    final Map<FileStatus, Long> commandBlocksCount = new HashMap<>();

    if (stat1.getSuccessDeleteFiles() != null) {
      successDeleteFiles.addAll(stat1.getSuccessDeleteFiles());
    }
    if (stat2.getSuccessDeleteFiles() != null) {
      successDeleteFiles.addAll(stat2.getSuccessDeleteFiles());
    }
    if (stat1.getFailedDeleteFiles() != null) {
      failedDeleteFiles.addAll(stat1.getFailedDeleteFiles());
    }
    if (stat2.getFailedDeleteFiles() != null) {
      failedDeleteFiles.addAll(stat2.getFailedDeleteFiles());
    }
    if (stat1.getCommandBlocksCount() != null) {
      commandBlocksCount.putAll(stat1.getCommandBlocksCount());
    }
    if (stat2.getCommandBlocksCount() != null) {
      commandBlocksCount.putAll(stat2.getCommandBlocksCount());
    }
    return new HoodieRollbackStat(stat1.getPartitionPath(), successDeleteFiles, failedDeleteFiles, commandBlocksCount);
  }

  /**
   * Common method used for cleaning out parquet files under a partition path during rollback of a set of commits.
   */
  private Map<FileStatus, Boolean> deleteCleanedFiles(HoodieTableMetaClient metaClient, HoodieWriteConfig config,
      Map<FileStatus, Boolean> results, String partitionPath, PathFilter filter) throws IOException {
    logger.info("Cleaning path " + partitionPath);
    FileSystem fs = metaClient.getFs();
    FileStatus[] toBeDeleted = fs.listStatus(FSUtils.getPartitionPath(config.getBasePath(), partitionPath), filter);
    for (FileStatus file : toBeDeleted) {
      boolean success = fs.delete(file.getPath(), false);
      results.put(file, success);
      logger.info("Delete file " + file.getPath() + "\t" + success);
    }
    return results;
  }

  /**
   * Common method used for cleaning out parquet files under a partition path during rollback of a set of commits.
   */
  private Map<FileStatus, Boolean> deleteCleanedFiles(HoodieTableMetaClient metaClient, HoodieWriteConfig config,
      Map<FileStatus, Boolean> results, String commit, String partitionPath) throws IOException {
    logger.info("Cleaning path " + partitionPath);
    FileSystem fs = metaClient.getFs();
    PathFilter filter = (path) -> {
      if (path.toString().contains(".parquet")) {
        String fileCommitTime = FSUtils.getCommitTime(path.getName());
        return commit.equals(fileCommitTime);
      }
      return false;
    };
    FileStatus[] toBeDeleted = fs.listStatus(FSUtils.getPartitionPath(config.getBasePath(), partitionPath), filter);
    for (FileStatus file : toBeDeleted) {
      boolean success = fs.delete(file.getPath(), false);
      results.put(file, success);
      logger.info("Delete file " + file.getPath() + "\t" + success);
    }
    return results;
  }

  private Map<HeaderMetadataType, String> generateHeader(String commit) {
    // generate metadata
    Map<HeaderMetadataType, String> header = Maps.newHashMap();
    header.put(HeaderMetadataType.INSTANT_TIME, metaClient.getActiveTimeline().lastInstant().get().getTimestamp());
    header.put(HeaderMetadataType.TARGET_INSTANT_TIME, commit);
    header.put(HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlockTypeEnum.ROLLBACK_PREVIOUS_BLOCK.ordinal()));
    return header;
  }

  public interface SerializablePathFilter extends PathFilter, Serializable {

  }
}
