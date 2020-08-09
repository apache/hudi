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
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Writer;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock.HoodieCommandBlockTypeEnum;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieRollbackException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import scala.Tuple2;

/**
 * Performs Rollback of Hoodie Tables.
 */
public class ListingBasedRollbackHelper implements Serializable {

  private static final Logger LOG = LogManager.getLogger(ListingBasedRollbackHelper.class);

  private final HoodieTableMetaClient metaClient;
  private final HoodieWriteConfig config;

  public ListingBasedRollbackHelper(HoodieTableMetaClient metaClient, HoodieWriteConfig config) {
    this.metaClient = metaClient;
    this.config = config;
  }

  /**
   * Performs all rollback actions that we have collected in parallel.
   */
  public List<HoodieRollbackStat> performRollback(JavaSparkContext jsc, HoodieInstant instantToRollback, List<ListingBasedRollbackRequest> rollbackRequests) {
    int sparkPartitions = Math.max(Math.min(rollbackRequests.size(), config.getRollbackParallelism()), 1);
    jsc.setJobGroup(this.getClass().getSimpleName(), "Perform rollback actions");
    JavaPairRDD<String, HoodieRollbackStat> partitionPathRollbackStatsPairRDD = maybeDeleteAndCollectStats(jsc, instantToRollback, rollbackRequests, sparkPartitions, true);
    return partitionPathRollbackStatsPairRDD.reduceByKey(RollbackUtils::mergeRollbackStat).map(Tuple2::_2).collect();
  }

  /**
   * Collect all file info that needs to be rollbacked.
   */
  public List<HoodieRollbackStat> collectRollbackStats(JavaSparkContext jsc, HoodieInstant instantToRollback, List<ListingBasedRollbackRequest> rollbackRequests) {
    int sparkPartitions = Math.max(Math.min(rollbackRequests.size(), config.getRollbackParallelism()), 1);
    jsc.setJobGroup(this.getClass().getSimpleName(), "Collect rollback stats for upgrade/downgrade");
    JavaPairRDD<String, HoodieRollbackStat> partitionPathRollbackStatsPairRDD = maybeDeleteAndCollectStats(jsc, instantToRollback, rollbackRequests, sparkPartitions, false);
    return partitionPathRollbackStatsPairRDD.map(Tuple2::_2).collect();
  }

  /**
   * May be delete interested files and collect stats or collect stats only.
   *
   * @param jsc instance of {@link JavaSparkContext} to use.
   * @param instantToRollback {@link HoodieInstant} of interest for which deletion or collect stats is requested.
   * @param rollbackRequests List of {@link ListingBasedRollbackRequest} to be operated on.
   * @param sparkPartitions number of spark partitions to use for parallelism.
   * @param doDelete {@code true} if deletion has to be done. {@code false} if only stats are to be collected w/o performing any deletes.
   * @return stats collected with or w/o actual deletions.
   */
  JavaPairRDD<String, HoodieRollbackStat> maybeDeleteAndCollectStats(JavaSparkContext jsc, HoodieInstant instantToRollback, List<ListingBasedRollbackRequest> rollbackRequests,
                                                                     int sparkPartitions, boolean doDelete) {
    return jsc.parallelize(rollbackRequests, sparkPartitions).mapToPair(rollbackRequest -> {
      switch (rollbackRequest.getType()) {
        case DELETE_DATA_FILES_ONLY: {
          final Map<FileStatus, Boolean> filesToDeletedStatus = deleteBaseFiles(metaClient, config, instantToRollback.getTimestamp(),
              rollbackRequest.getPartitionPath(), doDelete);
          return new Tuple2<>(rollbackRequest.getPartitionPath(),
              HoodieRollbackStat.newBuilder().withPartitionPath(rollbackRequest.getPartitionPath())
                  .withDeletedFileResults(filesToDeletedStatus).build());
        }
        case DELETE_DATA_AND_LOG_FILES: {
          final Map<FileStatus, Boolean> filesToDeletedStatus = deleteBaseAndLogFiles(metaClient, config, instantToRollback.getTimestamp(), rollbackRequest.getPartitionPath(), doDelete);
          return new Tuple2<>(rollbackRequest.getPartitionPath(),
              HoodieRollbackStat.newBuilder().withPartitionPath(rollbackRequest.getPartitionPath())
                  .withDeletedFileResults(filesToDeletedStatus).build());
        }
        case APPEND_ROLLBACK_BLOCK: {
          Writer writer = null;
          try {
            writer = HoodieLogFormat.newWriterBuilder()
                .onParentPath(FSUtils.getPartitionPath(metaClient.getBasePath(), rollbackRequest.getPartitionPath()))
                .withFileId(rollbackRequest.getFileId().get())
                .overBaseCommit(rollbackRequest.getLatestBaseInstant().get()).withFs(metaClient.getFs())
                .withFileExtension(HoodieLogFile.DELTA_EXTENSION).build();

            // generate metadata
            if (doDelete) {
              Map<HeaderMetadataType, String> header = generateHeader(instantToRollback.getTimestamp());
              // if update belongs to an existing log file
              writer = writer.appendBlock(new HoodieCommandBlock(header));
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
              metaClient.getFs().getFileStatus(Objects.requireNonNull(writer).getLogFile().getPath()),
              1L
          );
          return new Tuple2<>(rollbackRequest.getPartitionPath(),
              HoodieRollbackStat.newBuilder().withPartitionPath(rollbackRequest.getPartitionPath())
                  .withRollbackBlockAppendResults(filesToNumBlocksRollback).build());
        }
        default:
          throw new IllegalStateException("Unknown Rollback action " + rollbackRequest);
      }
    });
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

  private Map<HeaderMetadataType, String> generateHeader(String commit) {
    // generate metadata
    Map<HeaderMetadataType, String> header = new HashMap<>(3);
    header.put(HeaderMetadataType.INSTANT_TIME, metaClient.getActiveTimeline().lastInstant().get().getTimestamp());
    header.put(HeaderMetadataType.TARGET_INSTANT_TIME, commit);
    header.put(HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlockTypeEnum.ROLLBACK_PREVIOUS_BLOCK.ordinal()));
    return header;
  }

  public interface SerializablePathFilter extends PathFilter, Serializable {

  }
}
