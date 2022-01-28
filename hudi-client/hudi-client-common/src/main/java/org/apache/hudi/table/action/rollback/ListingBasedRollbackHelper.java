/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table.action.rollback;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hudi.avro.model.HoodieRollbackRequest;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.table.action.rollback.BaseRollbackHelper.EMPTY_STRING;

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
   * Collects info for Rollback plan.
   */
  public List<HoodieRollbackRequest> getRollbackRequestsForRollbackPlan(HoodieEngineContext context, HoodieInstant instantToRollback, List<ListingBasedRollbackRequest> rollbackRequests) {
    int sparkPartitions = Math.max(Math.min(rollbackRequests.size(), config.getRollbackParallelism()), 1);
    context.setJobStatus(this.getClass().getSimpleName(), "Creating Rollback Plan");
    return getListingBasedRollbackRequests(context, instantToRollback, rollbackRequests, sparkPartitions);
  }

  /**
   * May be delete interested files and collect stats or collect stats only.
   *
   * @param context           instance of {@link HoodieEngineContext} to use.
   * @param instantToRollback {@link HoodieInstant} of interest for which deletion or collect stats is requested.
   * @param rollbackRequests  List of {@link ListingBasedRollbackRequest} to be operated on.
   * @param numPartitions     number of spark partitions to use for parallelism.
   * @return stats collected with or w/o actual deletions.
   */
  private List<HoodieRollbackRequest> getListingBasedRollbackRequests(HoodieEngineContext context, HoodieInstant instantToRollback,
                                                                      List<ListingBasedRollbackRequest> rollbackRequests, int numPartitions) {
    return context.map(rollbackRequests, rollbackRequest -> {
      switch (rollbackRequest.getType()) {
        case DELETE_DATA_FILES_ONLY: {
          final FileStatus[] filesToDeletedStatus = getBaseFilesToBeDeleted(metaClient, config, instantToRollback.getTimestamp(),
              rollbackRequest.getPartitionPath(), metaClient.getFs());
          List<String> filesToBeDeleted = Arrays.stream(filesToDeletedStatus).map(fileStatus -> {
            String fileToBeDeleted = fileStatus.getPath().toString();
            // strip scheme
            return fileToBeDeleted.substring(fileToBeDeleted.indexOf(":") + 1);
          }).collect(Collectors.toList());
          return new HoodieRollbackRequest(rollbackRequest.getPartitionPath(),
              EMPTY_STRING, EMPTY_STRING, filesToBeDeleted, Collections.EMPTY_MAP);
        }
        case DELETE_DATA_AND_LOG_FILES: {
          final FileStatus[] filesToDeletedStatus = getBaseAndLogFilesToBeDeleted(instantToRollback.getTimestamp(), rollbackRequest.getPartitionPath(), metaClient.getFs());
          List<String> filesToBeDeleted = Arrays.stream(filesToDeletedStatus).map(fileStatus -> {
            String fileToBeDeleted = fileStatus.getPath().toString();
            // strip scheme
            return fileToBeDeleted.substring(fileToBeDeleted.indexOf(":") + 1);
          }).collect(Collectors.toList());
          return new HoodieRollbackRequest(rollbackRequest.getPartitionPath(), EMPTY_STRING, EMPTY_STRING, filesToBeDeleted, Collections.EMPTY_MAP);
        }
        case APPEND_ROLLBACK_BLOCK: {
          String fileId = rollbackRequest.getFileId().get();
          String latestBaseInstant = rollbackRequest.getLatestBaseInstant().get();
          HoodieWriteStat writeStat = rollbackRequest.getWriteStat().get();

          Path fullLogFilePath = FSUtils.getPartitionPath(config.getBasePath(), writeStat.getPath());

          Map<String, Long> logFilesWithBlocksToRollback =
              Collections.singletonMap(fullLogFilePath.toString(), writeStat.getTotalWriteBytes());

          return new HoodieRollbackRequest(rollbackRequest.getPartitionPath(), fileId, latestBaseInstant,
              Collections.EMPTY_LIST, logFilesWithBlocksToRollback);
        }
        default:
          throw new IllegalStateException("Unknown Rollback action " + rollbackRequest);
      }
    }, numPartitions);
  }

  private FileStatus[] getBaseFilesToBeDeleted(HoodieTableMetaClient metaClient, HoodieWriteConfig config,
                                               String commit, String partitionPath, FileSystem fs) throws IOException {
    LOG.info("Collecting files to be cleaned/rolledback up for path " + partitionPath + " and commit " + commit);
    String basefileExtension = metaClient.getTableConfig().getBaseFileFormat().getFileExtension();
    PathFilter filter = (path) -> {
      if (path.toString().contains(basefileExtension)) {
        String fileCommitTime = FSUtils.getCommitTime(path.getName());
        return commit.equals(fileCommitTime);
      }
      return false;
    };
    return fs.listStatus(FSUtils.getPartitionPath(config.getBasePath(), partitionPath), filter);
  }

  private FileStatus[] getBaseAndLogFilesToBeDeleted(String commit, String partitionPath, FileSystem fs) throws IOException {
    String basefileExtension = metaClient.getTableConfig().getBaseFileFormat().getFileExtension();
    BaseRollbackHelper.SerializablePathFilter filter = (path) -> {
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
    return fs.listStatus(FSUtils.getPartitionPath(config.getBasePath(), partitionPath), filter);
  }
}
