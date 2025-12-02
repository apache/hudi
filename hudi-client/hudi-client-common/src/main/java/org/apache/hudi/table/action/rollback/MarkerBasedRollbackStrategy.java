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

import org.apache.hudi.avro.model.HoodieRollbackRequest;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.MarkerBasedRollbackUtils;
import org.apache.hudi.table.marker.WriteMarkers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.hudi.common.util.StringUtils.EMPTY_STRING;

/**
 * Performs rollback using marker files generated during the writes.
 */
public class MarkerBasedRollbackStrategy<T, I, K, O> implements BaseRollbackPlanActionExecutor.RollbackStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(MarkerBasedRollbackStrategy.class);

  protected final HoodieTable<?, ?, ?, ?> table;
  protected final transient HoodieEngineContext context;
  protected final HoodieWriteConfig config;
  protected final String basePath;
  protected final String instantTime;

  public MarkerBasedRollbackStrategy(HoodieTable<?, ?, ?, ?> table, HoodieEngineContext context, HoodieWriteConfig config, String instantTime) {
    this.table = table;
    this.context = context;
    this.basePath = table.getMetaClient().getBasePath().toString();
    this.config = config;
    this.instantTime = instantTime;
  }

  @Override
  public List<HoodieRollbackRequest> getRollbackRequests(HoodieInstant instantToRollback) {
    try {
      List<String> markerPaths = MarkerBasedRollbackUtils.getAllMarkerPaths(
          table, context, instantToRollback.requestedTime(), config.getRollbackParallelism());
      int parallelism = Math.max(Math.min(markerPaths.size(), config.getRollbackParallelism()), 1);
      List<HoodieRollbackRequest> rollbackRequestList = context.map(markerPaths, markerFilePath -> {
        String typeStr = markerFilePath.substring(markerFilePath.lastIndexOf(".") + 1);
        IOType type = IOType.valueOf(typeStr);
        String filePathStr = WriteMarkers.stripMarkerSuffix(markerFilePath);
        StoragePath filePath = new StoragePath(basePath, filePathStr);
        String partitionPath = FSUtils.getRelativePartitionPath(new StoragePath(basePath), filePath.getParent());
        String fileId = FSUtils.getFileIdFromFilePath(filePath);

        switch (type) {
          case MERGE:
          case CREATE:
            return createRollbackRequestForCreateAndMerge(fileId, partitionPath, filePath, instantToRollback);
          case APPEND:
            return createRollbackRequestForAppend(fileId, partitionPath, filePath, instantToRollback, filePathStr);
          default:
            throw new HoodieRollbackException("Unknown marker type, during rollback of " + instantToRollback);
        }
      }, parallelism);
      // The rollback requests for append only exist in table version 6 and below which require groupBy
      return table.version().greaterThanOrEquals(HoodieTableVersion.EIGHT)
          ? rollbackRequestList
          : RollbackUtils.groupRollbackRequestsBasedOnFileGroup(rollbackRequestList);
    } catch (Exception e) {
      throw new HoodieRollbackException("Error rolling back using marker files written for " + instantToRollback, e);
    }
  }

  protected HoodieRollbackRequest createRollbackRequestForCreateAndMerge(String fileId,
                                                                         String partitionPath,
                                                                         StoragePath filePath,
                                                                         HoodieInstant instantToRollback) {
    if (table.version().greaterThanOrEquals(HoodieTableVersion.EIGHT)) {
      return new HoodieRollbackRequest(partitionPath, fileId, instantToRollback.requestedTime(),
          Collections.singletonList(filePath.toString()), Collections.emptyMap());
    } else {
      String baseInstantTime = null;
      if (FSUtils.isBaseFile(filePath)) {
        HoodieBaseFile baseFileToDelete = new HoodieBaseFile(filePath.toString());
        fileId = baseFileToDelete.getFileId();
        baseInstantTime = baseFileToDelete.getCommitTime();
      } else if (FSUtils.isLogFile(filePath)) {
        throw new HoodieRollbackException("Log files should have only APPEND as IOTypes " + filePath);
      }
      Objects.requireNonNull(fileId, "Cannot find valid fileId from path: " + filePath);
      Objects.requireNonNull(baseInstantTime, "Cannot find valid base instant from path: " + filePath);
      return new HoodieRollbackRequest(partitionPath, fileId, baseInstantTime,
          Collections.singletonList(filePath.toString()),
          Collections.emptyMap());
    }
  }

  protected HoodieRollbackRequest createRollbackRequestForAppend(String fileId,
                                                                 String relativePartitionPath,
                                                                 StoragePath filePath,
                                                                 HoodieInstant instantToRollback,
                                                                 String filePathToRollback) {
    if (table.version().greaterThanOrEquals(HoodieTableVersion.EIGHT)) {
      return new HoodieRollbackRequest(relativePartitionPath, fileId, instantToRollback.requestedTime(), Collections.emptyList(),
          Collections.singletonMap(filePath.toString(), 1L));
    } else {
      StoragePath fullFilePath = new StoragePath(basePath, filePathToRollback);
      String baseCommitTime;
      Option<HoodieLogFile> latestLogFileOption;
      Map<String, Long> logBlocksToBeDeleted = new HashMap<>();
      // Old marker files may be generated from base file name before HUDI-1517. keep compatible with them.
      if (FSUtils.isBaseFile(fullFilePath)) {
        LOG.info("Found old marker type for log file: {}", filePathToRollback);
        baseCommitTime = FSUtils.getCommitTime(fullFilePath.getName());
        StoragePath partitionPath = FSUtils.constructAbsolutePath(config.getBasePath(), relativePartitionPath);

        // NOTE: Since we're rolling back incomplete Delta Commit, it only could have appended its
        //       block to the latest log-file
        try {
          latestLogFileOption = FSUtils.getLatestLogFile(table.getMetaClient().getStorage(), partitionPath, fileId,
              HoodieFileFormat.HOODIE_LOG.getFileExtension(), baseCommitTime);
          if (latestLogFileOption.isPresent() && baseCommitTime.equals(instantToRollback.requestedTime())) {
            // Log file can be deleted if the commit to rollback is also the commit that created the fileGroup
            StoragePath fullDeletePath = new StoragePath(partitionPath, latestLogFileOption.get().getFileName());
            return new HoodieRollbackRequest(relativePartitionPath, EMPTY_STRING, EMPTY_STRING,
                Collections.singletonList(fullDeletePath.toString()),
                Collections.emptyMap());
          }
          if (latestLogFileOption.isPresent()) {
            HoodieLogFile latestLogFile = latestLogFileOption.get();
            // NOTE: Markers don't carry information about the cumulative size of the blocks that have been appended,
            //       therefore we simply stub this value.
            logBlocksToBeDeleted = Collections.singletonMap(latestLogFile.getPathInfo().getPath().toString(), latestLogFile.getPathInfo().getLength());
          }
          return new HoodieRollbackRequest(relativePartitionPath, fileId, baseCommitTime, Collections.emptyList(), logBlocksToBeDeleted);
        } catch (IOException ioException) {
          throw new HoodieIOException(
              "Failed to get latestLogFile for fileId: " + fileId + " in partition: " + partitionPath,
              ioException);
        }
      } else {
        HoodieLogFile logFileToRollback = new HoodieLogFile(fullFilePath);
        fileId = logFileToRollback.getFileId();
        // For tbl version 6, deltaCommitTime is the commit time of the base file for the file slice
        baseCommitTime = logFileToRollback.getDeltaCommitTime();
        try {
          StoragePathInfo pathInfo = table.getMetaClient().getStorage().getPathInfo(logFileToRollback.getPath());
          if (pathInfo != null) {
            if (baseCommitTime.equals(instantToRollback.requestedTime())) {
              // delete the log file that creates a new file group
              return new HoodieRollbackRequest(relativePartitionPath, EMPTY_STRING, EMPTY_STRING,
                  Collections.singletonList(logFileToRollback.getPath().toString()),
                  Collections.emptyMap());
            }
            // append a rollback block to the log block that is added to an existing file group
            logBlocksToBeDeleted = Collections.singletonMap(
                logFileToRollback.getPath().getName(), pathInfo.getLength());
          } else {
            LOG.debug(
                "File info of {} is null indicating the file does not exist;"
                    + " there is no need to include it in the rollback.",
                fullFilePath);
          }
        } catch (FileNotFoundException e) {
          LOG.debug(
              "Log file {} is not found so there is no need to include it in the rollback.",
              fullFilePath);
        } catch (IOException e) {
          throw new HoodieIOException("Failed to get the file status of " + fullFilePath, e);
        }
      }
      return new HoodieRollbackRequest(relativePartitionPath, fileId, baseCommitTime, Collections.emptyList(), logBlocksToBeDeleted);
    }
  }
}
