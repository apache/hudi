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

import org.apache.hadoop.fs.Path;

import org.apache.hudi.avro.model.HoodieRollbackRequest;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.MarkerBasedRollbackUtils;
import org.apache.hudi.table.marker.WriteMarkers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

/**
 * Performs rollback using marker files generated during the write..
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
    this.basePath = table.getMetaClient().getBasePath();
    this.config = config;
    this.instantTime = instantTime;
  }

  @Override
  public List<HoodieRollbackRequest> getRollbackRequests(HoodieInstant instantToRollback) {
    try {
      List<String> markerPaths = MarkerBasedRollbackUtils.getAllMarkerPaths(
          table, context, instantToRollback.getTimestamp(), config.getRollbackParallelism());
      int parallelism = Math.max(Math.min(markerPaths.size(), config.getRollbackParallelism()), 1);
      return context.map(markerPaths, markerFilePath -> {
        String typeStr = markerFilePath.substring(markerFilePath.lastIndexOf(".") + 1);
        IOType type = IOType.valueOf(typeStr);
        String partitionFilePath = WriteMarkers.stripMarkerSuffix(markerFilePath);
        Path fullFilePath = new Path(basePath, partitionFilePath);
        String partitionPath = FSUtils.getRelativePartitionPath(new Path(basePath), fullFilePath.getParent());
        switch (type) {
          case MERGE:
          case CREATE:
            String fileId = null;
            String baseInstantTime = null;
            if (FSUtils.isBaseFile(fullFilePath)) {
              HoodieBaseFile baseFileToDelete = new HoodieBaseFile(fullFilePath.toString());
              fileId = baseFileToDelete.getFileId();
              baseInstantTime = baseFileToDelete.getCommitTime();
            } else if (FSUtils.isLogFile(fullFilePath)) {
              checkArgument(type != IOType.MERGE, "Log file should not support merge io type");
              HoodieLogFile logFileToDelete = new HoodieLogFile(fullFilePath.toString());
              fileId = logFileToDelete.getFileId();
              baseInstantTime = logFileToDelete.getBaseCommitTime();
            }
            Objects.requireNonNull(fileId, "Cannot find valid fileId from path: " + fullFilePath);
            Objects.requireNonNull(baseInstantTime, "Cannot find valid base instant from path: " + fullFilePath);
            return new HoodieRollbackRequest(partitionPath, fileId, baseInstantTime,
                Collections.singletonList(fullFilePath.toString()),
                Collections.emptyMap());
          case APPEND:
            HoodieRollbackRequest rollbackRequestForAppend = getRollbackRequestForAppend(partitionFilePath);
            return rollbackRequestForAppend;
          default:
            throw new HoodieRollbackException("Unknown marker type, during rollback of " + instantToRollback);
        }
      }, parallelism);
    } catch (Exception e) {
      throw new HoodieRollbackException("Error rolling back using marker files written for " + instantToRollback, e);
    }
  }

  protected HoodieRollbackRequest getRollbackRequestForAppend(String markerFilePath) throws IOException {
    Path filePath = new Path(basePath, markerFilePath);
    String fileId;
    String baseCommitTime;
    String relativePartitionPath;
    Option<HoodieLogFile> latestLogFileOption;

    // Old marker files may be generated from base file name before HUDI-1517. keep compatible with them.
    // TODO: may deprecated in the future. @guanziyue.gzy
    if (FSUtils.isBaseFile(filePath)) {
      LOG.warn("Find old marker type for log file: " + markerFilePath);
      fileId = FSUtils.getFileIdFromFilePath(filePath);
      baseCommitTime = FSUtils.getCommitTime(filePath.getName());
      relativePartitionPath = FSUtils.getRelativePartitionPath(new Path(basePath), filePath.getParent());
      Path partitionPath = FSUtils.getPartitionPath(config.getBasePath(), relativePartitionPath);

      // NOTE: Since we're rolling back incomplete Delta Commit, it only could have appended its
      //       block to the latest log-file
      try {
        latestLogFileOption = FSUtils.getLatestLogFile(table.getMetaClient().getFs(), partitionPath, fileId,
            HoodieFileFormat.HOODIE_LOG.getFileExtension(), baseCommitTime);
      } catch (IOException ioException) {
        throw new HoodieIOException(
            "Failed to get latestLogFile for fileId: " + fileId + " in partition: " + partitionPath,
            ioException);
      }
    } else {
      HoodieLogFile latestLogFile = new HoodieLogFile(filePath);
      fileId = latestLogFile.getFileId();
      baseCommitTime = latestLogFile.getBaseCommitTime();
      relativePartitionPath = FSUtils.getRelativePartitionPath(new Path(basePath), filePath.getParent());
      latestLogFileOption = Option.of(latestLogFile);
    }

    Map<String, Long> logFilesWithBlocsToRollback = new HashMap<>();
    if (latestLogFileOption.isPresent()) {
      HoodieLogFile latestLogFile = latestLogFileOption.get();
      // NOTE: Marker's don't carry information about the cumulative size of the blocks that have been appended,
      //       therefore we simply stub this value.
      logFilesWithBlocsToRollback = Collections.singletonMap(latestLogFile.getPath().toString(), -1L);
    }
    return new HoodieRollbackRequest(relativePartitionPath, fileId, baseCommitTime, Collections.emptyList(),
        logFilesWithBlocsToRollback);
  }
}
