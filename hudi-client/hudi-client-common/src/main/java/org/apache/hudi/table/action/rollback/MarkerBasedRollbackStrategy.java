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
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.function.SerializableBiFunction;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.MarkerBasedRollbackUtils;
import org.apache.hudi.table.marker.WriteMarkers;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.hudi.common.util.StringUtils.EMPTY_STRING;

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
      List<HoodieRollbackRequest> rollbackRequests = context.map(markerPaths, markerFilePath -> {
        String typeStr = markerFilePath.substring(markerFilePath.lastIndexOf(".") + 1);
        IOType type = IOType.valueOf(typeStr);
        String fileNameWithPartitionToRollback = WriteMarkers.stripMarkerSuffix(markerFilePath);
        Path fullFilePathToRollback = new Path(basePath, fileNameWithPartitionToRollback);
        String partitionPath = FSUtils.getRelativePartitionPath(new Path(basePath), fullFilePathToRollback.getParent());
        switch (type) {
          case MERGE:
          case CREATE:
            String fileId = null;
            String baseInstantTime = null;
            if (FSUtils.isBaseFile(fullFilePathToRollback)) {
              HoodieBaseFile baseFileToDelete = new HoodieBaseFile(fullFilePathToRollback.toString());
              fileId = baseFileToDelete.getFileId();
              baseInstantTime = baseFileToDelete.getCommitTime();
            } else if (FSUtils.isLogFile(fullFilePathToRollback)) {
              throw new HoodieRollbackException("Log files should have only APPEND as IOTypes " + fullFilePathToRollback);
            }
            Objects.requireNonNull(fileId, "Cannot find valid fileId from path: " + fullFilePathToRollback);
            Objects.requireNonNull(baseInstantTime, "Cannot find valid base instant from path: " + fullFilePathToRollback);
            return new HoodieRollbackRequest(partitionPath, fileId, baseInstantTime,
                Collections.singletonList(fullFilePathToRollback.toString()),
                Collections.emptyMap());
          case APPEND:
            HoodieRollbackRequest rollbackRequestForAppend = getRollbackRequestForAppend(instantToRollback, fileNameWithPartitionToRollback);
            return rollbackRequestForAppend;
          default:
            throw new HoodieRollbackException("Unknown marker type, during rollback of " + instantToRollback);
        }
      }, parallelism);

      if (rollbackRequests.isEmpty()) {
        return rollbackRequests;
      }
      // we need to ensure we return one rollback request per partition path, fileId, baseInstant triplet.
      HoodieData<HoodieRollbackRequest> processedRollbackRequests = context.parallelize(rollbackRequests)
          .mapToPair(rollbackRequest -> Pair.of(Pair.of(rollbackRequest.getPartitionPath(), rollbackRequest.getFileId()), rollbackRequest))
          .reduceByKey((SerializableBiFunction<HoodieRollbackRequest, HoodieRollbackRequest, HoodieRollbackRequest>) RollbackUtils::mergeRollbackRequest, parallelism).values();

      SerializableConfiguration serializableConfiguration = new SerializableConfiguration(context.getHadoopConf());
      // fetch valid sizes for log files to rollback. Optimize to make minimum calls (one per partition or file slice since fs calls has to be made)
      return fetchFileSizesForLogFilesToRollback(processedRollbackRequests, serializableConfiguration).collectAsList();
    } catch (Exception e) {
      throw new HoodieRollbackException("Error rolling back using marker files written for " + instantToRollback, e);
    }
  }

  protected HoodieData<HoodieRollbackRequest> fetchFileSizesForLogFilesToRollback(HoodieData<HoodieRollbackRequest> rollbackRequestHoodieData,
                                                                                  SerializableConfiguration serializableConfiguration) {

    // for log blocks to be deleted, lets fetch the actual size from FS
    return rollbackRequestHoodieData
        .map((SerializableFunction<HoodieRollbackRequest, HoodieRollbackRequest>) rollbackRequest -> {
          if (rollbackRequest.getLogBlocksToBeDeleted() == null || rollbackRequest.getLogBlocksToBeDeleted().isEmpty()) {
            return rollbackRequest;
          }
          // if there are log blocks to delete, fetch the appropriate size and set them.
          String relativePartitionPathStr = rollbackRequest.getPartitionPath();
          Path relativePartitionPath = new Path(basePath + "/" + relativePartitionPathStr);
          String fileId = rollbackRequest.getFileId();
          String baseCommitTime = rollbackRequest.getLatestBaseInstant();
          List<String> filesToDelete = rollbackRequest.getFilesToBeDeleted();
          Map<String, Long> logBlocksToBeDeleted = rollbackRequest.getLogBlocksToBeDeleted();
          Map<String, Long> logBlocksWithValidSizes = new HashMap<>();
          List<String> logFilesNeedingFileSizes = new ArrayList<>();

          // for those which has valid sizes already, we don't need to fetch them again
          logBlocksToBeDeleted.forEach((k, v) -> {
            if (v != -1L) {
              logBlocksWithValidSizes.put(k, v);
            } else {
              logFilesNeedingFileSizes.add(k);
            }
          });

          FileSystem fs = relativePartitionPath.getFileSystem(serializableConfiguration.get());
          List<Option<FileStatus>> fileStatuses = FSUtils.getFileStatusesUnderPartition(fs, relativePartitionPath,
              new HashSet<>(logFilesNeedingFileSizes), true);
          fileStatuses.stream().filter(Option::isPresent).map(Option::get).forEach(fileStatus -> {
            logBlocksWithValidSizes.put(fileStatus.getPath().getName(), fileStatus.getLen());
          });
          return new HoodieRollbackRequest(relativePartitionPathStr, fileId, baseCommitTime, filesToDelete,
              logBlocksWithValidSizes);
        });
  }

  protected HoodieRollbackRequest getRollbackRequestForAppend(HoodieInstant instantToRollback, String fileNameWithPartitionToRollback) throws IOException {
    Path fullLogFilePath = new Path(basePath, fileNameWithPartitionToRollback);
    String fileId;
    String baseCommitTime;
    String relativePartitionPath;
    Option<HoodieLogFile> latestLogFileOption;

    // Old marker files may be generated from base file name before HUDI-1517. keep compatible with them.
    Map<String, Long> logFilesWithBlocksToRollback = new HashMap<>();
    if (FSUtils.isBaseFile(fullLogFilePath)) {
      LOG.warn("Find old marker type for log file: " + fileNameWithPartitionToRollback);
      fileId = FSUtils.getFileIdFromFilePath(fullLogFilePath);
      baseCommitTime = FSUtils.getCommitTime(fullLogFilePath.getName());
      relativePartitionPath = FSUtils.getRelativePartitionPath(new Path(basePath), fullLogFilePath.getParent());
      Path partitionPath = FSUtils.getPartitionPath(config.getBasePath(), relativePartitionPath);

      // NOTE: Since we're rolling back incomplete Delta Commit, it only could have appended its
      //       block to the latest log-file
      try {
        latestLogFileOption = FSUtils.getLatestLogFile(table.getMetaClient().getFs(), partitionPath, fileId,
            HoodieFileFormat.HOODIE_LOG.getFileExtension(), baseCommitTime);
        if (latestLogFileOption.isPresent() && baseCommitTime.equals(instantToRollback.getTimestamp())) {
          Path fullDeletePath = new Path(partitionPath, latestLogFileOption.get().getFileName());
          return new HoodieRollbackRequest(relativePartitionPath, EMPTY_STRING, EMPTY_STRING,
              Collections.singletonList(fullDeletePath.toString()),
              Collections.emptyMap());
        }

        Map<String, Long> logFilesWithBlocsToRollback = new HashMap<>();
        if (latestLogFileOption.isPresent()) {
          HoodieLogFile latestLogFile = latestLogFileOption.get();
          // NOTE: Marker's don't carry information about the cumulative size of the blocks that have been appended,
          //       therefore we simply stub this value.
          logFilesWithBlocsToRollback = Collections.singletonMap(latestLogFile.getFileStatus().getPath().toString(), -1L);
        }
        return new HoodieRollbackRequest(relativePartitionPath, fileId, baseCommitTime, Collections.emptyList(),
            logFilesWithBlocsToRollback);
      } catch (IOException ioException) {
        throw new HoodieIOException(
            "Failed to get latestLogFile for fileId: " + fileId + " in partition: " + partitionPath,
            ioException);
      }
    } else {
      HoodieLogFile logFileToRollback = new HoodieLogFile(fullLogFilePath);
      fileId = logFileToRollback.getFileId();
      baseCommitTime = logFileToRollback.getBaseCommitTime();
      relativePartitionPath = FSUtils.getRelativePartitionPath(new Path(basePath), fullLogFilePath.getParent());
      logFilesWithBlocksToRollback = Collections.singletonMap(logFileToRollback.getPath().getName(), -1L);
    }

    return new HoodieRollbackRequest(relativePartitionPath, fileId, baseCommitTime, Collections.emptyList(),
        logFilesWithBlocksToRollback);
  }
}
