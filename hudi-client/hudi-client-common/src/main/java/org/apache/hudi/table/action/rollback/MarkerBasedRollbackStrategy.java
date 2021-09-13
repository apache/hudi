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
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.MarkerBasedRollbackUtils;
import org.apache.hudi.table.marker.WriteMarkers;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.table.action.rollback.BaseRollbackHelper.EMPTY_STRING;

/**
 * Performs rollback using marker files generated during the write..
 */
public class MarkerBasedRollbackStrategy<T extends HoodieRecordPayload, I, K, O> implements BaseRollbackPlanActionExecutor.RollbackStrategy {

  private static final Logger LOG = LogManager.getLogger(MarkerBasedRollbackStrategy.class);

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
        switch (type) {
          case MERGE:
          case CREATE:
            String fileToDelete = WriteMarkers.stripMarkerSuffix(markerFilePath);
            Path fullDeletePath = new Path(basePath, fileToDelete);
            String partitionPath = FSUtils.getRelativePartitionPath(new Path(basePath), fullDeletePath.getParent());
            return new HoodieRollbackRequest(partitionPath, EMPTY_STRING, EMPTY_STRING,
                Collections.singletonList(fullDeletePath.toString()),
                Collections.emptyMap());
          case APPEND:
            return getRollbackRequestForAppend(WriteMarkers.stripMarkerSuffix(markerFilePath));
          default:
            throw new HoodieRollbackException("Unknown marker type, during rollback of " + instantToRollback);
        }
      }, parallelism).stream().collect(Collectors.toList());
    } catch (Exception e) {
      throw new HoodieRollbackException("Error rolling back using marker files written for " + instantToRollback, e);
    }
  }

  protected HoodieRollbackRequest getRollbackRequestForAppend(String appendBaseFilePath) throws IOException {
    Path baseFilePathForAppend = new Path(basePath, appendBaseFilePath);
    String fileId = FSUtils.getFileIdFromFilePath(baseFilePathForAppend);
    String baseCommitTime = FSUtils.getCommitTime(baseFilePathForAppend.getName());
    String partitionPath = FSUtils.getRelativePartitionPath(new Path(basePath), new Path(basePath, appendBaseFilePath).getParent());
    Map<FileStatus, Long> writtenLogFileSizeMap = getWrittenLogFileSizeMap(partitionPath, baseCommitTime, fileId);
    Map<String, Long> writtenLogFileStrSizeMap = new HashMap<>();
    for (Map.Entry<FileStatus, Long> entry : writtenLogFileSizeMap.entrySet()) {
      writtenLogFileStrSizeMap.put(entry.getKey().getPath().toString(), entry.getValue());
    }
    return new HoodieRollbackRequest(partitionPath, fileId, baseCommitTime, Collections.emptyList(), writtenLogFileStrSizeMap);
  }

  /**
   * Returns written log file size map for the respective baseCommitTime to assist in metadata table syncing.
   *
   * @param partitionPathStr partition path of interest
   * @param baseCommitTime   base commit time of interest
   * @param fileId           fileId of interest
   * @return Map<FileStatus, File size>
   * @throws IOException
   */
  private Map<FileStatus, Long> getWrittenLogFileSizeMap(String partitionPathStr, String baseCommitTime, String fileId) throws IOException {
    // collect all log files that is supposed to be deleted with this rollback
    return FSUtils.getAllLogFiles(table.getMetaClient().getFs(),
        FSUtils.getPartitionPath(config.getBasePath(), partitionPathStr), fileId, HoodieFileFormat.HOODIE_LOG.getFileExtension(), baseCommitTime)
        .collect(Collectors.toMap(HoodieLogFile::getFileStatus, value -> value.getFileStatus().getLen()));
  }
}
