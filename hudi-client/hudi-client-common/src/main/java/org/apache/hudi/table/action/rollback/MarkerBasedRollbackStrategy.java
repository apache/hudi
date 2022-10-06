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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.MarkerBasedRollbackUtils;
import org.apache.hudi.table.marker.WriteMarkers;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
          table, context, instantToRollback.getTimestamp(), config.getInt(HoodieWriteConfig.ROLLBACK_PARALLELISM_VALUE));
      int parallelism = Math.max(Math.min(markerPaths.size(), config.getInt(HoodieWriteConfig.ROLLBACK_PARALLELISM_VALUE)), 1);
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
            // NOTE: This marker file-path does NOT correspond to a log-file, but rather is a phony
            //       path serving as a "container" for the following components:
            //          - Base file's file-id
            //          - Base file's commit instant
            //          - Partition path
            return getRollbackRequestForAppend(WriteMarkers.stripMarkerSuffix(markerFilePath));
          default:
            throw new HoodieRollbackException("Unknown marker type, during rollback of " + instantToRollback);
        }
      }, parallelism);
    } catch (Exception e) {
      throw new HoodieRollbackException("Error rolling back using marker files written for " + instantToRollback, e);
    }
  }

  protected HoodieRollbackRequest getRollbackRequestForAppend(String markerFilePath) throws IOException {
    Path baseFilePathForAppend = new Path(basePath, markerFilePath);
    String fileId = FSUtils.getFileIdFromFilePath(baseFilePathForAppend);
    String baseCommitTime = FSUtils.getCommitTime(baseFilePathForAppend.getName());
    String relativePartitionPath = FSUtils.getRelativePartitionPath(new Path(basePath), baseFilePathForAppend.getParent());
    Path partitionPath = FSUtils.getPartitionPath(config.getBasePath(), relativePartitionPath);

    // NOTE: Since we're rolling back incomplete Delta Commit, it only could have appended its
    //       block to the latest log-file
    // TODO(HUDI-1517) use provided marker-file's path instead
    Option<HoodieLogFile> latestLogFileOption = FSUtils.getLatestLogFile(table.getMetaClient().getFs(), partitionPath, fileId,
        HoodieFileFormat.HOODIE_LOG.getFileExtension(), baseCommitTime);
    
    Map<String, Long> logFilesWithBlocsToRollback = new HashMap<>();
    if (latestLogFileOption.isPresent()) {
      HoodieLogFile latestLogFile = latestLogFileOption.get();
      // NOTE: Marker's don't carry information about the cumulative size of the blocks that have been appended,
      //       therefore we simply stub this value.
      logFilesWithBlocsToRollback = Collections.singletonMap(latestLogFile.getFileStatus().getPath().toString(), -1L);
    }

    return new HoodieRollbackRequest(relativePartitionPath, fileId, baseCommitTime, Collections.emptyList(),
        logFilesWithBlocsToRollback);
  }
}
