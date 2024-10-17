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
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.MarkerBasedRollbackUtils;
import org.apache.hudi.table.marker.WriteMarkers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

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
    this.basePath = table.getMetaClient().getBasePath().toString();
    this.config = config;
    this.instantTime = instantTime;
  }

  @Override
  public List<HoodieRollbackRequest> getRollbackRequests(HoodieInstant instantToRollback) {
    try {
      List<String> markerPaths = MarkerBasedRollbackUtils.getAllMarkerPaths(
          table, context, instantToRollback.getRequestTime(), config.getRollbackParallelism());
      int parallelism = Math.max(Math.min(markerPaths.size(), config.getRollbackParallelism()), 1);
      return context.map(markerPaths, markerFilePath -> {
        String typeStr = markerFilePath.substring(markerFilePath.lastIndexOf(".") + 1);
        IOType type = IOType.valueOf(typeStr);
        String filePathStr = WriteMarkers.stripMarkerSuffix(markerFilePath);
        StoragePath filePath = new StoragePath(basePath, filePathStr);
        String partitionPath = FSUtils.getRelativePartitionPath(new StoragePath(basePath), filePath.getParent());
        String fileId = FSUtils.getFileIdFromFilePath(filePath);
        switch (type) {
          case MERGE:
          case CREATE:
            return new HoodieRollbackRequest(partitionPath, fileId, instantToRollback.getRequestTime(),
                Collections.singletonList(filePath.toString()),
                Collections.emptyMap());
          case APPEND:
            return new HoodieRollbackRequest(partitionPath, fileId, instantToRollback.getRequestTime(), Collections.emptyList(),
              Collections.singletonMap(filePath.toString(), 1L));
          default:
            throw new HoodieRollbackException("Unknown marker type, during rollback of " + instantToRollback);
        }
      }, parallelism);
    } catch (Exception e) {
      throw new HoodieRollbackException("Error rolling back using marker files written for " + instantToRollback, e);
    }
  }
}
