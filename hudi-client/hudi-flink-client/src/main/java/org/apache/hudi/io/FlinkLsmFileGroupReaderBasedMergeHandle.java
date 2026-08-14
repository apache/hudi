/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.io;

import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Iterator;

/**
 * Flink mini-batch merge handle backed by the LSM file-group reader.
 */
@Slf4j
public class FlinkLsmFileGroupReaderBasedMergeHandle<T, I, K, O>
    extends LsmFileGroupReaderBasedMergeHandle<T, I, K, O>
    implements MiniBatchHandle {

  public FlinkLsmFileGroupReaderBasedMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                                 Iterator<HoodieRecord<T>> recordItr, String partitionPath, String fileId,
                                                 TaskContextSupplier taskContextSupplier) {
    this(config, instantTime, hoodieTable, recordItr, partitionPath, fileId, taskContextSupplier, getLatestBaseFile(hoodieTable, partitionPath, fileId));
  }

  public FlinkLsmFileGroupReaderBasedMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                                 Iterator<HoodieRecord<T>> recordItr, String partitionPath, String fileId,
                                                 TaskContextSupplier taskContextSupplier, HoodieBaseFile hoodieBaseFile) {
    super(config, instantTime, hoodieTable, recordItr, partitionPath, fileId, taskContextSupplier, hoodieBaseFile, Option.empty());
    if (getAttemptId() > 0) {
      deleteInvalidDataFile(getAttemptId() - 1);
    }
  }

  @Override
  protected long getMaxMemoryForMerge() {
    return Long.MAX_VALUE;
  }

  private void deleteInvalidDataFile(long lastAttemptId) {
    final String lastWriteToken = FSUtils.makeWriteToken(getPartitionId(), getStageId(), lastAttemptId);
    final String lastDataFileName = FSUtils.makeBaseFileName(instantTime,
        lastWriteToken, this.fileId, hoodieTable.getBaseFileExtension());
    final StoragePath path = makeNewFilePath(partitionPath, lastDataFileName);
    if (path.equals(oldFilePath)) {
      return;
    }
    try {
      if (storage.exists(path)) {
        log.info("Deleting invalid MERGE base file due to task retry: {}", lastDataFileName);
        storage.deleteFile(path);
      }
    } catch (IOException e) {
      throw new HoodieException("Error while deleting the MERGE base file due to task retry: " + lastDataFileName, e);
    }
  }

  @Override
  protected void createMarkerFile(String partitionPath, String dataFileName) {
    WriteMarkers writeMarkers = WriteMarkersFactory.get(config.getMarkersType(), hoodieTable, instantTime);
    writeMarkers.createIfNotExists(partitionPath, dataFileName, getIOType());
  }

  @Override
  boolean needsUpdateLocation() {
    return false;
  }

  @Override
  public void closeGracefully() {
    if (isClosed()) {
      return;
    }
    try {
      close();
    } catch (Throwable throwable) {
      log.error("Failed to close the MERGE handle", throwable);
      try {
        storage.deleteFile(newFilePath);
        log.info("Successfully deleted the intermediate MERGE data file: {}", newFilePath);
      } catch (IOException e) {
        log.warn("Failed to delete the intermediate MERGE data file: {}", newFilePath, e);
      }
    }
  }

  @Override
  public StoragePath getWritePath() {
    return newFilePath;
  }
}
