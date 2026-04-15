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

package org.apache.hudi.io;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.exception.HoodieInsertException;
import org.apache.hudi.execution.FileMetadataWriteStatusConverter;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.execution.FileMetadataWriteStatusConverter.PREV_COMMIT;
import static org.apache.hudi.execution.FileMetadataWriteStatusConverter.TIME_TAKEN;

/**
 * Write handle that is used to work on top of files rather than on individual records.
 */
public class ExternalFileClusteringWriteHandle<T extends HoodieRecordPayload, I, K, O> extends HoodieWriteHandle<T, I, K, O> {

  private static final Logger LOG = LoggerFactory.getLogger(ExternalFileClusteringWriteHandle.class);
  private final StoragePath path;
  private final String prevCommit;

  public ExternalFileClusteringWriteHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                               String partitionPath, String fileId, TaskContextSupplier taskContextSupplier,
                               StoragePath oldFilePath) {
    super(config, instantTime, partitionPath, fileId, hoodieTable, taskContextSupplier, true);

    // Output file path.
    this.path = makeNewPath(partitionPath);
    // Get the prev commit from existing or old file.
    this.prevCommit = FSUtils.getCommitTime(oldFilePath.getName());

    // Create inProgress marker file
    createMarkerFile(partitionPath, path.getName());
    LOG.info("New ExternalFileClusteringWriteHandle for partition :" + partitionPath + " with fileId " + fileId);
  }

  /**
   * Complete writing of the file.
   * @return WriteStatuses, ideally it will be only one object.
   */
  @Override
  public List<WriteStatus> close() {
    try {
      if (!hoodieTable.getStorage().exists(path)) {
        throw new HoodieClusteringException("Output file does not exist, transformation may not have been invoked: " + path);
      }

      Map<String, Object> executionConfigs = new HashMap<>();
      executionConfigs.put(PREV_COMMIT, prevCommit);
      executionConfigs.put(TIME_TAKEN, timer.endTimer());

      this.writeStatus = generateWriteStatus(path.toString(), partitionPath, executionConfigs);
      LOG.info(String.format("ExternalFileClusteringWriteHandle for partitionPath %s fileID %s, took %d ms.",
          writeStatus.getStat().getPartitionPath(), writeStatus.getStat().getFileId(),
          writeStatus.getStat().getRuntimeStats().getTotalCreateTime()));

      return Collections.singletonList(writeStatus);
    } catch (IOException e) {
      throw new HoodieInsertException("Failed to close the ExternalFileClusteringWriteHandle for path " + path, e);
    } finally {
      markClosed();
    }
  }

  /**
   * Given a parquet file it generates WriteStatus object for a parquet file.
   * @param outputFile parquet file
   * @param partitionPath partition information of the parquet file
   * @param executionConfigs Some configs collected during execution.
   * @return WriteStatus object.
   * @throws IOException
   */
  protected WriteStatus generateWriteStatus(
      String outputFile, String partitionPath, Map<String, Object> executionConfigs) throws IOException {
    return new FileMetadataWriteStatusConverter<>(hoodieTable, config).convert(outputFile, partitionPath, executionConfigs);
  }

  @Override
  public IOType getIOType() {
    return IOType.CREATE;
  }

  public StoragePath getPath() {
    return path;
  }
}
