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
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * A {@link HoodieWriteMergeHandle} that supports MERGE write incrementally(small data buffers).
 *
 * <P>This handle is needed from the second mini-batch write for COW data bucket
 * when the data bucket is written using multiple mini-batches.
 *
 * <p>For the incremental data buffer, it initializes and sets up the next file path to write,
 * then closes the file and remove the old file. Finally, the file path field in the write status
 * for the previous mini-batch ingestion will be rewritten as the final file path before committing,
 * which behaves like all the data are written into the last file.
 */
@Slf4j
public class FlinkIncrementalMergeHandle<T, I, K, O>
    extends FlinkMergeHandle<T, I, K, O>
    implements MiniBatchHandle {

  public FlinkIncrementalMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                     Iterator<HoodieRecord<T>> recordItr, String partitionPath, String fileId,
                                     TaskContextSupplier taskContextSupplier, StoragePath basePath) {
    super(config, instantTime, hoodieTable, recordItr, partitionPath, fileId, taskContextSupplier, new HoodieBaseFile(basePath.toString()));
  }

  @Override
  protected String createNewFileName(String oldFileName) {
    int rollNumber = MergeHandleUtils.calcRollNumberForBaseFile(oldFileName, writeToken);
    return newFileNameWithRollover(rollNumber);
  }

  /**
   * Use the writeToken + "-" + rollNumber as the new writeToken of a mini-batch write.
   */
  protected String newFileNameWithRollover(int rollNumber) {
    return FSUtils.makeBaseFileName(instantTime, writeToken + "-" + rollNumber,
        this.fileId, hoodieTable.getBaseFileExtension());
  }

  public void finalizeWrite() {
    // The file visibility should be kept by the configured ConsistencyGuard instance.
    try {
      storage.deleteFile(oldFilePath);
    } catch (IOException e) {
      throw new HoodieIOException("Error while cleaning the old base file: " + oldFilePath, e);
    }
  }

  @Override
  public List<WriteStatus> close() {
    if (isClosed()) {
      return getWriteStatuses();
    }
    List<WriteStatus> writeStatuses = super.close();
    finalizeWrite();
    return writeStatuses;
  }
}
