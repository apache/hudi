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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.commit.BucketType;

import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.List;

/**
 * Flink mini-batch append handle for native MOR log files.
 */
@Slf4j
public class FlinkNativeLogAppendHandle<T, I, K, O>
    extends HoodieNativeLogAppendHandle<T, I, K, O> implements MiniBatchHandle {

  private boolean isClosed = false;
  private final BucketType bucketType;

  public FlinkNativeLogAppendHandle(
      HoodieWriteConfig config,
      String instantTime,
      HoodieTable<T, I, K, O> hoodieTable,
      String partitionPath,
      String fileId,
      BucketType bucketType,
      Iterator<HoodieRecord<T>> recordItr,
      TaskContextSupplier taskContextSupplier) {
    super(config, instantTime, hoodieTable, partitionPath, fileId, recordItr, taskContextSupplier);
    this.bucketType = bucketType;
  }

  @Override
  public boolean canWrite(HoodieRecord record) {
    return true;
  }

  @Override
  protected boolean needsUpdateLocation() {
    return false;
  }

  @Override
  protected boolean isUpdateRecord(HoodieRecord<T> hoodieRecord) {
    return bucketType == BucketType.UPDATE;
  }

  @Override
  public List<WriteStatus> close() {
    try {
      return super.close();
    } finally {
      this.isClosed = true;
    }
  }

  @Override
  public void closeGracefully() {
    if (isClosed) {
      return;
    }
    try {
      close();
    } catch (Throwable throwable) {
      log.warn("Failed to close the APPEND handle", throwable);
    }
  }

  @Override
  public StoragePath getWritePath() {
    return getLogFilePath();
  }
}
