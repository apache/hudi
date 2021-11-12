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
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

/**
 * A {@link HoodieAppendHandle} that supports APPEND write incrementally(mini-batches).
 *
 * <p>For the first mini-batch, it initializes and sets up the next file path to write,
 * then closes the file writer. The subsequent mini-batches are appended to the same file
 * through a different append handle with same write file name.
 *
 * <p>The back-up writer may rollover on condition(for e.g, the filesystem does not support append
 * or the file size hits the configured threshold).
 */
public class FlinkAppendHandle<T extends HoodieRecordPayload, I, K, O>
    extends HoodieAppendHandle<T, I, K, O> implements MiniBatchHandle {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkAppendHandle.class);

  private boolean isClosed = false;
  private final WriteMarkers writeMarkers;

  public FlinkAppendHandle(
      HoodieWriteConfig config,
      String instantTime,
      HoodieTable<T, I, K, O> hoodieTable,
      String partitionPath,
      String fileId,
      Iterator<HoodieRecord<T>> recordItr,
      TaskContextSupplier taskContextSupplier) {
    super(config, instantTime, hoodieTable, partitionPath, fileId, recordItr, taskContextSupplier);
    this.writeMarkers = WriteMarkersFactory.get(config.getMarkersType(), hoodieTable, instantTime);
  }

  @Override
  protected void createMarkerFile(String partitionPath, String dataFileName) {
    // In some rare cases, the task was pulled up again with same write file name,
    // for e.g, reuse the small log files from last commit instant.

    // Just skip the marker creation if it already exists, the new data would append to
    // the file directly.
    writeMarkers.createIfNotExists(partitionPath, dataFileName, getIOType());
  }

  @Override
  protected boolean needsUpdateLocation() {
    return false;
  }

  @Override
  protected boolean isUpdateRecord(HoodieRecord<T> hoodieRecord) {
    // do not use the HoodieRecord operation because hoodie writer has its own
    // INSERT/MERGE bucket for 'UPSERT' semantics. For e.g, a hoodie record with fresh new key
    // and operation HoodieCdcOperation.DELETE would be put into either an INSERT bucket or UPDATE bucket.
    return hoodieRecord.getCurrentLocation() != null
        && hoodieRecord.getCurrentLocation().getInstantTime().equals("U");
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
      // The intermediate log file can still append based on the incremental MERGE semantics,
      // there is no need to delete the file.
      LOG.warn("Error while trying to dispose the APPEND handle", throwable);
    }
  }

  @Override
  public Path getWritePath() {
    return writer.getLogFile().getPath();
  }

  @Override
  public boolean shouldReplace() {
    // log files can append new data buffer directly
    return false;
  }
}
