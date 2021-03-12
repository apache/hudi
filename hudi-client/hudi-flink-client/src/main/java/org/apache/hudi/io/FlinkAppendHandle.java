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
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A {@link HoodieAppendHandle} that supports append write incrementally(mini-batches).
 *
 * <p>For the first mini-batch, it initialize and set up the next file path to write,
 * but does not close the file writer until all the mini-batches write finish. Each mini-batch
 * data are appended to this handle, the back-up writer may rollover on condition.
 *
 * @param <T> Payload type
 * @param <I> Input type
 * @param <K> Key type
 * @param <O> Output type
 */
public class FlinkAppendHandle<T extends HoodieRecordPayload, I, K, O> extends HoodieAppendHandle<T, I, K, O> implements MiniBatchHandle {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkAppendHandle.class);
  private boolean needBootStrap = true;
  // Total number of bytes written to file
  private long sizeInBytes = 0;

  public FlinkAppendHandle(
      HoodieWriteConfig config,
      String instantTime,
      HoodieTable<T, I, K, O> hoodieTable,
      String partitionPath,
      String fileId,
      Iterator<HoodieRecord<T>> recordItr,
      TaskContextSupplier taskContextSupplier) {
    super(config, instantTime, hoodieTable, partitionPath, fileId, recordItr, taskContextSupplier);
  }

  @Override
  protected boolean needsUpdateLocation() {
    return false;
  }

  @Override
  protected boolean isUpdateRecord(HoodieRecord<T> hoodieRecord) {
    return hoodieRecord.getCurrentLocation() != null
        && hoodieRecord.getCurrentLocation().getInstantTime().equals("U");
  }

  /**
   * Returns whether there is need to bootstrap this file handle.
   * E.G. the first time that the handle is created.
   */
  public boolean isNeedBootStrap() {
    return this.needBootStrap;
  }

  /**
   * Appends new records into this append handle.
   * @param recordItr The new records iterator
   */
  public void appendNewRecords(Iterator<HoodieRecord<T>> recordItr) {
    this.recordItr = recordItr;
  }

  @Override
  public List<WriteStatus> close() {
    needBootStrap = false;
    // flush any remaining records to disk
    appendDataAndDeleteBlocks(header);
    // need to fix that the incremental write size in bytes may be lost
    List<WriteStatus> ret = new ArrayList<>(statuses);
    statuses.clear();
    return ret;
  }

  @Override
  public void finishWrite() {
    LOG.info("Closing the file " + writeStatus.getFileId() + " as we are done with all the records " + recordsWritten);
    try {
      if (writer != null) {
        writer.close();
      }
    } catch (IOException e) {
      throw new HoodieUpsertException("Failed to close append handle", e);
    }
  }
}
