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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieInsertException;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A {@link HoodieCreateHandle} that supports create write incrementally(mini-batches).
 *
 * <p>For the first mini-batch, it initialize and set up the next file path to write,
 * but does not close the file writer until all the mini-batches write finish. Each mini-batch
 * data are appended to the same file.
 *
 * @param <T> Payload type
 * @param <I> Input type
 * @param <K> Key type
 * @param <O> Output type
 */
public class FlinkCreateHandle<T extends HoodieRecordPayload, I, K, O>
    extends HoodieCreateHandle<T, I, K, O> implements MiniBatchHandle {

  private static final Logger LOG = LogManager.getLogger(FlinkCreateHandle.class);
  private long lastFileSize = 0L;

  public FlinkCreateHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                           String partitionPath, String fileId, TaskContextSupplier taskContextSupplier) {
    this(config, instantTime, hoodieTable, partitionPath, fileId, getWriterSchemaIncludingAndExcludingMetadataPair(config),
        taskContextSupplier);
  }

  public FlinkCreateHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                           String partitionPath, String fileId, Pair<Schema, Schema> writerSchemaIncludingAndExcludingMetadataPair,
                           TaskContextSupplier taskContextSupplier) {
    super(config, instantTime, hoodieTable, partitionPath, fileId, writerSchemaIncludingAndExcludingMetadataPair,
        taskContextSupplier);
  }

  /**
   * Called by the compactor code path.
   */
  public FlinkCreateHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                           String partitionPath, String fileId, Map<String, HoodieRecord<T>> recordMap,
                           TaskContextSupplier taskContextSupplier) {
    super(config, instantTime, hoodieTable, partitionPath, fileId, recordMap, taskContextSupplier);
  }

  /**
   * Get the incremental write status. In mini-batch write mode,
   * this handle would be reused for a checkpoint bucket(the bucket is appended as mini-batches),
   * thus, after a mini-batch append finish, we do not close the underneath writer but return
   * the incremental WriteStatus instead.
   *
   * @return the incremental write status
   */
  private WriteStatus getIncrementalWriteStatus() {
    try {
      setUpWriteStatus();
      // reset the write status
      recordsWritten = 0;
      recordsDeleted = 0;
      insertRecordsWritten = 0;
      timer = new HoodieTimer().startTimer();
      writeStatus.setTotalErrorRecords(0);
      return writeStatus;
    } catch (IOException e) {
      throw new HoodieInsertException("Failed to close the Insert Handle for path " + path, e);
    }
  }

  /**
   * Set up the write status.
   *
   * @throws IOException if error occurs
   */
  private void setUpWriteStatus() throws IOException {
    long fileSizeInBytes = fileWriter.getBytesWritten();
    long incFileSizeInBytes = fileSizeInBytes - lastFileSize;
    this.lastFileSize = fileSizeInBytes;
    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setPartitionPath(writeStatus.getPartitionPath());
    stat.setNumWrites(recordsWritten);
    stat.setNumDeletes(recordsDeleted);
    stat.setNumInserts(insertRecordsWritten);
    stat.setPrevCommit(HoodieWriteStat.NULL_COMMIT);
    stat.setFileId(writeStatus.getFileId());
    stat.setPath(new Path(config.getBasePath()), path);
    stat.setTotalWriteBytes(incFileSizeInBytes);
    stat.setFileSizeInBytes(fileSizeInBytes);
    stat.setTotalWriteErrors(writeStatus.getTotalErrorRecords());
    HoodieWriteStat.RuntimeStats runtimeStats = new HoodieWriteStat.RuntimeStats();
    runtimeStats.setTotalCreateTime(timer.endTimer());
    stat.setRuntimeStats(runtimeStats);
    writeStatus.setStat(stat);
  }

  public void finishWrite() {
    LOG.info("Closing the file " + writeStatus.getFileId() + " as we are done with all the records " + recordsWritten);
    try {
      fileWriter.close();
    } catch (IOException e) {
      throw new HoodieInsertException("Failed to close the Insert Handle for path " + path, e);
    }
  }

  /**
   * Performs actions to durably, persist the current changes and returns a WriteStatus object.
   */
  @Override
  public List<WriteStatus> close() {
    return Collections.singletonList(getIncrementalWriteStatus());
  }
}
