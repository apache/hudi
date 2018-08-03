/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.io;

import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodiePartitionMetadata;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordLocation;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.model.HoodieWriteStat;
import com.uber.hoodie.common.model.HoodieWriteStat.RuntimeStats;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.ReflectionUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieInsertException;
import com.uber.hoodie.io.storage.HoodieStorageWriter;
import com.uber.hoodie.io.storage.HoodieStorageWriterFactory;
import com.uber.hoodie.table.HoodieTable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;

public class HoodieCreateHandle<T extends HoodieRecordPayload> extends HoodieIOHandle<T> {

  private static Logger logger = LogManager.getLogger(HoodieCreateHandle.class);

  private final WriteStatus status;
  private final HoodieStorageWriter<IndexedRecord> storageWriter;
  private final Path path;
  private Path tempPath = null;
  private long recordsWritten = 0;
  private long recordsDeleted = 0;
  private Iterator<HoodieRecord<T>> recordIterator;

  public HoodieCreateHandle(HoodieWriteConfig config, String commitTime, HoodieTable<T> hoodieTable,
      String partitionPath, String fileId) {
    super(config, commitTime, hoodieTable);
    this.status = ReflectionUtils.loadClass(config.getWriteStatusClassName());
    status.setFileId(fileId);
    status.setPartitionPath(partitionPath);

    final int sparkPartitionId = TaskContext.getPartitionId();
    this.path = makeNewPath(partitionPath, sparkPartitionId, status.getFileId());
    if (config.shouldUseTempFolderForCopyOnWriteForCreate()) {
      this.tempPath = makeTempPath(partitionPath, sparkPartitionId, status.getFileId(),
          TaskContext.get().stageId(), TaskContext.get().taskAttemptId());
    }

    try {
      HoodiePartitionMetadata partitionMetadata = new HoodiePartitionMetadata(fs, commitTime,
          new Path(config.getBasePath()), new Path(config.getBasePath(), partitionPath));
      partitionMetadata.trySave(TaskContext.getPartitionId());
      this.storageWriter = HoodieStorageWriterFactory
          .getStorageWriter(commitTime, getStorageWriterPath(), hoodieTable, config, schema);
    } catch (IOException e) {
      throw new HoodieInsertException(
          "Failed to initialize HoodieStorageWriter for path " + getStorageWriterPath(), e);
    }
    logger.info("New InsertHandle for partition :" + partitionPath + " with fileId " + fileId);
  }

  public HoodieCreateHandle(HoodieWriteConfig config, String commitTime, HoodieTable<T> hoodieTable,
      String partitionPath, String fileId, Iterator<HoodieRecord<T>> recordIterator) {
    this(config, commitTime, hoodieTable, partitionPath, fileId);
    this.recordIterator = recordIterator;
  }

  @Override
  public boolean canWrite(HoodieRecord record) {
    return storageWriter.canWrite() && record.getPartitionPath().equals(status.getPartitionPath());
  }

  /**
   * Perform the actual writing of the given record into the backing file.
   */
  public void write(HoodieRecord record, Optional<IndexedRecord> avroRecord) {
    Optional recordMetadata = record.getData().getMetadata();
    try {
      if (avroRecord.isPresent()) {
        storageWriter.writeAvroWithMetadata(avroRecord.get(), record);
        // update the new location of record, so we know where to find it next
        record.setNewLocation(new HoodieRecordLocation(commitTime, status.getFileId()));
        recordsWritten++;
      } else {
        recordsDeleted++;
      }
      status.markSuccess(record, recordMetadata);
      // deflate record payload after recording success. This will help users access payload as a
      // part of marking
      // record successful.
      record.deflate();
    } catch (Throwable t) {
      // Not throwing exception from here, since we don't want to fail the entire job
      // for a single record
      status.markFailure(record, t, recordMetadata);
      logger.error("Error writing record " + record, t);
    }
  }

  /**
   * Writes all records passed
   */
  public void write() {
    try {
      while (recordIterator.hasNext()) {
        HoodieRecord<T> record = recordIterator.next();
        write(record, record.getData().getInsertValue(schema));
      }
    } catch (IOException io) {
      throw new HoodieInsertException(
          "Failed to insert records for path " + getStorageWriterPath(), io);
    }
  }

  @Override
  public WriteStatus getWriteStatus() {
    return status;
  }

  /**
   * Performs actions to durably, persist the current changes and returns a WriteStatus object
   */
  @Override
  public WriteStatus close() {
    logger.info("Closing the file " + status.getFileId() + " as we are done with all the records "
        + recordsWritten);
    try {
      storageWriter.close();

      HoodieWriteStat stat = new HoodieWriteStat();
      stat.setNumWrites(recordsWritten);
      stat.setNumDeletes(recordsDeleted);
      stat.setPrevCommit(HoodieWriteStat.NULL_COMMIT);
      stat.setFileId(status.getFileId());
      stat.setPaths(new Path(config.getBasePath()), path, tempPath);
      stat.setTotalWriteBytes(FSUtils.getFileSize(fs, getStorageWriterPath()));
      stat.setTotalWriteErrors(status.getFailedRecords().size());
      RuntimeStats runtimeStats = new RuntimeStats();
      runtimeStats.setTotalCreateTime(timer.endTimer());
      stat.setRuntimeStats(runtimeStats);
      status.setStat(stat);

      return status;
    } catch (IOException e) {
      throw new HoodieInsertException("Failed to close the Insert Handle for path " + path, e);
    }
  }

  private Path getStorageWriterPath() {
    // Use tempPath for storage writer if possible
    return (this.tempPath == null) ? this.path : this.tempPath;
  }
}
