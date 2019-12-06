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

import org.apache.hudi.WriteStatus;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.HoodieWriteStat.RuntimeStats;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieInsertException;
import org.apache.hudi.io.storage.HoodieStorageWriter;
import org.apache.hudi.io.storage.HoodieStorageWriterFactory;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;

import java.io.IOException;
import java.util.Iterator;

public class HoodieCreateHandle<T extends HoodieRecordPayload> extends HoodieWriteHandle<T> {

  private static Logger logger = LogManager.getLogger(HoodieCreateHandle.class);

  private final HoodieStorageWriter<IndexedRecord> storageWriter;
  private final Path path;
  private long recordsWritten = 0;
  private long insertRecordsWritten = 0;
  private long recordsDeleted = 0;
  private Iterator<HoodieRecord<T>> recordIterator;
  private boolean useWriterSchema = false;

  public HoodieCreateHandle(HoodieWriteConfig config, String commitTime, HoodieTable<T> hoodieTable,
      String partitionPath, String fileId) {
    super(config, commitTime, fileId, hoodieTable);
    writeStatus.setFileId(fileId);
    writeStatus.setPartitionPath(partitionPath);

    this.path = makeNewPath(partitionPath);

    try {
      HoodiePartitionMetadata partitionMetadata = new HoodiePartitionMetadata(fs, commitTime,
          new Path(config.getBasePath()), FSUtils.getPartitionPath(config.getBasePath(), partitionPath));
      partitionMetadata.trySave(TaskContext.getPartitionId());
      createMarkerFile(partitionPath);
      this.storageWriter =
          HoodieStorageWriterFactory.getStorageWriter(commitTime, path, hoodieTable, config, writerSchema);
    } catch (IOException e) {
      throw new HoodieInsertException("Failed to initialize HoodieStorageWriter for path " + path, e);
    }
    logger.info("New CreateHandle for partition :" + partitionPath + " with fileId " + fileId);
  }

  /**
   * Called by the compactor code path.
   */
  public HoodieCreateHandle(HoodieWriteConfig config, String commitTime, HoodieTable<T> hoodieTable,
      String partitionPath, String fileId, Iterator<HoodieRecord<T>> recordIterator) {
    this(config, commitTime, hoodieTable, partitionPath, fileId);
    this.recordIterator = recordIterator;
    this.useWriterSchema = true;
  }

  @Override
  public boolean canWrite(HoodieRecord record) {
    return storageWriter.canWrite() && record.getPartitionPath().equals(writeStatus.getPartitionPath());
  }

  /**
   * Perform the actual writing of the given record into the backing file.
   */
  public void write(HoodieRecord record, Option<IndexedRecord> avroRecord) {
    Option recordMetadata = record.getData().getMetadata();
    try {
      if (avroRecord.isPresent()) {
        // Convert GenericRecord to GenericRecord with hoodie commit metadata in schema
        IndexedRecord recordWithMetadataInSchema = rewriteRecord((GenericRecord) avroRecord.get());
        storageWriter.writeAvroWithMetadata(recordWithMetadataInSchema, record);
        // update the new location of record, so we know where to find it next
        record.unseal();
        record.setNewLocation(new HoodieRecordLocation(instantTime, writeStatus.getFileId()));
        record.seal();
        recordsWritten++;
        insertRecordsWritten++;
      } else {
        recordsDeleted++;
      }
      writeStatus.markSuccess(record, recordMetadata);
      // deflate record payload after recording success. This will help users access payload as a
      // part of marking
      // record successful.
      record.deflate();
    } catch (Throwable t) {
      // Not throwing exception from here, since we don't want to fail the entire job
      // for a single record
      writeStatus.markFailure(record, t, recordMetadata);
      logger.error("Error writing record " + record, t);
    }
  }

  /**
   * Writes all records passed.
   */
  public void write() {
    try {
      while (recordIterator.hasNext()) {
        HoodieRecord<T> record = recordIterator.next();
        if (useWriterSchema) {
          write(record, record.getData().getInsertValue(writerSchema));
        } else {
          write(record, record.getData().getInsertValue(originalSchema));
        }
      }
    } catch (IOException io) {
      throw new HoodieInsertException("Failed to insert records for path " + path, io);
    }
  }

  @Override
  public WriteStatus getWriteStatus() {
    return writeStatus;
  }

  /**
   * Performs actions to durably, persist the current changes and returns a WriteStatus object.
   */
  @Override
  public WriteStatus close() {
    logger
        .info("Closing the file " + writeStatus.getFileId() + " as we are done with all the records " + recordsWritten);
    try {

      storageWriter.close();

      HoodieWriteStat stat = new HoodieWriteStat();
      stat.setPartitionPath(writeStatus.getPartitionPath());
      stat.setNumWrites(recordsWritten);
      stat.setNumDeletes(recordsDeleted);
      stat.setNumInserts(insertRecordsWritten);
      stat.setPrevCommit(HoodieWriteStat.NULL_COMMIT);
      stat.setFileId(writeStatus.getFileId());
      stat.setPath(new Path(config.getBasePath()), path);
      long fileSizeInBytes = FSUtils.getFileSize(fs, path);
      stat.setTotalWriteBytes(fileSizeInBytes);
      stat.setFileSizeInBytes(fileSizeInBytes);
      stat.setTotalWriteErrors(writeStatus.getTotalErrorRecords());
      RuntimeStats runtimeStats = new RuntimeStats();
      runtimeStats.setTotalCreateTime(timer.endTimer());
      stat.setRuntimeStats(runtimeStats);
      writeStatus.setStat(stat);

      logger.info(String.format("CreateHandle for partitionPath %s fileID %s, took %d ms.", stat.getPartitionPath(),
          stat.getFileId(), runtimeStats.getTotalCreateTime()));

      return writeStatus;
    } catch (IOException e) {
      throw new HoodieInsertException("Failed to close the Insert Handle for path " + path, e);
    }
  }
}
