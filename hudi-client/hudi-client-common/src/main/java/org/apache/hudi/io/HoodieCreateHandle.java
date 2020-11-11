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

import org.apache.avro.Schema;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.HoodieWriteStat.RuntimeStats;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieInsertException;
import org.apache.hudi.io.storage.HoodieFileWriter;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class HoodieCreateHandle<T extends HoodieRecordPayload, I, K, O> extends HoodieWriteHandle<T, I, K, O> {

  private static final Logger LOG = LogManager.getLogger(HoodieCreateHandle.class);

  private final HoodieFileWriter<IndexedRecord> fileWriter;
  private final Path path;
  private long recordsWritten = 0;
  private long insertRecordsWritten = 0;
  private long recordsDeleted = 0;
  private Map<String, HoodieRecord<T>> recordMap;
  private boolean useWriterSchema = false;

  public HoodieCreateHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                            String partitionPath, String fileId, TaskContextSupplier taskContextSupplier) {
    this(config, instantTime, hoodieTable, partitionPath, fileId, getWriterSchemaIncludingAndExcludingMetadataPair(config),
        taskContextSupplier);
  }

  public HoodieCreateHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                            String partitionPath, String fileId, Pair<Schema, Schema> writerSchemaIncludingAndExcludingMetadataPair,
                            TaskContextSupplier taskContextSupplier) {
    super(config, instantTime, partitionPath, fileId, hoodieTable, writerSchemaIncludingAndExcludingMetadataPair,
        taskContextSupplier);
    writeStatus.setFileId(fileId);
    writeStatus.setPartitionPath(partitionPath);

    this.path = makeNewPath(partitionPath);

    try {
      HoodiePartitionMetadata partitionMetadata = new HoodiePartitionMetadata(fs, instantTime,
          new Path(config.getBasePath()), FSUtils.getPartitionPath(config.getBasePath(), partitionPath));
      partitionMetadata.trySave(getPartitionId());
      createMarkerFile(partitionPath, FSUtils.makeDataFileName(this.instantTime, this.writeToken, this.fileId, hoodieTable.getBaseFileExtension()));
      this.fileWriter = HoodieFileWriterFactory.getFileWriter(instantTime, path, hoodieTable, config, writerSchemaWithMetafields, this.taskContextSupplier);
    } catch (IOException e) {
      throw new HoodieInsertException("Failed to initialize HoodieStorageWriter for path " + path, e);
    }
    LOG.info("New CreateHandle for partition :" + partitionPath + " with fileId " + fileId);
  }

  /**
   * Called by the compactor code path.
   */
  public HoodieCreateHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
      String partitionPath, String fileId, Map<String, HoodieRecord<T>> recordMap,
      TaskContextSupplier taskContextSupplier) {
    this(config, instantTime, hoodieTable, partitionPath, fileId, taskContextSupplier);
    this.recordMap = recordMap;
    this.useWriterSchema = true;
  }

  @Override
  public boolean canWrite(HoodieRecord record) {
    return fileWriter.canWrite() && record.getPartitionPath().equals(writeStatus.getPartitionPath());
  }

  /**
   * Perform the actual writing of the given record into the backing file.
   */
  @Override
  public void write(HoodieRecord record, Option<IndexedRecord> avroRecord) {
    Option recordMetadata = record.getData().getMetadata();
    try {
      if (avroRecord.isPresent()) {
        // Convert GenericRecord to GenericRecord with hoodie commit metadata in schema
        IndexedRecord recordWithMetadataInSchema = rewriteRecord((GenericRecord) avroRecord.get());
        fileWriter.writeAvroWithMetadata(recordWithMetadataInSchema, record);
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
      LOG.error("Error writing record " + record, t);
    }
  }

  /**
   * Writes all records passed.
   */
  public void write() {
    Iterator<String> keyIterator;
    if (hoodieTable.requireSortedRecords()) {
      // Sorting the keys limits the amount of extra memory required for writing sorted records
      keyIterator = recordMap.keySet().stream().sorted().iterator();
    } else {
      keyIterator = recordMap.keySet().stream().iterator();
    }
    try {
      while (keyIterator.hasNext()) {
        final String key = keyIterator.next();
        HoodieRecord<T> record = recordMap.get(key);
        if (useWriterSchema) {
          write(record, record.getData().getInsertValue(writerSchemaWithMetafields));
        } else {
          write(record, record.getData().getInsertValue(writerSchema));
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

  @Override
  public IOType getIOType() {
    return IOType.CREATE;
  }

  /**
   * Performs actions to durably, persist the current changes and returns a WriteStatus object.
   */
  @Override
  public WriteStatus close() {
    LOG
        .info("Closing the file " + writeStatus.getFileId() + " as we are done with all the records " + recordsWritten);
    try {

      fileWriter.close();

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

      LOG.info(String.format("CreateHandle for partitionPath %s fileID %s, took %d ms.", stat.getPartitionPath(),
          stat.getFileId(), runtimeStats.getTotalCreateTime()));

      return writeStatus;
    } catch (IOException e) {
      throw new HoodieInsertException("Failed to close the Insert Handle for path " + path, e);
    }
  }
}
