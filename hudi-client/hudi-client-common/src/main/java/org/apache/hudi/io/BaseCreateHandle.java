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
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.model.MetadataValues;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieInsertException;
import org.apache.hudi.io.storage.HoodieFileWriter;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.fs.Path;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Abstract base class for CreateHandle
 * <p>
 * This class provides common functionality used by CreateHandle.
 * It manages the lifecycle of file creation including:
 * <ul>
 *   <li>File writer initialization and management</li>
 *   <li>Record writing and validation</li>
 *   <li>Partition metadata and marker file creation</li>
 *   <li>Write statistics tracking (records written, deleted, file size, ...)</li>
 * </ul>
 * <p>
 * Concrete implementations should extend this class and implement table format-specific
 * details for file creation and record writing.
 */
public abstract class BaseCreateHandle<T, I, K, O> extends HoodieWriteHandle<T, I, K, O> {
  private static final Logger LOG = LoggerFactory.getLogger(BaseCreateHandle.class);

  protected HoodieTable hoodieTable;
  protected HoodieFileWriter fileWriter;
  protected final Path path;
  protected long recordsWritten = 0;
  protected long insertRecordsWritten = 0;
  protected long recordsDeleted = 0;
  protected final boolean preserveMetadata;

  protected Map<String, HoodieRecord<T>> recordMap;
  protected boolean useWriterSchema = false;

  public BaseCreateHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                          String partitionPath, String fileId, Option<Schema> overriddenSchema,
                          TaskContextSupplier taskContextSupplier, boolean preserveMetadata) {
    super(config, instantTime, partitionPath, fileId, hoodieTable, overriddenSchema, taskContextSupplier);
    this.hoodieTable = hoodieTable;
    this.preserveMetadata = preserveMetadata;
    writeStatus.setFileId(fileId);
    writeStatus.setPartitionPath(partitionPath);
    writeStatus.setStat(new HoodieWriteStat());
    this.path = makeNewPath(partitionPath);
  }

  /**
   * Creates partition metadata and marker file for tracking the write operation.
   */
  protected void createPartitionMetadataAndMarkerFile() {
    HoodiePartitionMetadata partitionMetadata = new HoodiePartitionMetadata(fs, instantTime,
        new Path(config.getBasePath()), FSUtils.getPartitionPath(config.getBasePath(), partitionPath),
            hoodieTable.getPartitionMetafileFormat());
    partitionMetadata.trySave(getPartitionId());
    createMarkerFile(partitionPath, FSUtils.makeBaseFileName(this.instantTime,
        this.writeToken,this.fileId, hoodieTable.getBaseFileExtension()));
  }

  /**
   * Writes all records stored in the record map.
   */
  public void write() {
    Iterator<String> keyIterator;
    if (hoodieTable.requireSortedRecords()) {
      // Sorting the keys limits the amount of extra memory required for writing sorted records
      keyIterator = recordMap.keySet().stream().sorted().iterator();
    } else {
      keyIterator = recordMap.keySet().stream().iterator();
    }
    while (keyIterator.hasNext()) {
      final String key = keyIterator.next();
      HoodieRecord<T> record = recordMap.get(key);
      write(record, useWriterSchema ? writeSchemaWithMetaFields : writeSchema, config.getProps());
    }
  }

  /**
   * Checks if the given record can be written to this handle.
   *
   * @param record The record to check
   * @return true if the record can be written to this handle, false otherwise
   */
  @Override
  public boolean canWrite(HoodieRecord record) {
    return (fileWriter.canWrite() && record.getPartitionPath().equals(writeStatus.getPartitionPath()))
      || layoutControlsNumFiles();
  }

  @Override
  public IOType getIOType() {
    return IOType.CREATE;
  }

  /**
   * Performs the actual writing of the given record into the current file.
   *
   * @param record The record to write
   * @param schema The schema to use for writing
   * @param props  Additional properties for writing
   */
  @Override
  protected void doWrite(HoodieRecord record, Schema schema, TypedProperties props) {
    Option<Map<String, String>> recordMetadata = record.getMetadata();
    try {
      if (!HoodieOperation.isDelete(record.getOperation()) && !record.isDelete(schema, config.getProps())) {
        if (record.shouldIgnore(schema, config.getProps())) {
          return;
        }

        writeRecordToFile(record, schema, config.getProps());

        // Update the new location of record, so we know where to find it next
        record.unseal();
        record.setNewLocation(newRecordLocation);
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
      LOG.error("Error writing record {}", record, t);
    }
  }

  /**
   * Writes a single record to the file with appropriate metadata.
   *
   * @param record The record to write
   * @param schema The schema to use for writing
   * @param props  Additional properties for writing
   * @throws IOException if an error occurs during writing
   */
  protected void writeRecordToFile(HoodieRecord record, Schema schema, TypedProperties props) throws IOException {
    MetadataValues metadataValues = new MetadataValues().setFileName(path.getName());
    HoodieRecord preparedRecord = record.prependMetaFields(schema, writeSchemaWithMetaFields, metadataValues, props);

    if (preserveMetadata) {
      fileWriter.write(record.getRecordKey(), preparedRecord, writeSchemaWithMetaFields);
    } else {
      fileWriter.writeWithMetadata(record.getKey(), preparedRecord, writeSchemaWithMetaFields);
    }
  }

  /**
   * Closes the handle and conducts any post-processing
   *
   * @return A singleton list containing the WriteStatus for this handle
   * @throws HoodieInsertException if an error occurs during closing
   */
  @Override
  public List<WriteStatus> close() {
    LOG.info("Closing the file {} as we are done with all the records {}", writeStatus.getFileId(), recordsWritten);
    try {
      if (isClosed()) {
        // Handle has already been closed
        return Collections.emptyList();
      }

      markClosed();

      if (fileWriter != null) {
        fileWriter.close();
        fileWriter = null;
      }

      setupWriteStatus();

      LOG.info("CreateHandle for partitionPath {} fileID {}, took {} ms.",
          writeStatus.getStat().getPartitionPath(), writeStatus.getStat().getFileId(),
          writeStatus.getStat().getRuntimeStats().getTotalCreateTime());

      return Collections.singletonList(writeStatus);
    } catch (IOException e) {
      throw new HoodieInsertException("Failed to close the Insert Handle for path " + path, e);
    }
  }

  /**
   * Sets up the write status with statistics about the write operation.
   * Includes information about records written, deleted, file size, and runtime metrics.
   *
   * @throws IOException if an error occurs while gathering file statistics
   */
  protected void setupWriteStatus() throws IOException {
    HoodieWriteStat stat = writeStatus.getStat();
    stat.setPartitionPath(writeStatus.getPartitionPath());
    stat.setNumWrites(recordsWritten);
    stat.setNumDeletes(recordsDeleted);
    stat.setNumInserts(insertRecordsWritten);
    stat.setPrevCommit(HoodieWriteStat.NULL_COMMIT);
    stat.setFileId(writeStatus.getFileId());
    stat.setPath(new Path(config.getBasePath()), path);
    stat.setTotalWriteErrors(writeStatus.getTotalErrorRecords());

    long fileSize = FSUtils.getFileSize(fs, path);
    stat.setTotalWriteBytes(fileSize);
    stat.setFileSizeInBytes(fileSize);

    HoodieWriteStat.RuntimeStats runtimeStats = new HoodieWriteStat.RuntimeStats();
    runtimeStats.setTotalCreateTime(timer.endTimer());
    stat.setRuntimeStats(runtimeStats);
  }
}
