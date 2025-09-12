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

package org.apache.hudi.io.storage.row;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.model.HoodieInternalRow;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordDelegate;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieInsertException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Create handle with InternalRow for datasource implementation of bulk insert.
 */
public class HoodieRowCreateHandle implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(HoodieRowCreateHandle.class);
  private static final AtomicLong GLOBAL_SEQ_NO = new AtomicLong(1);

  private final HoodieTable table;
  private final HoodieWriteConfig writeConfig;

  private final String partitionPath;
  private final StoragePath path;
  private final String fileId;

  private final boolean populateMetaFields;

  private final UTF8String fileName;
  private final UTF8String commitTime;
  private final Function<Long, String> seqIdGenerator;

  private final boolean shouldPreserveHoodieMetadata;

  private final HoodieTimer currTimer;

  protected final HoodieInternalRowFileWriter fileWriter;
  protected final WriteStatus writeStatus;
  private final HoodieRecordLocation newRecordLocation;

  public HoodieRowCreateHandle(HoodieTable table,
                               HoodieWriteConfig writeConfig,
                               String partitionPath,
                               String fileId,
                               String instantTime,
                               int taskPartitionId,
                               long taskId,
                               long taskEpochId,
                               StructType structType) {
    this(table, writeConfig, partitionPath, fileId, instantTime, taskPartitionId, taskId, taskEpochId,
        structType, false);
  }

  public HoodieRowCreateHandle(HoodieTable table,
                               HoodieWriteConfig writeConfig,
                               String partitionPath,
                               String fileId,
                               String instantTime,
                               int taskPartitionId,
                               long taskId,
                               long taskEpochId,
                               StructType structType,
                               boolean shouldPreserveHoodieMetadata) {
    this.partitionPath = partitionPath;
    this.table = table;
    this.writeConfig = writeConfig;
    this.fileId = fileId;
    this.newRecordLocation = new HoodieRecordLocation(instantTime, fileId);

    this.currTimer = HoodieTimer.start();

    HoodieStorage storage = table.getStorage();

    String writeToken = getWriteToken(taskPartitionId, taskId, taskEpochId);
    String fileName = FSUtils.makeBaseFileName(instantTime, writeToken, this.fileId,
        table.getBaseFileExtension());
    this.path = makeNewPath(storage, partitionPath, fileName, writeConfig);

    this.populateMetaFields = writeConfig.populateMetaFields();
    this.fileName = UTF8String.fromString(path.getName());
    this.commitTime = UTF8String.fromString(instantTime);
    this.seqIdGenerator = (id) -> HoodieRecord.generateSequenceId(instantTime, taskPartitionId, id);

    this.writeStatus = new WriteStatus(table.shouldTrackSuccessRecords(),
        writeConfig.getWriteStatusFailureFraction());
    this.shouldPreserveHoodieMetadata = shouldPreserveHoodieMetadata;

    writeStatus.setPartitionPath(partitionPath);
    writeStatus.setFileId(fileId);
    writeStatus.setStat(new HoodieWriteStat());
    try {
      HoodiePartitionMetadata partitionMetadata =
          new HoodiePartitionMetadata(
              storage,
              instantTime,
              new StoragePath(writeConfig.getBasePath()),
              FSUtils.constructAbsolutePath(writeConfig.getBasePath(), partitionPath),
              table.getPartitionMetafileFormat());
      partitionMetadata.trySave();

      createMarkerFile(partitionPath, fileName, instantTime, table, writeConfig);

      this.fileWriter = HoodieInternalRowFileWriterFactory.getInternalRowFileWriter(path, table, writeConfig, structType);
    } catch (IOException e) {
      throw new HoodieInsertException("Failed to initialize file writer for path " + path, e);
    }

    LOG.info("New handle created for partition: {} with fileId {}", partitionPath, fileId);
  }

  /**
   * Writes an {@link InternalRow} to the underlying HoodieInternalRowFileWriter. Before writing, value for meta columns are computed as required
   * and wrapped in {@link HoodieInternalRow}. {@link HoodieInternalRow} is what gets written to HoodieInternalRowFileWriter.
   *
   * @param row instance of {@link InternalRow} that needs to be written to the fileWriter.
   * @throws IOException
   */
  public void write(InternalRow row) throws IOException {
    if (populateMetaFields) {
      writeRow(row);
    } else {
      writeRowNoMetaFields(row);
    }
  }

  private void writeRow(InternalRow row) {
    try {
      // NOTE: PLEASE READ THIS CAREFULLY BEFORE MODIFYING
      //       This code lays in the hot-path, and substantial caution should be
      //       exercised making changes to it to minimize amount of excessive:
      //          - Conversions b/w Spark internal types and JVM native ones (like [[UTF8String]]
      //          and [[String]])
      //          - Repeated computations (for ex, converting file-path to [[UTF8String]] over and
      //          over again)
      UTF8String recordKey = row.getUTF8String(HoodieRecord.RECORD_KEY_META_FIELD_ORD);
      UTF8String partitionPath = row.getUTF8String(HoodieRecord.PARTITION_PATH_META_FIELD_ORD);
      // This is the only meta-field that is generated dynamically, hence conversion b/w
      // [[String]] and [[UTF8String]] is unavoidable if preserveHoodieMetadata is false
      UTF8String seqId = shouldPreserveHoodieMetadata ? row.getUTF8String(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD_ORD)
          : UTF8String.fromString(seqIdGenerator.apply(GLOBAL_SEQ_NO.getAndIncrement()));
      UTF8String writeCommitTime = shouldPreserveHoodieMetadata ? row.getUTF8String(HoodieRecord.COMMIT_TIME_METADATA_FIELD_ORD)
          : commitTime;

      InternalRow updatedRow = new HoodieInternalRow(writeCommitTime, seqId, recordKey,
          partitionPath, fileName, row, true);
      try {
        fileWriter.writeRow(recordKey, updatedRow);
        // NOTE: To avoid conversion on the hot-path we only convert [[UTF8String]] into [[String]]
        //       in cases when successful records' writes are being tracked
        HoodieRecordDelegate recordDelegate = writeStatus.isTrackingSuccessfulWrites()
            ? HoodieRecordDelegate.create(recordKey.toString(), partitionPath.toString(), null, newRecordLocation) : null;
        writeStatus.markSuccess(recordDelegate, Option.empty());
      } catch (Exception t) {
        writeStatus.markFailure(recordKey.toString(), partitionPath.toString(), t);
      }
    } catch (Exception e) {
      writeStatus.setGlobalError(e);
      throw e;
    }
  }

  private void writeRowNoMetaFields(InternalRow row) {
    try {
      // TODO make sure writing w/ and w/o meta fields is consistent (currently writing w/o
      //      meta-fields would fail if any record will, while when writing w/ meta-fields it won't)
      fileWriter.writeRow(row);
      // when metafields disabled, we do not need to track individual records
      writeStatus.markSuccess((HoodieRecordDelegate) null, Option.empty());
    } catch (Exception e) {
      writeStatus.setGlobalError(e);
      throw new HoodieException("Exception thrown while writing spark InternalRows to file ", e);
    }
  }

  /**
   * Returns {@code true} if this handle can take in more writes. else {@code false}.
   */
  public boolean canWrite() {
    return fileWriter.canWrite();
  }

  /**
   * Closes the {@link HoodieRowCreateHandle} and returns an instance of {@link WriteStatus} containing the stats and
   * status of the writes to this handle.
   *
   * @return the {@link WriteStatus} containing the stats and status of the writes to this handle.
   */
  public WriteStatus close() throws IOException {
    fileWriter.close();
    HoodieWriteStat stat = writeStatus.getStat();
    stat.setPartitionPath(partitionPath);
    stat.setNumWrites(writeStatus.getTotalRecords());
    stat.setNumDeletes(0);
    stat.setNumInserts(writeStatus.getTotalRecords());
    stat.setPrevCommit(HoodieWriteStat.NULL_COMMIT);
    stat.setFileId(fileId);
    stat.setPath(new StoragePath(writeConfig.getBasePath()), path);
    long fileSizeInBytes = FSUtils.getFileSize(table.getStorage(), path);
    stat.setTotalWriteBytes(fileSizeInBytes);
    stat.setFileSizeInBytes(fileSizeInBytes);
    stat.setTotalWriteErrors(writeStatus.getTotalErrorRecords());
    for (Pair<HoodieRecordDelegate, Throwable> pair : writeStatus.getFailedRecords()) {
      LOG.error("Failed to write {}", pair.getLeft(), pair.getRight());
    }
    HoodieWriteStat.RuntimeStats runtimeStats = new HoodieWriteStat.RuntimeStats();
    runtimeStats.setTotalCreateTime(currTimer.endTimer());
    stat.setRuntimeStats(runtimeStats);
    return writeStatus;
  }

  public String getFileName() {
    return path.getName();
  }

  private static StoragePath makeNewPath(HoodieStorage storage, String partitionPath, String fileName, HoodieWriteConfig writeConfig) {
    StoragePath path = new StoragePath(writeConfig.getBasePath(), partitionPath);
    try {
      if (!storage.exists(path)) {
        storage.createDirectory(path); // create a new partition as needed.
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to make dir " + path, e);
    }
    return new StoragePath(path, fileName);
  }

  /**
   * Creates an empty marker file corresponding to storage writer path.
   *
   * @param partitionPath Partition path
   */
  private static void createMarkerFile(String partitionPath,
                                       String dataFileName,
                                       String instantTime,
                                       HoodieTable<?, ?, ?, ?> table,
                                       HoodieWriteConfig writeConfig) {
    WriteMarkersFactory.get(writeConfig.getMarkersType(), table, instantTime)
        .create(partitionPath, dataFileName, IOType.CREATE);
  }

  // TODO extract to utils
  private static String getWriteToken(int taskPartitionId, long taskId, long taskEpochId) {
    return taskPartitionId + "-" + taskId + "-" + taskEpochId;
  }

}
