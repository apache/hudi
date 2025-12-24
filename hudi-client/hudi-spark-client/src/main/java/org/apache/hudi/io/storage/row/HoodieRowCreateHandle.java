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

import org.apache.hudi.client.HoodieInternalWriteStatus;
import org.apache.hudi.client.model.HoodieInternalRow;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.storage.HoodieStorageStrategy;
import org.apache.hudi.common.storage.HoodieStorageStrategyFactory;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieInsertException;
import org.apache.hudi.hadoop.CachingPath;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.bucket.BucketStrategist;
import org.apache.hudi.index.bucket.BucketStrategistFactory;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.apache.hudi.common.config.HoodieStorageConfig.DATASKETCH_ENABLED;

/**
 * Create handle with InternalRow for datasource implementation of bulk insert.
 */
public class HoodieRowCreateHandle implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LogManager.getLogger(HoodieRowCreateHandle.class);
  protected static final AtomicLong GLOBAL_SEQ_NO = new AtomicLong(1);

  protected final HoodieTable table;
  protected final HoodieWriteConfig writeConfig;

  private final String partitionPath;
  private final String version;
  private final String levelNumber;
  private final Path path;
  protected final String fileId;

  protected final boolean populateMetaFields;

  protected final String fileName;

  protected final UTF8String commitTime;
  protected final Function<Long, String> seqIdGenerator;

  protected final boolean shouldPreserveHoodieMetadata;

  private final HoodieTimer currTimer;

  protected final HoodieInternalRowFileWriter fileWriter;
  protected final HoodieInternalWriteStatus writeStatus;

  private final HoodieStorageStrategy hoodieStorageStrategy;
  protected final StructType structType;
  protected final UTF8String finalFilename;

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
    this(table, writeConfig, partitionPath, fileId, instantTime, taskPartitionId, taskId, taskEpochId,
        structType, shouldPreserveHoodieMetadata, "", "");
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
                               boolean shouldPreserveHoodieMetadata,
                               String version,
                               String levelNumber) {
    this.structType = structType;
    this.hoodieStorageStrategy = HoodieStorageStrategyFactory.getInstant(table.getMetaClient());
    this.version = version;
    this.levelNumber = levelNumber;
    if (!StringUtils.isNullOrEmpty(levelNumber) && Integer.parseInt(levelNumber) == 1 && writeConfig.getParquetBlockSize() == 32 * 1024 * 1024) {
      writeConfig.setValue(HoodieStorageConfig.PARQUET_BLOCK_SIZE.key(), HoodieStorageConfig.PARQUET_BLOCK_SIZE.defaultValue());
    }
    Option<BucketStrategist> bucketStrategist;
    if (writeConfig.getIndexType().equals(HoodieIndex.IndexType.BUCKET) && writeConfig.isBucketIndexAtPartitionLevel()) {
      bucketStrategist = Option.of(BucketStrategistFactory.getInstant(writeConfig, table.getMetaClient().getFs()));
    } else {
      bucketStrategist = Option.empty();
    }
    this.partitionPath = partitionPath;
    this.table = table;
    if (HoodieTableType.COPY_ON_WRITE.equals(table.getMetaClient().getTableType())) {
      writeConfig.getStorageConfig().setValue(DATASKETCH_ENABLED.key(), "false");
      writeConfig.setValue(DATASKETCH_ENABLED.key(), "false");
    }
    this.writeConfig = writeConfig;
    this.fileId = fileId;

    this.currTimer = HoodieTimer.start();

    FileSystem fs = table.getMetaClient().getFs();

    String writeToken = getWriteToken(taskPartitionId, taskId, taskEpochId);
    String fileName = makeFileName(instantTime, writeToken, version, levelNumber);
    this.path = makeNewPath(fs, partitionPath, fileName, instantTime);

    this.populateMetaFields = writeConfig.populateMetaFields();
    this.fileName = path.getName();
    this.finalFilename = getFileNameMetaValue(fileName);
    this.commitTime = UTF8String.fromString(instantTime);
    this.seqIdGenerator = (id) -> HoodieRecord.generateSequenceId(instantTime, taskPartitionId, id);

    this.writeStatus = new HoodieInternalWriteStatus(!table.getIndex().isImplicitWithStorage(),
        writeConfig.getWriteStatusFailureFraction());
    this.shouldPreserveHoodieMetadata = shouldPreserveHoodieMetadata;

    writeStatus.setPartitionPath(partitionPath);
    writeStatus.setFileId(fileId);
    writeStatus.setStat(new HoodieWriteStat());
    try {
      HoodiePartitionMetadata partitionMetadata =
          new HoodiePartitionMetadata(
              fs,
              instantTime,
              new Path(writeConfig.getBasePath()),
              hoodieStorageStrategy.storageLocation(partitionPath, instantTime),
              table.getPartitionMetafileFormat(),
              hoodieStorageStrategy,
              bucketStrategist.map(strategist -> strategist.computeBucketNumber(partitionPath)));
      partitionMetadata.trySave(taskPartitionId);

      createMarkerFile(partitionPath, fileName, instantTime, table, writeConfig);

      this.fileWriter = HoodieInternalRowFileWriterFactory.getInternalRowFileWriter(path, table, writeConfig, structType);
    } catch (IOException e) {
      throw new HoodieInsertException("Failed to initialize file writer for path " + path, e);
    }

    LOG.info("New handle created for partition: " + partitionPath + " with fileId " + fileId);
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
      writeRow(row, null);
    } else {
      writeRowNoMetaFields(row);
    }
  }

  public String makeFileName(String instantTime, String writeToken, String version, String levelNumber) {
    return FSUtils.makeBaseFileName(instantTime, writeToken, this.fileId, table.getBaseFileExtension());
  }

  protected void writeRow(InternalRow row, UTF8String recordKey) {
    try {
      if (recordKey == null) {
        recordKey = row.getUTF8String(HoodieRecord.RECORD_KEY_META_FIELD_ORD);
      }
      UTF8String partitionPath = row.getUTF8String(HoodieRecord.PARTITION_PATH_META_FIELD_ORD);
      // This is the only meta-field that is generated dynamically, hence conversion b/w
      // [[String]] and [[UTF8String]] is unavoidable if preserveHoodieMetadata is false
      UTF8String seqId = shouldPreserveHoodieMetadata ? row.getUTF8String(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD_ORD)
          : UTF8String.fromString(seqIdGenerator.apply(GLOBAL_SEQ_NO.getAndIncrement()));
      UTF8String writeCommitTime = shouldPreserveHoodieMetadata ? row.getUTF8String(HoodieRecord.COMMIT_TIME_METADATA_FIELD_ORD)
          : commitTime;

      InternalRow updatedRow = new HoodieInternalRow(writeCommitTime, seqId, recordKey,
          partitionPath, finalFilename, row, true);
      try {
        fileWriter.writeRow(recordKey, updatedRow);
        // NOTE: To avoid conversion on the hot-path we only convert [[UTF8String]] into [[String]]
        //       in cases when successful records' writes are being tracked
        writeStatus.markSuccess(writeStatus.isTrackingSuccessfulWrites() ? recordKey.toString() : null);
      } catch (Exception t) {
        writeStatus.markFailure(recordKey.toString(), t);
      }
    } catch (Exception e) {
      writeStatus.setGlobalError(e);
      throw e;
    }
  }

  protected UTF8String getFileNameMetaValue(String fileName) {
    return UTF8String.fromString(fileName);
  }

  protected void writeRowNoMetaFields(InternalRow row) {
    try {
      // TODO make sure writing w/ and w/o meta fields is consistent (currently writing w/o
      //      meta-fields would fail if any record will, while when writing w/ meta-fields it won't)
      fileWriter.writeRow(row);
      writeStatus.markSuccess();
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
   * Closes the {@link HoodieRowCreateHandle} and returns an instance of {@link HoodieInternalWriteStatus} containing the stats and
   * status of the writes to this handle.
   *
   * @return the {@link HoodieInternalWriteStatus} containing the stats and status of the writes to this handle.
   */
  public HoodieInternalWriteStatus close() throws IOException {
    fileWriter.close();
    HoodieWriteStat stat = writeStatus.getStat();
    stat.setPartitionPath(partitionPath);
    stat.setNumWrites(writeStatus.getTotalRecords());
    stat.setNumDeletes(0);
    stat.setNumInserts(writeStatus.getTotalRecords());
    stat.setPrevCommit(HoodieWriteStat.NULL_COMMIT);
    stat.setFileId(fileId);
    stat.setPath(hoodieStorageStrategy.getRelativePath(path));
    long fileSizeInBytes = FSUtils.getFileSize(table.getMetaClient().getFs(), path);
    stat.setTotalWriteBytes(fileSizeInBytes);
    stat.setFileSizeInBytes(fileSizeInBytes);
    stat.setTotalWriteErrors(writeStatus.getFailedRowsSize());
    HoodieWriteStat.RuntimeStats runtimeStats = new HoodieWriteStat.RuntimeStats();
    runtimeStats.setTotalCreateTime(currTimer.endTimer());
    stat.setRuntimeStats(runtimeStats);
    return writeStatus;
  }

  public String getFileName() {
    return path.getName();
  }

  protected Path makeNewPath(FileSystem fs, String partitionPath, String fileName, String instantTime) {
    Path path = hoodieStorageStrategy.storageLocation(partitionPath, instantTime);
    try {
      if (!fs.exists(path)) {
        fs.mkdirs(path); // create a new partition as needed.
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to make dir " + path, e);
    }
    return new CachingPath(path.toString(), fileName);
  }

  /**
   * Creates an empty marker file corresponding to storage writer path.
   *
   * @param partitionPath Partition path
   */
  protected void createMarkerFile(String partitionPath,
                                       String dataFileName,
                                       String instantTime,
                                       HoodieTable<?, ?, ?, ?> table,
                                       HoodieWriteConfig writeConfig) {
    stopIfAborted();
    WriteMarkersFactory.get(writeConfig.getMarkersType(), table, instantTime)
        .create(partitionPath, dataFileName, IOType.CREATE);
  }

  // TODO extract to utils
  protected static String getWriteToken(int taskPartitionId, long taskId, long taskEpochId) {
    return taskPartitionId + "-" + taskId + "-" + taskEpochId;
  }

  public boolean tryCleanWrittenFiles() {
    try {
      LOG.warn("Cleaning file " + path);
      return table.getMetaClient().getFs().delete(path, false);
    } catch (IOException e) {
      return false;
    }
  }

  private void stopIfAborted() {
    if (table.getTaskContextSupplier().isAborted()) {
      throw new HoodieIOException("The task is already aborted, stop handling new records...");
    }
  }
}
