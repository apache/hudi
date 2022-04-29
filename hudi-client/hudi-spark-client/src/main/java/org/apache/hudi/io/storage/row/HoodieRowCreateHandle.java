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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieInsertException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Create handle with InternalRow for datasource implementation of bulk insert.
 */
public class HoodieRowCreateHandle implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LogManager.getLogger(HoodieRowCreateHandle.class);
  private static final AtomicLong SEQGEN = new AtomicLong(1);

  private final String instantTime;
  private final int taskPartitionId;
  private final long taskId;
  private final long taskEpochId;
  private final HoodieTable table;
  private final HoodieWriteConfig writeConfig;
  protected final HoodieInternalRowFileWriter fileWriter;
  private final String partitionPath;
  private final Path path;
  private final String fileId;
  private final FileSystem fs;
  protected final HoodieInternalWriteStatus writeStatus;
  private final HoodieTimer currTimer;

  public HoodieRowCreateHandle(HoodieTable table, HoodieWriteConfig writeConfig, String partitionPath, String fileId,
                               String instantTime, int taskPartitionId, long taskId, long taskEpochId,
                               StructType structType) {
    this.partitionPath = partitionPath;
    this.table = table;
    this.writeConfig = writeConfig;
    this.instantTime = instantTime;
    this.taskPartitionId = taskPartitionId;
    this.taskId = taskId;
    this.taskEpochId = taskEpochId;
    this.fileId = fileId;
    this.currTimer = new HoodieTimer();
    this.currTimer.startTimer();
    this.fs = table.getMetaClient().getFs();
    this.path = makeNewPath(partitionPath);
    this.writeStatus = new HoodieInternalWriteStatus(!table.getIndex().isImplicitWithStorage(),
        writeConfig.getWriteStatusFailureFraction());
    writeStatus.setPartitionPath(partitionPath);
    writeStatus.setFileId(fileId);
    writeStatus.setStat(new HoodieWriteStat());
    try {
      HoodiePartitionMetadata partitionMetadata =
          new HoodiePartitionMetadata(
              fs,
              instantTime,
              new Path(writeConfig.getBasePath()),
              FSUtils.getPartitionPath(writeConfig.getBasePath(), partitionPath),
              table.getPartitionMetafileFormat());
      partitionMetadata.trySave(taskPartitionId);
      createMarkerFile(partitionPath, FSUtils.makeDataFileName(this.instantTime, getWriteToken(), this.fileId, table.getBaseFileExtension()));
      this.fileWriter = createNewFileWriter(path, table, writeConfig, structType);
    } catch (IOException e) {
      throw new HoodieInsertException("Failed to initialize file writer for path " + path, e);
    }
    LOG.info("New handle created for partition :" + partitionPath + " with fileId " + fileId);
  }

  /**
   * Writes an {@link InternalRow} to the underlying HoodieInternalRowFileWriter. Before writing, value for meta columns are computed as required
   * and wrapped in {@link HoodieInternalRow}. {@link HoodieInternalRow} is what gets written to HoodieInternalRowFileWriter.
   *
   * @param record instance of {@link InternalRow} that needs to be written to the fileWriter.
   * @throws IOException
   */
  public void write(InternalRow record) throws IOException {
    try {
      final String partitionPath = String.valueOf(record.getUTF8String(3));
      final String seqId = HoodieRecord.generateSequenceId(instantTime, taskPartitionId, SEQGEN.getAndIncrement());
      final String recordKey = String.valueOf(record.getUTF8String(2));
      HoodieInternalRow internalRow = new HoodieInternalRow(instantTime, seqId, recordKey, partitionPath, path.getName(),
          record);
      try {
        fileWriter.writeRow(recordKey, internalRow);
        writeStatus.markSuccess(recordKey);
      } catch (Throwable t) {
        writeStatus.markFailure(recordKey, t);
      }
    } catch (Throwable ge) {
      writeStatus.setGlobalError(ge);
      throw ge;
    }
  }

  /**
   * @returns {@code true} if this handle can take in more writes. else {@code false}.
   */
  public boolean canWrite() {
    return fileWriter.canWrite();
  }

  /**
   * Closes the {@link HoodieRowCreateHandle} and returns an instance of {@link HoodieInternalWriteStatus} containing the stats and
   * status of the writes to this handle.
   *
   * @return the {@link HoodieInternalWriteStatus} containing the stats and status of the writes to this handle.
   * @throws IOException
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
    stat.setPath(new Path(writeConfig.getBasePath()), path);
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

  private Path makeNewPath(String partitionPath) {
    Path path = FSUtils.getPartitionPath(writeConfig.getBasePath(), partitionPath);
    try {
      if (!fs.exists(path)) {
        fs.mkdirs(path); // create a new partition as needed.
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to make dir " + path, e);
    }
    HoodieTableConfig tableConfig = table.getMetaClient().getTableConfig();
    return new Path(path.toString(), FSUtils.makeDataFileName(instantTime, getWriteToken(), fileId,
        tableConfig.getBaseFileFormat().getFileExtension()));
  }

  /**
   * Creates an empty marker file corresponding to storage writer path.
   *
   * @param partitionPath Partition path
   */
  private void createMarkerFile(String partitionPath, String dataFileName) {
    WriteMarkersFactory.get(writeConfig.getMarkersType(), table, instantTime)
        .create(partitionPath, dataFileName, IOType.CREATE);
  }

  private String getWriteToken() {
    return taskPartitionId + "-" + taskId + "-" + taskEpochId;
  }

  protected HoodieInternalRowFileWriter createNewFileWriter(
      Path path, HoodieTable hoodieTable, HoodieWriteConfig config, StructType schema)
      throws IOException {
    return HoodieInternalRowFileWriterFactory.getInternalRowFileWriter(
        path, hoodieTable, config, schema);
  }
}
