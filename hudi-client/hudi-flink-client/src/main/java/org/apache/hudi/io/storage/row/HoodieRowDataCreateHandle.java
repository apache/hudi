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
import org.apache.hudi.client.model.HoodieRowData;
import org.apache.hudi.client.model.HoodieRowDataCreation;
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
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Create handle with RowData for datasource implemention of bulk insert.
 */
public class HoodieRowDataCreateHandle implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LogManager.getLogger(HoodieRowDataCreateHandle.class);
  private static final AtomicLong SEQGEN = new AtomicLong(1);

  private final String instantTime;
  private final int taskPartitionId;
  private final long taskId;
  private final long taskEpochId;
  private final HoodieTable table;
  private final HoodieWriteConfig writeConfig;
  protected final HoodieRowDataFileWriter fileWriter;
  private final String partitionPath;
  private final Path path;
  private final String fileId;
  private final boolean preserveHoodieMetadata;
  private final FileSystem fs;
  protected final HoodieInternalWriteStatus writeStatus;
  private final HoodieTimer currTimer;

  public HoodieRowDataCreateHandle(HoodieTable table, HoodieWriteConfig writeConfig, String partitionPath, String fileId,
                                   String instantTime, int taskPartitionId, long taskId, long taskEpochId,
                                   RowType rowType, boolean preserveHoodieMetadata) {
    this.partitionPath = partitionPath;
    this.table = table;
    this.writeConfig = writeConfig;
    this.instantTime = instantTime;
    this.taskPartitionId = taskPartitionId;
    this.taskId = taskId;
    this.taskEpochId = taskEpochId;
    this.fileId = fileId;
    this.preserveHoodieMetadata = preserveHoodieMetadata;
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
      createMarkerFile(partitionPath, FSUtils.makeBaseFileName(this.instantTime, getWriteToken(), this.fileId, table.getBaseFileExtension()));
      this.fileWriter = createNewFileWriter(path, table, writeConfig, rowType);
    } catch (IOException e) {
      throw new HoodieInsertException("Failed to initialize file writer for path " + path, e);
    }
    LOG.info("New handle created for partition :" + partitionPath + " with fileId " + fileId);
  }

  /**
   * Writes an {@link RowData} to the underlying {@link HoodieRowDataFileWriter}.
   * Before writing, value for meta columns are computed as required
   * and wrapped in {@link HoodieRowData}. {@link HoodieRowData} is what gets written to HoodieRowDataFileWriter.
   *
   * @param recordKey     The record key
   * @param partitionPath The partition path
   * @param record        instance of {@link RowData} that needs to be written to the fileWriter.
   * @throws IOException
   */
  public void write(String recordKey, String partitionPath, RowData record) throws IOException {
    try {
      String seqId = preserveHoodieMetadata
          ? record.getString(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD_ORD).toString()
          : HoodieRecord.generateSequenceId(instantTime, taskPartitionId, SEQGEN.getAndIncrement());
      String commitInstant = preserveHoodieMetadata
          ? record.getString(HoodieRecord.COMMIT_TIME_METADATA_FIELD_ORD).toString()
          : instantTime;
      RowData rowData = HoodieRowDataCreation.create(commitInstant, seqId, recordKey, partitionPath, path.getName(),
          record, writeConfig.allowOperationMetadataField(), preserveHoodieMetadata);
      try {
        fileWriter.writeRow(recordKey, rowData);
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
   * Returns {@code true} if this handle can take in more writes. else {@code false}.
   */
  public boolean canWrite() {
    return fileWriter.canWrite();
  }

  /**
   * Closes the {@link HoodieRowDataCreateHandle} and returns an instance of {@link HoodieInternalWriteStatus} containing the stats and
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
    return new Path(path.toString(), FSUtils.makeBaseFileName(instantTime, getWriteToken(), fileId,
        tableConfig.getBaseFileFormat().getFileExtension()));
  }

  /**
   * Creates an empty marker file corresponding to storage writer path.
   *
   * @param partitionPath Partition path
   */
  private void createMarkerFile(String partitionPath, String dataFileName) {
    WriteMarkers writeMarkers = WriteMarkersFactory.get(writeConfig.getMarkersType(), table, instantTime);
    writeMarkers.create(partitionPath, dataFileName, IOType.CREATE);
  }

  private String getWriteToken() {
    return taskPartitionId + "-" + taskId + "-" + taskEpochId;
  }

  protected HoodieRowDataFileWriter createNewFileWriter(
      Path path, HoodieTable hoodieTable, HoodieWriteConfig config, RowType rowType)
      throws IOException {
    return HoodieRowDataFileWriterFactory.getRowDataFileWriter(
        path, hoodieTable, config, rowType);
  }
}
