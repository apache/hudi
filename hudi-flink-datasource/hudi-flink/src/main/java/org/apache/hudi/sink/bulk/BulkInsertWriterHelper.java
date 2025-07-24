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

package org.apache.hudi.sink.bulk;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.storage.row.HoodieRowDataCreateHandle;
import org.apache.hudi.metrics.FlinkStreamWriteMetrics;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.util.DataTypeUtils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.FutureUtils.allOf;

/**
 * Helper class for bulk insert used by Flink.
 */
public class BulkInsertWriterHelper {

  private static final Logger LOG = LoggerFactory.getLogger(BulkInsertWriterHelper.class);

  protected final String instantTime;
  protected final int taskPartitionId;
  protected final long totalSubtaskNum;
  protected final long taskEpochId;
  protected final HoodieTable hoodieTable;
  protected final HoodieWriteConfig writeConfig;
  protected final RowType rowType;
  protected final boolean preserveHoodieMetadata;
  protected final boolean isAppendMode;
  // used for Append mode only, if true then only initial row data without metacolumns is written
  protected final boolean populateMetaFields;
  protected final Boolean isInputSorted;
  private final List<WriteStatus> writeStatusList = new ArrayList<>();
  protected HoodieRowDataCreateHandle handle;
  private String lastKnownPartitionPath = null;
  private final String fileIdPrefix;
  private int numFilesWritten = 0;
  protected final Map<String, HoodieRowDataCreateHandle> handles = new HashMap<>();
  @Nullable
  protected final RowDataKeyGen keyGen;

  protected final Option<FlinkStreamWriteMetrics> writeMetrics;

  public BulkInsertWriterHelper(Configuration conf, HoodieTable hoodieTable, HoodieWriteConfig writeConfig,
                                String instantTime, int taskPartitionId, long totalSubtaskNum, long taskEpochId, RowType rowType) {
    this(conf, hoodieTable, writeConfig, instantTime, taskPartitionId, totalSubtaskNum, taskEpochId, rowType, false, Option.empty());
  }

  public BulkInsertWriterHelper(Configuration conf, HoodieTable hoodieTable, HoodieWriteConfig writeConfig,
                                String instantTime, int taskPartitionId, long taskId, long taskEpochId, RowType rowType, boolean preserveHoodieMetadata) {
    this(conf, hoodieTable, writeConfig, instantTime, taskPartitionId, taskId, taskEpochId, rowType, preserveHoodieMetadata, Option.empty());
  }

  public BulkInsertWriterHelper(Configuration conf, HoodieTable hoodieTable, HoodieWriteConfig writeConfig,
                                String instantTime, int taskPartitionId, long totalSubtaskNum, long taskEpochId, RowType rowType,
                                boolean preserveHoodieMetadata, Option<FlinkStreamWriteMetrics> writeMetrics) {
    this.hoodieTable = hoodieTable;
    this.writeConfig = writeConfig;
    this.instantTime = instantTime;
    this.taskPartitionId = taskPartitionId;
    this.totalSubtaskNum = totalSubtaskNum;
    this.taskEpochId = taskEpochId;
    this.isAppendMode = OptionsResolver.isAppendMode(conf);
    this.populateMetaFields = writeConfig.populateMetaFields();
    this.rowType = preserveHoodieMetadata || (isAppendMode && !populateMetaFields)
        ? rowType
        : DataTypeUtils.addMetadataFields(rowType, writeConfig.allowOperationMetadataField());
    this.preserveHoodieMetadata = preserveHoodieMetadata;
    this.isInputSorted = OptionsResolver.isBulkInsertOperation(conf) && conf.get(FlinkOptions.WRITE_BULK_INSERT_SORT_INPUT);
    this.fileIdPrefix = UUID.randomUUID().toString();
    this.keyGen = preserveHoodieMetadata ? null : RowDataKeyGens.instance(conf, rowType, taskPartitionId, instantTime);
    this.writeMetrics = writeMetrics;
  }

  /**
   * Returns the write instant time.
   */
  public String getInstantTime() {
    return this.instantTime;
  }

  public void write(RowData record) throws IOException {
    try {
      String recordKey = preserveHoodieMetadata
          ? record.getString(HoodieRecord.RECORD_KEY_META_FIELD_ORD).toString()
          : keyGen.getRecordKey(record);
      String partitionPath = preserveHoodieMetadata
          ? record.getString(HoodieRecord.PARTITION_PATH_META_FIELD_ORD).toString()
          : keyGen.getPartitionPath(record);

      if ((lastKnownPartitionPath == null) || !lastKnownPartitionPath.equals(partitionPath) || !handle.canWrite()) {
        handle = getRowCreateHandle(partitionPath);
        lastKnownPartitionPath = partitionPath;
        writeMetrics.ifPresent(FlinkStreamWriteMetrics::markHandleSwitch);
      }
      handle.write(recordKey, partitionPath, record);
      writeMetrics.ifPresent(FlinkStreamWriteMetrics::markRecordIn);
    } catch (Throwable t) {
      IOException ioException = new IOException("Exception happened when bulk insert.", t);
      LOG.error("Global error thrown while trying to write records in HoodieRowCreateHandle ", ioException);
      throw new IOException(ioException);
    }
  }

  private HoodieRowDataCreateHandle getRowCreateHandle(String partitionPath) throws IOException {
    if (!handles.containsKey(partitionPath)) { // if there is no handle corresponding to the partition path
      // if records are sorted, we can close all existing handles
      if (isInputSorted) {
        close();
      }

      LOG.info("Creating new file for partition path " + partitionPath);
      writeMetrics.ifPresent(FlinkStreamWriteMetrics::startHandleCreation);
      HoodieRowDataCreateHandle rowCreateHandle = new HoodieRowDataCreateHandle(hoodieTable, writeConfig, partitionPath, getNextFileId(),
          instantTime, taskPartitionId, totalSubtaskNum, taskEpochId, rowType, preserveHoodieMetadata, isAppendMode && !populateMetaFields);
      handles.put(partitionPath, rowCreateHandle);

      writeMetrics.ifPresent(FlinkStreamWriteMetrics::increaseNumOfOpenHandle);
    } else if (!handles.get(partitionPath).canWrite()) {
      // even if there is a handle to the partition path, it could have reached its max size threshold. So, we close the handle here and
      // create a new one.
      LOG.info("Rolling max-size file for partition path " + partitionPath);
      writeStatusList.add(closeWriteHandle(handles.remove(partitionPath)));
      HoodieRowDataCreateHandle rowCreateHandle = createWriteHandle(partitionPath);
      handles.put(partitionPath, rowCreateHandle);
      writeMetrics.ifPresent(FlinkStreamWriteMetrics::increaseNumOfFilesWritten);
    }
    return handles.get(partitionPath);
  }

  public void close() throws IOException {
    if (handles.isEmpty()) {
      return;
    }
    int handsSize = Math.min(handles.size(), 10);
    ExecutorService executorService = Executors.newFixedThreadPool(handsSize);
    allOf(handles.values().stream()
        .map(rowCreateHandle -> CompletableFuture.supplyAsync(() -> {
          try {
            LOG.info("Closing bulk insert file " + rowCreateHandle.getFileName());
            return rowCreateHandle.close();
          } catch (IOException e) {
            throw new HoodieIOException("IOE during rowCreateHandle.close()", e);
          }
        }, executorService))
        .collect(Collectors.toList())
    ).whenComplete((result, throwable) -> {
      writeStatusList.addAll(result);
    }).join();
    try {
      executorService.shutdown();
      executorService.awaitTermination(10, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    handles.clear();
    handle = null;
  }

  private String getNextFileId() {
    return String.format("%s-%d", fileIdPrefix, numFilesWritten++);
  }

  public List<WriteStatus> getWriteStatuses(int taskID) {
    try {
      close();
      return writeStatusList;
    } catch (IOException e) {
      throw new HoodieException("Error collect the write status for task [" + taskID + "]", e);
    }
  }

  private HoodieRowDataCreateHandle createWriteHandle(String  partitionPath) {
    writeMetrics.ifPresent(FlinkStreamWriteMetrics::startHandleCreation);
    HoodieRowDataCreateHandle rowCreateHandle = new HoodieRowDataCreateHandle(hoodieTable, writeConfig, partitionPath, getNextFileId(),
        instantTime, taskPartitionId, totalSubtaskNum, taskEpochId, rowType, preserveHoodieMetadata, isAppendMode && !populateMetaFields);
    writeMetrics.ifPresent(FlinkStreamWriteMetrics::endHandleCreation);
    return rowCreateHandle;
  }

  private WriteStatus closeWriteHandle(HoodieRowDataCreateHandle rowCreateHandle) throws IOException {
    writeMetrics.ifPresent(FlinkStreamWriteMetrics::startFileFlush);
    WriteStatus status = rowCreateHandle.close();
    writeMetrics.ifPresent(FlinkStreamWriteMetrics::endFileFlush);
    return status;
  }

}

