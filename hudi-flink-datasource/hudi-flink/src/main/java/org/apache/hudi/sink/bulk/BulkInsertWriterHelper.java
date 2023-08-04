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
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.storage.row.HoodieRowDataCreateHandle;
import org.apache.hudi.table.HoodieTable;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
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
  protected final Boolean isInputSorted;
  private final List<WriteStatus> writeStatusList = new ArrayList<>();
  protected HoodieRowDataCreateHandle handle;
  private String lastKnownPartitionPath = null;
  private final String fileIdPrefix;
  private int numFilesWritten = 0;
  protected final Map<String, HoodieRowDataCreateHandle> handles = new HashMap<>();
  @Nullable
  protected final RowDataKeyGen keyGen;

  public BulkInsertWriterHelper(Configuration conf, HoodieTable hoodieTable, HoodieWriteConfig writeConfig,
                                String instantTime, int taskPartitionId, long totalSubtaskNum, long taskEpochId, RowType rowType) {
    this(conf, hoodieTable, writeConfig, instantTime, taskPartitionId, totalSubtaskNum, taskEpochId, rowType, false);
  }

  public BulkInsertWriterHelper(Configuration conf, HoodieTable hoodieTable, HoodieWriteConfig writeConfig,
                                String instantTime, int taskPartitionId, long totalSubtaskNum, long taskEpochId, RowType rowType,
                                boolean preserveHoodieMetadata) {
    this.hoodieTable = hoodieTable;
    this.writeConfig = writeConfig;
    this.instantTime = instantTime;
    this.taskPartitionId = taskPartitionId;
    this.totalSubtaskNum = totalSubtaskNum;
    this.taskEpochId = taskEpochId;
    this.rowType = preserveHoodieMetadata ? rowType : addMetadataFields(rowType, writeConfig.allowOperationMetadataField()); // patch up with metadata fields
    this.preserveHoodieMetadata = preserveHoodieMetadata;
    this.isInputSorted = OptionsResolver.isBulkInsertOperation(conf) && conf.getBoolean(FlinkOptions.WRITE_BULK_INSERT_SORT_INPUT);
    this.fileIdPrefix = UUID.randomUUID().toString();
    this.keyGen = preserveHoodieMetadata ? null : RowDataKeyGens.instance(conf, rowType, taskPartitionId, instantTime);
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
      }
      handle.write(recordKey, partitionPath, record);
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
      HoodieRowDataCreateHandle rowCreateHandle = new HoodieRowDataCreateHandle(hoodieTable, writeConfig, partitionPath, getNextFileId(),
          instantTime, taskPartitionId, totalSubtaskNum, taskEpochId, rowType, preserveHoodieMetadata);
      handles.put(partitionPath, rowCreateHandle);
    } else if (!handles.get(partitionPath).canWrite()) {
      // even if there is a handle to the partition path, it could have reached its max size threshold. So, we close the handle here and
      // create a new one.
      LOG.info("Rolling max-size file for partition path " + partitionPath);
      writeStatusList.add(handles.remove(partitionPath).close());
      HoodieRowDataCreateHandle rowCreateHandle = new HoodieRowDataCreateHandle(hoodieTable, writeConfig, partitionPath, getNextFileId(),
          instantTime, taskPartitionId, totalSubtaskNum, taskEpochId, rowType, preserveHoodieMetadata);
      handles.put(partitionPath, rowCreateHandle);
    }
    return handles.get(partitionPath);
  }

  public void close() throws IOException {
    for (HoodieRowDataCreateHandle rowCreateHandle : handles.values()) {
      LOG.info("Closing bulk insert file " + rowCreateHandle.getFileName());
      writeStatusList.add(rowCreateHandle.close());
    }
    handles.clear();
    handle = null;
  }

  private String getNextFileId() {
    return String.format("%s-%d", fileIdPrefix, numFilesWritten++);
  }

  /**
   * Adds the Hoodie metadata fields to the given row type.
   */
  public static RowType addMetadataFields(RowType rowType, boolean withOperationField) {
    List<RowType.RowField> mergedFields = new ArrayList<>();

    LogicalType metadataFieldType = DataTypes.STRING().getLogicalType();
    RowType.RowField commitTimeField =
        new RowType.RowField(HoodieRecord.COMMIT_TIME_METADATA_FIELD, metadataFieldType, "commit time");
    RowType.RowField commitSeqnoField =
        new RowType.RowField(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, metadataFieldType, "commit seqno");
    RowType.RowField recordKeyField =
        new RowType.RowField(HoodieRecord.RECORD_KEY_METADATA_FIELD, metadataFieldType, "record key");
    RowType.RowField partitionPathField =
        new RowType.RowField(HoodieRecord.PARTITION_PATH_METADATA_FIELD, metadataFieldType, "partition path");
    RowType.RowField fileNameField =
        new RowType.RowField(HoodieRecord.FILENAME_METADATA_FIELD, metadataFieldType, "field name");

    mergedFields.add(commitTimeField);
    mergedFields.add(commitSeqnoField);
    mergedFields.add(recordKeyField);
    mergedFields.add(partitionPathField);
    mergedFields.add(fileNameField);

    if (withOperationField) {
      RowType.RowField operationField =
          new RowType.RowField(HoodieRecord.OPERATION_METADATA_FIELD, metadataFieldType, "operation");
      mergedFields.add(operationField);
    }

    mergedFields.addAll(rowType.getFields());

    return new RowType(false, mergedFields);
  }

  public List<WriteStatus> getWriteStatuses(int taskID) {
    try {
      close();
      return writeStatusList;
    } catch (IOException e) {
      throw new HoodieException("Error collect the write status for task [" + taskID + "]", e);
    }
  }

}

