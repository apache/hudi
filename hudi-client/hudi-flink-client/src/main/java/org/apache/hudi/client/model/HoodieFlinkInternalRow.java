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

package org.apache.hudi.client.model;

import org.apache.hudi.common.model.HoodieOperation;

import lombok.Getter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.BooleanValue;

import java.io.Serializable;

/**
 * Flink {@link RowData} with added Hudi metadata.
 * For serialization/deserialization {@code HoodieFlinkInternalRowTypeInfo} and {@link HoodieFlinkInternalRowSerializer} are used
 * to optimize serialization/deserialization costs.
 */
public class HoodieFlinkInternalRow implements Serializable {

  private static final long serialVersionUID = 1L;

  // the number of fields without nesting
  public static final int ARITY = 7;

  private final StringData recordKey;

  private final StringData partitionPath;

  private StringData fileId;

  private StringData instantTime;

  /**
   * {@link HoodieOperation}
   */
  private StringData operationType;

  // there is no rowData for index record
  private final BooleanValue isIndexRecord;

  @Getter
  private final RowData rowData;

  public HoodieFlinkInternalRow(String recordKey, String partitionPath, String operationType, RowData rowData) {
    this(recordKey, partitionPath, "", "", operationType, false, rowData);
  }

  // constructor for index records without row data
  public HoodieFlinkInternalRow(String recordKey, String partitionPath, String fileId, String instantTime) {
    this(recordKey, partitionPath, fileId, instantTime, "", true, null);
  }

  public HoodieFlinkInternalRow(String recordKey,
                                String partitionPath,
                                String fileId,
                                String instantTime,
                                String operationType,
                                boolean isIndexRecord,
                                RowData rowData) {
    this.recordKey = StringData.fromString(recordKey);
    this.partitionPath = StringData.fromString(partitionPath);
    this.fileId = StringData.fromString(fileId);
    this.instantTime = StringData.fromString(instantTime);
    this.operationType = StringData.fromString(operationType);
    this.isIndexRecord = new BooleanValue(isIndexRecord);
    this.rowData = rowData;
  }

  public String getRecordKey() {
    return String.valueOf(recordKey);
  }

  public String getPartitionPath() {
    return String.valueOf(partitionPath);
  }

  public void setFileId(String fileId) {
    this.fileId = StringData.fromString(fileId);
  }

  public String getFileId() {
    return String.valueOf(fileId);
  }

  public void setInstantTime(String instantTime) {
    this.instantTime = StringData.fromString(instantTime);
  }

  public String getInstantTime() {
    return String.valueOf(instantTime);
  }

  public void setOperationType(String operationType) {
    this.operationType = StringData.fromString(operationType);
  }

  public String getOperationType() {
    return String.valueOf(operationType);
  }

  public boolean isIndexRecord() {
    return isIndexRecord.getValue();
  }

  public HoodieFlinkInternalRow copy(RowDataSerializer rowDataSerializer) {
    return new HoodieFlinkInternalRow(
        this.recordKey.toString(),
        this.partitionPath.toString(),
        this.fileId.toString(),
        this.instantTime.toString(),
        this.operationType.toString(),
        this.isIndexRecord.getValue(),
        rowDataSerializer.copy(this.rowData));
  }
}
