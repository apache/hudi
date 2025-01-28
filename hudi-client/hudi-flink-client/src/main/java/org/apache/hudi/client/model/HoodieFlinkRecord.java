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
import org.apache.hudi.exception.HoodieException;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.StringDataTypeInfo;
import org.apache.flink.types.BooleanValue;

import static org.apache.flink.api.java.typeutils.ValueTypeInfo.BOOLEAN_VALUE_TYPE_INFO;

/**
 * Flink {@link RowData} with added Hudi metadata in data structure based on Flink {@link Tuple}.
 * Strict ordering and fixed arity are expected.
 */
public class HoodieFlinkRecord extends Tuple {

  private static final long serialVersionUID = 1L;
  // Arity shouldn't exceed 25, because tuples in Flink are limited by Tuple25.
  // But if it will be needed in the future, then nested structure could be used.
  private static final int ARITY = 7;

  private StringData recordKey;
  private StringData partitionPath;
  private StringData fileId;
  private StringData instantTime;
  /**
   * {@link HoodieOperation}
   */
  private StringData operationType;
  private BooleanValue isIndexRecord;
  private RowData rowData;

  public HoodieFlinkRecord(String recordKey, String partitionPath, RowData rowData) {
    this(recordKey, partitionPath, "", "", "", false, rowData);
  }

  public HoodieFlinkRecord(String recordKey, String partitionPath, boolean isIndexRecord, RowData rowData) {
    this(recordKey, partitionPath, "", "", "", isIndexRecord, rowData);
  }

  public HoodieFlinkRecord(String recordKey,
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

  public <T extends Tuple> HoodieFlinkRecord(T tuple) {
    if (tuple.getArity() == ARITY) {
      for (int i = 0;  i < ARITY; i++) {
        setField(tuple.getField(i), i);
      }
    } else {
      throw new HoodieException("Tuple with arity " + tuple.getArity() + " is not reconciled with 'HoodieFlinkRecord', for which arity " + ARITY + " is expected");
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T getField(int pos) {
    switch (pos) {
      case 0:
        return (T) this.recordKey;
      case 1:
        return (T) this.partitionPath;
      case 2:
        return (T) this.fileId;
      case 3:
        return (T) this.instantTime;
      case 4:
        return (T) this.operationType;
      case 5:
        return (T) this.isIndexRecord;
      case 6:
        return (T) this.rowData;
      default:
        throw new IndexOutOfBoundsException("There is no corresponding object in 'HoodieFlinkRecord' for position '" + pos + "'");
    }
  }

  @Override
  public <T> void setField(T value, int pos) {
    switch (pos) {
      case 0:
        this.recordKey = (StringData) value;
        break;
      case 1:
        this.partitionPath = (StringData) value;
        break;
      case 2:
        this.fileId = (StringData) value;
        break;
      case 3:
        this.instantTime = (StringData) value;
        break;
      case 4:
        this.operationType = (StringData) value;
        break;
      case 5:
        this.isIndexRecord = (BooleanValue) value;
        break;
      case 6:
        this.rowData = (RowData) value;
        break;
      default:
        throw new IndexOutOfBoundsException("There is no corresponding object in 'HoodieFlinkRecord' for position '" + pos + "'");
    }
  }

  @Override
  public int getArity() {
    return ARITY;
  }

  @Override
  public HoodieFlinkRecord copy() {
    return new HoodieFlinkRecord(
        this.recordKey.toString(),
        this.partitionPath.toString(),
        this.fileId.toString(),
        this.instantTime.toString(),
        this.operationType.toString(),
        this.isIndexRecord.getValue(),
        this.rowData);
  }

  /**
   * Returns {@link TypeInformation} for proper serde with forced use of {@link TupleSerializer}.
   */
  public static TupleTypeInfo getTypeInfo(TypeInformation<RowData> rowDataInfo) {
    return new TupleTypeInfo<>(
        StringDataTypeInfo.INSTANCE,
        StringDataTypeInfo.INSTANCE,
        StringDataTypeInfo.INSTANCE,
        StringDataTypeInfo.INSTANCE,
        StringDataTypeInfo.INSTANCE,
        BOOLEAN_VALUE_TYPE_INFO,
        rowDataInfo);
  }

  public String getRecordKey() {
    return String.valueOf(recordKey);
  }

  public String getPartitionPath() {
    return String.valueOf(partitionPath);
  }

  public RowData getRowData() {
    return rowData;
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
}
