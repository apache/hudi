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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.typeutils.StringDataSerializer;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.Objects;

/**
 * Serializer for {@link HoodieFlinkInternalRow}
 */
public class HoodieFlinkInternalRowSerializer extends TypeSerializer<HoodieFlinkInternalRow> {

  private static final long serialVersionUID = 1L;

  protected RowType rowType;

  protected RowDataSerializer rowDataSerializer;

  protected StringDataSerializer stringDataSerializer;

  public HoodieFlinkInternalRowSerializer(RowType rowType) {
    this.rowType = rowType;
    this.rowDataSerializer = new RowDataSerializer(rowType);
    this.stringDataSerializer = StringDataSerializer.INSTANCE;
  }

  @Override
  public boolean isImmutableType() {
    return false;
  }

  @Override
  public TypeSerializer<HoodieFlinkInternalRow> duplicate() {
    return new HoodieFlinkInternalRowSerializer(rowType);
  }

  @Override
  public HoodieFlinkInternalRow createInstance() {
    throw new UnsupportedOperationException("HoodieFlinkInternalRow doesn't allow creation with some defaults.");
  }

  @Override
  public HoodieFlinkInternalRow copy(HoodieFlinkInternalRow from) {
    return from.copy(rowDataSerializer);
  }

  @Override
  public HoodieFlinkInternalRow copy(HoodieFlinkInternalRow from, HoodieFlinkInternalRow reuse) {
    throw new UnsupportedOperationException("HoodieFlinkInternalRow doesn't allow reusing.");
  }

  @Override
  public int getLength() {
    // -1 for variable length data types
    return -1;
  }

  @Override
  public void serialize(HoodieFlinkInternalRow record, DataOutputView target) throws IOException {
    boolean isIndexRecord = record.isIndexRecord();
    target.writeBoolean(isIndexRecord);
    stringDataSerializer.serialize(StringData.fromString(record.getRecordKey()), target);
    stringDataSerializer.serialize(StringData.fromString(record.getPartitionPath()), target);
    stringDataSerializer.serialize(StringData.fromString(record.getFileId()), target);
    stringDataSerializer.serialize(StringData.fromString(record.getInstantTime()), target);
    stringDataSerializer.serialize(StringData.fromString(record.getOperationType()), target);
    if (!isIndexRecord) {
      rowDataSerializer.serialize(record.getRowData(), target);
    }
  }

  @Override
  public HoodieFlinkInternalRow deserialize(DataInputView source) throws IOException {
    boolean isIndexRecord = source.readBoolean();
    StringData recordKey = stringDataSerializer.deserialize(source);
    StringData partition = stringDataSerializer.deserialize(source);
    StringData fileId = stringDataSerializer.deserialize(source);
    StringData instantTime = stringDataSerializer.deserialize(source);
    StringData operationType = stringDataSerializer.deserialize(source);
    HoodieFlinkInternalRow record;
    if (!isIndexRecord) {
      RowData rowData = rowDataSerializer.deserialize(source);
      record = new HoodieFlinkInternalRow(
          recordKey.toString(),
          partition.toString(),
          fileId.toString(),
          instantTime.toString(),
          operationType.toString(),
          isIndexRecord,
          rowData);
    } else {
      record = new HoodieFlinkInternalRow(
          recordKey.toString(),
          partition.toString(),
          fileId.toString(),
          instantTime.toString());
    }
    return record;
  }

  @Override
  public HoodieFlinkInternalRow deserialize(HoodieFlinkInternalRow reuse, DataInputView source) throws IOException {
    return deserialize(source);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    boolean isIndexRecord = source.readBoolean();
    target.writeBoolean(isIndexRecord);
    stringDataSerializer.copy(source, target);
    stringDataSerializer.copy(source, target);
    stringDataSerializer.copy(source, target);
    stringDataSerializer.copy(source, target);
    stringDataSerializer.copy(source, target);
    if (!isIndexRecord) {
      rowDataSerializer.copy(source, target);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (null == obj || getClass() != obj.getClass()) {
      return false;
    }

    HoodieFlinkInternalRowSerializer that = (HoodieFlinkInternalRowSerializer) obj;
    return Objects.equals(rowDataSerializer, that.rowDataSerializer)
        && Objects.equals(stringDataSerializer, that.stringDataSerializer);
  }

  @Override
  public int hashCode() {
    return 31 * Objects.hashCode(rowDataSerializer) + Objects.hashCode(stringDataSerializer);
  }

  @Override
  public TypeSerializerSnapshot<HoodieFlinkInternalRow> snapshotConfiguration() {
    throw new UnsupportedOperationException();
  }
}
