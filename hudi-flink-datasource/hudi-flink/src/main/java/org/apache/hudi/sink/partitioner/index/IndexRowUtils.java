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

package org.apache.hudi.sink.partitioner.index;

import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieMetadataPayload;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

/**
 * Utils for get/set/convert operations on index {@link RowData}.
 */
public class IndexRowUtils {

  // current only RLI is supported, may support SI later.
  public static final byte RLI_TYPE = 0;

  private static final int INDEX_TYPE_ORD = 0;
  private static final int KEY_ORD = 1;
  private static final int PARTITION_ORD = 2;
  private static final int FILE_ID_ORD = 3;
  private static final int INSTANT_ORD = 4;

  public static final RowType INDEX_ROW_TYPE = RowType.of(
      DataTypes.TINYINT().getLogicalType(),
      DataTypes.STRING().getLogicalType(),
      DataTypes.STRING().getLogicalType(),
      DataTypes.STRING().getLogicalType(),
      DataTypes.STRING().getLogicalType(),
      DataTypes.STRING().getLogicalType());

  public static RowData createRecordIndexRow(HoodieFlinkInternalRow internalRow) {
    GenericRowData indexRow = new GenericRowData(INDEX_ROW_TYPE.getFieldCount());
    indexRow.setField(INDEX_TYPE_ORD, RLI_TYPE);
    indexRow.setField(KEY_ORD, StringData.fromString(internalRow.getRecordKey()));
    indexRow.setField(PARTITION_ORD, StringData.fromString(internalRow.getPartitionPath()));
    indexRow.setField(FILE_ID_ORD, StringData.fromString(internalRow.getFileId()));
    switch (internalRow.getOperationType()) {
      case "I":
        indexRow.setRowKind(RowKind.INSERT);
        break;
      case "D":
        indexRow.setRowKind(RowKind.DELETE);
        break;
      default:
        throw new HoodieException("Unexpected operation type: " + internalRow.getOperationType());
    }
    return indexRow;
  }

  public static HoodieRecord convertToHoodieRecord(String instant, RowData indexRow, HoodieWriteConfig dataWriteConfig) {
    if (indexRow.getByte(INDEX_TYPE_ORD) == RLI_TYPE) {
      switch (indexRow.getRowKind()) {
        case INSERT:
          return HoodieMetadataPayload.createRecordIndexUpdate(
              String.valueOf(indexRow.getString(KEY_ORD)),
              String.valueOf(indexRow.getString(PARTITION_ORD)),
              String.valueOf(indexRow.getString(FILE_ID_ORD)),
              instant,
              dataWriteConfig.getWritesFileIdEncoding());
        case DELETE:
          return HoodieMetadataPayload.createRecordIndexDelete(
              String.valueOf(indexRow.getString(KEY_ORD)),
              String.valueOf(indexRow.getString(PARTITION_ORD)),
              dataWriteConfig.isRecordLevelIndexEnabled());
        default:
          throw new HoodieException("Unsupported operation type for index row: " + indexRow.getRowKind());
      }
    } else {
      throw new HoodieException("Unsupported type for index row: " + indexRow.getByte(INDEX_TYPE_ORD));
    }
  }

  public static HoodieKey getHoodieKey(RowData indexRow) {
    return new HoodieKey(getRecordKey(indexRow), getPartition(indexRow));
  }

  public static String getRecordKey(RowData indexRow) {
    return String.valueOf(indexRow.getString(KEY_ORD));
  }

  public static String getInstant(RowData indexRow) {
    return String.valueOf(indexRow.getString(INSTANT_ORD));
  }

  public static String getPartition(RowData indexRow) {
    return String.valueOf(indexRow.getString(PARTITION_ORD));
  }
}
