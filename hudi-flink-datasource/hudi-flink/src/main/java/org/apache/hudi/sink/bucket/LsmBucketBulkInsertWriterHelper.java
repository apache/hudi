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

package org.apache.hudi.sink.bucket;

import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.bucket.partition.NumBucketsFunction;
import org.apache.hudi.sink.bulk.RowDataKeyGen;
import org.apache.hudi.sink.bulk.sort.SortOperatorGen;
import org.apache.hudi.table.HoodieTable;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Bucket-index bulk-insert writer helper for LSM input rows.
 *
 * <p>The input row contains file ID, encoded record key, and the original table row.
 */
public class LsmBucketBulkInsertWriterHelper extends BucketBulkInsertWriterHelper {

  private static final String RECORD_KEY_FIELD = "_record_key";
  private static final String RECORD_FIELD = "record";

  public LsmBucketBulkInsertWriterHelper(
      Configuration conf,
      HoodieTable<?, ?, ?, ?> hoodieTable,
      HoodieWriteConfig writeConfig,
      String instantTime,
      int taskPartitionId,
      long taskId,
      long taskEpochId,
      RowType rowType) {
    super(conf, hoodieTable, writeConfig, instantTime, taskPartitionId, taskId, taskEpochId, rowType);
  }

  @Override
  public void write(RowData sortRow) throws IOException {
    String fileId = sortRow.getString(0).toString();
    String recordKey = sortRow.getString(1).toString();
    RowData record = sortRow.getRow(2, recordArity);
    String partitionPath = keyGen.getPartitionPath(record);
    writeRecord(recordKey, partitionPath, fileId, record);
  }

  public static RowData rowWithFileIdAndKey(
      Map<String, String> bucketIdToFileId,
      RowDataKeyGen keyGen,
      RowData record,
      List<String> indexKeyFields,
      NumBucketsFunction numBucketsFunction,
      boolean needFixedFileIdSuffix) {
    String recordKey = keyGen.getRecordKey(record);
    String partitionPath = keyGen.getPartitionPath(record);
    String fileId = getFileId(
        bucketIdToFileId,
        recordKey,
        partitionPath,
        indexKeyFields,
        numBucketsFunction,
        needFixedFileIdSuffix);
    return GenericRowData.of(
        StringData.fromString(fileId),
        StringData.fromString(recordKey),
        record);
  }

  /**
   * Returns the internal row type used to sort LSM bucket bulk-insert records by file ID and
   * record key.
   *
   * <p>The fields are ordered as file ID, encoded record key, and original table row.
   */
  public static RowType rowTypeWithFileIdAndKey(RowType rowType) {
    LogicalType[] types = new LogicalType[] {
        DataTypes.STRING().getLogicalType(),
        DataTypes.STRING().getLogicalType(),
        rowType
    };
    String[] names =
        new String[] {FILE_GROUP_META_FIELD, RECORD_KEY_FIELD, RECORD_FIELD};
    return RowType.of(types, names);
  }

  /**
   * Creates an external sorter ordered by file ID and encoded record key.
   *
   * <p>The nested payload is deliberately excluded from the sort keys, so duplicate record keys
   * are retained without comparing or aggregating their payloads.
   */
  public static SortOperatorGen getFileIdAndKeySorterGen(RowType rowType) {
    return new SortOperatorGen(
        rowType, new String[] {FILE_GROUP_META_FIELD, RECORD_KEY_FIELD});
  }
}
