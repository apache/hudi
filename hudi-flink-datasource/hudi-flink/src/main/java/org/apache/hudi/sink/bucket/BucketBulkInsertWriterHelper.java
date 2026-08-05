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

import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.index.bucket.partition.NumBucketsFunction;
import org.apache.hudi.io.storage.row.HoodieRowDataCreateHandle;
import org.apache.hudi.sink.bulk.BulkInsertWriterHelper;
import org.apache.hudi.sink.bulk.RowDataKeyGen;
import org.apache.hudi.sink.bulk.sort.SortOperatorGen;
import org.apache.hudi.table.HoodieTable;

import lombok.extern.slf4j.Slf4j;
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
 * Helper class for bucket index bulk insert used by Flink.
 */
@Slf4j
public class BucketBulkInsertWriterHelper extends BulkInsertWriterHelper {
  public static final String FILE_GROUP_META_FIELD = "_fg";
  public static final String PARTITION_PATH_META_FIELD = "_partition_path";

  protected final int recordArity;
  protected final boolean isNonBlockingConcurrencyControl;

  private String lastFileId; // for efficient code path
  private String lastPartitionPath; // only used by NBCC where file IDs repeat across partitions

  public BucketBulkInsertWriterHelper(Configuration conf, HoodieTable<?, ?, ?, ?> hoodieTable, HoodieWriteConfig writeConfig,
                                      String instantTime, int taskPartitionId, long taskId, long taskEpochId, RowType rowType) {
    super(conf, hoodieTable, writeConfig, instantTime, taskPartitionId, taskId, taskEpochId, rowType);
    this.recordArity = rowType.getFieldCount();
    this.isNonBlockingConcurrencyControl = OptionsResolver.isNonBlockingConcurrencyControl(conf);
  }

  public void write(RowData tuple) throws IOException {
    try {
      int fieldOffset = isNonBlockingConcurrencyControl ? 1 : 0;
      RowData record = tuple.getRow(fieldOffset + 1, this.recordArity);
      String recordKey = keyGen.getRecordKey(record);
      String partitionPath = isNonBlockingConcurrencyControl
          ? tuple.getString(0).toString()
          : keyGen.getPartitionPath(record);
      String fileId = tuple.getString(fieldOffset).toString();
      writeRecord(recordKey, partitionPath, fileId, record);
    } catch (Throwable throwable) {
      IOException ioException = new IOException("Exception happened when bulk insert.", throwable);
      log.error("Global error thrown while trying to write records in HoodieRowDataCreateHandle", ioException);
      throw ioException;
    }
  }

  protected void writeRecord(
      String recordKey,
      String partitionPath,
      String fileId,
      RowData record) throws IOException {
    if ((lastFileId == null)
        || !lastFileId.equals(fileId)
        || (isNonBlockingConcurrencyControl && !partitionPath.equals(lastPartitionPath))) {
      log.info("Creating new file for partition path {}", partitionPath);
      handle = getRowCreateHandle(partitionPath, fileId);
      lastFileId = fileId;
      lastPartitionPath = partitionPath;
    }
    handle.write(recordKey, partitionPath, record);
  }

  private HoodieRowDataCreateHandle getRowCreateHandle(String partitionPath, String fileId) throws IOException {
    Object handleKey = isNonBlockingConcurrencyControl
        ? new HoodieFileGroupId(partitionPath, fileId)
        : fileId;
    if (!handles.containsKey(handleKey)) { // if there is no handle corresponding to the file group
      if (this.isInputSorted) {
        // if records are sorted, we can close all existing handles
        close();
      }
      HoodieRowDataCreateHandle rowCreateHandle = new HoodieRowDataCreateHandle(hoodieTable, writeConfig, partitionPath, fileId,
          instantTime, taskPartitionId, totalSubtaskNum, taskEpochId, writerSchema, preserveHoodieMetadata, isAppendMode && !populateMetaFields);
      handles.put(handleKey, rowCreateHandle);
    }
    return handles.get(handleKey);
  }

  public static SortOperatorGen getFileIdSorterGen(
      RowType rowType, boolean isNonBlockingConcurrencyControl) {
    return new SortOperatorGen(rowType, isNonBlockingConcurrencyControl
        ? new String[] {PARTITION_PATH_META_FIELD, FILE_GROUP_META_FIELD}
        : new String[] {FILE_GROUP_META_FIELD});
  }

  static String getFileId(
      Map<String, String> bucketIdToFileId,
      String recordKey,
      String partitionPath,
      List<String> indexKeyFields,
      NumBucketsFunction numBucketsFunction,
      boolean needFixedFileIdSuffix) {
    final int numBuckets = numBucketsFunction.getNumBuckets(partitionPath);
    final int bucketNum = BucketIdentifier.getBucketId(recordKey, indexKeyFields, numBuckets);
    String bucketId = partitionPath + bucketNum;
    return bucketIdToFileId.computeIfAbsent(bucketId, k -> needFixedFileIdSuffix ? BucketIdentifier.newBucketFileIdForNBCC(bucketNum) : BucketIdentifier.newBucketFileIdPrefix(bucketNum));
  }

  public static RowData rowWithFileId(Map<String, String> bucketIdToFileId, RowDataKeyGen keyGen, RowData record, List<String> indexKeyFields,
                                      NumBucketsFunction numBucketsFunction, boolean needFixedFileIdSuffix) {
    String recordKey = keyGen.getRecordKey(record);
    String partitionPath = keyGen.getPartitionPath(record);
    final String fileId = getFileId(
        bucketIdToFileId,
        recordKey,
        partitionPath,
        indexKeyFields,
        numBucketsFunction,
        needFixedFileIdSuffix);
    return needFixedFileIdSuffix
        ? GenericRowData.of(
            StringData.fromString(partitionPath), StringData.fromString(fileId), record)
        : GenericRowData.of(StringData.fromString(fileId), record);
  }

  public static RowType rowTypeWithFileId(
      RowType rowType, boolean isNonBlockingConcurrencyControl) {
    LogicalType[] types;
    String[] names;
    if (isNonBlockingConcurrencyControl) {
      types = new LogicalType[] {
          DataTypes.STRING().getLogicalType(), DataTypes.STRING().getLogicalType(), rowType};
      names = new String[] {PARTITION_PATH_META_FIELD, FILE_GROUP_META_FIELD, "record"};
    } else {
      types = new LogicalType[] {DataTypes.STRING().getLogicalType(), rowType};
      names = new String[] {FILE_GROUP_META_FIELD, "record"};
    }
    return RowType.of(types, names);
  }
}
