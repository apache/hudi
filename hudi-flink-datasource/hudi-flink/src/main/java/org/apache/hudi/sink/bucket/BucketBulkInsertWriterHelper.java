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
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.io.storage.row.HoodieRowDataCreateHandle;
import org.apache.hudi.sink.bulk.BulkInsertWriterHelper;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Helper class for bucket index bulk insert used by Flink.
 */
public class BucketBulkInsertWriterHelper extends BulkInsertWriterHelper {
  private static final Logger LOG = LoggerFactory.getLogger(BucketBulkInsertWriterHelper.class);
  public static final String FILE_GROUP_META_FIELD = "_fg";

  private final int recordArity;

  private String lastFileId; // for efficient code path

  public BucketBulkInsertWriterHelper(Configuration conf, HoodieTable<?, ?, ?, ?> hoodieTable, HoodieWriteConfig writeConfig,
                                      String instantTime, int taskPartitionId, long taskId, long taskEpochId, RowType rowType) {
    super(conf, hoodieTable, writeConfig, instantTime, taskPartitionId, taskId, taskEpochId, rowType);
    this.recordArity = rowType.getFieldCount();
  }

  public void write(RowData tuple) throws IOException {
    try {
      RowData record = tuple.getRow(1, this.recordArity);
      String recordKey = keyGen.getRecordKey(record);
      String partitionPath = keyGen.getPartitionPath(record);
      String fileId = tuple.getString(0).toString();
      if ((lastFileId == null) || !lastFileId.equals(fileId)) {
        LOG.info("Creating new file for partition path " + partitionPath);
        handle = getRowCreateHandle(partitionPath, fileId);
        lastFileId = fileId;
      }
      handle.write(recordKey, partitionPath, record);
    } catch (Throwable throwable) {
      LOG.error("Global error thrown while trying to write records in HoodieRowDataCreateHandle", throwable);
      throw throwable;
    }
  }

  private HoodieRowDataCreateHandle getRowCreateHandle(String partitionPath, String fileId) throws IOException {
    if (!handles.containsKey(fileId)) { // if there is no handle corresponding to the fileId
      if (this.isInputSorted) {
        // if records are sorted, we can close all existing handles
        close();
      }
      HoodieRowDataCreateHandle rowCreateHandle = new HoodieRowDataCreateHandle(hoodieTable, writeConfig, partitionPath, fileId,
          instantTime, taskPartitionId, taskId, taskEpochId, rowType);
      handles.put(fileId, rowCreateHandle);
    }
    return handles.get(fileId);
  }

  public static SortOperatorGen getFileIdSorterGen(RowType rowType) {
    return new SortOperatorGen(rowType, new String[] {FILE_GROUP_META_FIELD});
  }

  private static String getFileId(Map<String, String> bucketIdToFileId, RowDataKeyGen keyGen, RowData record, String indexKeys, int numBuckets) {
    String recordKey = keyGen.getRecordKey(record);
    String partition = keyGen.getPartitionPath(record);
    final int bucketNum = BucketIdentifier.getBucketId(recordKey, indexKeys, numBuckets);
    String bucketId = partition + bucketNum;
    return bucketIdToFileId.computeIfAbsent(bucketId, k -> BucketIdentifier.newBucketFileIdPrefix(bucketNum));
  }

  public static RowData rowWithFileId(Map<String, String> bucketIdToFileId, RowDataKeyGen keyGen, RowData record, String indexKeys, int numBuckets) {
    final String fileId = getFileId(bucketIdToFileId, keyGen, record, indexKeys, numBuckets);
    return GenericRowData.of(StringData.fromString(fileId), record);
  }

  public static RowType rowTypeWithFileId(RowType rowType) {
    LogicalType[] types = new LogicalType[] {DataTypes.STRING().getLogicalType(), rowType};
    String[] names = new String[] {FILE_GROUP_META_FIELD, "record"};
    return RowType.of(types, names);
  }
}
