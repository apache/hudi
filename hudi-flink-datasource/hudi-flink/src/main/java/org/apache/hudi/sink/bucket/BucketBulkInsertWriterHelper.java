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
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.io.storage.row.HoodieRowDataCreateHandle;
import org.apache.hudi.sink.bulk.BulkInsertWriterHelper;
import org.apache.hudi.table.HoodieTable;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Helper class for bucket index bulk insert used by Flink.
 */
public class BucketBulkInsertWriterHelper extends BulkInsertWriterHelper {
  private static final Logger LOG = LoggerFactory.getLogger(BucketBulkInsertWriterHelper.class);

  private final int bucketNum;
  private final String indexKeyFields;

  public BucketBulkInsertWriterHelper(Configuration conf, HoodieTable<?, ?, ?, ?> hoodieTable, HoodieWriteConfig writeConfig,
                                      String instantTime, int taskPartitionId, long taskId, long taskEpochId, RowType rowType) {
    super(conf, hoodieTable, writeConfig, instantTime, taskPartitionId, taskId, taskEpochId, rowType);
    this.bucketNum = conf.getInteger(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS);
    this.indexKeyFields = conf.getString(FlinkOptions.INDEX_KEY_FIELD);
  }

  public void write(RowData record) throws IOException {
    try {
      String recordKey = keyGen.getRecordKey(record);
      String partitionPath = keyGen.getPartitionPath(record);
      final int bucketNum = BucketIdentifier.getBucketId(recordKey, indexKeyFields, this.bucketNum);
      String fileId = BucketIdentifier.newBucketFileIdPrefix(bucketNum);
      getRowCreateHandle(partitionPath, fileId).write(recordKey, partitionPath, record);
    } catch (Throwable throwable) {
      LOG.error("Global error thrown while trying to write records in HoodieRowDataCreateHandle", throwable);
      throw throwable;
    }
  }

  private HoodieRowDataCreateHandle getRowCreateHandle(String partitionPath, String fileId) {
    if (!handles.containsKey(fileId)) { // if there is no handle corresponding to the fileId
      HoodieRowDataCreateHandle rowCreateHandle = new HoodieRowDataCreateHandle(hoodieTable, writeConfig, partitionPath, fileId,
          instantTime, taskPartitionId, taskId, taskEpochId, rowType);
      handles.put(fileId, rowCreateHandle);
    }
    return handles.get(fileId);
  }
}
