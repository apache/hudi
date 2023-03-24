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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.io.storage.row.HoodieRowCreateHandle;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BulkInsertBucketInternalWriterHelper extends BulkInsertDataInternalWriterHelper {

  private static final Logger LOG = LogManager.getLogger(BulkInsertBucketInternalWriterHelper.class);

  private int lastKnownBucketNum = -1;
  // p -> (bucketNum -> handle)
  private Map<String, Map<Integer, HoodieRowCreateHandle>> bucketHandles = new HashMap<>();
  private String indexKeyFields;
  private int bucketNum = -1;

  public BulkInsertBucketInternalWriterHelper(HoodieTable hoodieTable, HoodieWriteConfig writeConfig,
      String instantTime, int taskPartitionId, long taskId, long taskEpochId, StructType structType,
      boolean populateMetaFields, boolean arePartitionRecordsSorted) {
    super(hoodieTable, writeConfig, instantTime, taskPartitionId, taskId, taskEpochId, structType, populateMetaFields, arePartitionRecordsSorted);
    indexKeyFields = writeConfig.getStringOrDefault(HoodieIndexConfig.BUCKET_INDEX_HASH_FIELD, writeConfig.getString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key()));
    bucketNum = writeConfig.getInt(HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS);
  }

  @Override
  protected void tryCreateNewHandle(UTF8String partitionPath, UTF8String recordKey) throws IOException {
    int bucketId = BucketIdentifier.getBucketId(String.valueOf(recordKey), indexKeyFields, bucketNum);
    if ((lastKnownPartitionPath == null) || !lastKnownPartitionPath.equals(partitionPath) || !handle.canWrite() || bucketId != lastKnownBucketNum) {
      handle = getBucketRowCreateHandle(String.valueOf(partitionPath), bucketId);
      lastKnownPartitionPath = partitionPath;
      lastKnownBucketNum = bucketId;
    }
  }

  protected HoodieRowCreateHandle getBucketRowCreateHandle(String partitionPath, int bucketId) throws IOException {
    Map<Integer, HoodieRowCreateHandle> bucketHandleMap = bucketHandles.getOrDefault(partitionPath, new HashMap<>());
    if (!bucketHandleMap.isEmpty() && bucketHandleMap.containsKey(bucketId)) {
      return bucketHandleMap.get(bucketId);
    }
    LOG.info("Creating new file for partition path " + partitionPath);
    HoodieRowCreateHandle rowCreateHandle = new HoodieRowCreateHandle(hoodieTable, writeConfig, partitionPath, getNextBucketFileId(bucketId),
        instantTime, taskPartitionId, taskId, taskEpochId, structType, shouldPreserveHoodieMetadata);
    bucketHandleMap.put(bucketId, rowCreateHandle);
    bucketHandles.put(partitionPath, bucketHandleMap);
    return rowCreateHandle;
  }

  @Override
  public void close() throws IOException {
    for (Map<Integer, HoodieRowCreateHandle> entry : bucketHandles.values()) {
      for (HoodieRowCreateHandle rowCreateHandle : entry.values()) {
        LOG.info("Closing bulk insert file " + rowCreateHandle.getFileName());
        writeStatusList.add(rowCreateHandle.close());
      }
      entry.clear();
    }
    bucketHandles.clear();
  }

  protected String getNextBucketFileId(int bucketInt) {
    return BucketIdentifier.newBucketFileIdPrefix(getNextFileId(), bucketInt);
  }
}
