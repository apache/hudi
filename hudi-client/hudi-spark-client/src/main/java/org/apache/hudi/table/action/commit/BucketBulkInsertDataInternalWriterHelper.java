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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.index.bucket.BucketStrategist;
import org.apache.hudi.index.bucket.BucketStrategistFactory;
import org.apache.hudi.io.storage.row.HoodieRowCreateHandle;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.HoodieTable;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Helper class for native row writer for bulk_insert with bucket index.
 */
public class BucketBulkInsertDataInternalWriterHelper extends BulkInsertDataInternalWriterHelper {

  private static final Logger LOG = LoggerFactory.getLogger(BucketBulkInsertDataInternalWriterHelper.class);

  private int lastKnownBucketNum = -1;
  // p -> (bucketNum -> handle)
  protected final Map<String, Map<Integer, HoodieRowCreateHandle>> bucketHandles;
  protected final String indexKeyFields;
  protected final BucketStrategist bucketStrategist;

  public BucketBulkInsertDataInternalWriterHelper(HoodieTable hoodieTable, HoodieWriteConfig writeConfig,
                                                  String instantTime, int taskPartitionId, long taskId, long taskEpochId, StructType structType,
                                                  boolean populateMetaFields, boolean arePartitionRecordsSorted) {
    this(hoodieTable, writeConfig, instantTime, taskPartitionId, taskId, taskEpochId, structType, populateMetaFields, arePartitionRecordsSorted, false);
  }

  public BucketBulkInsertDataInternalWriterHelper(HoodieTable hoodieTable, HoodieWriteConfig writeConfig,
                                                  String instantTime, int taskPartitionId, long taskId, long taskEpochId, StructType structType,
                                                  boolean populateMetaFields, boolean arePartitionRecordsSorted, boolean shouldPreserveHoodieMetadata) {
    super(hoodieTable, writeConfig, instantTime, taskPartitionId, taskId, taskEpochId, structType, populateMetaFields, arePartitionRecordsSorted, shouldPreserveHoodieMetadata);
    this.indexKeyFields = writeConfig.getStringOrDefault(HoodieIndexConfig.BUCKET_INDEX_HASH_FIELD, writeConfig.getString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key()));
    this.bucketStrategist = BucketStrategistFactory.getInstant(writeConfig, hoodieTable.getMetaClient().getFs());
    this.bucketHandles = new HashMap<>();
  }

  public void write(InternalRow row) throws IOException {
    try {
      UTF8String partitionPath = extractPartitionPath(row);
      int bucketId;
      if (hoodieTable.getMetaClient().getTableConfig().isLSMBasedLogFormat()) {
        // for common write we get bucketID, for clustering we get file name, so that we need to get bucketID from filename
        String fileName = row.getString(HoodieRecord.FILENAME_META_FIELD_ORD);
        if (writeConfig.getOperationType().equalsIgnoreCase(WriteOperationType.CLUSTER.value())) {
          bucketId = Integer.parseInt(FSUtils.getFileId(fileName));
        } else {
          bucketId = Integer.parseInt(fileName);
        }
      } else {
        UTF8String recordKey = extractRecordKey(row);
        bucketId = BucketIdentifier.getBucketId(String.valueOf(recordKey), indexKeyFields, bucketStrategist.computeBucketNumber(partitionPath.toString()));
      }
      if (lastKnownPartitionPath == null || !Objects.equals(lastKnownPartitionPath, partitionPath) || !handle.canWrite() || bucketId != lastKnownBucketNum) {
        handle = getBucketRowCreateHandle(String.valueOf(partitionPath), bucketId);
        // NOTE: It's crucial to make a copy here, since [[UTF8String]] could be pointing into
        //       a mutable underlying buffer
        lastKnownPartitionPath = partitionPath.clone();
        lastKnownBucketNum = bucketId;
      }

      handle.write(row);
    } catch (Throwable t) {
      LOG.error("Global error thrown while trying to write records in HoodieRowCreateHandle ", t);
      throw t;
    }
  }

  protected UTF8String extractRecordKey(InternalRow row) {
    if (populateMetaFields) {
      // In case meta-fields are materialized w/in the table itself, we can just simply extract
      // partition path from there
      //
      // NOTE: Helper keeps track of [[lastKnownPartitionPath]] as [[UTF8String]] to avoid
      //       conversion from Catalyst internal representation into a [[String]]
      return row.getUTF8String(HoodieRecord.RECORD_KEY_META_FIELD_ORD);
    } else if (keyGeneratorOpt.isPresent()) {
      return keyGeneratorOpt.get().getRecordKey(row, structType);
    } else {
      return UTF8String.EMPTY_UTF8;
    }
  }

  protected HoodieRowCreateHandle getBucketRowCreateHandle(String partitionPath, int bucketId) {
    Map<Integer, HoodieRowCreateHandle> bucketHandleMap = bucketHandles.computeIfAbsent(partitionPath, p -> new HashMap<>());
    if (!bucketHandleMap.isEmpty() && bucketHandleMap.containsKey(bucketId)) {
      return bucketHandleMap.get(bucketId);
    }
    LOG.info("Creating new file for partition path {} and bucket {}", partitionPath, bucketId);
    HoodieRowCreateHandle rowCreateHandle = new HoodieRowCreateHandle(hoodieTable, writeConfig, partitionPath, getNextBucketFileId(bucketId),
        instantTime, taskPartitionId, taskId, taskEpochId, structType, shouldPreserveHoodieMetadata);
    bucketHandleMap.put(bucketId, rowCreateHandle);
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
    handle = null;
  }

  @Override
  public void abort() {
    for (Map<Integer, HoodieRowCreateHandle> entry : bucketHandles.values()) {
      for (HoodieRowCreateHandle rowCreateHandle : entry.values()) {
        rowCreateHandle.tryCleanWrittenFiles();
      }
      entry.clear();
    }
  }

  protected String getNextBucketFileId(int bucketInt) {
    return BucketIdentifier.newBucketFileIdPrefix(getNextFileId(), bucketInt);
  }
}
