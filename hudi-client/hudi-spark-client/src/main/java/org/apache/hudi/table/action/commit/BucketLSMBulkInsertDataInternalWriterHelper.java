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

import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.FutureUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.storage.row.HoodieRowCreateHandle;
import org.apache.hudi.io.storage.row.LSMHoodieRowCreateHandle;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hudi.config.HoodieClusteringConfig.LSM_CLUSTERING_OUT_PUT_LEVEL;

/**
 * Helper class for native row writer for bulk_insert with bucket index.
 */
public class BucketLSMBulkInsertDataInternalWriterHelper extends BucketBulkInsertDataInternalWriterHelper {

  private static final Logger LOG = LoggerFactory.getLogger(BucketLSMBulkInsertDataInternalWriterHelper.class);
  private final int levelNumber;

  public BucketLSMBulkInsertDataInternalWriterHelper(HoodieTable hoodieTable, HoodieWriteConfig writeConfig,
                                                     String instantTime, int taskPartitionId, long taskId, long taskEpochId, StructType structType,
                                                     boolean populateMetaFields, boolean arePartitionRecordsSorted) {
    this(hoodieTable, writeConfig, instantTime, taskPartitionId, taskId, taskEpochId, structType, populateMetaFields, arePartitionRecordsSorted, false);
  }

  public BucketLSMBulkInsertDataInternalWriterHelper(HoodieTable hoodieTable, HoodieWriteConfig writeConfig,
                                                     String instantTime, int taskPartitionId, long taskId, long taskEpochId, StructType structType,
                                                     boolean populateMetaFields, boolean arePartitionRecordsSorted, boolean shouldPreserveHoodieMetadata) {
    super(hoodieTable, writeConfig, instantTime, taskPartitionId, taskId, taskEpochId, structType, populateMetaFields, arePartitionRecordsSorted, shouldPreserveHoodieMetadata);
    boolean isBulkInsert = WriteOperationType.BULK_INSERT == WriteOperationType.fromValue(writeConfig.getOperationType());
    if (((writeConfig.isForcingWriteToL1() && isBulkInsert)) || isOverWrite(writeConfig)) {
      this.levelNumber = 1;
    } else if (writeConfig.getOperationType().equalsIgnoreCase(WriteOperationType.CLUSTER.value())) {
      this.levelNumber = Integer.parseInt(writeConfig.getProps().getString(LSM_CLUSTERING_OUT_PUT_LEVEL));
    } else {
      this.levelNumber = 0;
    }
  }

  private boolean isOverWrite(HoodieWriteConfig writeConfig) {
    if (writeConfig.getBulkInsertOverWriteOperationType() == null) {
      return false;
    }
    switch (WriteOperationType.fromValue(writeConfig.getBulkInsertOverWriteOperationType())) {
      case INSERT_OVERWRITE:
      case INSERT_OVERWRITE_TABLE:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected HoodieRowCreateHandle getBucketRowCreateHandle(String partitionPath, int bucketId) {
    Map<Integer, HoodieRowCreateHandle> bucketHandleMap = bucketHandles.computeIfAbsent(partitionPath, p -> new HashMap<>());
    if (!bucketHandleMap.isEmpty() && bucketHandleMap.containsKey(bucketId)) {
      return bucketHandleMap.get(bucketId);
    }
    LOG.info("Creating new LSM file for partition path {} and bucket {}", partitionPath, bucketId);
    HoodieRowCreateHandle rowCreateHandle;
    // use bucketId as fileID directly
    rowCreateHandle = new LSMHoodieRowCreateHandle(hoodieTable, writeConfig, partitionPath, String.valueOf(bucketId),
        instantTime, taskPartitionId, taskId, taskEpochId, structType, shouldPreserveHoodieMetadata, 0, levelNumber);
    bucketHandleMap.put(bucketId, rowCreateHandle);
    return rowCreateHandle;
  }

  @Override
  public void close() throws IOException {
    ThreadPoolExecutor executor = new ThreadPoolExecutor(
        writeConfig.getLsmFlushConcurrency(), writeConfig.getLsmFlushConcurrency(),
        0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

    FutureUtils.allOf(bucketHandles.values().stream().flatMap(entry -> {
      return entry.values().stream();
    }).map(handle -> CompletableFuture.supplyAsync(() -> {
      try {
        return handle.close();
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
    }, executor)).collect(Collectors.toList())).whenComplete((result, throwable) -> {
      writeStatusList.addAll(result);
    }).join();

    try {
      executor.shutdown();
      executor.awaitTermination(24, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    bucketHandles.clear();
    handle = null;
  }
}
