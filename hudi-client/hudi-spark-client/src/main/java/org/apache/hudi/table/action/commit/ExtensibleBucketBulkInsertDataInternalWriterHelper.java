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
import org.apache.hudi.common.model.SortMarker;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.index.bucket.ExtensibleBucketIdentifier;
import org.apache.hudi.index.bucket.ExtensibleBucketIndexUtils;
import org.apache.hudi.io.storage.row.HoodieRowCreateHandle;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;

/**
 * Helper class for native row writer for bulk_insert with extensible bucket index.
 * Only deal with one partition => one file group.
 */
public class ExtensibleBucketBulkInsertDataInternalWriterHelper extends BucketBulkInsertDataInternalWriterHelper {

  private static final Logger LOG = LoggerFactory.getLogger(ExtensibleBucketBulkInsertDataInternalWriterHelper.class);

  // Only present when `arePartitionRecordsSorted` is true
  protected Option<SortMarker> sortMarkerOption = Option.empty();

  public ExtensibleBucketBulkInsertDataInternalWriterHelper(HoodieTable hoodieTable, HoodieWriteConfig writeConfig, String instantTime, int taskPartitionId, long taskId, long taskEpochId,
                                                            StructType structType, boolean populateMetaFields, boolean arePartitionRecordsSorted) {
    this(hoodieTable, writeConfig, instantTime, taskPartitionId, taskId, taskEpochId, structType, populateMetaFields, arePartitionRecordsSorted, false, null);
  }

  public ExtensibleBucketBulkInsertDataInternalWriterHelper(HoodieTable hoodieTable, HoodieWriteConfig writeConfig, String instantTime, int taskPartitionId, long taskId, long taskEpochId,
                                                            StructType structType, boolean populateMetaFields, boolean arePartitionRecordsSorted, boolean shouldPreserveHoodieMetadata,
                                                            SortMarker sortMarker) {
    super(hoodieTable, writeConfig, instantTime, taskPartitionId, taskId, taskEpochId, structType, populateMetaFields, arePartitionRecordsSorted, shouldPreserveHoodieMetadata);
    if (sortMarker != null) {
      ValidationUtils.checkArgument(arePartitionRecordsSorted, "Sort marker is only valid when records are sorted");
      this.sortMarkerOption = Option.of(sortMarker);
    }
  }

  @Override
  public void write(InternalRow row) throws IOException {
    try {
      if (handle == null) {
        // We can ensure that one writer has only one handle
        String partitionPath = String.valueOf(extractPartitionPath(row));
        String recordKey = String.valueOf(extractRecordKey(row));
        handle = getBucketRowCreateHandle(partitionPath, recordKey);
      }
      handle.write(row);
    } catch (Throwable t) {
      LOG.error("Global error thrown while trying to write records in HoodieRowCreateHandle for ExtensibleBucket ", t);
      throw new IOException(t);
    }
  }

  private HoodieRowCreateHandle getBucketRowCreateHandle(String partitionPath, String recordKey) {
    ExtensibleBucketIdentifier bucketIdentifier = getBucketIdentifier(partitionPath);
    int bucketId = bucketIdentifier.getBucketId(recordKey, indexKeyFields);
    short bucketVersion = bucketIdentifier.getMetadata().getBucketVersion();
    String fileIdPrefix = BucketIdentifier.newExtensibleBucketFileIdFixedSuffix(bucketId, bucketVersion);
    String fileId = FSUtils.createNewFileId(fileIdPrefix, 0);

    ValidationUtils.checkArgument(bucketIdentifier.isPending() || !bucketIdentifier.isPartitionExist(),
        "Extensible Bucket bulk_insert only support write to new file group");

    HoodieRowCreateHandle createHandle = new HoodieRowCreateHandle(hoodieTable, writeConfig, partitionPath, fileId,
        instantTime, taskPartitionId, taskId, taskEpochId, structType, shouldPreserveHoodieMetadata);
    if (sortMarkerOption.isPresent()) {
      createHandle.setSortMarker(sortMarkerOption.get());
    }
    return createHandle;
  }

  private ExtensibleBucketIdentifier getBucketIdentifier(String partition) {
    return ExtensibleBucketIndexUtils.fetchLatestUncommittedExtensibleBucketIdentifier(hoodieTable, Collections.singleton(partition), true).get(partition);
  }

  @Override
  public void close() throws IOException {
    if (handle != null) {
      LOG.info("closing bulk_inset file : {}", handle.getFileName());
      writeStatusList.add(handle.close());
      handle = null;
    }
  }
}
