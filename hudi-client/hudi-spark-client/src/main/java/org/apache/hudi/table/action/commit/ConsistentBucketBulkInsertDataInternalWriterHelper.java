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
import org.apache.hudi.common.model.ConsistentHashingNode;
import org.apache.hudi.common.model.HoodieConsistentHashingMetadata;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.bucket.ConsistentBucketIdentifier;
import org.apache.hudi.index.bucket.ConsistentBucketIndexUtils;
import org.apache.hudi.io.storage.row.HoodieRowCreateHandle;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.cluster.util.ConsistentHashingUpdateStrategyUtils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Helper class for native row writer for bulk_insert with consistent hashing bucket index.
 */
public class ConsistentBucketBulkInsertDataInternalWriterHelper extends BucketBulkInsertDataInternalWriterHelper {

  private static final Logger LOG = LoggerFactory.getLogger(ConsistentBucketBulkInsertDataInternalWriterHelper.class);

  public ConsistentBucketBulkInsertDataInternalWriterHelper(HoodieTable hoodieTable, HoodieWriteConfig writeConfig,
                                                            String instantTime, int taskPartitionId, long taskId, long taskEpochId, StructType structType,
                                                            boolean populateMetaFields, boolean arePartitionRecordsSorted) {
    this(hoodieTable, writeConfig, instantTime, taskPartitionId, taskId, taskEpochId, structType, populateMetaFields, arePartitionRecordsSorted, false);
  }

  public ConsistentBucketBulkInsertDataInternalWriterHelper(HoodieTable hoodieTable, HoodieWriteConfig writeConfig,
                                                            String instantTime, int taskPartitionId, long taskId, long taskEpochId, StructType structType,
                                                            boolean populateMetaFields, boolean arePartitionRecordsSorted, boolean shouldPreserveHoodieMetadata) {
    super(hoodieTable, writeConfig, instantTime, taskPartitionId, taskId, taskEpochId, structType, populateMetaFields, arePartitionRecordsSorted, shouldPreserveHoodieMetadata);
  }

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
      LOG.error("Global error thrown while trying to write records in HoodieRowCreateHandle ", t);
      throw new IOException(t);
    }
  }

  private HoodieRowCreateHandle getBucketRowCreateHandle(String partitionPath, String recordKey) {
    ConsistentBucketIdentifier identifier = getBucketIdentifier(partitionPath);
    final ConsistentHashingNode node = identifier.getBucket(recordKey, indexKeyFields);
    String fileId = FSUtils.createNewFileId(node.getFileIdPrefix(), 0);

    ValidationUtils.checkArgument(node.getTag() != ConsistentHashingNode.NodeTag.NORMAL
            || hoodieTable.getFileSystemView()
            .getAllFileGroups(partitionPath)
            // Find file groups with committed file slices
            .filter(fg -> fg.getAllFileSlices().findAny().isPresent())
            // Ensure only bulk insert into new file group
            .noneMatch(fg -> fg.getFileGroupId().getFileId().equals(fileId)),
        "Consistent Hashing bulk_insert only support write to new file group");

    // Always create new base file for bulk_insert
    return new HoodieRowCreateHandle(hoodieTable, writeConfig, partitionPath, fileId,
        instantTime, taskPartitionId, taskId, taskEpochId, structType, schema, shouldPreserveHoodieMetadata);
  }

  private ConsistentBucketIdentifier getBucketIdentifier(String partition) {
    Set<HoodieFileGroupId> fileGroupsInPendingClustering = hoodieTable.getFileSystemView().getFileGroupsInPendingClustering()
        .map(Pair::getKey).collect(Collectors.toSet());
    if (fileGroupsInPendingClustering.stream().anyMatch(f -> f.getPartitionPath().equals(partition))) {
      Pair<String, ConsistentBucketIdentifier> bucketIdentifierPair =
          ConsistentHashingUpdateStrategyUtils.constructPartitionToIdentifier(Collections.singleton(partition), hoodieTable).get(partition);
      return bucketIdentifierPair.getRight();
    } else {
      HoodieConsistentHashingMetadata metadata = ConsistentBucketIndexUtils.loadOrCreateMetadata(hoodieTable, String.valueOf(partition), bucketNum);
      ValidationUtils.checkState(metadata != null);
      return new ConsistentBucketIdentifier(metadata);
    }
  }

  @Override
  public void close() throws IOException {
    if (handle != null) {
      LOG.info("Closing bulk insert file {}", handle.getFileName());
      writeStatusList.add(handle.close());
      handle = null;
    }
  }

}
