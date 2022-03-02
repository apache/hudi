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

package org.apache.hudi.index.bucket;

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.table.HoodieTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Simple bucket index implementation, with fixed bucket number.
 */
public class HoodieSimpleBucketIndex extends HoodieBucketIndex {

  private static final Logger LOG = LogManager.getLogger(HoodieSimpleBucketIndex.class);

  /**
   * Mapping from partitionPath -> bucketId -> fileInfo
   */
  private Map<String, Map<Integer, HoodieRecordLocation>> partitionPathFileIDList;

  public HoodieSimpleBucketIndex(HoodieWriteConfig config) {
    super(config);
  }

  private Map<Integer, HoodieRecordLocation> loadPartitionBucketIdFileIdMapping(
      HoodieTable hoodieTable,
      String partition) {
    // bucketId -> fileIds
    Map<Integer, HoodieRecordLocation> bucketIdToFileIdMapping = new HashMap<>();
    hoodieTable.getMetaClient().reloadActiveTimeline();
    HoodieIndexUtils
        .getLatestBaseFilesForPartition(partition, hoodieTable)
        .forEach(file -> {
          String fileId = file.getFileId();
          String commitTime = file.getCommitTime();
          int bucketId = BucketIdentifier.bucketIdFromFileId(fileId);
          if (!bucketIdToFileIdMapping.containsKey(bucketId)) {
            bucketIdToFileIdMapping.put(bucketId, new HoodieRecordLocation(commitTime, fileId));
          } else {
            // Check if bucket data is valid
            throw new HoodieIOException("Find multiple files at partition path="
                + partition + " belongs to the same bucket id = " + bucketId);
          }
        });
    return bucketIdToFileIdMapping;
  }

  @Override
  public boolean canIndexLogFiles() {
    return false;
  }

  @Override
  protected void initialize(HoodieTable table, List<String> partitions) {
    partitionPathFileIDList = new HashMap<>();
    partitions.forEach(p -> partitionPathFileIDList.put(p, loadPartitionBucketIdFileIdMapping(table, p)));
  }

  @Override
  protected HoodieRecordLocation getBucket(HoodieKey key, String partitionPath) {
    int bucketId = BucketIdentifier.getBucketId(key, config.getBucketIndexHashField(), numBuckets);
    Map<Integer, HoodieRecordLocation> bucketIdToFileIdMapping = partitionPathFileIDList.get(partitionPath);
    return bucketIdToFileIdMapping.getOrDefault(bucketId, null);
  }
}
