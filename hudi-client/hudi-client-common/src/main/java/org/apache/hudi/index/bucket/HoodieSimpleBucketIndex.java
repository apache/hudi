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
import org.apache.hudi.common.util.collection.Pair;
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

  private static final Logger LOG =  LogManager.getLogger(HoodieSimpleBucketIndex.class);

  /**
   * partitionPath -> bucketId -> fileInfo
   */
  Map<String, Map<Integer, Pair<String, String>>> partitionPathFileIDList;

  public HoodieSimpleBucketIndex(HoodieWriteConfig config) {
    super(config);
  }

  private Map<Integer, Pair<String, String>> loadPartitionBucketIdFileIdMapping(
      HoodieTable hoodieTable,
      String partition) {
    // bucketId -> fileIds
    Map<Integer, Pair<String, String>> fileIDList = new HashMap<>();
    hoodieTable.getMetaClient().reloadActiveTimeline();
    HoodieIndexUtils
        .getLatestBaseFilesForPartition(partition, hoodieTable)
        .forEach(file -> {
          String fileId = file.getFileId();
          String commitTime = file.getCommitTime();
          int bucketId = BucketIdentifier.bucketIdFromFileId(fileId);
          if (!fileIDList.containsKey(bucketId)) {
            fileIDList.put(bucketId, Pair.of(fileId, commitTime));
          } else {
            // check if bucket data is valid
            throw new HoodieIOException("Find multiple files at partition path="
                + partition + " belongs to the same bucket id = " + bucketId);
          }
        });
    return fileIDList;
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
    if (partitionPathFileIDList.get(partitionPath).containsKey(bucketId)) {
      Pair<String, String> fileInfo = partitionPathFileIDList.get(partitionPath).get(bucketId);
      return new HoodieRecordLocation(fileInfo.getRight(), fileInfo.getLeft());
    }
    return null;
  }
}
