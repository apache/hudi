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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.utils.LazyIterableIterator;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.table.HoodieTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


import static org.apache.hudi.index.HoodieIndexUtils.tagAsNewRecordIfNeeded;

/**
 * Simple bucket index implementation, with fixed bucket number.
 */
public class HoodieSimpleBucketIndex extends HoodieBucketIndex {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieSimpleBucketIndex.class);

  public HoodieSimpleBucketIndex(HoodieWriteConfig config) {
    super(config);
  }

  public Map<Integer, HoodieRecordLocation> loadBucketIdToFileIdMappingForPartition(
      HoodieTable hoodieTable,
      String partition) {
    // bucketId -> fileIds
    Map<Integer, HoodieRecordLocation> bucketIdToFileIdMapping = new HashMap<>();
    hoodieTable.getMetaClient().reloadActiveTimeline();
    HoodieIndexUtils
        .getLatestFileSlicesForPartition(partition, hoodieTable)
        .forEach(fileSlice -> {
          String fileId = fileSlice.getFileId();
          String commitTime = fileSlice.getBaseInstantTime();

          int bucketId = BucketIdentifier.bucketIdFromFileId(fileId);
          if (!bucketIdToFileIdMapping.containsKey(bucketId)) {
            bucketIdToFileIdMapping.put(bucketId, new HoodieRecordLocation(commitTime, fileId));
          } else {
            // Finding the instants which conflict with the bucket id
            Set<String> instants = findTheConflictBucketIdInPartition(hoodieTable ,partition, bucketId);

            // Check if bucket data is valid
            throw new HoodieIOException("Find multiple files at partition path="
                + partition + " belongs to the same bucket id = " + bucketId
                + ", these instants need to rollback: " + instants.toString()
                + ", you can use rollback_to_instant procedure to recovery");
          }
        });
    return bucketIdToFileIdMapping;
  }


  /**
   * Find out the conflict files in bucket partition with bucekt id
   */
  public HashSet<String> findTheConflictBucketIdInPartition(HoodieTable hoodieTable, String partition, int bucketId) {
    HashSet<String> instants = new HashSet<>();
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    Path basePath = metaClient.getBasePathV2();
    Path partitionPath = FSUtils.getPartitionPath(basePath, partition);
    List<HoodieInstant> pendingInstants = metaClient.getActiveTimeline().getCommitsTimeline().filterInflightsAndRequested().getInstants();

    for (HoodieInstant i : pendingInstants) {
      if (judgeInstantInPath(metaClient, partitionPath, i, bucketId)) {
        instants.add(i.getTimestamp());
      }
    }
    return instants;
  }

  public Boolean judgeInstantInPath(HoodieTableMetaClient metaClient, Path path, HoodieInstant instant, int bucketId) {
    Boolean ret = false;
    try {
      FileStatus[] fileStatuses = metaClient.getFs().listStatus(path);
      for (FileStatus status : fileStatuses) {
        String fileName = status.getPath().getName();

        try {
          if (status.isFile() && BucketIdentifier.bucketIdFromFileId(fileName) == bucketId && fileName.contains(instant.getTimestamp())) {
            ret = true;
            break;
          }
        } catch (NumberFormatException e) {
          LOG.warn("file is not bucket file");
        }

      }
    } catch (IOException e) {
      LOG.warn("partition {} is not exists", path.toString());
      ret = false;
    }

    return ret;
  }

  public int getBucketID(HoodieKey key) {
    return BucketIdentifier.getBucketId(key, indexKeyFields, numBuckets);
  }

  @Override
  public boolean canIndexLogFiles() {
    return false;
  }

  @Override
  public <R> HoodieData<HoodieRecord<R>> tagLocation(
      HoodieData<HoodieRecord<R>> records, HoodieEngineContext context,
      HoodieTable hoodieTable)
      throws HoodieIndexException {
    Map<String, Map<Integer, HoodieRecordLocation>> partitionPathFileIDList = new HashMap<>();
    return records.mapPartitions(iterator -> new LazyIterableIterator<HoodieRecord<R>, HoodieRecord<R>>(iterator) {
      @Override
      protected HoodieRecord<R> computeNext() {
        HoodieRecord record = inputItr.next();
        int bucketId = getBucketID(record.getKey());
        String partitionPath = record.getPartitionPath();
        if (!partitionPathFileIDList.containsKey(partitionPath)) {
          partitionPathFileIDList.put(partitionPath, loadBucketIdToFileIdMappingForPartition(hoodieTable, partitionPath));
        }
        HoodieRecordLocation loc = partitionPathFileIDList.get(partitionPath).getOrDefault(bucketId, null);
        return tagAsNewRecordIfNeeded(record, Option.ofNullable(loc));
      }
      }, false);
  }
}
