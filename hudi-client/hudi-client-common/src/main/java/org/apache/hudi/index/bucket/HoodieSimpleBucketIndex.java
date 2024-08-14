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

import org.apache.hudi.client.utils.LazyIterableIterator;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    HoodieActiveTimeline hoodieActiveTimeline = hoodieTable.getMetaClient().reloadActiveTimeline();
    Set<String> pendingInstants = hoodieActiveTimeline.filterInflights().getInstantsAsStream().map(HoodieInstant::getTimestamp).collect(Collectors.toSet());

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
            List<String> instants = findConflictInstantsInPartition(hoodieTable, partition, bucketId, pendingInstants);

            // Check if bucket data is valid
            throw new HoodieIOException("Find multiple files at partition path="
                + partition + " that belong to the same bucket id = " + bucketId
                + ", these instants need to rollback: " + instants.toString()
                + ", you can use 'rollback_to_instant' procedure to revert the conflicts.");
          }
        });
    return bucketIdToFileIdMapping;
  }

  /**
   * Find out the conflict instants with given partition and bucket id.
   */
  public List<String> findConflictInstantsInPartition(HoodieTable hoodieTable, String partition, int bucketId, Set<String> pendingInstants) {
    List<String> instants = new ArrayList<>();
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    StoragePath partitionPath = new StoragePath(metaClient.getBasePath(), partition);

    List<StoragePathInfo> filesInPartition = listFilesFromPartition(metaClient, partitionPath);

    Stream<FileSlice> latestFileSlicesIncludingInflight = hoodieTable.getSliceView().getLatestFileSlicesIncludingInflight(partition);
    List<String> candidates = latestFileSlicesIncludingInflight.map(FileSlice::getLatestInstantTime)
        .filter(pendingInstants::contains)
        .collect(Collectors.toList());

    for (String i : candidates) {
      if (hasPendingDataFilesForInstant(filesInPartition, i, bucketId)) {
        instants.add(i);
      }
    }
    return instants;
  }

  private static List<StoragePathInfo> listFilesFromPartition(HoodieTableMetaClient metaClient, StoragePath partitionPath) {
    try {
      return metaClient.getStorage().listFiles(partitionPath);
    } catch (IOException e) {
      // ignore the exception though
      return Collections.emptyList();
    }
  }

  public Boolean hasPendingDataFilesForInstant(List<StoragePathInfo> filesInPartition, String instant, int bucketId) {
    for (StoragePathInfo status : filesInPartition) {
      String fileName = status.getPath().getName();

      try {
        if (status.isFile() && BucketIdentifier.bucketIdFromFileId(fileName) == bucketId && fileName.contains(instant)) {
          return true;
        }
      } catch (NumberFormatException e) {
        LOG.warn("File is not bucket file");
      }
    }
    return false;
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
