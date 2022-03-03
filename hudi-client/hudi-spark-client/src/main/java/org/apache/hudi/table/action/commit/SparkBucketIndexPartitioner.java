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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hudi.index.bucket.BucketIdentifier;
import scala.Tuple2;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.bucket.HoodieBucketIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;

/**
 * Packs incoming records to be inserted into buckets (1 bucket = 1 RDD partition).
 */
public class SparkBucketIndexPartitioner<T extends HoodieRecordPayload<T>> extends
    SparkHoodiePartitioner<T> {

  private final int numBuckets;
  private final String indexKeyField;
  private final int totalPartitionPaths;
  private final List<String> partitionPaths;
  /**
   * Helps get the RDD partition id, partition id is partition offset + bucket id.
   * The partition offset is a multiple of the bucket num.
   */
  private final Map<String, Integer> partitionPathOffset;

  /**
   * Partition path and file groups in it pair. Decide the file group an incoming update should go to.
   */
  private Map<String, Set<String>> updatePartitionPathFileIds;

  public SparkBucketIndexPartitioner(WorkloadProfile profile,
                                     HoodieEngineContext context,
                                     HoodieTable table,
                                     HoodieWriteConfig config) {
    super(profile, table);
    if (!(table.getIndex() instanceof HoodieBucketIndex)) {
      throw new HoodieException(
          " Bucket index partitioner should only be used by BucketIndex other than "
              + table.getIndex().getClass().getSimpleName());
    }
    this.numBuckets = ((HoodieBucketIndex) table.getIndex()).getNumBuckets();
    this.indexKeyField = config.getBucketIndexHashField();
    this.totalPartitionPaths = profile.getPartitionPaths().size();
    partitionPaths = new ArrayList<>(profile.getPartitionPaths());
    partitionPathOffset = new HashMap<>();
    int i = 0;
    for (Object partitionPath : profile.getPartitionPaths()) {
      partitionPathOffset.put(partitionPath.toString(), i);
      i += numBuckets;
    }
    assignUpdates(profile);
  }

  private void assignUpdates(WorkloadProfile profile) {
    updatePartitionPathFileIds = new HashMap<>();
    // each update location gets a partition
    Set<Entry<String, WorkloadStat>> partitionStatEntries = profile.getInputPartitionPathStatMap()
        .entrySet();
    for (Entry<String, WorkloadStat> partitionStat : partitionStatEntries) {
      if (!updatePartitionPathFileIds.containsKey(partitionStat.getKey())) {
        updatePartitionPathFileIds.put(partitionStat.getKey(), new HashSet<>());
      }
      for (Entry<String, Pair<String, Long>> updateLocEntry :
          partitionStat.getValue().getUpdateLocationToCount().entrySet()) {
        updatePartitionPathFileIds.get(partitionStat.getKey()).add(updateLocEntry.getKey());
      }
    }
  }

  @Override
  public BucketInfo getBucketInfo(int bucketNumber) {
    String partitionPath = partitionPaths.get(bucketNumber / numBuckets);
    String bucketId = BucketIdentifier.bucketIdStr(bucketNumber % numBuckets);
    Option<String> fileIdOption = Option.fromJavaOptional(updatePartitionPathFileIds
        .getOrDefault(partitionPath, Collections.emptySet()).stream()
        .filter(e -> e.startsWith(bucketId))
        .findFirst());
    if (fileIdOption.isPresent()) {
      return new BucketInfo(BucketType.UPDATE, fileIdOption.get(), partitionPath);
    } else {
      return new BucketInfo(BucketType.INSERT, BucketIdentifier.newBucketFileIdPrefix(bucketId), partitionPath);
    }
  }

  @Override
  public int numPartitions() {
    return totalPartitionPaths * numBuckets;
  }

  @Override
  public int getPartition(Object key) {
    Tuple2<HoodieKey, Option<HoodieRecordLocation>> keyLocation = (Tuple2<HoodieKey, Option<HoodieRecordLocation>>) key;
    String partitionPath = keyLocation._1.getPartitionPath();
    Option<HoodieRecordLocation> location = keyLocation._2;
    int bucketId = location.isPresent()
        ? BucketIdentifier.bucketIdFromFileId(location.get().getFileId())
        : BucketIdentifier.getBucketId(keyLocation._1, indexKeyField, numBuckets);
    return partitionPathOffset.get(partitionPath) + bucketId;
  }
}
