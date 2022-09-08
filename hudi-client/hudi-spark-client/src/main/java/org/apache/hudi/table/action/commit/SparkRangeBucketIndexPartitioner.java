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

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.index.bucket.HoodieRangeBucketIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;
import scala.Tuple2;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Packs incoming records to be inserted into buckets (1 bucket = 1 RDD partition).
 */
public class SparkRangeBucketIndexPartitioner<T extends HoodieRecordPayload<T>> extends
    SparkHoodiePartitioner<T> {

  private final int bucketRangeStepSize;

  private final Map<Pair<String, Integer>, Integer> partitionBucketNumIndexMap;

  private final Map<Integer, Pair<String, Integer>> indexPartitionBucketNumMap;

  /**
   * Partition path and file groups in it pair. Decide the file group an incoming update should go to.
   */
  private Map<String, Set<String>> updatePartitionPathFileIds;

  public SparkRangeBucketIndexPartitioner(WorkloadProfile profile,
                                          HoodieEngineContext context,
                                          HoodieTable table,
                                          HoodieWriteConfig config) {
    super(profile, table);
    if (!(table.getIndex() instanceof HoodieRangeBucketIndex)) {
      throw new HoodieException(
        " Bucket index partitioner should only be used by BucketIndex other than "
          + table.getIndex().getClass().getSimpleName());
    }
    this.bucketRangeStepSize = config.getBucketRangeStepSize();
    this.indexPartitionBucketNumMap = ((HoodieRangeBucketIndex) table.getIndex()).getIndexPartitionBucketNumMap();
    this.partitionBucketNumIndexMap = ((HoodieRangeBucketIndex) table.getIndex()).getPartitionBucketNumIndexMap();

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
  public BucketInfo getBucketInfo(int partitionNum) {
    Pair<String, Integer> pmod = indexPartitionBucketNumMap.get(partitionNum);
    String partitionPath = pmod.getLeft();
    String bucketId = BucketIdentifier.bucketIdStr(pmod.getRight());
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
    return indexPartitionBucketNumMap.size();
  }

  @Override
  public int getPartition(Object key) {
    Tuple2<HoodieKey, Option<HoodieRecordLocation>> keyLocation = (Tuple2<HoodieKey, Option<HoodieRecordLocation>>) key;
    String partitionPath = keyLocation._1.getPartitionPath();

    String recordKey = keyLocation._1.getRecordKey();
    if (recordKey.contains(":")) {
      recordKey = recordKey.substring(recordKey.indexOf(":") + 1);
    }
    long l = Long.parseLong(recordKey) / bucketRangeStepSize;
    return partitionBucketNumIndexMap.get(Pair.of(partitionPath, (int) l));
  }

}
