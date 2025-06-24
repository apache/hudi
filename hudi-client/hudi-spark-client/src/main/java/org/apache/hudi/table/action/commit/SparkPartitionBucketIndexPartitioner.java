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
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.index.bucket.HoodieBucketIndex;
import org.apache.hudi.index.bucket.partition.NumBucketsFunction;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import scala.Tuple2;

import static org.apache.hudi.common.model.WriteOperationType.INSERT_OVERWRITE;
import static org.apache.hudi.common.model.WriteOperationType.INSERT_OVERWRITE_TABLE;

/**
 * Packs incoming records to be inserted into buckets (1 bucket = 1 RDD partition).
 *
 * For Partition Level bucket index
 * Creates direct lookup arrays during initialization, Provides O(1) lookup time for partition path and bucket ID,
 * The trade-off is a small amount of additional memory usage for the lookup arrays.
 */
public class SparkPartitionBucketIndexPartitioner<T> extends SparkHoodiePartitioner<T> {

  private final int totalPartitions;
  private final NumBucketsFunction numBucketsFunction;
  private final String indexKeyField;
  private final int totalPartitionPaths;
  private final List<String> partitionPaths;
  /**
   * Helps get the RDD partition id, partition id is partition offset + bucket id.
   * The partition offset is a multiple of the bucket num.
   */
  private final Map<String, Integer> partitionPathOffset;
  private final boolean isOverwrite;

  /**
   * Partition path and file groups in it pair. Decide the file group an incoming update should go to.
   */
  private Map<String, Set<String>> updatePartitionPathFileIds;

  private final boolean isNonBlockingConcurrencyControl;
  /**
   * Direct mapping from partition number to partition path.
   */
  private final String[] partitionNumberToPath;

  /**
   * Direct mapping from partition number to local bucket ID.
   */
  private final Integer[] partitionNumberToLocalBucketId;

  public SparkPartitionBucketIndexPartitioner(WorkloadProfile profile,
                                              HoodieEngineContext context,
                                              HoodieTable table,
                                              HoodieWriteConfig config) {
    super(profile, table);
    if (!(table.getIndex() instanceof HoodieBucketIndex)) {
      throw new HoodieException(
          " Bucket index partitioner should only be used by BucketIndex other than "
              + table.getIndex().getClass().getSimpleName());
    }
    HoodieWriteConfig writeConfig = table.getConfig();
    this.numBucketsFunction = NumBucketsFunction.fromWriteConfig(writeConfig);

    this.indexKeyField = config.getBucketIndexHashField();
    this.totalPartitionPaths = profile.getPartitionPaths().size();
    partitionPaths = new ArrayList<>(profile.getPartitionPaths());
    partitionPathOffset = new HashMap<>();
    int i = 0;
    for (Object partitionPath : profile.getPartitionPaths()) {
      partitionPathOffset.put(partitionPath.toString(), i);
      i += numBucketsFunction.getNumBuckets(partitionPath.toString());
    }
    this.totalPartitions = i;
    assignUpdates(profile);
    WriteOperationType operationType = profile.getOperationType();
    this.isOverwrite = INSERT_OVERWRITE.equals(operationType) || INSERT_OVERWRITE_TABLE.equals(operationType);
    this.isNonBlockingConcurrencyControl = config.isNonBlockingConcurrencyControl();

    this.partitionNumberToPath = new String[totalPartitions];
    this.partitionNumberToLocalBucketId = new Integer[totalPartitions];

    for (String partitionPath : partitionPaths) {
      int offset = partitionPathOffset.get(partitionPath);
      int numBuckets = numBucketsFunction.getNumBuckets(partitionPath);

      for (int j = 0; j < numBuckets; j++) {
        int partitionNumber = offset + j;
        partitionNumberToPath[partitionNumber] = partitionPath;
        partitionNumberToLocalBucketId[partitionNumber] = j;
      }
    }
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
  public SparkBucketInfoGetter getSparkBucketInfoGetter() {
    return new SparkPartitionBucketIndexBucketInfoGetter(partitionNumberToLocalBucketId, partitionNumberToPath,
        updatePartitionPathFileIds, isOverwrite, isNonBlockingConcurrencyControl);
  }

  @Override
  public int numPartitions() {
    return totalPartitions;
  }

  @Override
  public int getPartition(Object key) {
    Tuple2<HoodieKey, Option<HoodieRecordLocation>> keyLocation = (Tuple2<HoodieKey, Option<HoodieRecordLocation>>) key;
    String partitionPath = keyLocation._1.getPartitionPath();
    Option<HoodieRecordLocation> location = keyLocation._2;
    int bucketId = location.isPresent()
        ? BucketIdentifier.bucketIdFromFileId(location.get().getFileId())
        : BucketIdentifier.getBucketId(keyLocation._1.getRecordKey(), indexKeyField, numBucketsFunction.getNumBuckets(partitionPath));
    return partitionPathOffset.get(partitionPath) + bucketId;
  }
}
