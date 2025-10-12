/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table.action.commit;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.index.bucket.HoodieBucketIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hudi.common.model.WriteOperationType.INSERT_OVERWRITE;
import static org.apache.hudi.common.model.WriteOperationType.INSERT_OVERWRITE_TABLE;

/**
 * Packs incoming records to be inserted into buckets (1 bucket = 1 partition) for Java engine.
 * Equivalent to SparkBucketIndexPartitioner.
 * TODO: Reduce duplicate code between Spark and Java version.
 */
public class JavaBucketIndexPartitioner implements JavaPartitioner {

  private final int numBuckets;
  private final String indexKeyField;
  private final int totalPartitionPaths;
  private final List<String> partitionPaths;
  /**
   * Helps get the partition id, partition id is partition offset + bucket id.
   * The partition offset is a multiple of the bucket num.
   */
  private final Map<String, Integer> partitionPathOffset;
  private final boolean isOverwrite;

  /**
   * Partition path and file groups in it pair. Decide the file group an incoming update should go to.
   */
  private final Map<String, Set<String>> updatePartitionPathFileIds;

  /**
   * Bucket number to bucket info mapping.
   */
  private final Map<Integer, BucketInfo> bucketInfoMap;

  private final boolean isNonBlockingConcurrencyControl;

  public JavaBucketIndexPartitioner(WorkloadProfile profile,
                                    HoodieEngineContext context,
                                    HoodieTable table,
                                    HoodieWriteConfig config) {
    if (!(table.getIndex() instanceof HoodieBucketIndex)) {
      throw new HoodieException(
          "Bucket index partitioner should only be used by BucketIndex other than "
              + table.getIndex().getClass().getSimpleName());
    }
    this.numBuckets = ((HoodieBucketIndex) table.getIndex()).getNumBuckets();
    this.indexKeyField = config.getBucketIndexHashField();
    this.totalPartitionPaths = profile.getPartitionPaths().size();
    this.partitionPaths = new ArrayList<>(profile.getPartitionPaths());
    this.partitionPathOffset = new HashMap<>();
    int i = 0;
    for (Object partitionPath : profile.getPartitionPaths()) {
      partitionPathOffset.put(partitionPath.toString(), i);
      i += numBuckets;
    }
    this.updatePartitionPathFileIds = new HashMap<>();
    this.bucketInfoMap = new HashMap<>();
    assignUpdates(profile);
    WriteOperationType operationType = profile.getOperationType();
    this.isOverwrite = INSERT_OVERWRITE.equals(operationType) || INSERT_OVERWRITE_TABLE.equals(operationType);
    this.isNonBlockingConcurrencyControl = config.isNonBlockingConcurrencyControl();

    if (isOverwrite) {
      ValidationUtils.checkArgument(!isNonBlockingConcurrencyControl,
          "Insert overwrite is not supported with non-blocking concurrency control");
    }
  }

  private void assignUpdates(WorkloadProfile profile) {
    // Each update location gets tracked
    Set<Map.Entry<String, WorkloadStat>> partitionStatEntries = profile.getInputPartitionPathStatMap()
        .entrySet();
    for (Map.Entry<String, WorkloadStat> partitionStat : partitionStatEntries) {
      if (!updatePartitionPathFileIds.containsKey(partitionStat.getKey())) {
        updatePartitionPathFileIds.put(partitionStat.getKey(), new HashSet<>());
      }
      for (Map.Entry<String, Pair<String, Long>> updateLocEntry :
          partitionStat.getValue().getUpdateLocationToCount().entrySet()) {
        updatePartitionPathFileIds.get(partitionStat.getKey()).add(updateLocEntry.getKey());
      }
    }
  }

  @Override
  public int getNumPartitions() {
    return totalPartitionPaths * numBuckets;
  }

  @Override
  public int getPartition(Object key) {
    Pair<HoodieKey, Option<HoodieRecordLocation>> keyLocation = (Pair<HoodieKey, Option<HoodieRecordLocation>>) key;
    String partitionPath = keyLocation.getLeft().getPartitionPath();
    Option<HoodieRecordLocation> location = keyLocation.getRight();
    int bucketId = location.isPresent()
        ? BucketIdentifier.bucketIdFromFileId(location.get().getFileId())
        : BucketIdentifier.getBucketId(keyLocation.getLeft().getRecordKey(), indexKeyField, numBuckets);
    return partitionPathOffset.get(partitionPath) + bucketId;
  }

  public BucketInfo getBucketInfo(int bucketNumber) {
    return bucketInfoMap.computeIfAbsent(bucketNumber, k -> {
      int bucketId = bucketNumber % numBuckets;
      String partitionPath = partitionPaths.get(bucketNumber / numBuckets);
      return getBucketInfo(bucketId, partitionPath);
    });
  }

  protected BucketInfo getBucketInfo(int bucketId, String partitionPath) {
    String bucketIdStr = BucketIdentifier.bucketIdStr(bucketId);
    // Insert overwrite always generates new bucket file id
    if (isOverwrite) {
      return new BucketInfo(BucketType.INSERT, BucketIdentifier.newBucketFileIdPrefix(bucketIdStr), partitionPath);
    }
    Option<String> fileIdOption = Option.fromJavaOptional(updatePartitionPathFileIds
        .getOrDefault(partitionPath, Collections.emptySet()).stream()
        .filter(e -> e.startsWith(bucketIdStr))
        .findFirst());
    if (fileIdOption.isPresent()) {
      return new BucketInfo(BucketType.UPDATE, fileIdOption.get(), partitionPath);
    } else {
      // Always write into log file instead of base file if using NB-CC
      if (isNonBlockingConcurrencyControl) {
        return new BucketInfo(BucketType.UPDATE, BucketIdentifier.newBucketFileIdForNBCC(bucketIdStr), partitionPath);
      }
      return new BucketInfo(BucketType.INSERT, BucketIdentifier.newBucketFileIdPrefix(bucketIdStr), partitionPath);
    }
  }

  @Override
  public List<String> getSmallFileIds() {
    return Collections.emptyList();
  }
}
