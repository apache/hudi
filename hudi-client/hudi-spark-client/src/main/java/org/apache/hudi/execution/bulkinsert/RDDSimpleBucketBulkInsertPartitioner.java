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

package org.apache.hudi.execution.bulkinsert;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.index.bucket.HoodieSimpleBucketIndex;
import org.apache.hudi.index.bucket.partition.NumBucketsFunction;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;

public class RDDSimpleBucketBulkInsertPartitioner<T extends HoodieRecordPayload> extends RDDBucketIndexPartitioner<T> {

  private final boolean isNonBlockingConcurrencyControl;
  private final NumBucketsFunction numBucketsFunction;

  public RDDSimpleBucketBulkInsertPartitioner(HoodieTable table) {
    super(table, null, false);
    ValidationUtils.checkArgument(table.getIndex() instanceof HoodieSimpleBucketIndex);
    HoodieWriteConfig writeConfig = table.getConfig();
    this.isNonBlockingConcurrencyControl = writeConfig.isNonBlockingConcurrencyControl();
    this.numBucketsFunction = NumBucketsFunction.fromWriteConfig(writeConfig);
  }

  @Override
  public JavaRDD<HoodieRecord<T>> repartitionRecords(JavaRDD<HoodieRecord<T>> records, int outputPartitions) {
    HoodieSimpleBucketIndex index = (HoodieSimpleBucketIndex) table.getIndex();
    Map<String, Integer> fileIdPrefixToBucketIndex = new HashMap<>();

    // Map <partition, <bucketNo, fileID>>
    Map<String, Map<Integer, String>> partitionMapper = getPartitionMapper(records, fileIdPrefixToBucketIndex);

    return doPartition(records, new Partitioner() {
      @Override
      public int numPartitions() {
        return partitionMapper.values().stream().mapToInt(Map::size).sum();
      }

      @Override
      public int getPartition(Object key) {
        HoodieKey hoodieKey = (HoodieKey) key;
        String partitionPath = hoodieKey.getPartitionPath();
        int bucketID = index.getBucketID(hoodieKey, numBucketsFunction.getNumBuckets(partitionPath));
        String fileID = partitionMapper.get(partitionPath).get(bucketID);
        return fileIdPrefixToBucketIndex.get(fileID);
      }
    });
  }

  Map<String, Map<Integer, String>> getPartitionMapper(JavaRDD<HoodieRecord<T>> records,
                                                       Map<String, Integer> fileIdPrefixToBucketIndex) {

    HoodieSimpleBucketIndex index = (HoodieSimpleBucketIndex) table.getIndex();
    return records
        .map(HoodieRecord::getPartitionPath)
        .distinct().collect().stream()
        .collect(Collectors.toMap(p -> p, p -> {
          // p ==> partition path
          Map<Integer, HoodieRecordLocation> locationMap = index.loadBucketIdToFileIdMappingForPartition(table, p);
          Map<Integer, String> bucketIdToFileIdPrefixMap = new HashMap<>();
          HashSet<Integer> existsBucketID = new HashSet<>();

          // Load an existing index
          locationMap.forEach((k, v) -> {
            String prefix = FSUtils.getFileIdPfxFromFileId(v.getFileId());
            bucketIdToFileIdPrefixMap.put(k, prefix);
            fileIdPrefixToBucketIndex.put(prefix, fileIdPfxList.size());
            fileIdPfxList.add(prefix);
            existsBucketID.add(BucketIdentifier.bucketIdFromFileId(prefix));
            doAppend.add(true);
          });

          int numBuckets = numBucketsFunction.getNumBuckets(p);
          // Generate a file that does not exist
          for (int i = 0; i < numBuckets; i++) {
            if (!existsBucketID.contains(i)) {
              String fileIdPrefix = BucketIdentifier.newBucketFileIdPrefix(i, isNonBlockingConcurrencyControl);
              fileIdPrefixToBucketIndex.put(fileIdPrefix, fileIdPfxList.size());
              fileIdPfxList.add(fileIdPrefix);
              doAppend.add(false);
              bucketIdToFileIdPrefixMap.put(i, fileIdPrefix);
            }
          }
          return bucketIdToFileIdPrefixMap;
        }));
  }
}
