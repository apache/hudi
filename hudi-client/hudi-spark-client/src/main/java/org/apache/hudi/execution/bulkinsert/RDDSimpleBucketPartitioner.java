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

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.index.bucket.HoodieSimpleBucketIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;

public class RDDSimpleBucketPartitioner<T extends HoodieRecordPayload> extends RDDBucketIndexPartitioner<T> {


  public RDDSimpleBucketPartitioner(HoodieTable table) {
    super(table, null, false);
    ValidationUtils.checkArgument(table.getIndex() instanceof HoodieSimpleBucketIndex);
  }

  @Override
  public JavaRDD<HoodieRecord<T>> repartitionRecords(JavaRDD<HoodieRecord<T>> records, int outputPartitions) {
    HoodieSimpleBucketIndex index = (HoodieSimpleBucketIndex) table.getIndex();
    HashMap<String, Integer> fileIdToIdx = new HashMap<>();

    // Map <partition, <bucketNo, fileID>>
    Map<String, HashMap<Integer, String>> partitionMapper = getPartitionMapper(records, fileIdToIdx);

    return doPartition(records, new Partitioner() {
      @Override
      public int numPartitions() {
        return index.getNumBuckets() * partitionMapper.size();
      }

      @Override
      public int getPartition(Object key) {
        HoodieKey hoodieKey = (HoodieKey) key;
        String partitionPath = hoodieKey.getPartitionPath();
        int bucketID = index.getBucketID(hoodieKey);
        String fileID = partitionMapper.get(partitionPath).get(bucketID);
        return fileIdToIdx.get(fileID);
      }
    });
  }

  Map<String, HashMap<Integer, String>> getPartitionMapper(JavaRDD<HoodieRecord<T>> records,
                                                           HashMap<String, Integer> fileIdToIdx) {

    HoodieSimpleBucketIndex index = (HoodieSimpleBucketIndex) table.getIndex();
    int numBuckets = index.getNumBuckets();
    return records
        .map(HoodieRecord::getPartitionPath)
        .distinct().collect().stream()
        .collect(Collectors.toMap(p -> p, p -> {
          Map<Integer, HoodieRecordLocation> locationMap = index.loadPartitionBucketIdFileIdMapping(table, p);
          HashMap<Integer, String> fileIdMap = new HashMap<>();
          HashSet<Integer> existsBucketID = new HashSet<>();

          // Load an existing index
          locationMap.forEach((k, v) -> {
            String fileId = v.getFileId();
            fileIdMap.put(k, fileId);
            fileIdToIdx.put(fileId, fileIdPfxList.size());
            fileIdPfxList.add(fileId);
            existsBucketID.add(BucketIdentifier.bucketIdFromFileId(fileId));
            doAppend.add(true);
          });

          // Generate a file that does not exist
          for (int i = 0; i < numBuckets; i++) {
            if (!existsBucketID.contains(i)) {
              String fileId = BucketIdentifier.newBucketFileIdPrefix(i);
              fileIdToIdx.put(fileId, fileIdPfxList.size());
              fileIdPfxList.add(fileId);
              doAppend.add(false);
              fileIdMap.put(i, fileId);
            }
          }
          return fileIdMap;
        }));
  }
}

