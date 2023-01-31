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
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.index.bucket.HoodieSimpleBucketIndex;
import org.apache.hudi.io.AppendHandleFactory;
import org.apache.hudi.io.SingleFileHandleCreateFactory;
import org.apache.hudi.io.WriteHandleFactory;
import org.apache.hudi.table.HoodieTable;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RDDSimpleBucketPartitioner<T extends HoodieRecordPayload> extends RDDBucketIndexPartitioner<T> {

  private final HoodieTable table;
  private final List<String> fileIdPfxList = new ArrayList<>();
  private final List<Boolean> doAppend = new ArrayList<>();

  public RDDSimpleBucketPartitioner(HoodieTable table) {
    this.table = table;
    ValidationUtils.checkArgument(table.getIndex() instanceof HoodieSimpleBucketIndex);
    ValidationUtils.checkArgument(table.getMetaClient().getTableType().equals(HoodieTableType.MERGE_ON_READ),
        "COW table with bucket index doesn't support bulk_insert");
  }

  @Override
  public JavaRDD<HoodieRecord<T>> repartitionRecords(JavaRDD<HoodieRecord<T>> records, int outputPartitions) {
    HoodieSimpleBucketIndex index = (HoodieSimpleBucketIndex) table.getIndex();
    HashMap<String, Integer> fileIdToIdx = new HashMap<>();
    int numBuckets = index.getNumBuckets();

    Map<String, HashMap<Integer, String>> partitionMapper = records
        .map(HoodieRecord::getPartitionPath)
        .distinct().collect().stream()
        .collect(Collectors.toMap(p -> p, p -> {
          Map<Integer, HoodieRecordLocation> locationMap = index.loadPartitionBucketIdFileIdMapping(table, p);
          HashMap<Integer, String> fileIdMap = new HashMap<>();
          ArrayList<Integer> existsBucketID = new ArrayList<>();
          locationMap.forEach((k, v) -> {
            fileIdMap.put(k, v.getFileId());
            fileIdToIdx.put(v.getFileId(), fileIdPfxList.size());
            fileIdPfxList.add(v.getFileId());
            existsBucketID.add(BucketIdentifier.bucketIdFromFileId(v.getFileId()));
            doAppend.add(true);
          });
          for (int i = 0; i < numBuckets; i++) {
            if (!existsBucketID.contains(i)) {
              String id = BucketIdentifier.newBucketFileIdPrefix(i);
              fileIdToIdx.put(id, fileIdPfxList.size());
              fileIdPfxList.add(id);
              doAppend.add(false);
              fileIdMap.put(i, id);
            }
          }
          return fileIdMap;
        }));

    Partitioner partitioner = new Partitioner() {

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
    };

    return records
        .mapToPair(record -> new Tuple2<>(record.getKey(), record))
        .partitionBy(partitioner)
        .map(Tuple2::_2);
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return false;
  }

  @Override
  public String getFileIdPfx(int partitionId) {
    return fileIdPfxList.get(partitionId);
  }

  @Override
  public Option<WriteHandleFactory> getWriteHandleFactory(int idx) {
    return doAppend.get(idx) ? Option.of(new AppendHandleFactory()) :
        Option.of(new SingleFileHandleCreateFactory(FSUtils.createNewFileId(getFileIdPfx(idx), 0), false));
  }
}
