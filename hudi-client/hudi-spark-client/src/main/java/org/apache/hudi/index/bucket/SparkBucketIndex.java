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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.action.commit.Partitioner;
import org.apache.hudi.table.action.commit.SparkBucketIndexPartitioner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.utils.BucketUtils;
import org.apache.spark.api.java.JavaRDD;

public class SparkBucketIndex<T extends HoodieRecordPayload>
    extends HoodieIndex<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {

  private static final Logger LOG =  LogManager.getLogger(SparkBucketIndex.class);

  private final int numBuckets;

  public SparkBucketIndex(HoodieWriteConfig config) {
    super(config);
    numBuckets = config.getBucketIndexNumBuckets();
    LOG.info("use bucket index, numBuckets=" + numBuckets);
  }

  @Override
  public HoodieData<WriteStatus> updateLocation(HoodieData<WriteStatus> writeStatuses,
      HoodieEngineContext context,
      HoodieTable hoodieTable)
      throws HoodieIndexException {
    return writeStatuses;
  }

  @Override
  public HoodieData<HoodieRecord<T>> tagLocation(HoodieData<HoodieRecord<T>> records,
      HoodieEngineContext context,
      HoodieTable hoodieTable)
      throws HoodieIndexException {
    List<String> partitions = records.map(HoodieRecord::getPartitionPath)
        .distinct().collectAsList();
    Map<String, Map<Integer, List<Triple<String, String, String>>>> partitionPathFileIDList =
        loadPartitionBucketIdFileIdMapping(
            context, hoodieTable, partitions);

    if (partitionPathFileIDList.isEmpty()) {
      // first write
      return records;
    }

    HoodieData<HoodieRecord<T>> taggedRecords = records.map(record -> {
      int bucketId = BucketUtils.bucketId(record.getKey().getIndexKey(), numBuckets);
      String partitionPath = record.getPartitionPath();
      if (partitionPathFileIDList.containsKey(partitionPath)) {
        if (partitionPathFileIDList.get(partitionPath).containsKey(bucketId)) {
          Triple<String, String, String> triple = partitionPathFileIDList
              .get(partitionPath)
              .get(bucketId).get(0);
          String fileId = triple.getMiddle();
          String instanceTime = triple.getRight();
          HoodieRecordLocation loc = new HoodieRecordLocation(instanceTime, fileId);
          return HoodieIndexUtils.getTaggedRecord(record, Option.of(loc));
        }
      }
      return record;
    });
    return taggedRecords;
  }

  private Map<String, Map<Integer, List<Triple<String, String, String>>>> loadPartitionBucketIdFileIdMapping(
      HoodieEngineContext context,
      HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> hoodieTable,
      List<String> partitions) {
    // partitionPath -> bucketId -> fileIds
    Map<String, Map<Integer, List<Triple<String, String, String>>>> partitionPathFileIDList = new HashMap<>();
    HoodieIndexUtils
        .getLatestBaseFilesForAllPartitions(partitions, context, hoodieTable)
        .forEach(pair -> {
          String partitionPath = pair.getLeft();
          HoodieBaseFile file = pair.getRight();
          String fileId = file.getFileId();
          String commitTime = file.getCommitTime();
          int bucketId = BucketUtils.bucketIdFromFileId(fileId);
          if (!partitionPathFileIDList.containsKey(partitionPath)) {
            partitionPathFileIDList.put(partitionPath, new HashMap<>());
          }
          if (!partitionPathFileIDList.get(partitionPath).containsKey(bucketId)) {
            partitionPathFileIDList.get(partitionPath).put(bucketId, new ArrayList<>());
          }
          partitionPathFileIDList.get(partitionPath).get(bucketId)
              .add(Triple.of(partitionPath, fileId, commitTime));
        });
    // check if bucket data is valid
    partitionPathFileIDList
        .forEach((partitionPath, value) -> value.forEach((bucketId, value1) -> {
          if (value1.size() > 1) {
            throw new RuntimeException("find multiple files at partition path="
                + partitionPath + " belongs to the same bucket id = " + bucketId);
          }
        }));
    return partitionPathFileIDList;
  }

  @Override
  public boolean rollbackCommit(String instantTime) {
    return true;
  }

  @Override
  public boolean isGlobal() {
    return false;
  }

  @Override
  public boolean canIndexLogFiles() {
    return false;
  }

  @Override
  public boolean isImplicitWithStorage() {
    return true;
  }

  @Override
  public Option<Partitioner> getCustomizedPartitioner(WorkloadProfile profile,
      HoodieEngineContext context,
      HoodieTable table,
      HoodieWriteConfig writeConfig) {
    return Option.of(new SparkBucketIndexPartitioner<>(profile, context, table, config));
  }

  @Override
  public boolean needCustomizedPartitioner() {
    return true;
  }

  public int getNumBuckets() {
    return numBuckets;
  }
}
