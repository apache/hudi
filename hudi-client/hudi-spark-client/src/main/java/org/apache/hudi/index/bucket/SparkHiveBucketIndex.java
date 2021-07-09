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

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.index.SparkHoodieIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.utils.BucketUtils;
import org.apache.spark.api.java.JavaRDD;

public class SparkHiveBucketIndex<T extends HoodieRecordPayload> extends SparkHoodieIndex<T> {

  private static final Logger LOG = LoggerFactory.getLogger(SparkHiveBucketIndex.class);

  private final int numBuckets;

  public SparkHiveBucketIndex(HoodieWriteConfig config) {
    super(config);
    String tableNumBucket = config.getProps()
        .getProperty(HoodieTableConfig.HOODIE_TABLE_NUM_BUCKETS.key());
    String indexNumBucket = config.getProps()
        .getProperty(HoodieIndexConfig.BUCKET_INDEX_BUCKET_NUM.key());
    if (tableNumBucket == null && indexNumBucket == null) {
      throw new IllegalArgumentException(
          "Please set hoodie.index.hive.bucket.num or hoodie.table.numbuckets(deprecated) to a positive integer.");
    }
    this.numBuckets = Integer.parseInt(indexNumBucket != null ? indexNumBucket : tableNumBucket);
    LOG.info("use hive bucket index, numBuckets={}", numBuckets);
  }

  @Override
  public JavaRDD<WriteStatus> updateLocation(JavaRDD<WriteStatus> writeStatusRDD,
      HoodieEngineContext context,
      HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> hoodieTable)
      throws HoodieIndexException {
    return writeStatusRDD;
  }

  @Override
  public JavaRDD<HoodieRecord<T>> tagLocation(JavaRDD<HoodieRecord<T>> records,
      HoodieEngineContext context,
      HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> hoodieTable)
      throws HoodieIndexException {
    List<String> partitions = records.map(HoodieRecord::getPartitionPath)
        .distinct().collect();
    Map<String, Map<Integer, List<Triple<String, String, String>>>> partitionPathFileIDList =
        loadPartitionBucketIdFileIdMapping(
            context, hoodieTable, partitions);

    if (partitionPathFileIDList.isEmpty()) {
      // first write
      return records;
    }

    JavaRDD<HoodieRecord<T>> taggedRecordRDD = records.map(record -> {
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

    return taggedRecordRDD;
  }

  @NotNull
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
    // for hive bucket index, it's not safe to send inserts(new data in one file group) to log files
    // as we need the base file to locate the bucket id.
    // todo: try to loose constraint if possible
    return false;
  }

  @Override
  public boolean isImplicitWithStorage() {
    return true;
  }

  @Override
  public boolean needCustomizedPartitioner() {
    return true;
  }

  public int getNumBuckets() {
    return numBuckets;
  }
}
