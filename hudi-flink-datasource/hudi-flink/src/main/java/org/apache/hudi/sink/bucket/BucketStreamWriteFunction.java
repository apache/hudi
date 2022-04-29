/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.bucket;

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.sink.StreamWriteFunction;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A stream write function with bucket hash index.
 *
 * <p>The task holds a fresh new local index: {(partition + bucket number) &rarr fileId} mapping, this index
 * is used for deciding whether the incoming records in an UPDATE or INSERT.
 * The index is local because different partition paths have separate items in the index.
 *
 * @param <I> the input type
 */
public class BucketStreamWriteFunction<I> extends StreamWriteFunction<I> {

  private static final Logger LOG = LoggerFactory.getLogger(BucketStreamWriteFunction.class);

  private int parallelism;

  private int bucketNum;

  private String indexKeyFields;

  /**
   * BucketID should be loaded in this task.
   */
  private Set<Integer> bucketToLoad;

  /**
   * BucketID to file group mapping in each partition.
   * Map(partition -> Map(bucketId, fileID)).
   */
  private Map<String, Map<Integer, String>> bucketIndex;

  /**
   * Incremental bucket index of the current checkpoint interval,
   * it is needed because the bucket type('I' or 'U') should be decided based on the committed files view,
   * all the records in one bucket should have the same bucket type.
   */
  private Set<String> incBucketIndex;

  /**
   * Constructs a BucketStreamWriteFunction.
   *
   * @param config The config options
   */
  public BucketStreamWriteFunction(Configuration config) {
    super(config);
  }

  @Override
  public void open(Configuration parameters) throws IOException {
    super.open(parameters);
    this.bucketNum = config.getInteger(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS);
    this.indexKeyFields = config.getString(FlinkOptions.INDEX_KEY_FIELD);
    this.taskID = getRuntimeContext().getIndexOfThisSubtask();
    this.parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
    this.bucketToLoad = getBucketToLoad();
    this.bucketIndex = new HashMap<>();
    this.incBucketIndex = new HashSet<>();
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    super.initializeState(context);
  }

  @Override
  public void snapshotState() {
    super.snapshotState();
    this.incBucketIndex.clear();
  }

  @Override
  public void processElement(I i, ProcessFunction<I, Object>.Context context, Collector<Object> collector) throws Exception {
    HoodieRecord<?> record = (HoodieRecord<?>) i;
    final HoodieKey hoodieKey = record.getKey();
    final String partition = hoodieKey.getPartitionPath();
    final HoodieRecordLocation location;

    bootstrapIndexIfNeed(partition);
    Map<Integer, String> bucketToFileId = bucketIndex.computeIfAbsent(partition, p -> new HashMap<>());
    final int bucketNum = BucketIdentifier.getBucketId(hoodieKey, indexKeyFields, this.bucketNum);
    final String bucketId = partition + bucketNum;

    if (incBucketIndex.contains(bucketId)) {
      location = new HoodieRecordLocation("I", bucketToFileId.get(bucketNum));
    } else if (bucketToFileId.containsKey(bucketNum)) {
      location = new HoodieRecordLocation("U", bucketToFileId.get(bucketNum));
    } else {
      String newFileId = BucketIdentifier.newBucketFileIdPrefix(bucketNum);
      location = new HoodieRecordLocation("I", newFileId);
      bucketToFileId.put(bucketNum, newFileId);
      incBucketIndex.add(bucketId);
    }
    record.unseal();
    record.setCurrentLocation(location);
    record.seal();
    bufferRecord(record);
  }

  /**
   * Bootstrap bucket info from existing file system,
   * bucketNum % totalParallelism == this taskID belongs to this task.
   */
  private Set<Integer> getBucketToLoad() {
    Set<Integer> bucketToLoad = new HashSet<>();
    for (int i = 0; i < bucketNum; i++) {
      int partitionOfBucket = BucketIdentifier.mod(i, parallelism);
      if (partitionOfBucket == taskID) {
        bucketToLoad.add(i);
      }
    }
    LOG.info("Bucket number that belongs to task [{}/{}]: {}", taskID, parallelism, bucketToLoad);
    return bucketToLoad;
  }

  /**
   * Get partition_bucket -> fileID mapping from the existing hudi table.
   * This is a required operation for each restart to avoid having duplicate file ids for one bucket.
   */
  private void bootstrapIndexIfNeed(String partition) {
    if (bucketIndex.containsKey(partition)) {
      return;
    }
    LOG.info(String.format("Loading Hoodie Table %s, with path %s", this.metaClient.getTableConfig().getTableName(),
        this.metaClient.getBasePath() + "/" + partition));

    // Load existing fileID belongs to this task
    Map<Integer, String> bucketToFileIDMap = new HashMap<>();
    this.writeClient.getHoodieTable().getFileSystemView().getAllFileGroups(partition).forEach(fileGroup -> {
      String fileID = fileGroup.getFileGroupId().getFileId();
      int bucketNumber = BucketIdentifier.bucketIdFromFileId(fileID);
      if (bucketToLoad.contains(bucketNumber)) {
        LOG.info(String.format("Should load this partition bucket %s with fileID %s", bucketNumber, fileID));
        if (bucketToFileIDMap.containsKey(bucketNumber)) {
          throw new RuntimeException(String.format("Duplicate fileID %s from bucket %s of partition %s found "
              + "during the BucketStreamWriteFunction index bootstrap.", fileID, bucketNumber, partition));
        } else {
          LOG.info(String.format("Adding fileID %s to the bucket %s of partition %s.", fileID, bucketNumber, partition));
          bucketToFileIDMap.put(bucketNumber, fileID);
        }
      }
    });
    bucketIndex.put(partition, bucketToFileIDMap);
  }
}
