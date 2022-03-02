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

package org.apache.hudi.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.table.HoodieFlinkTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import static java.util.stream.Collectors.toList;

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

  private int maxParallelism;

  private int parallelism;

  private int bucketNum;

  private transient HoodieFlinkTable<?> table;

  private String indexKeyFields;

  private final HashMap<String, String> bucketToFileIDMap;

  /**
   * Constructs a BucketStreamWriteFunction.
   *
   * @param config The config options
   */
  public BucketStreamWriteFunction(Configuration config) {
    super(config);
    this.bucketToFileIDMap = new HashMap<>();
  }

  @Override
  public void open(Configuration parameters) throws IOException {
    super.open(parameters);
    this.bucketNum = config.getInteger(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS);
    this.indexKeyFields = config.getString(FlinkOptions.INDEX_KEY_FIELD);
    this.taskID = getRuntimeContext().getIndexOfThisSubtask();
    this.parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
    this.maxParallelism = getRuntimeContext().getMaxNumberOfParallelSubtasks();
    bootstrapIndex();
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    super.initializeState(context);
    this.table = this.writeClient.getHoodieTable();
  }

  @Override
  public void processElement(I i, ProcessFunction<I, Object>.Context context, Collector<Object> collector) throws Exception {
    HoodieRecord<?> record = (HoodieRecord<?>) i;
    final HoodieKey hoodieKey = record.getKey();
    final HoodieRecordLocation location;

    final int bucketNum = BucketIdentifier.getBucketId(hoodieKey, indexKeyFields, this.bucketNum);
    final String partitionBucketId = BucketIdentifier.partitionBucketIdStr(hoodieKey.getPartitionPath(), bucketNum);

    if (bucketToFileIDMap.containsKey(partitionBucketId)) {
      location = new HoodieRecordLocation("U", bucketToFileIDMap.get(partitionBucketId));
    } else {
      String newFileId = BucketIdentifier.newBucketFileIdPrefix(bucketNum);
      location = new HoodieRecordLocation("I", newFileId);
      bucketToFileIDMap.put(partitionBucketId, newFileId);
    }
    record.unseal();
    record.setCurrentLocation(location);
    record.seal();
    bufferRecord(record);
  }

  /**
   * Get partition_bucket -> fileID mapping from the existing hudi table.
   * This is a required operation for each restart to avoid having duplicate file ids for one bucket.
   */
  private void bootstrapIndex() throws IOException {
    Option<HoodieInstant> latestCommitTime = table.getFileSystemView().getTimeline().filterCompletedInstants().lastInstant();
    if (!latestCommitTime.isPresent()) {
      return;
    }
    // bootstrap bucket info from existing file system
    // bucketNum % totalParallelism == this taskID belongs to this task
    HashSet<Integer> bucketToLoad = new HashSet<>();
    for (int i = 0; i < bucketNum; i++) {
      int partitionOfBucket = BucketIdentifier.mod(i, parallelism);
      if (partitionOfBucket == taskID) {
        LOG.info(String.format("Bootstrapping index. Adding bucket %s , "
            + "Current parallelism: %s , Max parallelism: %s , Current task id: %s",
            i, parallelism, maxParallelism, taskID));
        bucketToLoad.add(i);
      }
    }
    bucketToLoad.forEach(bucket -> LOG.info(String.format("bucketToLoad contains %s", bucket)));

    LOG.info(String.format("Loading Hoodie Table %s, with path %s", table.getMetaClient().getTableConfig().getTableName(),
        table.getMetaClient().getBasePath()));

    // Iterate through all existing partitions to load existing fileID belongs to this task
    List<String> partitions = table.getMetadata().getAllPartitionPaths();
    for (String partitionPath : partitions) {
      List<FileSlice> latestFileSlices = table.getSliceView()
          .getLatestFileSlices(partitionPath)
          .collect(toList());
      for (FileSlice fileslice : latestFileSlices) {
        String fileID = fileslice.getFileId();
        int bucketNumber = BucketIdentifier.bucketIdFromFileId(fileID);
        if (bucketToLoad.contains(bucketNumber)) {
          String partitionBucketId = BucketIdentifier.partitionBucketIdStr(partitionPath, bucketNumber);
          LOG.info(String.format("Should load this partition bucket %s with fileID %s", partitionBucketId, fileID));
          if (bucketToFileIDMap.containsKey(partitionBucketId)) {
            throw new RuntimeException(String.format("Duplicate fileID %s from partitionBucket %s found "
              + "during the BucketStreamWriteFunction index bootstrap.", fileID, partitionBucketId));
          } else {
            LOG.info(String.format("Adding fileID %s to the partition bucket %s.", fileID, partitionBucketId));
            bucketToFileIDMap.put(partitionBucketId, fileID);
          }
        }
      }
    }
  }
}
