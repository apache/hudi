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

package org.apache.hudi.sink.bucket;

import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.hash.BucketIndexUtil;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.index.bucket.partition.NumBucketsFunction;
import org.apache.hudi.sink.StreamWriteFunction;
import org.apache.hudi.utils.RuntimeContextUtils;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A stream write function with simple bucket hash index.
 *
 * <p>The task holds a fresh new local index: {(partition + bucket number) &rarr fileId} mapping, this index
 * is used for deciding whether the incoming records in an UPDATE or INSERT.
 * The index is local because different partition paths have separate items in the index.
 */
@Slf4j
public class BucketStreamWriteFunction extends StreamWriteFunction {

  private int parallelism;

  private String indexKeyFields;

  private boolean isNonBlockingConcurrencyControl;

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
   * Functions for calculating the task partition to dispatch.
   */
  private Functions.Function3<Integer, String, Integer, Integer> partitionIndexFunc;
  /**
   * Function to calculate num buckets per partition.
   */
  private NumBucketsFunction numBucketsFunction;

  /**
   * To prevent strings compare for each record, define this only during open()
   */
  private boolean isInsertOverwrite;

  /**
   * Constructs a BucketStreamWriteFunction.
   *
   * @param config The config options
   */
  public BucketStreamWriteFunction(Configuration config, RowType rowType) {
    super(config, rowType);
  }

  @Override
  public void open(Configuration parameters) throws IOException {
    super.open(parameters);
    this.indexKeyFields = OptionsResolver.getIndexKeyField(config);
    this.isNonBlockingConcurrencyControl = OptionsResolver.isNonBlockingConcurrencyControl(config);
    this.taskID = RuntimeContextUtils.getIndexOfThisSubtask(getRuntimeContext());
    this.parallelism = RuntimeContextUtils.getNumberOfParallelSubtasks(getRuntimeContext());
    this.bucketIndex = new HashMap<>();
    this.incBucketIndex = new HashSet<>();
    this.partitionIndexFunc = BucketIndexUtil.getPartitionIndexFunc(parallelism);
    this.isInsertOverwrite = OptionsResolver.isInsertOverwrite(config);
    this.numBucketsFunction = new NumBucketsFunction(config.get(FlinkOptions.BUCKET_INDEX_PARTITION_EXPRESSIONS),
        config.get(FlinkOptions.BUCKET_INDEX_PARTITION_RULE), config.get(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS));
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
  public void processElement(HoodieFlinkInternalRow record,
                             ProcessFunction<HoodieFlinkInternalRow, RowData>.Context context,
                             Collector<RowData> collector) throws Exception {
    defineRecordLocation(record);
    bufferRecord(record);
  }

  private void defineRecordLocation(HoodieFlinkInternalRow record) {
    final String partition = record.getPartitionPath();
    // for insert overwrite operation skip `bucketIndex` loading
    if (!isInsertOverwrite) {
      bootstrapIndexIfNeed(partition);
    }
    Map<Integer, String> bucketToFileId = bucketIndex.computeIfAbsent(partition, p -> new HashMap<>());
    final int bucketNum = BucketIdentifier.getBucketId(record.getRecordKey(), indexKeyFields, numBucketsFunction.getNumBuckets(record.getPartitionPath()));
    final String bucketId = partition + "/" + bucketNum;

    if (incBucketIndex.contains(bucketId)) {
      record.setInstantTime("I");
      record.setFileId(bucketToFileId.get(bucketNum));
    } else if (bucketToFileId.containsKey(bucketNum)) {
      record.setInstantTime("U");
      record.setFileId(bucketToFileId.get(bucketNum));
    } else {
      String newFileId = isNonBlockingConcurrencyControl ? BucketIdentifier.newBucketFileIdForNBCC(bucketNum) : BucketIdentifier.newBucketFileIdPrefix(bucketNum);
      record.setInstantTime("I");
      record.setFileId(newFileId);
      bucketToFileId.put(bucketNum, newFileId);
      incBucketIndex.add(bucketId);
    }
  }

  /**
   * Determine whether the current fileID belongs to the current task.
   * partitionIndex == this taskID belongs to this task.
   */
  public boolean isBucketToLoad(int bucketNumber, String partition) {
    int numBuckets = numBucketsFunction.getNumBuckets(partition);
    return this.partitionIndexFunc.apply(numBuckets, partition, bucketNumber) == taskID;
  }

  /**
   * Get partition_bucket -> fileID mapping from the existing hudi table.
   * This is a required operation for each restart to avoid having duplicate file ids for one bucket.
   */
  private void bootstrapIndexIfNeed(String partition) {
    if (bucketIndex.containsKey(partition)) {
      return;
    }
    log.info("Loading Hoodie Table {}, with path {}/{}", this.metaClient.getTableConfig().getTableName(),
        this.metaClient.getBasePath(), partition);

    // Load existing fileID belongs to this task
    Map<Integer, String> bucketToFileIDMap = new HashMap<>();
    this.writeClient.getHoodieTable().getHoodieView().getLatestFileSlices(partition).forEach(fileSlice -> {
      String fileId = fileSlice.getFileId();
      int bucketNumber = BucketIdentifier.bucketIdFromFileId(fileId);
      if (isBucketToLoad(bucketNumber, partition)) {
        log.info(String.format("Should load this partition bucket %s with fileId %s", bucketNumber, fileId));
        // Validate that one bucketId has only ONE fileId
        if (bucketToFileIDMap.containsKey(bucketNumber)) {
          throw new RuntimeException(String.format("Duplicate fileId %s from bucket %s of partition %s found "
              + "during the BucketStreamWriteFunction index bootstrap.", fileId, bucketNumber, partition));
        } else {
          log.info(String.format("Adding fileId %s to the bucket %s of partition %s.", fileId, bucketNumber, partition));
          bucketToFileIDMap.put(bucketNumber, fileId);
        }
      }
    });
    bucketIndex.put(partition, bucketToFileIDMap);
  }
}
