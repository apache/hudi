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
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.LSMRemotePartitionerHelper;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.index.bucket.BucketStrategist;
import org.apache.hudi.index.bucket.BucketStrategistFactory;
import org.apache.hudi.sink.StreamWriteFunction;
import org.apache.hudi.util.FlinkWriteClients;

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

import static org.apache.hudi.config.HoodieWriteConfig.LSM_SHUFFLE_FACTOR_VALUE;

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
  protected int parallelism;

  protected String indexKeyFields;

  /**
   * BucketID to file group mapping in each partition.
   * Map(partition -> Map(bucketId, fileID)).
   */
  protected Map<String, Map<Integer, String>> bucketIndex;

  /**
   * Incremental bucket index of the current checkpoint interval,
   * it is needed because the bucket type('I' or 'U') should be decided based on the committed files view,
   * all the records in one bucket should have the same bucket type.
   */
  protected Set<String> incBucketIndex;
  protected BucketStrategist bucketStrategist;

  /**
   * Used to record the last replacement commit time
   */
  private transient String lastRefreshInstant;

  /**
   * Adapt remote partitioner when call index bootstrap.
   */
  private transient Boolean remotePartitionerEnable;
  private transient LSMRemotePartitionerHelper remotePartitionerHelper;
  private Integer factor;

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
    this.indexKeyFields = config.getString(FlinkOptions.INDEX_KEY_FIELD);
    this.taskID = getRuntimeContext().getIndexOfThisSubtask();
    this.parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
    this.bucketIndex = new HashMap<>();
    this.incBucketIndex = new HashSet<>();
    this.lastRefreshInstant = HoodieTimeline.INIT_INSTANT_TS;
    if (OptionsResolver.enableRemotePartitioner(config)) {
      LOG.info("BucketStreamWriteFunction enable remote partitioner.");
      this.remotePartitionerEnable = true;
      this.remotePartitionerHelper = new LSMRemotePartitionerHelper(writeClient.getConfig().getViewStorageConfig());
      this.factor = config.getInteger(LSM_SHUFFLE_FACTOR_VALUE.key(), Integer.valueOf(LSM_SHUFFLE_FACTOR_VALUE.defaultValue()));
    } else {
      this.remotePartitionerEnable = false;
    }
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    super.initializeState(context);
    this.bucketStrategist = BucketStrategistFactory.getInstant(FlinkWriteClients.getHoodieClientConfig(config), metaClient.getFs());
  }

  @Override
  public void snapshotState() throws IOException {
    super.snapshotState();
    // only partition level bucket index need remove cache
    if (bucketStrategist.isPartitionLevel()) {
      removePartitionCache();
    }
    this.incBucketIndex.clear();
  }

  private void removePartitionCache() throws IOException {
    HoodieTimeline timeline = writeClient.getHoodieTable().getActiveTimeline()
        .getCompletedReplaceTimeline().findInstantsAfter(lastRefreshInstant);
    if (!timeline.empty()) {
      for (HoodieInstant instant : timeline.getInstants()) {
        HoodieReplaceCommitMetadata commitMetadata = HoodieReplaceCommitMetadata.fromBytes(
            timeline.getInstantDetails(instant).get(), HoodieReplaceCommitMetadata.class);
        // only take care of insert operation here.
        if (commitMetadata.getOperationType().equals(WriteOperationType.INSERT_OVERWRITE)) {
          Set<String> affectedPartitions = commitMetadata.getPartitionToReplaceFileIds().keySet();
          LOG.info("Clear up cached hashing metadata because find a new insert overwrite commit.\n Instant: {}.\n Effected Partitions: {}.",  lastRefreshInstant, affectedPartitions);
          affectedPartitions.forEach(bucketStrategist::updateBucketInfo);
        }
      }
      this.lastRefreshInstant = timeline.lastInstant().get().getTimestamp();
    }
  }

  @Override
  public void processElement(I i, ProcessFunction<I, Object>.Context context, Collector<Object> collector) throws Exception {
    HoodieRecord<?> record = (HoodieRecord<?>) i;
    final HoodieKey hoodieKey = record.getKey();
    final String partition = hoodieKey.getPartitionPath();
    final HoodieRecordLocation location;

    bootstrapIndexIfNeed(partition);
    Map<Integer, String> bucketToFileId = bucketIndex.computeIfAbsent(partition, p -> new HashMap<>());
    final int bucketNum = BucketIdentifier.getBucketId(hoodieKey, indexKeyFields, bucketStrategist.getBucketNumber(partition));
    final String bucketId = partition + "/" + bucketNum;

    // 新增的桶的数据都是 I
    // 老桶的数据都是 U
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
   * Determine whether the current fileID belongs to the current task.
   * (partition + curBucket) % numPartitions == this taskID belongs to this task.
   */
  public boolean isBucketToLoad(int bucketNumber, String partition) {
    if (remotePartitionerEnable) {
      try {
        int subtaskId = remotePartitionerHelper.getPartition(
            bucketStrategist.computeBucketNumber(partition),
            partition,
            bucketNumber,
            parallelism,
            factor);
        return subtaskId == taskID;
      } catch (Exception e) {
        throw new RuntimeException("Get Remote Partition Failed.", e);
      }

    } else {
      final int partitionIndex = (partition.hashCode() & Integer.MAX_VALUE) % parallelism;
      int globalIndex = partitionIndex + bucketNumber;
      return BucketIdentifier.mod(globalIndex, parallelism) == taskID;
    }
  }

  /**
   * Get partition_bucket -> fileID mapping from the existing hudi table.
   * This is a required operation for each restart to avoid having duplicate file ids for one bucket.
   */
  private void bootstrapIndexIfNeed(String partition) {
    if (OptionsResolver.isInsertOverwrite(config)) {
      // skips the index loading for insert overwrite operation.
      return;
    }
    if (bucketIndex.containsKey(partition)) {
      return;
    }
    LOG.info(String.format("Loading Hoodie Table %s, with path %s", this.metaClient.getTableConfig().getTableName(),
        this.metaClient.getBasePath() + "/" + partition));

    // Load existing fileID belongs to this task
    Map<Integer, String> bucketToFileIDMap = new HashMap<>();
    this.writeClient.getHoodieTable().getHoodieView().getLatestFileSlices(partition).forEach(fileSlice -> {
      String fileId = fileSlice.getFileId();
      int bucketNumber = BucketIdentifier.bucketIdFromFileId(fileId);
      if (isBucketToLoad(bucketNumber, partition)) {
        LOG.info(String.format("Should load this partition bucket %s with fileId %s", bucketNumber, fileId));
        // Validate that one bucketId has only ONE fileId
        if (bucketToFileIDMap.containsKey(bucketNumber)) {
          throw new RuntimeException(String.format("Duplicate fileId %s from bucket %s of partition %s found "
              + "during the BucketStreamWriteFunction index bootstrap.", fileId, bucketNumber, partition));
        } else {
          LOG.info(String.format("Adding fileId %s to the bucket %s of partition %s.", fileId, bucketNumber, partition));
          bucketToFileIDMap.put(bucketNumber, fileId);
        }
      }
    });
    bucketIndex.put(partition, bucketToFileIDMap);
  }
}
