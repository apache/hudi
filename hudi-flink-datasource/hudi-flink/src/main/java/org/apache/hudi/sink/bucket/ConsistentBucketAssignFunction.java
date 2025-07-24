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

import org.apache.hudi.adapter.ProcessFunctionAdapter;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.ConsistentHashingNode;
import org.apache.hudi.common.model.HoodieConsistentHashingMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieLockException;
import org.apache.hudi.index.bucket.ConsistentBucketIdentifier;
import org.apache.hudi.index.bucket.ConsistentBucketIndexUtils;
import org.apache.hudi.util.FlinkWriteClients;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * The function to tag each incoming record with a location of a file based on consistent bucket index.
 */
public class ConsistentBucketAssignFunction extends ProcessFunctionAdapter<HoodieFlinkInternalRow, HoodieFlinkInternalRow> implements CheckpointedFunction {

  private static final Logger LOG = LoggerFactory.getLogger(ConsistentBucketAssignFunction.class);

  private final Configuration config;
  private final List<String> indexKeyFields;
  private final int bucketNum;
  private transient HoodieFlinkWriteClient writeClient;
  private transient Map<String, ConsistentBucketIdentifier> partitionToIdentifier;
  private transient String lastRefreshInstant;
  private final int maxRetries = 10;
  private final long maxWaitTimeInMs = 1000;

  public ConsistentBucketAssignFunction(Configuration conf) {
    this.config = conf;
    this.indexKeyFields = Arrays.asList(OptionsResolver.getIndexKeyField(conf).split(","));
    this.bucketNum = conf.get(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    try {
      // not load fs view storage config for incremental job graph, since embedded timeline server
      // is started in write coordinator which is started after bucket assigner operator finished
      this.writeClient = FlinkWriteClients.createWriteClient(this.config, getRuntimeContext(), !OptionsResolver.isIncrementalJobGraph(config));
      this.partitionToIdentifier = new HashMap<>();
      this.lastRefreshInstant = HoodieTimeline.INIT_INSTANT_TS;
    } catch (Throwable e) {
      LOG.error("Fail to initialize consistent bucket assigner", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void processElement(HoodieFlinkInternalRow income, Context context, Collector<HoodieFlinkInternalRow> collector) throws Exception {
    String recordKey = income.getRecordKey();
    String partition = income.getPartitionPath();

    final ConsistentHashingNode node = getBucketIdentifier(partition).getBucket(recordKey, indexKeyFields);
    Preconditions.checkArgument(
        StringUtils.nonEmpty(node.getFileIdPrefix()),
        "Consistent hashing node has no file group, partition: " + partition + ", meta: "
            + partitionToIdentifier.get(partition).getMetadata().getFilename() + ", record_key: " + recordKey);

    income.setInstantTime("U");
    income.setFileId(FSUtils.createNewFileId(node.getFileIdPrefix(), 0));
    collector.collect(income);
  }

  private ConsistentBucketIdentifier getBucketIdentifier(String partition) {
    return partitionToIdentifier.computeIfAbsent(partition, p -> {
      // NOTE: If the metadata does not exist, there maybe concurrent creation of the metadata. And we allow multiple subtask
      // trying to create the same metadata as the initial metadata always has the same content for the same partition.
      int retryCount = 0;
      HoodieConsistentHashingMetadata metadata = null;
      while (retryCount <= maxRetries) {
        try {
          metadata = ConsistentBucketIndexUtils.loadOrCreateMetadata(this.writeClient.getHoodieTable(), p, bucketNum);
          break;
        } catch (Exception e) {
          if (retryCount >= maxRetries) {
            throw new HoodieLockException("Fail to load or create metadata for partition " + partition, e);
          }
          try {
            TimeUnit.MILLISECONDS.sleep(maxWaitTimeInMs);
          } catch (InterruptedException ex) {
            // ignore InterruptedException here
          }
          LOG.info("Retrying to load or create metadata for partition {} for {} times", partition, retryCount + 1);
        } finally {
          retryCount++;
        }
      }
      ValidationUtils.checkState(metadata != null);
      return new ConsistentBucketIdentifier(metadata);
    });
  }

  @Override
  public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
    HoodieTimeline timeline = writeClient.getHoodieTable().getActiveTimeline().getCompletedReplaceTimeline().findInstantsAfter(lastRefreshInstant);
    if (!timeline.empty()) {
      for (HoodieInstant instant : timeline.getInstants()) {
        HoodieReplaceCommitMetadata commitMetadata = timeline.readReplaceCommitMetadata(instant);
        Set<String> affectedPartitions = commitMetadata.getPartitionToReplaceFileIds().keySet();
        LOG.info("Clear up cached hashing metadata because find a new replace commit.\n Instant: {}.\n Effected Partitions: {}.",  lastRefreshInstant, affectedPartitions);
        affectedPartitions.forEach(this.partitionToIdentifier::remove);
      }
      this.lastRefreshInstant = timeline.lastInstant().get().requestedTime();
    }
  }

  @Override
  public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
    // no operation
  }
}
