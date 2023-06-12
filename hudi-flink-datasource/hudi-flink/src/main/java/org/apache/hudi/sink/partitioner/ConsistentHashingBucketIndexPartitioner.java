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

package org.apache.hudi.sink.partitioner;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.model.HoodieConsistentHashingMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.index.bucket.ConsistentBucketIdentifier;
import org.apache.hudi.index.bucket.ConsistentBucketIndexHelper;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Consistent hashing bucket index partitioner.
 * The fields to hash can be a subset of the primary key fields.
 *
 * @param <T> The type of obj to hash
 */
public class ConsistentHashingBucketIndexPartitioner<T extends HoodieKey> implements Partitioner<T>, CheckpointListener {

  private static final Logger LOG = LoggerFactory.getLogger(ConsistentHashingBucketIndexPartitioner.class);

  private final Configuration config;
  protected final List<String> indexKeyFields;
  private final int bucketNum;
  private Map<String, ConsistentBucketIdentifier> partitionToBucketIdentifier;
  private transient boolean initialized = false;
  private transient String lastRefreshInstant = HoodieTimeline.INIT_INSTANT_TS;
  private transient HoodieTableMetaClient metaClient;
  private transient HoodieFlinkWriteClient writeClient;

  public ConsistentHashingBucketIndexPartitioner(Configuration conf) {
    this.config = conf;
    this.bucketNum = conf.getInteger(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS);
    this.indexKeyFields = Arrays.asList(OptionsResolver.getIndexKeyField(conf).split(","));
  }

  private void initialize() {
    try {
      this.metaClient = StreamerUtil.createMetaClient(this.config);
      this.writeClient = FlinkWriteClients.createWriteClient(this.config);
      this.partitionToBucketIdentifier = new HashMap<>();
      this.initialized = true;
    } catch (Exception e) {
      LOG.error("fail to initialize ConsistentHashingBucketIndexPartitioner", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public int partition(HoodieKey key, int numPartitions) {
    if (!initialized) {
      initialize();
    }
    int curBucket = getBucketIdentifier(key.getPartitionPath()).getBucket(key, indexKeyFields).getValue() % numPartitions;
    int partitionIndex = (key.getPartitionPath().hashCode() & Integer.MAX_VALUE) % numPartitions;
    int globalHash = partitionIndex + curBucket;
    return BucketIdentifier.mod(globalHash, numPartitions);
  }

  private ConsistentBucketIdentifier getBucketIdentifier(String partition) {
    return partitionToBucketIdentifier.computeIfAbsent(partition, p -> {
      // NOTE: If the metadata does not exist, there maybe concurrent creation of the metadata. And we allow multiple partitioner
      // trying to create the same metadata as the initial metadata always has the same content for the same partition.
      HoodieConsistentHashingMetadata metadata =
          ConsistentBucketIndexHelper.loadOrCreateMetadata(writeClient.getHoodieTable(), p, bucketNum);
      ValidationUtils.checkState(metadata != null);
      return new ConsistentBucketIdentifier(metadata);
    });
  }

  /**
   * Clear up cached bucket identifier if necessary (i.e., resizing happens).
   *
   * NOTE: There will be a windows where the partitioner may use outdated identifier (New metadata is
   *  generated async in coordinator). This does not affect the correctness, because the final file
   *  path where the records written to is determined by writers. And writers will always use the
   *  newest identifier during flushing.
   */
  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    Option<HoodieInstant> latestReplaceInstant =  metaClient.reloadActiveTimeline()
        .filter(instant -> instant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION)).lastInstant();
    if (latestReplaceInstant.isPresent() && latestReplaceInstant.get().getTimestamp().compareTo(lastRefreshInstant) > 0) {
      LOG.info("Clear up cached hashing metadata as new replace commit is spotted, instant: {}", lastRefreshInstant);
      // TODO Optimize it to clear only partitions affected by the concurrent replace_commits
      this.lastRefreshInstant = latestReplaceInstant.get().getTimestamp();
      this.partitionToBucketIdentifier.clear();
    }
  }
}
