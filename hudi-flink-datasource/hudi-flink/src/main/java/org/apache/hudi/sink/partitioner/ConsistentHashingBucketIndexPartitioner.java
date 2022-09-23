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

import org.apache.hudi.common.model.ConsistentHashingNode;
import org.apache.hudi.common.model.HoodieConsistentHashingMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.index.bucket.ConsistentBucketIdentifier;
import org.apache.hudi.index.bucket.ConsistentBucketIndexUtils;
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
 * Consistent hashing bucket index partitioner
 * The fields to hash can be a subset of the primary key fields
 *
 * @param <T> The type of obj to hash
 */
public class ConsistentHashingBucketIndexPartitioner<T extends HoodieKey> implements Partitioner<T>, CheckpointListener {

  private static final Logger LOG = LoggerFactory.getLogger(ConsistentHashingBucketIndexPartitioner.class);

  private final Configuration config;
  protected final List<String> indexKeyFields;
  private Map<String, ConsistentBucketIdentifier> partitionToBucketIdentifier;
  private transient HoodieTableMetaClient metaClient;
  private transient String lastRefreshInstant = HoodieTimeline.INIT_INSTANT_TS;

  public ConsistentHashingBucketIndexPartitioner(Configuration conf) {
    this.config = conf;
    this.indexKeyFields = Arrays.asList(conf.getString(FlinkOptions.INDEX_KEY_FIELD).split(","));
    this.partitionToBucketIdentifier = new HashMap<>();
  }

  @Override
  public int partition(HoodieKey key, int numPartitions) {
    ConsistentHashingNode node = getBucketIdentifier(key.getPartitionPath()).getBucket(key, indexKeyFields);
    int globalHash = ((key.getPartitionPath() + node.getValue()).hashCode()) & Integer.MAX_VALUE;
    return BucketIdentifier.mod(globalHash, numPartitions);
  }

  private ConsistentBucketIdentifier getBucketIdentifier(String partition) {
    if (metaClient == null) {
      metaClient = StreamerUtil.createMetaClient(config);
    }

    return partitionToBucketIdentifier.computeIfAbsent(partition, p -> {
      Option<HoodieConsistentHashingMetadata> metadataOption = ConsistentBucketIndexUtils.loadMetadata(metaClient, p);
      // The hashing metadata is not initialized yet, use a temporal metadata for routing
      if (!metadataOption.isPresent()) {
        metadataOption = Option.of(new HoodieConsistentHashingMetadata(partition, config.getInteger(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS)));
      }
      return new ConsistentBucketIdentifier(metadataOption.get());
    });
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    Option<HoodieInstant> latestReplaceInstant = metaClient.reloadActiveTimeline()
        .filter(instant -> instant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION)).lastInstant();
    if (latestReplaceInstant.isPresent() && latestReplaceInstant.get().getTimestamp().compareTo(lastRefreshInstant) > 0) {
      LOG.info("Clear up cached hashing metadata as new replace commit is spotted, instant: " + lastRefreshInstant);
      this.lastRefreshInstant = latestReplaceInstant.get().getTimestamp();
      this.partitionToBucketIdentifier.clear();
    }
  }
}
