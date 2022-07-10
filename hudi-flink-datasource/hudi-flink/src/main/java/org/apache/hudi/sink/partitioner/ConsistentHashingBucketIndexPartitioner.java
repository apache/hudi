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
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.index.bucket.ConsistentBucketIdentifier;
import org.apache.hudi.index.bucket.ConsistentBucketIndexUtils;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.configuration.Configuration;

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
public class ConsistentHashingBucketIndexPartitioner<T extends HoodieKey> implements Partitioner<T> {

  private final Configuration config;
  protected final List<String> indexKeyFields;
  private Map<String, ConsistentBucketIdentifier> partitionToBucketNum;
  private transient HoodieTableMetaClient metaClient;

  public ConsistentHashingBucketIndexPartitioner(Configuration conf) {
    this.config = conf;
    this.indexKeyFields = Arrays.asList(conf.getString(FlinkOptions.INDEX_KEY_FIELD).split(","));
    this.partitionToBucketNum = new HashMap<>();
  }

  @Override
  public int partition(HoodieKey key, int numPartitions) {
    ConsistentHashingNode node = getBucketIdentifier(key.getPartitionPath()).getBucket(key, indexKeyFields);
    int globalHash = (node.getFileIdPrefix() + key.getPartitionPath()).hashCode() & Integer.MAX_VALUE;
    return BucketIdentifier.mod(globalHash, numPartitions);
  }

  private ConsistentBucketIdentifier getBucketIdentifier(String partition) {
    if (metaClient == null) {
      metaClient = StreamerUtil.createMetaClient(config);
    }

    return partitionToBucketNum.computeIfAbsent(partition, p -> {
      HoodieConsistentHashingMetadata node = ConsistentBucketIndexUtils.loadOrCreateMetadata(metaClient, p,
          config.getInteger(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS));
      ValidationUtils.checkArgument(node != null, "Consistent hashing metadata node should not be null");
      return new ConsistentBucketIdentifier(node);
    });
  }
}
