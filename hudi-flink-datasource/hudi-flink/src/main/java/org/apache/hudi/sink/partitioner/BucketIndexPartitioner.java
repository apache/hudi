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

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.bucket.BucketIdentifier;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.configuration.Configuration;

/**
 * Bucket index input partitioner.
 * The fields to hash can be a subset of the primary key fields.
 *
 * @param <T> The type of obj to hash
 */
public class BucketIndexPartitioner<T extends HoodieKey> implements Partitioner<T> {

  private final int bucketNum;
  private final String indexKeyFields;

  public BucketIndexPartitioner(Configuration conf) {
    this.bucketNum = conf.getInteger(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS);
    this.indexKeyFields = conf.getString(FlinkOptions.INDEX_KEY_FIELD);
  }

  @Override
  public int partition(HoodieKey key, int numPartitions) {
    int curBucket = BucketIdentifier.getBucketId(key, indexKeyFields, bucketNum);
    int globalHash = (key.getPartitionPath() + curBucket).hashCode() & Integer.MAX_VALUE;
    return BucketIdentifier.mod(globalHash, numPartitions);
  }

  public static Partitioner instance(Configuration conf) {
    String bucketEngineType = conf.get(FlinkOptions.BUCKET_INDEX_ENGINE_TYPE);
    if (bucketEngineType.equalsIgnoreCase(HoodieIndex.BucketIndexEngineType.SIMPLE.name())) {
      return new BucketIndexPartitioner(conf);
    } else if (bucketEngineType.equalsIgnoreCase(HoodieIndex.BucketIndexEngineType.CONSISTENT_HASHING.name())) {
      return new ConsistentHashingBucketIndexPartitioner(conf);
    } else {
      throw new HoodieException("Unknown bucket index engine type: " + bucketEngineType);
    }
  }
}
