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
import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.hash.BucketIndexUtil;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.index.bucket.partition.NumBucketsFunction;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.configuration.Configuration;

/**
 * Bucket index input partitioner.
 * The fields to hash can be a subset of the primary key fields.
 *
 * @param <T> The type of obj to hash
 */
public class BucketIndexPartitioner<T extends HoodieKey> implements Partitioner<T> {

  private final String indexKeyFields;
  private final NumBucketsFunction numBucketsFunction;

  private Functions.Function3<Integer, String, Integer, Integer> partitionIndexFunc;

  public BucketIndexPartitioner(Configuration conf, String indexKeyFields) {
    this.indexKeyFields = indexKeyFields;
    this.numBucketsFunction = new NumBucketsFunction(conf.get(FlinkOptions.BUCKET_INDEX_PARTITION_EXPRESSIONS),
        conf.get(FlinkOptions.BUCKET_INDEX_PARTITION_RULE), conf.get(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS));
  }

  @Override
  public int partition(HoodieKey key, int numPartitions) {
    if (this.partitionIndexFunc == null) {
      this.partitionIndexFunc = BucketIndexUtil.getPartitionIndexFunc(numPartitions);
    }
    int numBuckets = numBucketsFunction.getNumBuckets(key.getPartitionPath());
    int curBucket = BucketIdentifier.getBucketId(key.getRecordKey(), indexKeyFields, numBuckets);
    return this.partitionIndexFunc.apply(numBuckets, key.getPartitionPath(), curBucket);
  }
}
