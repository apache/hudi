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

package org.apache.hudi.source.split.assign;

import org.apache.flink.configuration.Configuration;
import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.hash.BucketIndexUtil;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.index.bucket.partition.NumBucketsFunction;
import org.apache.hudi.source.split.HoodieSourceSplit;

/**
 * Implementation of {@link HoodieSplitAssigner} that assigns Hoodie
 * source splits to task IDs using bucket index information for co-location.
 */
public class HoodieSplitBucketAssigner implements HoodieSplitAssigner {
  private final NumBucketsFunction numBucketsFunction;
  private Functions.Function3<Integer, String, Integer, Integer> partitionIndexFunc;

  /**
   * Creates a new HoodieSplitBucketAssigner using bucket index configuration.
   *
   * @param parallelism the number of parallel tasks (must be positive)
   * @param conf        Flink configuration containing bucket index settings
   * @throws IllegalArgumentException if parallelism is less than or equal to 0
   */
  public HoodieSplitBucketAssigner(int parallelism, Configuration conf) {
    if (parallelism <= 0) {
      throw new IllegalArgumentException("Parallelism must be positive, but was: " + parallelism);
    }
    this.numBucketsFunction = new NumBucketsFunction(conf.get(FlinkOptions.BUCKET_INDEX_PARTITION_EXPRESSIONS),
            conf.get(FlinkOptions.BUCKET_INDEX_PARTITION_RULE), conf.get(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS));
    this.partitionIndexFunc = BucketIndexUtil.getPartitionIndexFunc(parallelism);
  }

  @Override
  public int assign(HoodieSourceSplit split) {

    int curBucket = BucketIdentifier.bucketIdFromFileId(split.getFileId());
    int numBuckets = numBucketsFunction.getNumBuckets(split.getPartitionPath());
    return this.partitionIndexFunc.apply(numBuckets, split.getPartitionPath(), curBucket);
  }
}
