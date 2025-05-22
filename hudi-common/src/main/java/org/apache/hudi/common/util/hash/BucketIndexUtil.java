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

package org.apache.hudi.common.util.hash;

import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.ValidationUtils;

/**
 * Utility class for bucket index.
 */
public class BucketIndexUtil {

  /**
   * This method is used to get the partition index calculation function of a bucket.
   * "partition.hashCode() / (parallelism / bucketNum) * bucketNum" divides the parallelism into
   * sub-intervals of length bucket_num, different partitions will be mapped to different sub-interval,
   * ensure that the data across multiple partitions is evenly distributed.
   *
   * @param bucketNum   Bucket number per partition
   * @param parallelism Parallelism of the task
   * @return The partition index of this bucket.
   */
  public static Functions.Function3<Integer, String, Integer, Integer> getPartitionIndexFunc(int parallelism) {
    return (bucketNum, partition, curBucket) -> {
      long partitionIndex = (partition.hashCode() & Integer.MAX_VALUE) % parallelism * (long) bucketNum;
      long globalIndex = partitionIndex + curBucket;
      int partitionId = (int) (globalIndex % parallelism);
      ValidationUtils.checkArgument(partitionId >= 0 && partitionId < parallelism,
          () -> "Partition id should be in range [0, " + parallelism + "), but got " + partitionId);
      return partitionId;
    };
  }
}
