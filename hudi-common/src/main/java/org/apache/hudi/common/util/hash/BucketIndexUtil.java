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
   * Gets a function that calculates the target partition index for a specific bucket.
   *
   * <p>The returned function implements a round-robin distribution of buckets from a single
   * data partition (e.g., country=US) across the available task parallelisms.
   *
   * <p>The calculation logic is equivalent to:
   * {@code ((partition.hashCode() & Integer.MAX_VALUE) % parallelism * bucketNum + curBucket) % parallelism}
   *
   * <p>This logic works as follows:
   * <ol>
   * <li><b>Calculate a base task for the partition:</b>
   * {@code (partition.hashCode() & Integer.MAX_VALUE) % parallelism}. This maps the
   * partition string to a starting task.
   * <li><b>Calculate a global bucket index:</b> The "base" task is multiplied by
   * {@code bucketNum} to get a starting index for that partition's buckets. The
   * {@code curBucket} (from 0 to bucketNum-1) is added to get the unique global
   * index for this specific bucket.
   * <li><b>Map to final partition:</b> The global index is mapped back to a final
   * task ID using {@code % parallelism}.
   * </ol>
   *
   * This strategy ensures that the {@code bucketNum} buckets from a single partition are
   * spread evenly across different downstream tasks, rather than all landing on the same task.
   *
   * @param parallelism Parallelism of the task (the total number of target partitions)
   * @return A function that takes (bucketNum, partition, curBucket) and returns the calculated
   * partition index (from 0 to parallelism-1).
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
