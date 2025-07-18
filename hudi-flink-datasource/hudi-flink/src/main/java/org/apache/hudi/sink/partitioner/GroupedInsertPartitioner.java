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
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.configuration.Configuration;

import java.util.Random;

/**
 * Insert input partitioner.
 *
 * 1. Split numPartitions into multi bucket based on numPartitions / groupLength
 * 2. Route each record into related Bucket based on (partitionPath.hashCode() & Integer.MAX_VALUE) % groupNumber
 * 3. Using random to get specific index number.
 *
 * Need to take care of the last bucket to avoid resource wast.
 */
public class GroupedInsertPartitioner<T extends HoodieKey> implements Partitioner<T> {

  private final int groupLength;
  private final Random random;
  private int groupNumber = -1;
  private int remaining = -1;

  public GroupedInsertPartitioner(Configuration conf) {
    this.groupLength = conf.get(FlinkOptions.DEFAULT_PARALLELISM_PER_PARTITION); // default 30 ==> parallelism per partition
    this.random = new Random();
  }

  /**
   * Make sure that data with the same partition will only be routed to the same flink task group(groupIndex).
   * @param hoodieKey
   * @param numPartitions
   * @return
   */
  @Override
  public int partition(HoodieKey hoodieKey, int numPartitions) {
    setupIfNecessary(numPartitions);
    String partitionPath = hoodieKey.getPartitionPath();
    int groupNumber = numPartitions / groupLength;
    int remaining = numPartitions - groupNumber * groupLength;
    ValidationUtils.checkArgument(groupNumber != 0,
        String.format("write.insert.partitioner.parallelism.per.partition are greater than numPartitions %d.", numPartitions));

    int groupIndex = (partitionPath.hashCode() & Integer.MAX_VALUE) % groupNumber;
    int step;

    if (remaining > 0 && groupIndex == groupNumber - 1) {
      // the last group contains remaining partitions.
      step = random.nextInt(groupLength + remaining);
    } else {
      step = random.nextInt(groupLength);
    }
    return groupIndex * groupLength + step;
  }

  /**
   * set up groupNumber and remaining for the first time, avoid unnecessary calculation.
   * @param numPartitions
   */
  private void setupIfNecessary(int numPartitions) {
    if (groupNumber == -1 || remaining == -1) {
      groupNumber = Math.max(1, numPartitions / groupLength);
      remaining = numPartitions - groupNumber * groupLength;
    }
  }
}
