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
 * 假设：
 * Flink并行度为 100
 * 期望每一个数据partition下对应10个task写数据(控制文件数)
 * 则 1,2,3,4,..,100 ==> group1-[1,2,3,..,10], group2-[11,12,13,...,20],...,group10-[91,92,..,100]
 * partition1 使用 group1
 */
public class DefaultInsertPartitioner<T extends HoodieKey> implements Partitioner<T> {

  private final int groupLength;
  private final Random random;

  public DefaultInsertPartitioner(Configuration conf) {
    this.groupLength = conf.get(FlinkOptions.DEFAULT_PARALLELISM_PER_PARTITION); // default 30 ==> parallelism per partition
    this.random = new Random();
  }

  @Override
  public int partition(HoodieKey hoodieKey, int numPartitions) {
    String partitionPath = hoodieKey.getPartitionPath();
    int groupNumber = numPartitions / groupLength;
    ValidationUtils.checkArgument(groupNumber != 0,
        String.format("write.insert.partitioner.default_parallelism_per_partition are greater than numPartitions %d.", numPartitions));

    int groupIndex = (partitionPath.hashCode() & Integer.MAX_VALUE) % groupNumber;
    int index = random.nextInt(groupLength);
    return groupIndex * groupLength + index;
  }
}
