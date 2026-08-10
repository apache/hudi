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

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestGroupedInsertPartitioner {

  @Test
  void testPartitioner() {
    Configuration conf = new Configuration();
    int para = 30;
    conf.set(FlinkOptions.DEFAULT_PARALLELISM_PER_PARTITION, para);
    GroupedInsertPartitioner partitioner = new GroupedInsertPartitioner(conf);
    int numberFlinkPartitions = 2023;
    int numberDataPartition = 1030;
    int recordsPerPartition = 20;
    HashMap<Integer, Integer> res = new HashMap<>();
    String partitionPath = "dt=2023-11-27/hour=01/index=";
    for (int partitionIndex = 0; partitionIndex < numberDataPartition; partitionIndex++) {
      for (int recordIndex = 0; recordIndex < recordsPerPartition; recordIndex++) {
        int id = partitioner.partition(new HoodieKey("id" + recordIndex, partitionPath + partitionIndex), numberFlinkPartitions);
        if (res.containsKey(id)) {
          Integer value = res.get(id);
          res.put(id, value + 1);
        } else {
          res.put(id, 1);
        }
      }
    }

    assertTrue(res.size() <= numberFlinkPartitions
        && res.size() >= (numberFlinkPartitions - numberFlinkPartitions % para));
  }

}