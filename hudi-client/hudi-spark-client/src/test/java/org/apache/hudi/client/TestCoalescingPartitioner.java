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

package org.apache.hudi.client;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestCoalescingPartitioner extends HoodieClientTestBase {

  @Test
  public void simplePassThroughPartitionerTest() {
    int numPartitions = 100;
    HoodieData<Integer> rddData = HoodieJavaRDD.of(jsc.parallelize(
        IntStream.rangeClosed(0, 100).boxed().collect(Collectors.toList()), numPartitions));

    // 100 keys spread across 100 partitions.
    CoalescingPartitioner coalescingPartitioner = new CoalescingPartitioner();
    assertEquals(1, coalescingPartitioner.numPartitions());
    rddData.collectAsList().forEach(entry -> {
      assertEquals(0, coalescingPartitioner.getPartition(entry));
    });

    // empty rdd
    rddData = HoodieJavaRDD.of(jsc.emptyRDD());
    CoalescingPartitioner coalescingPartitioner2 = new CoalescingPartitioner();
    assertEquals(1, coalescingPartitioner2.numPartitions());
    rddData.collectAsList().forEach(entry -> {
      // since there is only one partition, any getPartition will return just the same partition index
      assertEquals(0, coalescingPartitioner2.getPartition(entry));
    });
  }
}
