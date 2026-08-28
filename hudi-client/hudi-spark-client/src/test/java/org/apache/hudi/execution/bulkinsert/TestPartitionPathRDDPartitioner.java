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

package org.apache.hudi.execution.bulkinsert;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestPartitionPathRDDPartitioner {

  /**
   * Objects.hash(x) is 31 + x.hashCode(), so this partition path overflows it to
   * Integer.MIN_VALUE, which Math.abs leaves negative.
   */
  private static final String MIN_VALUE_HASH_PATH = "xfjfxsf";

  @Test
  void assertFixtureStillOverflowsToMinValue() {
    assertEquals(Integer.MIN_VALUE, Objects.hash(MIN_VALUE_HASH_PATH));
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8, 16})
  void assertPartitionIsInRangeForMinValueHash(int numPartitions) {
    PartitionPathRDDPartitioner partitioner =
        new PartitionPathRDDPartitioner(o -> MIN_VALUE_HASH_PATH, numPartitions);
    int partition = partitioner.getPartition(new Object());
    assertTrue(partition >= 0 && partition < numPartitions,
        "partition " + partition + " out of range for numPartitions " + numPartitions);
  }
}
