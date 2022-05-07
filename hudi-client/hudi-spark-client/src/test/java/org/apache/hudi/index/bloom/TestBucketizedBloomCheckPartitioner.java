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

package org.apache.hudi.index.bloom;

import org.apache.hudi.common.util.collection.Pair;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestBucketizedBloomCheckPartitioner {

  @Test
  public void testAssignmentCorrectness() {
    Map<String, Long> fileToComparisons = new HashMap<String, Long>() {
      {
        put("f1", 40L);
        put("f2", 35L);
        put("f3", 20L);
      }
    };
    BucketizedBloomCheckPartitioner p = new BucketizedBloomCheckPartitioner(4, fileToComparisons, 10);
    Map<String, List<Integer>> assignments = p.getFileGroupToPartitions();
    assertEquals(4, assignments.get("f1").size(), "f1 should have 4 buckets");
    assertEquals(4, assignments.get("f2").size(), "f2 should have 4 buckets");
    assertEquals(2, assignments.get("f3").size(), "f3 should have 2 buckets");
    assertArrayEquals(new Integer[] {0, 0, 1, 3}, assignments.get("f1").toArray(), "f1 spread across 3 partitions");
    assertArrayEquals(new Integer[] {1, 2, 2, 0}, assignments.get("f2").toArray(), "f2 spread across 3 partitions");
    assertArrayEquals(new Integer[] {3, 1}, assignments.get("f3").toArray(), "f3 spread across 2 partitions");
  }

  @Test
  public void testUniformPacking() {
    // evenly distribute 10 buckets/file across 100 partitions
    Map<String, Long> comparisons1 = new HashMap<String, Long>() {
      {
        IntStream.range(0, 10).forEach(f -> put("f" + f, 100L));
      }
    };
    BucketizedBloomCheckPartitioner partitioner = new BucketizedBloomCheckPartitioner(100, comparisons1, 10);
    Map<String, List<Integer>> assignments = partitioner.getFileGroupToPartitions();
    assignments.forEach((key, value) -> assertEquals(10, value.size()));
    Map<Integer, Long> partitionToNumBuckets =
        assignments.entrySet().stream().flatMap(e -> e.getValue().stream().map(p -> Pair.of(p, e.getKey())))
            .collect(Collectors.groupingBy(Pair::getLeft, Collectors.counting()));
    partitionToNumBuckets.forEach((key, value) -> assertEquals(1L, value.longValue()));
  }

  @Test
  public void testNumPartitions() {
    Map<String, Long> comparisons1 = new HashMap<String, Long>() {
      {
        IntStream.range(0, 10).forEach(f -> put("f" + f, 100L));
      }
    };
    BucketizedBloomCheckPartitioner p = new BucketizedBloomCheckPartitioner(10000, comparisons1, 10);
    assertEquals(100, p.numPartitions(), "num partitions must equal total buckets");
  }

  @Test
  public void testGetPartitions() {
    Map<String, Long> comparisons1 = new HashMap<String, Long>() {
      {
        IntStream.range(0, 100000).forEach(f -> put("f" + f, 100L));
      }
    };
    BucketizedBloomCheckPartitioner p = new BucketizedBloomCheckPartitioner(1000, comparisons1, 10);

    IntStream.range(0, 100000).forEach(f -> {
      int partition = p.getPartition(Pair.of("f" + f, "value"));
      assertTrue(0 <= partition && partition <= 1000, "partition is out of range: " + partition);
    });
  }

}
