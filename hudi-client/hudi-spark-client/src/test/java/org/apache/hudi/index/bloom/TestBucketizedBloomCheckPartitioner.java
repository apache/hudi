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

import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.util.collection.Pair;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestBucketizedBloomCheckPartitioner {
  private static Stream<Arguments> partitioningTestCases() {
    // Case 1
    Map<HoodieFileGroupId, Long> fileToComparisons = constructFileToComparisons(
        Pair.of(new HoodieFileGroupId("p1", "f1"), 30L),
        Pair.of(new HoodieFileGroupId("p1", "f2"), 35L),
        Pair.of(new HoodieFileGroupId("p1", "f3"), 20L)
    );
    // partitioning based on parallelism of 4
    Map<HoodieFileGroupId, List<Integer>> partitioning1 = constructPartitioning(
        Pair.of(new HoodieFileGroupId("p1", "f1"), new Integer[] {0, 0, 3}),
        Pair.of(new HoodieFileGroupId("p1", "f2"), new Integer[] {2, 2, 3, 1}),
        Pair.of(new HoodieFileGroupId("p1", "f3"), new Integer[] {1, 0})
    );
    List<LookUpKeyAndResult> lookUpKeyAndResults1 = constructLookUpKeyAndResults(
        LookUpKeyAndResult.of("p1", "f1", "k1", 3),
        LookUpKeyAndResult.of("p1", "f1", "k2", 0),
        LookUpKeyAndResult.of("p1", "f2", "k4", 2),
        LookUpKeyAndResult.of("p1", "f3", "k7", 1));
    // partitioning based on keys per bucket
    Map<HoodieFileGroupId, List<Integer>> partitioning2 = constructPartitioning(
        Pair.of(new HoodieFileGroupId("p1", "f1"), new Integer[] {0, 1, 6}),
        Pair.of(new HoodieFileGroupId("p1", "f2"), new Integer[] {3, 4, 5, 8}),
        Pair.of(new HoodieFileGroupId("p1", "f3"), new Integer[] {2, 7})
    );
    List<LookUpKeyAndResult> lookUpKeyAndResults2 = constructLookUpKeyAndResults(
        LookUpKeyAndResult.of("p1", "f1", "k1", 6),
        LookUpKeyAndResult.of("p1", "f1", "k2", 0),
        LookUpKeyAndResult.of("p1", "f2", "k4", 4),
        LookUpKeyAndResult.of("p1", "f3", "k6", 7));
    // keys per bucket is very large
    Map<HoodieFileGroupId, List<Integer>> partitioning3 = constructPartitioning(
        Pair.of(new HoodieFileGroupId("p1", "f1"), new Integer[] {0}),
        Pair.of(new HoodieFileGroupId("p1", "f2"), new Integer[] {2}),
        Pair.of(new HoodieFileGroupId("p1", "f3"), new Integer[] {1})
    );
    List<LookUpKeyAndResult> lookUpKeyAndResults3 = constructLookUpKeyAndResults(
        LookUpKeyAndResult.of("p1", "f1", "k1", 0),
        LookUpKeyAndResult.of("p1", "f1", "k2", 0),
        LookUpKeyAndResult.of("p1", "f2", "k4", 2),
        LookUpKeyAndResult.of("p1", "f3", "k6", 1));
    // keys per bucket is in the middle
    Map<HoodieFileGroupId, List<Integer>> partitioning4 = constructPartitioning(
        Pair.of(new HoodieFileGroupId("p1", "f1"), new Integer[] {0, 3}),
        Pair.of(new HoodieFileGroupId("p1", "f2"), new Integer[] {2, 0}),
        Pair.of(new HoodieFileGroupId("p1", "f3"), new Integer[] {1})
    );
    List<LookUpKeyAndResult> lookUpKeyAndResults4 = constructLookUpKeyAndResults(
        LookUpKeyAndResult.of("p1", "f1", "k1", 0),
        LookUpKeyAndResult.of("p1", "f1", "k2", 3),
        LookUpKeyAndResult.of("p1", "f2", "k4", 0),
        LookUpKeyAndResult.of("p1", "f3", "k6", 1));
    Map<HoodieFileGroupId, List<Integer>> partitioning5 = constructPartitioning(
        Pair.of(new HoodieFileGroupId("p1", "f1"), new Integer[] {0, 3}),
        Pair.of(new HoodieFileGroupId("p1", "f2"), new Integer[] {2, 4}),
        Pair.of(new HoodieFileGroupId("p1", "f3"), new Integer[] {1})
    );
    List<LookUpKeyAndResult> lookUpKeyAndResults5 = constructLookUpKeyAndResults(
        LookUpKeyAndResult.of("p1", "f1", "k1", 0),
        LookUpKeyAndResult.of("p1", "f1", "k2", 3),
        LookUpKeyAndResult.of("p1", "f2", "k4", 4),
        LookUpKeyAndResult.of("p1", "f3", "k6", 1));

    return Arrays.stream(new Arguments[] {
        // Configured parallelism should take effect
        Arguments.of(4, 6, fileToComparisons, 10, false, 4, partitioning1, lookUpKeyAndResults1),
        Arguments.of(4, 2, fileToComparisons, 10, false, 4, partitioning1, lookUpKeyAndResults1),
        // Input parallelism should take effect
        Arguments.of(0, 4, fileToComparisons, 10, false, 4, partitioning1, lookUpKeyAndResults1),
        // Dynamic parallelism based on the keys per bucket should kick in
        Arguments.of(0, 4, fileToComparisons, 10, true, 9, partitioning2, lookUpKeyAndResults2),
        // Dynamic parallelism based on the keys per bucket that is large
        Arguments.of(0, 4, fileToComparisons, 50, false, 3, partitioning3, lookUpKeyAndResults3),
        Arguments.of(0, 4, fileToComparisons, 50, true, 3, partitioning3, lookUpKeyAndResults3),
        // Dynamic parallelism based on the keys per bucket that is in the middle
        Arguments.of(0, 4, fileToComparisons, 25, false, 4, partitioning4, lookUpKeyAndResults4),
        Arguments.of(0, 4, fileToComparisons, 25, true, 5, partitioning5, lookUpKeyAndResults5)
    });
  }

  @ParameterizedTest
  @MethodSource("partitioningTestCases")
  void testPartitioning(int configuredParallelism,
                        int inputParallelism,
                        Map<HoodieFileGroupId, Long> fileToComparisons,
                        int keysPerBucket,
                        boolean shouldUseDynamicParallelism,
                        int expectedNumPartitions,
                        Map<HoodieFileGroupId, List<Integer>> expectedPartitioning,
                        List<LookUpKeyAndResult> lookUpKeyAndResults) {
    BucketizedBloomCheckPartitioner partitioner = new BucketizedBloomCheckPartitioner(
        configuredParallelism, inputParallelism, fileToComparisons, keysPerBucket, shouldUseDynamicParallelism);
    assertEquals(expectedNumPartitions, partitioner.numPartitions());
    Map<HoodieFileGroupId, List<Integer>> actualPartitioning = partitioner.getFileGroupToPartitions();
    assertEquals(expectedPartitioning.size(), actualPartitioning.size());
    for (HoodieFileGroupId id : actualPartitioning.keySet()) {
      assertTrue(expectedPartitioning.containsKey(id));
      assertArrayEquals(expectedPartitioning.get(id).toArray(), expectedPartitioning.get(id).toArray());
    }
    lookUpKeyAndResults.forEach(lookUpKeyAndResult ->
        assertEquals(lookUpKeyAndResult.expectedPartitionId, partitioner.getPartition(
            Tuple2.apply(lookUpKeyAndResult.fileGroupId, lookUpKeyAndResult.recordKey))));
  }

  @Test
  public void testUniformPacking() {
    // evenly distribute 10 buckets/file across 100 partitions
    Map<HoodieFileGroupId, Long> comparisons1 = new HashMap<HoodieFileGroupId, Long>() {
      {
        IntStream.range(0, 10).forEach(f -> put(new HoodieFileGroupId("p1", "f" + f), 100L));
      }
    };
    BucketizedBloomCheckPartitioner partitioner = new BucketizedBloomCheckPartitioner(
        100, 100, comparisons1, 10, false);
    Map<HoodieFileGroupId, List<Integer>> assignments = partitioner.getFileGroupToPartitions();
    assignments.forEach((key, value) -> assertEquals(10, value.size()));
    Map<Integer, Long> partitionToNumBuckets =
        assignments.entrySet().stream().flatMap(e -> e.getValue().stream().map(p -> Pair.of(p, e.getKey())))
            .collect(Collectors.groupingBy(Pair::getLeft, Collectors.counting()));
    partitionToNumBuckets.forEach((key, value) -> assertEquals(1L, value.longValue()));
  }

  @Test
  public void testNumPartitions() {
    Map<HoodieFileGroupId, Long> comparisons1 = new HashMap<HoodieFileGroupId, Long>() {
      {
        IntStream.range(0, 10).forEach(f -> put(new HoodieFileGroupId("p1", "f" + f), 100L));
      }
    };
    BucketizedBloomCheckPartitioner p = new BucketizedBloomCheckPartitioner(
        10000, 10000, comparisons1, 10, false);
    assertEquals(100, p.numPartitions(), "num partitions must equal total buckets");
  }

  @Test
  public void testGetPartitions() {
    Map<HoodieFileGroupId, Long> comparisons1 = new HashMap<HoodieFileGroupId, Long>() {
      {
        IntStream.range(0, 100000).forEach(f -> put(new HoodieFileGroupId("p1", "f" + f), 100L));
      }
    };
    BucketizedBloomCheckPartitioner p = new BucketizedBloomCheckPartitioner(
        1000, 1000, comparisons1, 10, false);

    IntStream.range(0, 100000).forEach(f -> {
      int partition = p.getPartition(Tuple2.apply(new HoodieFileGroupId("p1", "f" + f), "value"));
      assertTrue(0 <= partition && partition <= 1000, "partition is out of range: " + partition);
    });
  }

  private static Map<HoodieFileGroupId, Long> constructFileToComparisons(Pair<HoodieFileGroupId, Long>... entries) {
    Map<HoodieFileGroupId, Long> result = new HashMap<>();
    Arrays.stream(entries).forEach(e -> result.put(e.getKey(), e.getValue()));
    return result;
  }

  private static Map<HoodieFileGroupId, List<Integer>> constructPartitioning(Pair<HoodieFileGroupId, Integer[]>... entries) {
    Map<HoodieFileGroupId, List<Integer>> result = new HashMap<>();
    Arrays.stream(entries).forEach(e -> result.put(e.getKey(), Arrays.stream(e.getValue()).collect(Collectors.toList())));
    return result;
  }

  private static List<LookUpKeyAndResult> constructLookUpKeyAndResults(LookUpKeyAndResult... entries) {
    return Arrays.stream(entries).collect(Collectors.toList());
  }

  static class LookUpKeyAndResult {
    HoodieFileGroupId fileGroupId;
    String recordKey;
    int expectedPartitionId;

    private LookUpKeyAndResult(String partitionPath,
                               String fileId,
                               String recordKey,
                               int expectedPartitionId) {
      this.fileGroupId = new HoodieFileGroupId(partitionPath, fileId);
      this.recordKey = recordKey;
      this.expectedPartitionId = expectedPartitionId;
    }

    public static LookUpKeyAndResult of(String partitionPath,
                                        String fileId,
                                        String recordKey,
                                        int expectedPartitionId) {
      return new LookUpKeyAndResult(partitionPath, fileId, recordKey, expectedPartitionId);
    }
  }
}
