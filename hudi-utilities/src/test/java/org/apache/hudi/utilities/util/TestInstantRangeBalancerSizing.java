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

package org.apache.hudi.utilities.util;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * The balancer's driver-side arithmetic, exercised without a Spark context.
 *
 * <p>Kept apart from {@code TestInstantRangeBalancer}, which needs a real local Spark context: the
 * sizing rules are where the balance property is actually decided, and they should be testable in
 * milliseconds rather than behind a cluster start-up.
 */
class TestInstantRangeBalancerSizing {

  @Test
  void partitionCountIsDrivenByRecordsWhenRecordsDominate() {
    assertEquals(5, InstantRangeBalancer.partitionCount(50_000L, 2));
    assertEquals(4, InstantRangeBalancer.partitionCount(31_000L, 2));
  }

  /**
   * The parallelism floor: a small run on a large cluster still fans out, so each task's archived
   * timeline scan stays narrow and idle cores are used.
   */
  @Test
  void partitionCountIsFlooredByClusterParallelism() {
    assertEquals(200, InstantRangeBalancer.partitionCount(5_000L, 200));
    assertEquals(200, InstantRangeBalancer.partitionCount(0L, 200));
  }

  @Test
  void partitionCountIsNeverBelowOne() {
    assertEquals(1, InstantRangeBalancer.partitionCount(0L, 0));
    assertEquals(1, InstantRangeBalancer.partitionCount(0L, -4));
    assertEquals(1, InstantRangeBalancer.partitionCount(1L, 1));
  }

  @Test
  void uniformCountsSplitEvenly() {
    Map<String, Long> counts = new LinkedHashMap<>();
    counts.put("100", 10L);
    counts.put("200", 10L);
    counts.put("300", 10L);
    counts.put("400", 10L);

    assertEquals(Arrays.asList("100", "200", "300"),
        InstantRangeBalancer.computeBounds(counts, 4));
    assertEquals(Arrays.asList("200"), InstantRangeBalancer.computeBounds(counts, 2));
  }

  @Test
  void boundsAreSortedAndDistinct() {
    Map<String, Long> counts = new LinkedHashMap<>();
    counts.put("300", 10L);
    counts.put("100", 10L);
    counts.put("400", 10L);
    counts.put("200", 10L);

    List<String> bounds = InstantRangeBalancer.computeBounds(counts, 4);
    assertEquals(Arrays.asList("100", "200", "300"), bounds,
        "binarySearch requires ascending order regardless of the map's iteration order");
  }

  @Test
  void singleInstantProducesNoBounds() {
    assertEquals(Collections.emptyList(),
        InstantRangeBalancer.computeBounds(Collections.singletonMap("100", 500_000L), 8));
  }

  @Test
  void emptyCountsProduceNoBounds() {
    assertEquals(Collections.emptyList(),
        InstantRangeBalancer.computeBounds(Collections.emptyMap(), 8));
  }

  @Test
  void onePartitionRequestedProducesNoBounds() {
    Map<String, Long> counts = new LinkedHashMap<>();
    counts.put("100", 10L);
    counts.put("200", 10L);
    assertEquals(Collections.emptyList(), InstantRangeBalancer.computeBounds(counts, 1));
  }

  /**
   * More partitions asked for than there are instants to split between. The bound count is capped
   * at {@code distinct - 1} because the last instant is never a bound — closing on it would create
   * an empty trailing partition.
   */
  @Test
  void moreRequestedPartitionsThanInstantsIsCappedByTheInstantCount() {
    Map<String, Long> counts = new LinkedHashMap<>();
    counts.put("100", 1L);
    counts.put("200", 1L);
    counts.put("300", 1L);

    List<String> bounds = InstantRangeBalancer.computeBounds(counts, 64);
    assertEquals(Arrays.asList("100", "200"), bounds);
    assertEquals(3, bounds.size() + 1, "three instants can never yield more than three partitions");
  }

  /**
   * The sanctioned case: one instant holding more than its share cannot be split by a boundary
   * array, so it gets a partition to itself and the rest stay balanced.
   */
  @Test
  void hotInstantIsIsolatedAndTheRestStayBalanced() {
    Map<String, Long> counts = new LinkedHashMap<>();
    counts.put("100", 10L);
    counts.put("200", 40_000L);
    counts.put("300", 10L);
    counts.put("400", 10L);

    List<String> bounds = InstantRangeBalancer.computeBounds(counts, 4);
    InstantRangePartitioner partitioner = new InstantRangePartitioner(bounds);

    int hot = partitioner.getPartition("200");
    assertNotEquals(hot, partitioner.getPartition("100"),
        "a light instant immediately before a hot one must not be absorbed into its partition");
    assertNotEquals(hot, partitioner.getPartition("300"));
    assertNotEquals(hot, partitioner.getPartition("400"));
  }

  /**
   * More than one hot instant in a single run. Before the bound budget was widened by the hot-instant
   * count, isolating the first hot instant ({@code B}) spent both bounds a {@code numParts = 3} run
   * is allowed, so the second hot instant ({@code D}) could no longer get a bound of its own: it and
   * everything after it collapsed into one final partition holding 19,999 of this run's 30,000
   * records. Both hot instants must come out isolated, and the largest partition must stay at the
   * target rather than absorbing a neighbour.
   */
  @Test
  void multipleHotInstantsAreEachIsolatedWithoutExhaustingTheBoundBudget() {
    Map<String, Long> counts = new LinkedHashMap<>();
    counts.put("A", 1L);
    counts.put("B", 10_000L);
    counts.put("C", 1L);
    counts.put("D", 10_000L);
    counts.put("E", 9_998L);

    List<String> bounds = InstantRangeBalancer.computeBounds(counts, 3);
    InstantRangePartitioner partitioner = new InstantRangePartitioner(bounds);

    int hotB = partitioner.getPartition("B");
    int hotD = partitioner.getPartition("D");
    assertNotEquals(hotB, hotD, "two hot instants must not share a partition with each other");
    assertNotEquals(hotB, partitioner.getPartition("A"));
    assertNotEquals(hotB, partitioner.getPartition("C"));
    assertNotEquals(hotD, partitioner.getPartition("C"));
    assertNotEquals(hotD, partitioner.getPartition("E"));

    Map<Integer, Long> totalsByPartition = new HashMap<>();
    for (Map.Entry<String, Long> entry : counts.entrySet()) {
      totalsByPartition.merge(partitioner.getPartition(entry.getKey()), entry.getValue(), Long::sum);
    }
    assertEquals(10_000L, Collections.max(totalsByPartition.values()),
        "a hot instant's own partition should not also absorb a neighbour: " + totalsByPartition);
  }

  /**
   * No bound may be the largest instant: the sweep would then close a partition that has nothing
   * after it, and Spark would run an empty task for every such bound.
   */
  @Test
  void theLargestInstantIsNeverABound() {
    Map<String, Long> counts = new LinkedHashMap<>();
    counts.put("100", 1L);
    counts.put("200", 1L);
    counts.put("300", 100_000L);

    List<String> bounds = InstantRangeBalancer.computeBounds(counts, 8);
    assertFalse(bounds.contains("300"), "the last instant closed a partition: " + bounds);
  }
}
