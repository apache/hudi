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
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The partitioner's contract, exercised without a Spark context.
 *
 * <p>Totality is the property that matters most and the one with the least obvious payoff: a
 * partitioner that answered an unexpected key by killing the whole validation would force both
 * dimensions to filter the key space, costing records their adjudication. A partitioner that
 * cannot reject a key needs no such filter.
 */
class TestInstantRangePartitioner {

  private static final List<String> BOUNDS =
      Arrays.asList("20240102000000000", "20240104000000000", "20240106000000000");

  @Test
  void numPartitionsIsOneMoreThanTheBoundCount() {
    assertEquals(4, new InstantRangePartitioner(BOUNDS).numPartitions());
    assertEquals(1, new InstantRangePartitioner(Collections.emptyList()).numPartitions());
  }

  /**
   * A key equal to a bound belongs to the partition that bound CLOSES, not the next one. Getting
   * this off by one would split an instant across two partitions, and two tasks would then each
   * scan the archived timeline for it.
   */
  @Test
  void keyEqualToABoundClosesItsOwnPartition() {
    InstantRangePartitioner partitioner = new InstantRangePartitioner(BOUNDS);
    assertEquals(0, partitioner.getPartition("20240102000000000"));
    assertEquals(1, partitioner.getPartition("20240104000000000"));
    assertEquals(2, partitioner.getPartition("20240106000000000"));
  }

  @Test
  void keysBetweenBoundsFallIntoTheEnclosingPartition() {
    InstantRangePartitioner partitioner = new InstantRangePartitioner(BOUNDS);
    assertEquals(0, partitioner.getPartition("20240101000000000"));
    assertEquals(1, partitioner.getPartition("20240103000000000"));
    assertEquals(2, partitioner.getPartition("20240105000000000"));
    assertEquals(3, partitioner.getPartition("20240107000000000"));
  }

  /**
   * The property that replaces the subset filter: a key the driver never saw still routes, rather
   * than failing a precondition and taking the run with it.
   */
  @Test
  void anInstantAbsentFromTheBoundsStillRoutesToAValidPartition() {
    InstantRangePartitioner partitioner = new InstantRangePartitioner(BOUNDS);
    for (String unseen : Arrays.asList("00000000000000000", "19700101000000000",
        "20240103123456789", "99999999999999999", "not-an-instant")) {
      int index = partitioner.getPartition(unseen);
      assertTrue(index >= 0 && index < partitioner.numPartitions(),
          "an unseen key must route rather than throw, but " + unseen + " gave " + index);
    }
  }

  @Test
  void partitionIndicesAreNonDecreasingInKeyOrder() {
    InstantRangePartitioner partitioner = new InstantRangePartitioner(BOUNDS);
    List<String> ascending = Arrays.asList("20240101000000000", "20240102000000000",
        "20240103000000000", "20240104000000000", "20240105000000000", "20240106000000000",
        "20240107000000000");
    int previous = -1;
    for (String key : ascending) {
      int index = partitioner.getPartition(key);
      assertTrue(index >= previous, "ranges must be contiguous and ordered: " + key);
      previous = index;
    }
  }

  /**
   * Every instant, not one: a partitioner is a pure function of its key, so asking it about the
   * SAME instant fifty times only shows that it is deterministic -- which holds for any boundary
   * array, including a broken one. What has to hold is that no instant in the key space maps to
   * more than one partition, so the sweep is over the whole space and each instant's index is
   * recorded once.
   */
  @Test
  void everyRecordOfOneInstantLandsInOnePartition() {
    InstantRangePartitioner partitioner = new InstantRangePartitioner(BOUNDS);
    Map<String, Integer> partitionByInstant = new HashMap<>();
    for (int day = 1; day <= 9; day++) {
      String instant = String.format("2024010%d000000000", day);
      for (int repeat = 0; repeat < 5; repeat++) {
        Integer seen = partitionByInstant.putIfAbsent(instant, partitioner.getPartition(instant));
        if (seen != null) {
          assertEquals(seen.intValue(), partitioner.getPartition(instant),
              "an instant split across partitions is scanned twice: " + instant);
        }
      }
    }
    assertEquals(9, partitionByInstant.size(), "every instant in the sweep must have been routed");
    assertTrue(new HashSet<>(partitionByInstant.values()).size() > 1,
        "a partitioner that sent every instant to one partition would pass the co-location check "
            + "vacuously: " + partitionByInstant);
  }

  @Test
  void emptyBoundsSendEverythingToPartitionZero() {
    InstantRangePartitioner partitioner = new InstantRangePartitioner(Collections.emptyList());
    assertEquals(0, partitioner.getPartition("20240101000000000"));
    assertEquals(0, partitioner.getPartition("whatever"));
  }

  @Test
  void nullAndNonStringKeysAreRejected() {
    InstantRangePartitioner partitioner = new InstantRangePartitioner(BOUNDS);
    assertThrows(IllegalArgumentException.class, () -> partitioner.getPartition(null));
    assertThrows(IllegalArgumentException.class, () -> partitioner.getPartition(42));
  }

  /** Spark compares partitioners to decide whether a shuffle can be skipped. */
  @Test
  void equalBoundsMakeEqualPartitioners() {
    assertEquals(new InstantRangePartitioner(BOUNDS), new InstantRangePartitioner(BOUNDS));
    assertEquals(new InstantRangePartitioner(BOUNDS).hashCode(),
        new InstantRangePartitioner(BOUNDS).hashCode());
    assertNotEquals(new InstantRangePartitioner(BOUNDS),
        new InstantRangePartitioner(Collections.singletonList("20240102000000000")));
  }
}
