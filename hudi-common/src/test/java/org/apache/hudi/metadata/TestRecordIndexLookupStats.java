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

package org.apache.hudi.metadata;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the merge algebra that makes record index lookup collection idempotent under task retry,
 * speculation and RDD recomputation.
 */
class TestRecordIndexLookupStats {

  private static RecordIndexShardLookupStats shard(int index, long keys, long hits) {
    return new RecordIndexShardLookupStats(index, "fg-" + index, keys, hits, 2L, 1000L, 10L);
  }

  @Test
  void testShardMergeTakesFieldWiseMax() {
    RecordIndexShardLookupStats partial = new RecordIndexShardLookupStats(3, "fg-3", 100L, 40L, 2L, 800L, 5L);
    RecordIndexShardLookupStats complete = new RecordIndexShardLookupStats(3, "fg-3", 100L, 90L, 2L, 800L, 12L);

    RecordIndexShardLookupStats merged = partial.merge(complete);

    assertEquals(3, merged.getShardIndex());
    assertEquals("fg-3", merged.getFileGroupId());
    assertEquals(100L, merged.getKeysSubmitted());
    assertEquals(90L, merged.getKeysHit(), "a partial read followed by a complete one resolves to complete");
    assertEquals(2L, merged.getLogFilesRead());
    assertEquals(800L, merged.getBytesInShard());
    assertEquals(12L, merged.getLookupMillis());
  }

  @Test
  void testTotalsFoldFileLevelCounts() {
    RecordIndexLookupStats stats = RecordIndexLookupStats
        .of(new RecordIndexShardLookupStats(0, "fg-0", 100L, 70L, 3L, 1300L, 5L))
        .merge(RecordIndexLookupStats.of(new RecordIndexShardLookupStats(2, "fg-2", 50L, 10L, 1L, 600L, 3L)));

    assertEquals(2L, stats.getShardsRead());
    assertEquals(4L, stats.getLogFilesRead());
    assertEquals(1900L, stats.getBytesInShardsRead());
  }

  @Test
  void testRetriedShardIsIdempotentNotAdditive() {
    RecordIndexLookupStats once = RecordIndexLookupStats.of(shard(1, 500L, 300L));

    RecordIndexLookupStats tenTimes = once;
    for (int i = 0; i < 9; i++) {
      tenTimes = tenTimes.merge(RecordIndexLookupStats.of(shard(1, 500L, 300L)));
    }

    assertEquals(1L, tenTimes.getShardsRead(), "the same shard reported ten times is still one shard");
    assertEquals(500L, tenTimes.getKeysSubmitted(), "counts must not accumulate across retries");
    assertEquals(300L, tenTimes.getKeysHit());
  }

  @Test
  void testDistinctShardsAccumulate() {
    RecordIndexLookupStats stats = RecordIndexLookupStats.of(shard(0, 10L, 4L))
        .merge(RecordIndexLookupStats.of(shard(1, 20L, 11L)))
        .merge(RecordIndexLookupStats.of(shard(2, 30L, 30L)));

    assertEquals(3L, stats.getShardsRead());
    assertEquals(60L, stats.getKeysSubmitted());
    assertEquals(45L, stats.getKeysHit());
  }

  @Test
  void testMergeIsCommutativeForTheSameShard() {
    // Disjoint shards would only prove that HashMap union commutes. The case that matters is two
    // conflicting reports of the SAME shard: last-writer-wins would make this order-dependent.
    RecordIndexShardLookupStats early = new RecordIndexShardLookupStats(1, "fg-1", 80L, 20L, 1L, 400L, 30L);
    RecordIndexShardLookupStats late = new RecordIndexShardLookupStats(1, "fg-1", 80L, 75L, 4L, 1600L, 8L);

    RecordIndexLookupStats forward = RecordIndexLookupStats.of(early).merge(RecordIndexLookupStats.of(late));
    RecordIndexLookupStats backward = RecordIndexLookupStats.of(late).merge(RecordIndexLookupStats.of(early));

    assertEquals(forward.getShardStats(), backward.getShardStats());
    // And pin the resolved value, so the test fails if merge degrades to "pick one".
    assertEquals(75L, forward.getKeysHit());
    assertEquals(4L, forward.getLogFilesRead());
    assertEquals(1600L, forward.getBytesInShardsRead());
    assertEquals(30L, forward.getShardStats().get(1).getLookupMillis());
  }

  @Test
  void testMergeIsCommutativeAcrossEveryPairOrdering() {
    // Exhaustive over a small set: every unordered pair must agree in both directions, including
    // pairs that collide on a shard.
    List<RecordIndexShardLookupStats> reports = Arrays.asList(
        new RecordIndexShardLookupStats(0, "fg-0", 10L, 4L, 1L, 100L, 2L),
        new RecordIndexShardLookupStats(0, "fg-0", 10L, 9L, 3L, 900L, 1L),
        new RecordIndexShardLookupStats(1, "fg-1", 20L, 11L, 2L, 250L, 7L),
        new RecordIndexShardLookupStats(2, "fg-2", 30L, 30L, 0L, 50L, 4L));

    for (RecordIndexShardLookupStats left : reports) {
      for (RecordIndexShardLookupStats right : reports) {
        RecordIndexLookupStats forward = RecordIndexLookupStats.of(left).merge(RecordIndexLookupStats.of(right));
        RecordIndexLookupStats backward = RecordIndexLookupStats.of(right).merge(RecordIndexLookupStats.of(left));
        assertEquals(forward.getShardStats(), backward.getShardStats(),
            "order-dependent merge for " + left + " and " + right);
      }
    }
  }

  @Test
  void testMergeIsAssociativeUnderShuffledOrders() {
    List<RecordIndexLookupStats> parts = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      parts.add(RecordIndexLookupStats.of(shard(i % 3, 10L * (i + 1), 5L * (i + 1))));
    }

    RecordIndexLookupStats leftFold = RecordIndexLookupStats.empty();
    for (RecordIndexLookupStats part : parts) {
      leftFold = leftFold.merge(part);
    }

    // Spark merges executor-local copies in an unspecified order, so any permutation must agree.
    List<RecordIndexLookupStats> reversed = new ArrayList<>(parts);
    Collections.reverse(reversed);
    RecordIndexLookupStats rightFold = RecordIndexLookupStats.empty();
    for (RecordIndexLookupStats part : reversed) {
      rightFold = rightFold.merge(part);
    }

    assertEquals(leftFold.getShardStats(), rightFold.getShardStats());
  }

  @Test
  void testEmptyIsIdentity() {
    RecordIndexLookupStats stats = RecordIndexLookupStats.of(shard(7, 1L, 1L));

    assertTrue(RecordIndexLookupStats.empty().isEmpty());
    assertEquals(stats.getShardStats(), stats.merge(RecordIndexLookupStats.empty()).getShardStats());
    assertEquals(stats.getShardStats(), RecordIndexLookupStats.empty().merge(stats).getShardStats());
    assertEquals(0L, RecordIndexLookupStats.empty().getShardsRead());
  }
}
