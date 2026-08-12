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
  void testMergeIsCommutative() {
    RecordIndexLookupStats a = RecordIndexLookupStats.of(shard(0, 10L, 4L));
    RecordIndexLookupStats b = RecordIndexLookupStats.of(shard(1, 20L, 11L));

    assertEquals(a.merge(b).getShardStats(), b.merge(a).getShardStats());
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
