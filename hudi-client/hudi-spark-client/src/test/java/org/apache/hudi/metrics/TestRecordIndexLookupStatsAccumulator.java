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

package org.apache.hudi.metrics;

import org.apache.hudi.metadata.RecordIndexLookupStats;
import org.apache.hudi.metadata.RecordIndexLookupStatsCollector;
import org.apache.hudi.metadata.RecordIndexShardLookupStats;

import org.apache.spark.util.AccumulatorV2;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the accumulator that carries record index lookup stats from executors to the driver.
 */
class TestRecordIndexLookupStatsAccumulator {

  private static RecordIndexShardLookupStats shard(int index, long keys, long hits) {
    return new RecordIndexShardLookupStats(index, "fg-" + index, keys, hits, 2L, 700L, 5L);
  }

  @Test
  void testStartsEmpty() {
    RecordIndexLookupStatsAccumulator accumulator = new RecordIndexLookupStatsAccumulator();
    assertTrue(accumulator.isZero());
    assertTrue(accumulator.value().isEmpty());
  }

  @Test
  void testCollectAccumulatesDistinctShards() {
    RecordIndexLookupStatsAccumulator accumulator = new RecordIndexLookupStatsAccumulator();
    accumulator.collect(shard(0, 10L, 4L));
    accumulator.collect(shard(1, 20L, 15L));

    assertFalse(accumulator.isZero());
    assertEquals(2L, accumulator.value().getShardsRead());
    assertEquals(30L, accumulator.value().getKeysSubmitted());
    assertEquals(19L, accumulator.value().getKeysHit());
    assertEquals(4L, accumulator.value().getLogFilesRead());
    assertEquals(1400L, accumulator.value().getBytesInShardsRead());
  }

  @Test
  void testRepeatedCollectOfSameShardIsIdempotent() {
    RecordIndexLookupStatsAccumulator accumulator = new RecordIndexLookupStatsAccumulator();
    accumulator.collect(shard(3, 100L, 60L));
    accumulator.collect(shard(3, 100L, 60L));
    accumulator.collect(shard(3, 100L, 60L));

    assertEquals(1L, accumulator.value().getShardsRead());
    assertEquals(100L, accumulator.value().getKeysSubmitted(), "a retried task must not double count");
    assertEquals(700L, accumulator.value().getBytesInShardsRead(), "nor inflate the footprint");
  }

  @Test
  void testMergeCombinesTwoExecutorCopies() {
    RecordIndexLookupStatsAccumulator left = new RecordIndexLookupStatsAccumulator();
    left.collect(shard(0, 10L, 4L));
    RecordIndexLookupStatsAccumulator right = new RecordIndexLookupStatsAccumulator();
    right.collect(shard(1, 20L, 15L));

    left.merge(right);

    assertEquals(2L, left.value().getShardsRead());
    assertEquals(30L, left.value().getKeysSubmitted());
  }

  @Test
  void testMergeOfOverlappingCopiesDoesNotDoubleCount() {
    // Two executor copies that both saw shard 0, as happens under speculation.
    RecordIndexLookupStatsAccumulator left = new RecordIndexLookupStatsAccumulator();
    left.collect(shard(0, 10L, 4L));
    RecordIndexLookupStatsAccumulator right = new RecordIndexLookupStatsAccumulator();
    right.collect(shard(0, 10L, 4L));

    left.merge(right);

    assertEquals(1L, left.value().getShardsRead());
    assertEquals(10L, left.value().getKeysSubmitted());
  }

  @Test
  void testCopyIsIndependentOfOriginal() {
    RecordIndexLookupStatsAccumulator original = new RecordIndexLookupStatsAccumulator();
    original.collect(shard(0, 10L, 4L));

    AccumulatorV2<RecordIndexShardLookupStats, RecordIndexLookupStats> copy = original.copy();
    original.collect(shard(1, 20L, 15L));

    assertNotSame(original, copy);
    assertEquals(1L, copy.value().getShardsRead(), "copy must not observe later updates");
    assertEquals(2L, original.value().getShardsRead());
  }

  @Test
  void testResetClearsValue() {
    RecordIndexLookupStatsAccumulator accumulator = new RecordIndexLookupStatsAccumulator();
    accumulator.collect(shard(0, 10L, 4L));

    accumulator.reset();

    assertTrue(accumulator.isZero());
  }

  @Test
  void testDrainReturnsValueAndResets() {
    RecordIndexLookupStatsAccumulator accumulator = new RecordIndexLookupStatsAccumulator();
    accumulator.collect(shard(0, 10L, 4L));

    RecordIndexLookupStats drained = accumulator.drain();

    assertEquals(1L, drained.getShardsRead());
    assertTrue(accumulator.isZero(), "draining must reset so the next commit starts clean");
    assertTrue(accumulator.value().isEmpty());
    assertEquals(1L, drained.getShardsRead(), "the drained snapshot is unaffected by the reset");
  }

  @Test
  void testIsUsableAsACollector() {
    // The seam in hudi-common accepts a collector; the accumulator is passed directly as one.
    RecordIndexLookupStatsAccumulator accumulator = new RecordIndexLookupStatsAccumulator();
    RecordIndexLookupStatsCollector collector = accumulator;

    collector.collect(shard(4, 50L, 22L));

    assertEquals(1L, accumulator.value().getShardsRead());
    assertEquals(22L, accumulator.value().getKeysHit());
  }
}
