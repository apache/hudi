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

import org.apache.hudi.common.util.collection.ClosableIterator;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the shard-lookup counting decorator that reports stats when a lookup result is exhausted
 * or closed.
 */
class TestRecordIndexLookupStatsCollection {

  /** Records what the decorator emits, and whether the delegate was closed. */
  private static final class CapturingCollector implements RecordIndexLookupStatsCollector {
    private final List<RecordIndexShardLookupStats> collected = new ArrayList<>();

    @Override
    public void collect(RecordIndexShardLookupStats stats) {
      collected.add(stats);
    }
  }

  private static final class TrackingIterator implements ClosableIterator<String> {
    private final java.util.Iterator<String> delegate;
    private int closeCount;

    private TrackingIterator(List<String> items) {
      this.delegate = items.iterator();
    }

    @Override
    public boolean hasNext() {
      return delegate.hasNext();
    }

    @Override
    public String next() {
      return delegate.next();
    }

    @Override
    public void close() {
      closeCount++;
    }
  }

  private static RecordIndexLookupStatsCollectingIterator<String> decorate(
      TrackingIterator delegate, RecordIndexLookupStatsCollector collector, long keysSubmitted) {
    return new RecordIndexLookupStatsCollectingIterator<>(delegate, collector, 5, "fg-5",
        keysSubmitted, 3L, 2048L, System.currentTimeMillis());
  }

  @Test
  void testReportsOnceWhenFullyConsumed() {
    CapturingCollector collector = new CapturingCollector();
    TrackingIterator delegate = new TrackingIterator(Arrays.asList("a", "b", "c"));

    List<String> drained = new ArrayList<>();
    RecordIndexLookupStatsCollectingIterator<String> itr = decorate(delegate, collector, 10L);
    while (itr.hasNext()) {
      drained.add(itr.next());
    }

    assertEquals(Arrays.asList("a", "b", "c"), drained, "decoration must not alter the results");
    assertEquals(1, collector.collected.size());
    RecordIndexShardLookupStats stats = collector.collected.get(0);
    assertEquals(5, stats.getShardIndex());
    assertEquals("fg-5", stats.getFileGroupId());
    assertEquals(10L, stats.getKeysSubmitted());
    assertEquals(3L, stats.getKeysHit(), "hits are the records the lookup actually yielded");
    assertEquals(3L, stats.getLogFilesRead());
    assertEquals(2048L, stats.getBytesInShard());
  }

  @Test
  void testReportsZeroHitsWhenNothingMatched() {
    CapturingCollector collector = new CapturingCollector();
    RecordIndexLookupStatsCollectingIterator<String> itr =
        decorate(new TrackingIterator(new ArrayList<>()), collector, 7L);

    assertFalse(itr.hasNext());

    assertEquals(1, collector.collected.size());
    assertEquals(7L, collector.collected.get(0).getKeysSubmitted());
    assertEquals(0L, collector.collected.get(0).getKeysHit(), "all misses is a valid, reportable result");
  }

  @Test
  void testReportsOnlyOnceAcrossExhaustionAndClose() {
    CapturingCollector collector = new CapturingCollector();
    TrackingIterator delegate = new TrackingIterator(Arrays.asList("a"));

    RecordIndexLookupStatsCollectingIterator<String> itr = decorate(delegate, collector, 4L);
    while (itr.hasNext()) {
      itr.next();
    }
    itr.close();
    itr.close();

    assertEquals(1, collector.collected.size(), "exhaustion then double close must still report once");
    assertEquals(1L, collector.collected.get(0).getKeysHit());
    assertEquals(2, delegate.closeCount, "close must always reach the delegate");
  }

  @Test
  void testAbandonedIteratorStillReportsOnClose() {
    CapturingCollector collector = new CapturingCollector();
    TrackingIterator delegate = new TrackingIterator(Arrays.asList("a", "b", "c"));

    RecordIndexLookupStatsCollectingIterator<String> itr = decorate(delegate, collector, 9L);
    itr.hasNext();
    itr.next();
    itr.close();

    assertEquals(1, collector.collected.size());
    assertEquals(1L, collector.collected.get(0).getKeysHit(),
        "a caller that stops early under-reports hits rather than failing");
  }

  @Test
  void testHitsNeverExceedKeysSubmitted() {
    // The value type rejects keysHit > keysSubmitted, so the decorator must clamp rather than throw
    // if a lookup ever yields more rows than keys (for example a prefix-style match).
    CapturingCollector collector = new CapturingCollector();
    RecordIndexLookupStatsCollectingIterator<String> itr =
        decorate(new TrackingIterator(Arrays.asList("a", "b", "c")), collector, 1L);
    while (itr.hasNext()) {
      itr.next();
    }

    assertEquals(1, collector.collected.size());
    assertEquals(1L, collector.collected.get(0).getKeysHit());
  }

  @Test
  void testCollectorFailureNeverPropagates() {
    // Instrumentation must not be able to fail a write.
    RecordIndexLookupStatsCollector exploding = stats -> {
      throw new IllegalStateException("metrics backend is down");
    };
    TrackingIterator delegate = new TrackingIterator(Arrays.asList("a"));
    RecordIndexLookupStatsCollectingIterator<String> itr = decorate(delegate, exploding, 1L);

    while (itr.hasNext()) {
      itr.next();
    }
    itr.close();

    assertEquals(1, delegate.closeCount, "the delegate is still closed after a collector failure");
  }

  @Test
  void testNoopCollectorIsSerializableAndInert() {
    // The collector is captured in engine closures; a non-serializable one fails at task launch.
    assertTrue(Serializable.class.isAssignableFrom(RecordIndexLookupStatsCollector.class));
    RecordIndexLookupStatsCollector.NOOP.collect(
        new RecordIndexShardLookupStats(0, "fg-0", 10L, 5L, 1L, 500L, 1L));
  }
}
