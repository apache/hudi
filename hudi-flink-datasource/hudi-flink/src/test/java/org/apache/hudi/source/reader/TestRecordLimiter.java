/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.source.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link RecordLimiter}.
 */
public class TestRecordLimiter {

  @Test
  public void testIsLimitReachedFalseWhenUnlimited() {
    RecordLimiter limiter = new RecordLimiter(RecordLimiter.NO_LIMIT);
    assertFalse(limiter.isLimitReached());
  }

  @Test
  public void testIsLimitReachedTrueWhenLimitIsZero() {
    // totalReadCount(0) >= limit(0) immediately
    RecordLimiter limiter = new RecordLimiter(0L);
    assertTrue(limiter.isLimitReached());
  }

  @Test
  public void testIsLimitReachedFalseBeforeReadingAnyRecords() {
    RecordLimiter limiter = new RecordLimiter(5L);
    assertFalse(limiter.isLimitReached());
  }

  @Test
  public void testIsLimitReachedTrueAfterReadingUpToLimit() {
    RecordLimiter limiter = new RecordLimiter(3L);
    RecordsWithSplitIds<String> wrapped = limiter.wrap(makeRecords("a", "b", "c", "d"));
    wrapped.nextSplit();
    drainAll(wrapped);  // reads 3 (limit) then stops
    assertTrue(limiter.isLimitReached());
  }

  @Test
  public void testIsLimitReachedFalseWhenBelowLimit() {
    RecordLimiter limiter = new RecordLimiter(10L);
    RecordsWithSplitIds<String> wrapped = limiter.wrap(makeRecords("a", "b"));
    wrapped.nextSplit();
    drainAll(wrapped);  // only 2 records available, limit is 10
    assertFalse(limiter.isLimitReached());
  }

  // -------------------------------------------------------------------------
  //  wrap() — delegation to inner RecordsWithSplitIds
  // -------------------------------------------------------------------------

  @Test
  public void testWrapDelegatesNextSplit() {
    RecordLimiter limiter = new RecordLimiter(10L);
    RecordsWithSplitIds<String> inner = makeRecords("split-1", "x");
    RecordsWithSplitIds<String> wrapped = limiter.wrap(inner);
    assertEquals("split-1", wrapped.nextSplit());
  }

  @Test
  public void testWrapDelegatesFinishedSplits() {
    RecordLimiter limiter = new RecordLimiter(10L);
    Set<String> finishedSplits = Collections.singleton("done-split");
    RecordsWithSplitIds<String> inner = makeRecordsWithFinished("split-1", Collections.singletonList("x"), finishedSplits);
    RecordsWithSplitIds<String> wrapped = limiter.wrap(inner);
    assertEquals(finishedSplits, wrapped.finishedSplits());
  }

  @Test
  public void testWrapDelegatesRecycle() {
    RecordLimiter limiter = new RecordLimiter(10L);
    AtomicInteger recycleCount = new AtomicInteger(0);
    RecordsWithSplitIds<String> inner = new StubRecords("split-1", Collections.singletonList("x")) {
      @Override
      public void recycle() {
        recycleCount.incrementAndGet();
      }
    };
    RecordsWithSplitIds<String> wrapped = limiter.wrap(inner);
    wrapped.recycle();
    assertEquals(1, recycleCount.get());
  }

  // -------------------------------------------------------------------------
  //  wrap() — record counting and cutoff
  // -------------------------------------------------------------------------

  @Test
  public void testWrapReturnsAllRecordsWhenBelowLimit() {
    RecordLimiter limiter = new RecordLimiter(5L);
    RecordsWithSplitIds<String> wrapped = limiter.wrap(makeRecords("split-1", "a", "b", "c"));
    wrapped.nextSplit();
    assertEquals(3, drainAll(wrapped));
  }

  @Test
  public void testWrapCutsOffAtLimit() {
    RecordLimiter limiter = new RecordLimiter(2L);
    RecordsWithSplitIds<String> wrapped = limiter.wrap(makeRecords("split-1", "a", "b", "c", "d", "e"));
    wrapped.nextSplit();
    assertEquals(2, drainAll(wrapped));
  }

  @Test
  public void testWrapReturnsNullImmediatelyWhenLimitIsZero() {
    RecordLimiter limiter = new RecordLimiter(0L);
    RecordsWithSplitIds<String> wrapped = limiter.wrap(makeRecords("split-1", "a", "b"));
    wrapped.nextSplit();
    assertNull(wrapped.nextRecordFromSplit());
  }

  @Test
  public void testWrapDoesNotIncrementCountForNullInnerRecord() {
    // Inner iterator exhausted before limit; null returns from inner should not count.
    RecordLimiter limiter = new RecordLimiter(10L);
    RecordsWithSplitIds<String> wrapped = limiter.wrap(makeRecords("split-1", "a", "b"));
    wrapped.nextSplit();
    assertEquals(2, drainAll(wrapped));
    // Limit should not be reached since only 2 records were actually read.
    assertFalse(limiter.isLimitReached());
  }

  @Test
  public void testWrapCountAccumulatesAcrossMultipleWrappedBatches() {
    // The same limiter wraps two batches; the count must be shared.
    RecordLimiter limiter = new RecordLimiter(4L);

    RecordsWithSplitIds<String> batch1 = limiter.wrap(makeRecords("split-1", "a", "b", "c"));
    batch1.nextSplit();
    assertEquals(3, drainAll(batch1));  // reads 3; totalReadCount = 3

    assertFalse(limiter.isLimitReached());

    RecordsWithSplitIds<String> batch2 = limiter.wrap(makeRecords("split-2", "d", "e", "f"));
    batch2.nextSplit();
    assertEquals(1, drainAll(batch2));  // only 1 more allowed; totalReadCount = 4

    assertTrue(limiter.isLimitReached());
  }

  @Test
  public void testWrapExactLimitReturnsAllRecords() {
    // limit == number of records → all records returned, none cut off
    RecordLimiter limiter = new RecordLimiter(3L);
    RecordsWithSplitIds<String> wrapped = limiter.wrap(makeRecords("split-1", "a", "b", "c"));
    wrapped.nextSplit();
    assertEquals(3, drainAll(wrapped));
    assertTrue(limiter.isLimitReached());
  }

  // -------------------------------------------------------------------------
  //  Multi-threaded safety
  // -------------------------------------------------------------------------

  @Test
  public void testConcurrentReadsTotalCountIsAccurate() throws InterruptedException {
    // Each thread drains its own wrapped batch; with AtomicLong no updates should be lost.
    int threadCount = 8;
    int recordsPerBatch = 100;
    long limit = (long) threadCount * recordsPerBatch; // limit exactly matches total supply

    RecordLimiter limiter = new RecordLimiter(limit);
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threadCount);
    AtomicLong totalRead = new AtomicLong(0);

    for (int i = 0; i < threadCount; i++) {
      List<String> items = Collections.nCopies(recordsPerBatch, "x");
      RecordsWithSplitIds<String> wrapped = limiter.wrap(makeThreadSafeRecords("split-" + i, items));
      wrapped.nextSplit();
      Thread t = new Thread(() -> {
        try {
          start.await();
          totalRead.addAndGet(drainAll(wrapped));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          done.countDown();
        }
      });
      t.start();
    }

    start.countDown();
    done.await();

    assertEquals(limit, totalRead.get(), "All records should be counted when total equals limit");
    assertTrue(limiter.isLimitReached());
  }

  @Test
  public void testConcurrentReadsDoNotExceedLimitByMoreThanThreadCount() throws InterruptedException {
    // With many records available, threads race to read up to the limit.
    // The AtomicLong prevents lost updates; the check-then-increment allows at most
    // one overshoot per concurrent thread before each thread sees the limit.
    int threadCount = 8;
    int recordsPerBatch = 1000;
    long limit = 100L;

    RecordLimiter limiter = new RecordLimiter(limit);
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threadCount);
    AtomicLong totalRead = new AtomicLong(0);

    for (int i = 0; i < threadCount; i++) {
      List<String> items = Collections.nCopies(recordsPerBatch, "x");
      RecordsWithSplitIds<String> wrapped = limiter.wrap(makeThreadSafeRecords("split-" + i, items));
      wrapped.nextSplit();
      Thread t = new Thread(() -> {
        try {
          start.await();
          totalRead.addAndGet(drainAll(wrapped));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          done.countDown();
        }
      });
      t.start();
    }

    start.countDown();
    done.await();

    assertTrue(totalRead.get() >= limit, "At least limit records must be read");
    assertTrue(totalRead.get() <= limit + threadCount,
        "Overshoot is bounded by thread count due to check-then-increment race");
    assertTrue(limiter.isLimitReached());
  }

  @Test
  public void testIsLimitReachedVisibleAcrossThreads() throws InterruptedException {
    // A writer thread exhausts the limit; a reader thread started after must see it.
    long limit = 50L;
    RecordLimiter limiter = new RecordLimiter(limit);
    List<String> items = Collections.nCopies((int) limit, "x");

    RecordsWithSplitIds<String> wrapped = limiter.wrap(makeThreadSafeRecords("split-1", items));
    wrapped.nextSplit();

    CountDownLatch writerDone = new CountDownLatch(1);
    Thread writer = new Thread(() -> {
      drainAll(wrapped);
      writerDone.countDown();
    });
    writer.start();
    writerDone.await();

    AtomicBoolean readerSawLimit = new AtomicBoolean(false);
    Thread reader = new Thread(() -> readerSawLimit.set(limiter.isLimitReached()));
    reader.start();
    reader.join();

    assertTrue(readerSawLimit.get(), "isLimitReached() must be visible across threads after writer finishes");
  }

  // -------------------------------------------------------------------------
  //  Helpers
  // -------------------------------------------------------------------------

  /** Creates a StubRecords with the given split id and records. */
  private RecordsWithSplitIds<String> makeRecords(String splitId, String... items) {
    return new StubRecords(splitId, Arrays.asList(items));
  }

  /** Creates a StubRecords with custom finishedSplits. */
  private RecordsWithSplitIds<String> makeRecordsWithFinished(
      String splitId, List<String> items, Set<String> finished) {
    return new StubRecords(splitId, items) {
      @Override
      public Set<String> finishedSplits() {
        return finished;
      }
    };
  }

  /**
   * Creates a {@link RecordsWithSplitIds} whose {@code nextRecordFromSplit()} is thread-safe,
   * suitable for concurrent access by multiple threads.
   */
  private RecordsWithSplitIds<String> makeThreadSafeRecords(String splitId, List<String> items) {
    AtomicInteger idx = new AtomicInteger(0);
    return new StubRecords(splitId, Collections.emptyList()) {
      @Override
      public String nextRecordFromSplit() {
        int i = idx.getAndIncrement();
        return i < items.size() ? items.get(i) : null;
      }
    };
  }

  /** Drains all records from the current split, returning the count. */
  private int drainAll(RecordsWithSplitIds<String> records) {
    int count = 0;
    while (records.nextRecordFromSplit() != null) {
      count++;
    }
    return count;
  }

  /**
   * Minimal stub implementation of {@link RecordsWithSplitIds} backed by a list.
   */
  private static class StubRecords implements RecordsWithSplitIds<String> {
    private final String splitId;
    private final Iterator<String> iterator;
    private boolean splitConsumed = false;

    StubRecords(String splitId, List<String> items) {
      this.splitId = splitId;
      this.iterator = items.iterator();
    }

    @Override
    public String nextSplit() {
      if (!splitConsumed) {
        splitConsumed = true;
        return splitId;
      }
      return null;
    }

    @Override
    public String nextRecordFromSplit() {
      return iterator.hasNext() ? iterator.next() : null;
    }

    @Override
    public Set<String> finishedSplits() {
      return Collections.emptySet();
    }

    @Override
    public void recycle() {
      // No-op
    }
  }
}
