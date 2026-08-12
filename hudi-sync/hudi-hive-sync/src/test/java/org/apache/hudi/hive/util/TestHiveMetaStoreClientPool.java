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

package org.apache.hudi.hive.util;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for {@link HiveMetaStoreClientPool}: borrow/return bounding, batch fan-out,
 * abort-on-first-error, and close semantics. Uses mock clients so no metastore is needed.
 */
class TestHiveMetaStoreClientPool {

  private static List<IMetaStoreClient> mockClients(int n) {
    return IntStream.range(0, n)
        .mapToObj(i -> mock(IMetaStoreClient.class))
        .collect(Collectors.toList());
  }

  @Test
  void runBorrowsAndReturnsTheSameClient() throws Exception {
    List<IMetaStoreClient> clients = mockClients(1);
    try (HiveMetaStoreClientPool pool = new HiveMetaStoreClientPool(clients, 1)) {
      // Given a single-client pool, two sequential borrows must both succeed --
      // which can only happen if the first borrow returned its client.
      IMetaStoreClient first = pool.run(c -> c);
      IMetaStoreClient second = pool.run(c -> c);

      assertEquals(clients.get(0), first);
      assertEquals(first, second, "Client must be returned to the pool after each run");
    }
  }

  @Test
  void runReturnsClientEvenWhenActionThrows() throws Exception {
    List<IMetaStoreClient> clients = mockClients(1);
    try (HiveMetaStoreClientPool pool = new HiveMetaStoreClientPool(clients, 1)) {
      assertThrows(IllegalStateException.class, () -> pool.run(c -> {
        throw new IllegalStateException("boom");
      }));

      // If the failed borrow had leaked the client, this would block forever.
      IMetaStoreClient reused = pool.run(c -> c);
      assertEquals(clients.get(0), reused, "A failed action must still return its client");
    }
  }

  @Test
  void dispatchAllRunsEveryBatch() throws Exception {
    List<IMetaStoreClient> clients = mockClients(3);
    List<String> batches = Arrays.asList("b0", "b1", "b2", "b3", "b4");
    List<String> applied = Collections.synchronizedList(new ArrayList<>());
    try (HiveMetaStoreClientPool pool = new HiveMetaStoreClientPool(clients, 3)) {
      pool.awaitAll(pool.dispatchAll(batches, (client, batch) -> applied.add(batch)), "test");
    }

    assertEquals(5, applied.size());
    assertTrue(applied.containsAll(batches), "Every batch must be applied exactly once");
  }

  @Test
  void concurrentBatchesBoundedByPoolSize() throws Exception {
    int poolSize = 2;
    List<IMetaStoreClient> clients = mockClients(poolSize);
    AtomicInteger inFlight = new AtomicInteger();
    AtomicInteger maxInFlight = new AtomicInteger();
    try (HiveMetaStoreClientPool pool = new HiveMetaStoreClientPool(clients, poolSize)) {
      pool.awaitAll(pool.dispatchAll(Arrays.asList("a", "b", "c", "d", "e"), (client, batch) -> {
        int now = inFlight.incrementAndGet();
        maxInFlight.accumulateAndGet(now, Math::max);
        Thread.sleep(20);
        inFlight.decrementAndGet();
      }), "test");
    }

    assertTrue(maxInFlight.get() <= poolSize,
        "In-flight Thrift calls must never exceed the client count, saw " + maxInFlight.get());
  }

  @Test
  void eachConcurrentBatchGetsADistinctClient() throws Exception {
    int poolSize = 3;
    List<IMetaStoreClient> clients = mockClients(poolSize);
    Set<IMetaStoreClient> seenConcurrently = ConcurrentHashMap.newKeySet();
    CountDownLatch allBorrowed = new CountDownLatch(poolSize);
    try (HiveMetaStoreClientPool pool = new HiveMetaStoreClientPool(clients, poolSize)) {
      pool.awaitAll(pool.dispatchAll(Arrays.asList("a", "b", "c"), (client, batch) -> {
        seenConcurrently.add(client);
        // Hold every client at once so none can be recycled to another batch.
        allBorrowed.countDown();
        assertTrue(allBorrowed.await(5, TimeUnit.SECONDS));
      }), "test");
    }

    assertEquals(poolSize, seenConcurrently.size(),
        "Concurrent batches must not share a Thrift client");
  }

  @Test
  void awaitAllThrowsFirstError() {
    List<IMetaStoreClient> clients = mockClients(2);
    HiveMetaStoreClientPool pool = new HiveMetaStoreClientPool(clients, 2);
    try {
      Exception ex = assertThrows(Exception.class, () ->
          pool.awaitAll(pool.dispatchAll(Arrays.asList("ok", "boom"), (client, batch) -> {
            if (batch.equals("boom")) {
              throw new IllegalStateException("drop failed");
            }
          }), "test"));

      assertEquals("drop failed", ex.getMessage(),
          "The original cause must surface unwrapped, not as an ExecutionException");
    } finally {
      pool.close();
    }
  }

  /**
   * Regression for the in-order-await bug: waiting on futures in submission order lets
   * the executor keep starting queued batches after a sibling has already failed.
   *
   * <p>Given a single-client pool so batches run strictly in order, when the first batch
   * fails, then no later batch may reach the metastore -- the task-side abort flag has to
   * stop them, since {@code Future.cancel} from the awaiting thread is inherently late.
   */
  @Test
  void abortStopsQueuedBatchesAfterFirstFailure() {
    List<IMetaStoreClient> clients = mockClients(1);
    List<String> applied = Collections.synchronizedList(new ArrayList<>());
    HiveMetaStoreClientPool pool = new HiveMetaStoreClientPool(clients, 1);
    try {
      assertThrows(Exception.class, () ->
          pool.awaitAll(pool.dispatchAll(Arrays.asList("FAIL", "AFTER_A", "AFTER_B"),
              (client, batch) -> {
                applied.add(batch);
                if (batch.equals("FAIL")) {
                  throw new IllegalStateException("drop failed");
                }
              }), "test"));

      assertEquals(Collections.singletonList("FAIL"), applied,
          "Batches queued behind the failure must never reach the metastore");
    } finally {
      pool.close();
    }
  }

  /**
   * The same abort guarantee, but with the failure landing on a <i>later</i> future than
   * a still-running one. This is the interleaving Future-order waiting cannot handle: the
   * awaiting thread is parked on SLOW's future while the failing batch races ahead.
   */
  @Test
  void abortStopsQueuedBatchesWhenEarlierBatchIsSlow() throws Exception {
    List<IMetaStoreClient> clients = mockClients(2);
    List<String> applied = Collections.synchronizedList(new ArrayList<>());
    CountDownLatch failed = new CountDownLatch(1);
    CountDownLatch releaseSlow = new CountDownLatch(1);
    HiveMetaStoreClientPool pool = new HiveMetaStoreClientPool(clients, 2);
    try {
      // SLOW occupies one client and blocks until FAIL has thrown, pinning the
      // interleaving where awaitAll is still parked on future 0.
      List<String> batches = Arrays.asList("SLOW", "FAIL", "AFTER_FAIL", "AFTER_FAIL_2");
      ParallelDispatch dispatch = pool.dispatchAll(batches, (client, batch) -> {
        applied.add(batch);
        if (batch.equals("FAIL")) {
          failed.countDown();
          throw new IllegalStateException("drop failed");
        }
        if (batch.equals("SLOW")) {
          releaseSlow.await(5, TimeUnit.SECONDS);
        }
      });
      assertTrue(failed.await(5, TimeUnit.SECONDS), "FAIL must have run");
      releaseSlow.countDown();

      assertThrows(Exception.class, () -> pool.awaitAll(dispatch, "test"));

      assertFalse(applied.contains("AFTER_FAIL_2"),
          "Batches queued behind a failure must not be applied while an earlier "
              + "batch on another client is still running");
    } finally {
      pool.close();
    }
  }

  @Test
  void firstErrorIsTheFailureThatAbortedTheBatch() throws Exception {
    List<IMetaStoreClient> clients = mockClients(2);
    AtomicReference<ParallelDispatch> dispatchRef = new AtomicReference<>();
    HiveMetaStoreClientPool pool = new HiveMetaStoreClientPool(clients, 2);
    try {
      // Given a slow batch submitted first that fails only after a later batch has
      // already failed and tripped the abort. Selecting the error by submission order
      // would report "slow-later"; the batch was actually stopped by "fast-first".
      //
      // The slow batch waits on the abort flag itself rather than on a latch counted
      // down before the throw: only the flag proves fast-first has already reached
      // abort(), so this pins the interleaving instead of merely making it likely.
      List<String> batches = Arrays.asList("SLOW_LATER", "FAST_FIRST");
      ParallelDispatch dispatch = pool.dispatchAll(batches, (client, batch) -> {
        if (batch.equals("FAST_FIRST")) {
          throw new IllegalStateException("fast-first");
        }
        ParallelDispatch inFlight;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while ((inFlight = dispatchRef.get()) == null || !inFlight.aborted()) {
          if (System.nanoTime() > deadline) {
            throw new IllegalStateException("timed-out-waiting-for-abort");
          }
          Thread.sleep(1);
        }
        throw new IllegalStateException("slow-later");
      });
      dispatchRef.set(dispatch);

      Exception thrown = assertThrows(Exception.class, () -> pool.awaitAll(dispatch, "test"));

      assertEquals("fast-first", thrown.getMessage(),
          "The reported root cause must be the failure that aborted the batch, "
              + "not whichever failure was submitted earliest");
    } finally {
      pool.close();
    }
  }

  @Test
  void anErrorThatAbortsTheBatchIsNotAlsoReportedAsSuppressed() throws Exception {
    List<IMetaStoreClient> clients = mockClients(2);
    HiveMetaStoreClientPool pool = new HiveMetaStoreClientPool(clients, 2);
    try {
      // Given the aborting task throws an Error rather than an Exception. guard() catches
      // Throwable, so the Error is what trips the abort, but it cannot be reported as-is:
      // it gets wrapped in a RuntimeException. An identity check against that wrapper
      // would never match the raw cause coming back off the future, so the one real
      // failure would be reported twice -- once as the cause, once as suppressed.
      ParallelDispatch dispatch = pool.dispatchAll(
          Collections.singletonList("BOOM"),
          (client, batch) -> {
            throw new StackOverflowError("boom");
          });

      // Asserted on the Outcome rather than on awaitAll(): awaitAll only *logs* the
      // suppressed list, so a duplicate there is invisible to the thrown exception.
      ParallelDispatch.Outcome outcome = dispatch.awaitOutcome();

      assertTrue(outcome.suppressed().isEmpty(),
          "The failure that aborted the batch must not also appear in its own suppressed list");
      assertEquals("boom", outcome.firstError().getCause().getMessage(),
          "The wrapped Error must still be reported as the root cause");
    } finally {
      pool.close();
    }
  }

  @Test
  void interruptedAwaitIsReportedAsAFailureNotASuccess() throws Exception {
    List<IMetaStoreClient> clients = mockClients(2);
    HiveMetaStoreClientPool pool = new HiveMetaStoreClientPool(clients, 2);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch running = new CountDownLatch(1);
    try {
      // Given a batch that is still in flight when the awaiting thread is interrupted.
      // The interrupt must surface as a dispatch failure: reporting success here would let
      // HiveSyncTool advance the last-synced commit marker over work that never ran.
      ParallelDispatch dispatch = pool.dispatchAll(
          Collections.singletonList("BLOCKED"),
          (client, batch) -> {
            running.countDown();
            release.await(10, TimeUnit.SECONDS);
            return;
          });
      assertTrue(running.await(10, TimeUnit.SECONDS), "the batch must have started");

      AtomicReference<Throwable> thrown = new AtomicReference<>();
      AtomicBoolean returnedNormally = new AtomicBoolean(false);
      Thread awaiter = new Thread(() -> {
        try {
          pool.awaitAll(dispatch, "test");
          returnedNormally.set(true);
        } catch (Throwable t) {
          thrown.set(t);
        }
      }, "awaiter");
      awaiter.start();

      // When that thread is interrupted while parked in awaitAll.
      Thread.sleep(200);
      awaiter.interrupt();
      awaiter.join(10_000);
      release.countDown();

      // Then it reports a failure rather than a clean batch.
      assertFalse(returnedNormally.get(),
          "awaitAll must not report success when the wait was interrupted; the batch's "
              + "work was cancelled or is still in flight");
      assertNotNull(thrown.get(), "the interruption must surface as a thrown failure");
    } finally {
      release.countDown();
      pool.close();
    }
  }

  @Test
  void closeIsIdempotentAndClosesEveryClient() throws Exception {
    List<IMetaStoreClient> clients = mockClients(2);
    HiveMetaStoreClientPool pool = new HiveMetaStoreClientPool(clients, 2);

    pool.close();
    pool.close();

    for (IMetaStoreClient client : clients) {
      verify(client).close();
    }
    assertThrows(IllegalStateException.class, () -> pool.run(c -> c),
        "Borrowing from a closed pool must fail fast");
  }

  @Test
  void dispatchOnClosedPoolFailsFast() {
    HiveMetaStoreClientPool pool = new HiveMetaStoreClientPool(mockClients(1), 1);
    pool.close();

    assertThrows(IllegalStateException.class,
        () -> pool.dispatchAll(Collections.singletonList("a"), (client, batch) -> { }));
  }

  @Test
  void rejectsSizeMismatchAndNonPositiveSize() {
    assertThrows(IllegalArgumentException.class, () -> new HiveMetaStoreClientPool(mockClients(1), 2));
    assertThrows(IllegalArgumentException.class, () -> new HiveMetaStoreClientPool(mockClients(0), 0));
  }
}
