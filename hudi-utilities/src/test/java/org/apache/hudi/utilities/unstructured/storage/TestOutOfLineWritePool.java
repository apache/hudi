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

package org.apache.hudi.utilities.unstructured.storage;

import org.apache.hudi.common.engine.EngineProperty;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The out-of-line write pool's thread rules: a file is named with the write token of the thread that asks for its path (the
 * task's), never the pool thread's; close waits, through interrupts, for a running write that ignores them; every wait (close,
 * awaitAll, and submit waiting for room) gives up on a stuck write after the bound; and a closed pool refuses writes.
 */
class TestOutOfLineWritePool {

  private static final String INSTANT = "20261110120000000";
  /** The bound of the slow-drain tests: each of their writes takes {@link #SLOW_WRITE_MILLIS}, well within it. */
  private static final long SLOW_BOUND_MILLIS = 1000;
  private static final long SLOW_WRITE_MILLIS = 600;

  @TempDir
  Path temp;

  /** A task context whose token is 7-8-9 on the thread that built it (the task's) and 99-99-99 on any other. */
  private static final class ThreadTaskContext extends TaskContextSupplier {
    private final transient Thread task = Thread.currentThread();

    private <T> Supplier<T> onTask(T taskValue, T otherValue) {
      return () -> Thread.currentThread() == task ? taskValue : otherValue;
    }

    @Override
    public Supplier<Integer> getPartitionIdSupplier() {
      return onTask(7, 99);
    }

    @Override
    public Supplier<Integer> getStageIdSupplier() {
      return onTask(8, 99);
    }

    @Override
    public Supplier<Long> getAttemptIdSupplier() {
      return onTask(9L, 99L);
    }

    @Override
    public Option<String> getProperty(EngineProperty prop) {
      return Option.empty();
    }

    @Override
    public Supplier<Integer> getTaskAttemptNumberSupplier() {
      return onTask(0, 0);
    }

    @Override
    public Supplier<Integer> getStageAttemptNumberSupplier() {
      return onTask(0, 0);
    }
  }

  @Test
  void smallWritesHaveABoundedPendingCount() throws Exception {
    CountDownLatch started = new CountDownLatch(4);
    CountDownLatch release = new CountDownLatch(1);
    java.util.concurrent.ExecutorService caller = java.util.concurrent.Executors.newSingleThreadExecutor();
    try (OutOfLineWritePool pool = new OutOfLineWritePool()) {
      try {
        for (int i = 0; i < 4; i++) {
          pool.submit(1, () -> {
            started.countDown();
            release.await();
          });
        }
        assertTrue(started.await(10, TimeUnit.SECONDS));
        // One batch is running and one batch is queued, even for one-byte payloads.
        for (int i = 0; i < 4; i++) {
          pool.submit(1, () -> { });
        }
        java.util.concurrent.Future<?> next = caller.submit(() -> pool.submit(1, () -> { }));
        assertThrows(java.util.concurrent.TimeoutException.class, () -> next.get(200, TimeUnit.MILLISECONDS));
        release.countDown();
        next.get(10, TimeUnit.SECONDS);
        pool.awaitAll();
      } finally {
        release.countDown();
      }
    } finally {
      caller.shutdownNow();
      assertTrue(caller.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  void fileWrittenOnThePoolCarriesTheTokenOfTheTaskThatNamedIt() throws Exception {
    String base = temp.toUri().toString();
    HoodieStorage storage = new HoodieHadoopStorage(new StoragePath(base), HadoopFSUtils.getStorageConf(new Configuration()));
    OutOfLineWriter writer = new OutOfLineWriter(storage, base, INSTANT, Option.empty(), new ThreadTaskContext());
    byte[] bytes = {4, 5, 6};
    String relative = writer.newPath("p");
    try (OutOfLineWritePool pool = new OutOfLineWritePool()) {
      pool.submit(bytes.length, () -> writer.writeAt(relative, out -> out.write(bytes)));
      pool.awaitAll();
    }
    assertEquals("7-8-9", FSUtils.getWriteTokenFromBaseFile(relative.substring(relative.lastIndexOf('/') + 1)));
    assertArrayEquals(bytes, Files.readAllBytes(temp.resolve(relative)));
  }

  @Test
  void closeOnAnInterruptedThreadWaitsForARunningWriteThatIgnoresInterrupts() throws Exception {
    OutOfLineWritePool pool = new OutOfLineWritePool();
    CountDownLatch started = new CountDownLatch(1);
    AtomicBoolean finished = new AtomicBoolean();
    pool.submit(1, () -> {
      started.countDown();
      long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(300);
      while (System.nanoTime() < until) {
        try {
          // by design: simulates a write that ignores interrupts for 300 ms; not a wait
          Thread.sleep(10); // NOSONAR (by design: see above)
        } catch (InterruptedException ignored) {
          // as a store or marker call that swallows the interrupt and goes on
        }
      }
      finished.set(true);
    });
    assertTrue(started.await(10, TimeUnit.SECONDS));
    Thread.currentThread().interrupt();
    try {
      pool.close();
      assertTrue(finished.get(), "close returned before the running write ended");
      assertTrue(Thread.currentThread().isInterrupted(), "the interrupt is kept");
    } finally {
      Thread.interrupted();
    }
  }

  /**
   * A write that never ends (a stuck PUT) does not hold close forever: after the bound, close throws naming the write's thread,
   * so the task fails and the executor slot is freed; the stuck thread is a daemon.
   */
  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void closeGivesUpOnAStuckWriteAfterItsBoundNamingIt() throws Exception {
    OutOfLineWritePool pool = new OutOfLineWritePool(200);
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    pool.submit(1, ".hoodie/.blob/stuck.parquet", () -> {
      started.countDown();
      while (release.getCount() > 0) {
        try {
          release.await();
        } catch (InterruptedException ignored) {
          // as a store call that swallows the interrupt and stays stuck
        }
      }
    });
    assertTrue(started.await(10, TimeUnit.SECONDS));
    long begin = System.nanoTime();
    try {
      HoodieException stuck = assertTimeoutPreemptively(Duration.ofSeconds(2), () -> assertThrows(HoodieException.class, pool::close));
      assertTrue(stuck.getMessage().contains("out-of-line writes did not end within 200 ms"), stuck.getMessage());
      assertTrue(stuck.getMessage().contains(".hoodie/.blob/stuck.parquet on hudi-out-of-line-write-"), stuck.getMessage());
      assertTrue(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - begin) < 10, "close returned after its bound");
    } finally {
      release.countDown();
    }
  }

  /**
   * The end of input waits for the writes ({@link OutOfLineWritePool#awaitAll}) before it reaches close: a stuck write does not
   * hold that wait forever either; after the bound it throws the same failure naming the write.
   */
  @Test
  void awaitAllGivesUpOnAStuckWriteAfterItsBoundNamingIt() throws Exception {
    OutOfLineWritePool pool = new OutOfLineWritePool(200);
    CountDownLatch release = new CountDownLatch(1);
    try {
      submitStuckWrite(pool, 1, release);
      HoodieException stuck = assertTimeoutPreemptively(Duration.ofSeconds(2), () -> assertThrows(HoodieException.class, pool::awaitAll),
          "awaitAll waited past its bound for a stuck write");
      assertNamesTheStuckWrite(stuck);
    } finally {
      release.countDown();
      pool.close();
    }
  }

  /**
   * A stuck write that holds every byte of room does not block the next submit forever: after the bound, submit throws the same
   * failure naming the write.
   */
  @Test
  void submitGivesUpWaitingForRoomHeldByAStuckWriteAfterItsBound() throws Exception {
    OutOfLineWritePool pool = new OutOfLineWritePool(200);
    CountDownLatch release = new CountDownLatch(1);
    try {
      submitStuckWrite(pool, OutOfLineWritePool.MAX_PENDING_BYTES, release);
      HoodieException stuck = assertTimeoutPreemptively(Duration.ofSeconds(2),
          () -> assertThrows(HoodieException.class, () -> pool.submit(1, () -> { })), "submit waited past its bound for room");
      assertNamesTheStuckWrite(stuck);
    } finally {
      release.countDown();
      pool.close();
    }
  }

  /**
   * The bound is on a write that does not end, not on the whole drain: writes that each end within the bound but together take
   * longer than it (a slow, healthy store) are waited for by awaitAll.
   */
  @Test
  void awaitAllWaitsForASlowDrainLongerThanItsBoundWhileWritesKeepEnding() {
    OutOfLineWritePool pool = new OutOfLineWritePool(SLOW_BOUND_MILLIS);
    try {
      // three rounds of four writes: about 3 x 600 ms in all, each write well within the 1 s bound
      for (int i = 0; i < 3 * OutOfLineWritePool.THREADS; i++) {
        pool.submit(1, TestOutOfLineWritePool::slowWrite);
      }
      long begin = System.nanoTime();
      assertTimeoutPreemptively(Duration.ofSeconds(20), pool::awaitAll, "awaitAll gave up on a drain that kept making progress");
      assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin) > SLOW_BOUND_MILLIS, "the drain outlasted the bound");
    } finally {
      pool.close();
    }
  }

  /**
   * A submit waiting for room that earlier writes give back slowly, each within the bound but together after it, gets its room
   * rather than give up.
   */
  @Test
  void submitWaitsForRoomGivenBackSlowerThanItsBoundWhileWritesKeepEnding() {
    OutOfLineWritePool pool = new OutOfLineWritePool(SLOW_BOUND_MILLIS);
    try {
      // two rounds of four writes hold all the room; the next whole-bound payload needs every one of them to end
      for (int i = 0; i < 2 * OutOfLineWritePool.THREADS; i++) {
        pool.submit(OutOfLineWritePool.MAX_PENDING_BYTES / (2 * OutOfLineWritePool.THREADS), TestOutOfLineWritePool::slowWrite);
      }
      long begin = System.nanoTime();
      assertTimeoutPreemptively(Duration.ofSeconds(20), () -> pool.submit(OutOfLineWritePool.MAX_PENDING_BYTES, () -> { }),
          "submit gave up on room that kept coming back");
      assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin) > SLOW_BOUND_MILLIS, "the wait outlasted the bound");
      assertTimeoutPreemptively(Duration.ofSeconds(20), pool::awaitAll);
    } finally {
      pool.close();
    }
  }

  /** A write that takes {@link #SLOW_WRITE_MILLIS} and then ends (a slow store call that succeeds). */
  private static void slowWrite() throws InterruptedException {
    // by design: simulates a store call that takes a while and succeeds; not a wait
    Thread.sleep(SLOW_WRITE_MILLIS); // NOSONAR (by design: see above)
  }

  /** Submits a write of {@code bytes} that ignores interrupts and runs until {@code release}, and waits until it runs. */
  private static void submitStuckWrite(OutOfLineWritePool pool, long bytes, CountDownLatch release) throws InterruptedException {
    CountDownLatch started = new CountDownLatch(1);
    pool.submit(bytes, ".hoodie/.blob/stuck.parquet", () -> {
      started.countDown();
      while (release.getCount() > 0) {
        try {
          release.await();
        } catch (InterruptedException ignored) {
          // as a store call that swallows the interrupt and stays stuck
        }
      }
    });
    assertTrue(started.await(10, TimeUnit.SECONDS));
  }

  private static void assertNamesTheStuckWrite(HoodieException stuck) {
    assertTrue(stuck.getMessage().contains("out-of-line writes did not end within 200 ms"), stuck.getMessage());
    assertTrue(stuck.getMessage().contains(".hoodie/.blob/stuck.parquet on hudi-out-of-line-write-"), stuck.getMessage());
  }

  /**
   * Writes close drops before they start give back their room and count as ended, so nothing waits for them, and the
   * barrier fails: their files were never written.
   */
  @Test
  void writesDroppedByCloseReleaseTheirRoomAndEnd() throws Exception {
    OutOfLineWritePool pool = new OutOfLineWritePool();
    CountDownLatch hold = new CountDownLatch(1);
    CountDownLatch running = new CountDownLatch(4);
    for (int i = 0; i < 4; i++) {
      pool.submit(1, () -> {
        running.countDown();
        boolean released = false;
        while (!released) {
          try {
            released = hold.await(10, TimeUnit.SECONDS);
          } catch (InterruptedException ignored) {
            // a running write that ignores the interrupt close() sends
          }
        }
      });
    }
    assertTrue(running.await(10, TimeUnit.SECONDS), "the 4 threads are busy");
    for (int i = 0; i < 3; i++) {
      // queued behind the busy threads: dropped by close
      pool.submit(OutOfLineWritePool.MAX_PENDING_BYTES / 4, () -> {
        throw new IllegalStateException("a dropped write never runs");
      });
    }
    Thread closer = new Thread(pool::close);
    closer.start();
    // close has dropped the queue once it waits for the running writes
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (closer.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline) {
      closer.join(5);
    }
    assertEquals(Thread.State.TIMED_WAITING, closer.getState(), "close waits for the running writes");
    hold.countDown();
    closer.join(TimeUnit.SECONDS.toMillis(30));
    assertFalse(closer.isAlive(), "close returned");
    // the dropped writes ended (awaitAll does not wait for them) and none ran (their own failure is not the one thrown)
    HoodieException notWritten = assertThrows(HoodieException.class, pool::awaitAll);
    assertTrue(notWritten.getMessage().contains("3 out-of-line writes were dropped"), notWritten.getMessage());
  }

  @Test
  void closedPoolRefusesWrites() {
    OutOfLineWritePool pool = new OutOfLineWritePool();
    pool.close();
    HoodieException refused = assertThrows(HoodieException.class, () -> pool.submit(1, () -> { }));
    assertTrue(refused.getMessage().contains("closed"), refused.getMessage());
  }

  @Test
  void laterFailuresAreSuppressedIntoTheFirst() {
    OutOfLineWritePool pool = new OutOfLineWritePool();
    CountDownLatch go = new CountDownLatch(1);
    for (int i = 0; i < 3; i++) {
      int n = i;
      pool.submit(1, () -> {
        go.await();
        throw new IllegalStateException("write " + n);
      });
    }
    go.countDown();
    IllegalStateException first = assertThrows(IllegalStateException.class, pool::awaitAll);
    assertEquals(2, first.getSuppressed().length, "the two later failures");
    pool.close();
  }
}
