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

import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveSyncException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for {@link HiveDriverPool} that exercise bootstrap, dispatch, error
 * propagation, and close semantics without standing up a real Hive instance.
 */
class TestHiveDriverPool {

  private static HiveSyncConfig configWithEmptyHiveConf() {
    HiveSyncConfig config = mock(HiveSyncConfig.class);
    doAnswer(inv -> new HiveConf()).when(config).getHiveConf();
    doAnswer(inv -> "default").when(config).getStringOrDefault(
        org.mockito.ArgumentMatchers.any());
    return config;
  }

  @Test
  void bootstrapBuildsOneDriverPerSlot() throws Exception {
    HiveSyncConfig config = configWithEmptyHiveConf();
    AtomicInteger built = new AtomicInteger();
    HiveDriverPool.DriverFactory factory = (db) -> {
      built.incrementAndGet();
      return mock(Driver.class);
    };
    try (HiveDriverPool pool = new HiveDriverPool(config, 3, factory)) {
      assertEquals(3, pool.size());
      assertEquals(3, built.get(), "One Driver per slot should be constructed eagerly");
    }
  }

  @Test
  void bootstrapFailurePropagatesAndTearsDown() {
    HiveSyncConfig config = configWithEmptyHiveConf();
    AtomicInteger calls = new AtomicInteger();
    HiveDriverPool.DriverFactory factory = (db) -> {
      int n = calls.incrementAndGet();
      if (n == 2) {
        throw new RuntimeException("simulated driver build failure");
      }
      return mock(Driver.class);
    };
    HoodieException ex = assertThrows(HoodieException.class,
        () -> new HiveDriverPool(config, 3, factory));
    assertTrue(ex.getMessage().contains("Failed to construct HiveDriverPool"));
  }

  @Test
  void runAllDispatchesEachSqlAcrossWorkers() throws Exception {
    HiveSyncConfig config = configWithEmptyHiveConf();
    // Each worker counts how many SQLs it received and remembers the thread.
    ConcurrentHashMap<Driver, Set<String>> seenThreadsByDriver = new ConcurrentHashMap<>();
    HiveDriverPool.DriverFactory factory = (db) -> {
      Driver d = mock(Driver.class);
      seenThreadsByDriver.put(d, ConcurrentHashMap.newKeySet());
      doAnswer((InvocationOnMock inv) -> {
        seenThreadsByDriver.get(d).add(Thread.currentThread().getName());
        return null;
      }).when(d).run(anyString());
      return d;
    };
    try (HiveDriverPool pool = new HiveDriverPool(config, 2, factory)) {
      List<String> sqls = Arrays.asList("SELECT 1", "SELECT 2", "SELECT 3", "SELECT 4");
      HiveDriverPool.Dispatch futures = pool.dispatchAll(sqls);
      pool.awaitAll(futures);
      assertEquals(2, seenThreadsByDriver.size(), "Expected exactly 2 worker Drivers");
      int totalCalls = seenThreadsByDriver.values().stream().mapToInt(Set::size).sum();
      assertTrue(totalCalls >= 1, "At least one worker should have logged a thread");
      // Each Driver should have been invoked exactly twice (round-robin with 4 sqls, 2 workers).
      for (Driver d : seenThreadsByDriver.keySet()) {
        verify(d, times(2)).run(anyString());
      }
    }
  }

  @Test
  void awaitAllThrowsFirstError() throws Exception {
    HiveSyncConfig config = configWithEmptyHiveConf();
    HiveDriverPool.DriverFactory factory = (db) -> {
      Driver d = mock(Driver.class);
      doAnswer(inv -> {
        String sql = inv.getArgument(0);
        if (sql.equals("FAIL")) {
          throw new RuntimeException("boom: " + sql);
        }
        return null;
      }).when(d).run(anyString());
      return d;
    };
    try (HiveDriverPool pool = new HiveDriverPool(config, 2, factory)) {
      HiveDriverPool.Dispatch futures = pool.dispatchAll(Arrays.asList("OK", "FAIL", "OK"));
      HoodieHiveSyncException ex = assertThrows(HoodieHiveSyncException.class,
          () -> pool.awaitAll(futures));
      assertNotNull(ex.getCause());
      assertTrue(ex.getCause().getMessage().contains("boom"));
    }
  }

  @Test
  void concurrentDispatchBoundedByPoolSize() throws Exception {
    HiveSyncConfig config = configWithEmptyHiveConf();
    AtomicInteger inFlight = new AtomicInteger();
    AtomicInteger maxInFlight = new AtomicInteger();
    CountDownLatch hold = new CountDownLatch(1);
    HiveDriverPool.DriverFactory factory = (db) -> {
      Driver d = mock(Driver.class);
      doAnswer(inv -> {
        int now = inFlight.incrementAndGet();
        maxInFlight.updateAndGet(prev -> Math.max(prev, now));
        hold.await(2, TimeUnit.SECONDS);
        inFlight.decrementAndGet();
        return null;
      }).when(d).run(anyString());
      return d;
    };
    try (HiveDriverPool pool = new HiveDriverPool(config, 2, factory)) {
      // 5 SQLs against pool of size 2 → max in-flight should be 2.
      HiveDriverPool.Dispatch futures = pool.dispatchAll(Arrays.asList("a", "b", "c", "d", "e"));
      // Release after a short wait so all SQLs progress.
      Thread.sleep(150);
      hold.countDown();
      pool.awaitAll(futures);
      assertTrue(maxInFlight.get() <= 2,
          "Max concurrent dispatches must not exceed pool size, observed " + maxInFlight.get());
      assertTrue(maxInFlight.get() >= 1, "Sanity: at least one dispatch ran");
    }
  }

  @Test
  void closeIsIdempotentAndPreventsFurtherDispatch() throws Exception {
    HiveSyncConfig config = configWithEmptyHiveConf();
    HiveDriverPool.DriverFactory factory = (db) -> mock(Driver.class);
    HiveDriverPool pool = new HiveDriverPool(config, 2, factory);
    pool.close();
    pool.close();
    assertThrows(IllegalStateException.class,
        () -> pool.dispatchAll(Arrays.asList("anything")));
  }

  @Test
  void invalidSizeRejected() {
    HiveSyncConfig config = configWithEmptyHiveConf();
    HiveDriverPool.DriverFactory factory = (db) -> mock(Driver.class);
    assertThrows(IllegalArgumentException.class,
        () -> new HiveDriverPool(config, 0, factory));
  }

  /**
   * runOnEachWorker must execute the setup SQL on every worker (each on its bound
   * thread) before {@code dispatchAll()} fans the partition statements out. Without this,
   * Hive 2.x's SET LOCATION would silently route to the wrong database on the workers
   * that never saw the leading USE statement.
   */
  @Test
  void runOnEachWorkerRunsSetupOnEveryWorker() throws Exception {
    HiveSyncConfig config = configWithEmptyHiveConf();
    ConcurrentHashMap<Driver, List<String>> sqlsByDriver = new ConcurrentHashMap<>();
    HiveDriverPool.DriverFactory factory = (db) -> {
      Driver d = mock(Driver.class);
      sqlsByDriver.put(d, java.util.Collections.synchronizedList(new java.util.ArrayList<>()));
      doAnswer((InvocationOnMock inv) -> {
        sqlsByDriver.get(d).add(inv.getArgument(0));
        return null;
      }).when(d).run(anyString());
      return d;
    };
    try (HiveDriverPool pool = new HiveDriverPool(config, 3, factory)) {
      pool.runOnEachWorker(Arrays.asList("USE `db1`"));
      HiveDriverPool.Dispatch futures = pool.dispatchAll(Arrays.asList("ALTER 1", "ALTER 2", "ALTER 3"));
      pool.awaitAll(futures);

      assertEquals(3, sqlsByDriver.size(), "Expected one Driver per worker");
      for (Map.Entry<Driver, List<String>> e : sqlsByDriver.entrySet()) {
        List<String> seen = e.getValue();
        assertTrue(!seen.isEmpty() && seen.get(0).equals("USE `db1`"),
            "Each worker must see USE first; saw " + seen);
      }
    }
  }

  /**
   * Given a single-worker pool where the first statement fails, when awaitAll runs,
   * then it throws the original cause and neither queued statement is ever executed.
   *
   * <p>Deterministic: statements queued behind the failure observe the batch's abort
   * flag on entry and bail out without touching the Driver, so it does not matter
   * whether the worker dequeues them before or after awaitAll's cancel() sweep.
   */
  @Test
  void awaitAllCancelsPendingFuturesOnFirstError() throws Exception {
    HiveSyncConfig config = configWithEmptyHiveConf();
    List<String> executed = Collections.synchronizedList(new ArrayList<>());
    HiveDriverPool.DriverFactory factory = (db) -> {
      Driver d = mock(Driver.class);
      doAnswer(inv -> {
        String sql = inv.getArgument(0);
        executed.add(sql);
        if (sql.equals("FAIL")) {
          throw new RuntimeException("boom");
        }
        return null;
      }).when(d).run(anyString());
      return d;
    };
    try (HiveDriverPool pool = new HiveDriverPool(config, 1, factory)) {
      HiveDriverPool.Dispatch dispatch = pool.dispatchAll(Arrays.asList("FAIL", "PENDING_A", "PENDING_B"));

      HoodieHiveSyncException ex = assertThrows(HoodieHiveSyncException.class,
          () -> pool.awaitAll(dispatch));

      assertNotNull(ex.getCause());
      assertTrue(ex.getCause().getMessage().contains("boom"));
      assertEquals(Collections.singletonList("FAIL"), executed,
          "Statements queued behind the failure must never reach the Driver");
    }
  }

  /**
   * Regression for the in-order-await bug: a slow statement on worker 0 must not let
   * worker 1 keep applying partition DDL after worker 1 has already failed.
   *
   * <p>Given two workers, statements are dispatched round-robin — worker 0 gets
   * {@code SLOW} and worker 1 gets {@code FAIL} then {@code AFTER_FAIL}. When awaitAll
   * blocks in submission order, it parks on SLOW's future while worker 1 races ahead
   * and runs AFTER_FAIL. Then AFTER_FAIL must never execute: the abort flag is set by
   * FAIL before worker 1 can dequeue its next statement.
   *
   * <p>SLOW is released only after the batch has aborted, which pins the interleaving
   * the bug needs — without that, SLOW could finish first and mask the race.
   */
  @Test
  void awaitAllStopsLaterWorkerWhenEarlierFutureIsSlow() throws Exception {
    HiveSyncConfig config = configWithEmptyHiveConf();
    List<String> executed = Collections.synchronizedList(new ArrayList<>());
    CountDownLatch failed = new CountDownLatch(1);
    CountDownLatch releaseSlow = new CountDownLatch(1);
    HiveDriverPool.DriverFactory factory = (db) -> {
      Driver d = mock(Driver.class);
      doAnswer(inv -> {
        String sql = inv.getArgument(0);
        executed.add(sql);
        if (sql.equals("FAIL")) {
          failed.countDown();
          throw new RuntimeException("boom");
        }
        if (sql.equals("SLOW")) {
          // Hold worker 0 until worker 1 has failed, so awaitAll is definitely still
          // parked on future 0 at the moment worker 1 would pick up AFTER_FAIL.
          releaseSlow.await(5, TimeUnit.SECONDS);
        }
        return null;
      }).when(d).run(anyString());
      return d;
    };
    try (HiveDriverPool pool = new HiveDriverPool(config, 2, factory)) {
      // Round-robin over 2 workers: index 0 -> worker 0, indices 1 and 2 -> worker 1.
      HiveDriverPool.Dispatch dispatch =
          pool.dispatchAll(Arrays.asList("SLOW", "FAIL", "AFTER_FAIL"));
      assertTrue(failed.await(5, TimeUnit.SECONDS), "FAIL must have run");
      releaseSlow.countDown();

      HoodieHiveSyncException ex = assertThrows(HoodieHiveSyncException.class,
          () -> pool.awaitAll(dispatch));

      assertNotNull(ex.getCause());
      assertTrue(ex.getCause().getMessage().contains("boom"));
      assertFalse(executed.contains("AFTER_FAIL"),
          "Statement queued behind a failure on the same worker must not be applied, "
              + "even while an earlier future on another worker is still running");
    }
  }

  /**
   * Regression for the swallowed-failure race: awaitAll must still report the error when
   * its own cancel() sweep has already marked the failing task's future CANCELLED.
   *
   * <p>The failing worker aborts the batch from inside its catch block, which releases
   * awaitAll, but its exception only reaches the FutureTask after {@code call()} returns.
   * {@code FutureTask.cancel(false)} succeeds on any task still in state NEW - a task
   * mid-unwind included - so the cancel wins the state CAS, the later {@code setException}
   * becomes a no-op, and {@code get()} reports CancellationException instead of the error.
   * awaitAll then counted it as merely cancelled and returned normally, reporting a failed
   * partition-DDL batch as a successful sync.
   *
   * <p>Deterministic where the three tests above are not: instead of hoping the awaiting
   * thread wins, this cancels the future explicitly - exactly what cancelPending() does -
   * while the Driver is parked, and only then lets it throw.
   */
  @Test
  void awaitAllReportsFailureWhenFailingFutureIsCancelledMidFlight() throws Exception {
    HiveSyncConfig config = configWithEmptyHiveConf();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    HiveDriverPool.DriverFactory factory = (db) -> {
      Driver d = mock(Driver.class);
      doAnswer(inv -> {
        entered.countDown();
        release.await(10, TimeUnit.SECONDS);
        throw new RuntimeException("boom");
      }).when(d).run(anyString());
      return d;
    };
    try (HiveDriverPool pool = new HiveDriverPool(config, 1, factory)) {
      HiveDriverPool.Dispatch dispatch = pool.dispatchAll(Collections.singletonList("FAIL"));
      assertTrue(entered.await(10, TimeUnit.SECONDS), "Driver must have started the statement");
      assertTrue(dispatch.futureAt(0).cancel(false),
          "Sanity: a running FutureTask is still NEW, so cancel(false) must succeed");
      release.countDown();

      HoodieHiveSyncException ex = assertThrows(HoodieHiveSyncException.class,
          () -> pool.awaitAll(dispatch));
      assertNotNull(ex.getCause());
      assertTrue(ex.getCause().getMessage().contains("boom"));
    }
  }
}
