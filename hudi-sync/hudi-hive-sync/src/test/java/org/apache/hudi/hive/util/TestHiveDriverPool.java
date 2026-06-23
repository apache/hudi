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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
      List<Future<?>> futures = pool.runAll(sqls);
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
      List<Future<?>> futures = pool.runAll(Arrays.asList("OK", "FAIL", "OK"));
      HoodieHiveSyncException ex = assertThrows(HoodieHiveSyncException.class,
          () -> pool.awaitAll(futures));
      assertTrue(ex.getCause() != null && ex.getCause().getMessage().contains("boom"));
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
      List<Future<?>> futures = pool.runAll(Arrays.asList("a", "b", "c", "d", "e"));
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
        () -> pool.runAll(Arrays.asList("anything")));
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
   * thread) before {@code runAll()} fans the partition statements out. Without this,
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
      List<Future<?>> futures = pool.runAll(Arrays.asList("ALTER 1", "ALTER 2", "ALTER 3"));
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
   * On the first failure, awaitAll must throw the original cause and cancel any
   * futures that have not started yet. Futures already in-flight are not interrupted
   * (per the cancel-with-mayInterruptIfRunning=false contract).
   */
  @Test
  void awaitAllCancelsPendingFuturesOnFirstError() throws Exception {
    HiveSyncConfig config = configWithEmptyHiveConf();
    // Single-worker pool so SQLs run strictly sequentially → the 2nd SQL is
    // pending when the 1st errors, and must be cancelled.
    CountDownLatch fired = new CountDownLatch(1);
    HiveDriverPool.DriverFactory factory = (db) -> {
      Driver d = mock(Driver.class);
      doAnswer(inv -> {
        String sql = inv.getArgument(0);
        if (sql.equals("FAIL")) {
          fired.countDown();
          throw new RuntimeException("boom");
        }
        return null;
      }).when(d).run(anyString());
      return d;
    };
    try (HiveDriverPool pool = new HiveDriverPool(config, 1, factory)) {
      List<Future<?>> futures = pool.runAll(Arrays.asList("FAIL", "PENDING_A", "PENDING_B"));
      HoodieHiveSyncException ex = assertThrows(HoodieHiveSyncException.class,
          () -> pool.awaitAll(futures));
      assertTrue(ex.getCause() != null && ex.getCause().getMessage().contains("boom"));
      // The first future failed, so it's done (not cancelled). The remaining two
      // were pending behind it on the single worker and should now be cancelled.
      assertTrue(fired.await(1, TimeUnit.SECONDS), "Failing SQL must have run");
      assertTrue(futures.get(1).isCancelled(), "Pending future after error must be cancelled");
      assertTrue(futures.get(2).isCancelled(), "Pending future after error must be cancelled");
    }
  }
}
