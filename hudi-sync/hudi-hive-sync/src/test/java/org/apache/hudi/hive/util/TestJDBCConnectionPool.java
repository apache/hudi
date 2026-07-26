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

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for {@link JDBCConnectionPool} borrow/return/close semantics. These
 * tests never hit a real HiveServer2 — connections are mocked and injected via
 * the package-private constructor.
 */
class TestJDBCConnectionPool {

  private static List<Connection> mockConnections(int n) {
    List<Connection> conns = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      conns.add(mock(Connection.class));
    }
    return conns;
  }

  @Test
  void runReturnsConnectionToPoolOnSuccess() throws Exception {
    List<Connection> conns = mockConnections(2);
    JDBCConnectionPool pool = new JDBCConnectionPool(conns, 2);
    try {
      Connection borrowed = pool.run(c -> c);
      Set<Connection> seen = new HashSet<>();
      seen.add(pool.run(c -> c));
      seen.add(pool.run(c -> c));
      assertEquals(2, seen.size(), "Both connections should be reachable after returns");
      assertTrue(seen.contains(borrowed));
    } finally {
      pool.close();
    }
  }

  @Test
  void runReturnsConnectionToPoolOnFailure() {
    List<Connection> conns = mockConnections(1);
    JDBCConnectionPool pool = new JDBCConnectionPool(conns, 1);
    try {
      assertThrows(IllegalStateException.class, () -> pool.run(c -> {
        throw new IllegalStateException("boom");
      }));
      try {
        Connection again = pool.run(c -> c);
        assertSame(conns.get(0), again);
      } catch (Exception e) {
        throw new AssertionError("Pool should still be usable after action failure", e);
      }
    } finally {
      pool.close();
    }
  }

  @Test
  void concurrentBorrowsBlockUntilReturned() throws Exception {
    int poolSize = 2;
    int callers = 4;
    List<Connection> conns = mockConnections(poolSize);
    JDBCConnectionPool pool = new JDBCConnectionPool(conns, poolSize);
    AtomicInteger concurrent = new AtomicInteger(0);
    AtomicInteger maxConcurrent = new AtomicInteger(0);
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService threads = Executors.newFixedThreadPool(callers);
    try {
      List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < callers; i++) {
        futures.add(threads.submit(() -> {
          start.await();
          return pool.run(c -> {
            int now = concurrent.incrementAndGet();
            maxConcurrent.updateAndGet(prev -> Math.max(prev, now));
            Thread.sleep(50);
            concurrent.decrementAndGet();
            return null;
          });
        }));
      }
      start.countDown();
      for (java.util.concurrent.Future<?> f : futures) {
        f.get(10, TimeUnit.SECONDS);
      }
      assertTrue(maxConcurrent.get() <= poolSize,
          "Concurrent borrows must not exceed pool size, observed " + maxConcurrent.get());
      assertTrue(maxConcurrent.get() >= 1, "At least one borrow must have occurred");
    } finally {
      threads.shutdownNow();
      pool.close();
    }
  }

  @Test
  void closeReleasesAllConnections() throws Exception {
    int poolSize = 3;
    List<Connection> conns = mockConnections(poolSize);
    JDBCConnectionPool pool = new JDBCConnectionPool(conns, poolSize);
    pool.close();
    for (Connection c : conns) {
      verify(c).close();
    }
    assertThrows(IllegalStateException.class, () -> pool.run(c -> c));
  }

  @Test
  void closeIsIdempotent() throws Exception {
    int poolSize = 2;
    List<Connection> conns = mockConnections(poolSize);
    JDBCConnectionPool pool = new JDBCConnectionPool(conns, poolSize);
    pool.close();
    pool.close();
    for (Connection c : conns) {
      verify(c).close();
    }
  }

  @Test
  void invalidSizeRejected() {
    assertThrows(IllegalArgumentException.class,
        () -> new JDBCConnectionPool(mockConnections(0), 0));
  }

  @Test
  void sizeMismatchRejected() {
    assertThrows(IllegalArgumentException.class,
        () -> new JDBCConnectionPool(mockConnections(2), 3));
  }

  @Test
  void executorSizedToPool() {
    int poolSize = 4;
    JDBCConnectionPool pool = new JDBCConnectionPool(mockConnections(poolSize), poolSize);
    try {
      assertFalse(pool.executor().isShutdown());
      assertEquals(poolSize, pool.size());
    } finally {
      pool.close();
    }
    assertTrue(pool.executor().isShutdown());
  }
}
