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
 * Unit tests for {@link IMetaStoreClientPool} borrow/return/close semantics. These tests
 * never hit a real Hive Metastore — clients are mocked and injected via the package-
 * private constructor.
 */
class TestIMetaStoreClientPool {

  private static List<IMetaStoreClient> mockClients(int n) {
    List<IMetaStoreClient> clients = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      clients.add(mock(IMetaStoreClient.class));
    }
    return clients;
  }

  @Test
  void runReturnsClientToPoolOnSuccess() throws Exception {
    List<IMetaStoreClient> clients = mockClients(2);
    IMetaStoreClientPool pool = new IMetaStoreClientPool(clients, 2);
    try {
      IMetaStoreClient borrowed = pool.run(c -> c);
      // Both clients should be available again
      Set<IMetaStoreClient> seen = new HashSet<>();
      seen.add(pool.run(c -> c));
      seen.add(pool.run(c -> c));
      assertEquals(2, seen.size(), "Both clients should be reachable after returns");
      assertTrue(seen.contains(borrowed));
    } finally {
      pool.close();
    }
  }

  @Test
  void runReturnsClientToPoolOnFailure() {
    List<IMetaStoreClient> clients = mockClients(1);
    IMetaStoreClientPool pool = new IMetaStoreClientPool(clients, 1);
    try {
      assertThrows(IllegalStateException.class, () -> pool.run(c -> {
        throw new IllegalStateException("boom");
      }));
      // Pool should still be usable; the borrowed client was returned
      try {
        IMetaStoreClient again = pool.run(c -> c);
        assertSame(clients.get(0), again);
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
    List<IMetaStoreClient> clients = mockClients(poolSize);
    IMetaStoreClientPool pool = new IMetaStoreClientPool(clients, poolSize);
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
  void closeReleasesAllClients() throws Exception {
    int poolSize = 3;
    List<IMetaStoreClient> clients = mockClients(poolSize);
    IMetaStoreClientPool pool = new IMetaStoreClientPool(clients, poolSize);
    pool.close();
    for (IMetaStoreClient c : clients) {
      verify(c).close();
    }
    // Subsequent borrow should fail
    assertThrows(IllegalStateException.class, () -> pool.run(c -> c));
  }

  @Test
  void closeIsIdempotent() throws Exception {
    int poolSize = 2;
    List<IMetaStoreClient> clients = mockClients(poolSize);
    IMetaStoreClientPool pool = new IMetaStoreClientPool(clients, poolSize);
    pool.close();
    pool.close();
    // verify still passes — close() called only once per client
    for (IMetaStoreClient c : clients) {
      verify(c).close();
    }
  }

  @Test
  void invalidSizeRejected() {
    assertThrows(IllegalArgumentException.class,
        () -> new IMetaStoreClientPool(mockClients(0), 0));
  }

  @Test
  void sizeMismatchRejected() {
    assertThrows(IllegalArgumentException.class,
        () -> new IMetaStoreClientPool(mockClients(2), 3));
  }

  @Test
  void executorSizedToPool() {
    int poolSize = 4;
    IMetaStoreClientPool pool = new IMetaStoreClientPool(mockClients(poolSize), poolSize);
    try {
      assertFalse(pool.executor().isShutdown());
      assertEquals(poolSize, pool.size());
    } finally {
      pool.close();
    }
    assertTrue(pool.executor().isShutdown());
  }
}
