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

package org.apache.hudi.common.util.collection;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link RocksDBDAOFactory}.
 */
public class TestRocksDBDAOFactory {

  private static final String BASE_PATH = "/hudi/factory/test";
  private static final String FAMILY = "test_family";

  // -------------------------------------------------------------------------
  // Singleton identity
  // -------------------------------------------------------------------------

  @Test
  void testSameParametersReturnSameInstance(@TempDir File tempDir) {
    String rocksDBBasePath = tempDir.getAbsolutePath();

    RocksDBDAO dao1 = RocksDBDAOFactory.getOrCreate(BASE_PATH, rocksDBBasePath, new ConcurrentHashMap<>(), false);
    RocksDBDAO dao2 = RocksDBDAOFactory.getOrCreate(BASE_PATH, rocksDBBasePath, new ConcurrentHashMap<>(), false);
    try {
      assertSame(dao1, dao2, "Same parameters must return the identical RocksDBDAO instance");
    } finally {
      RocksDBDAOFactory.release(BASE_PATH, rocksDBBasePath);
      RocksDBDAOFactory.release(BASE_PATH, rocksDBBasePath);
    }
  }

  @Test
  void testDifferentRocksDBBasePathReturnDifferentInstances(@TempDir File dir1, @TempDir File dir2) {
    RocksDBDAO dao1 = RocksDBDAOFactory.getOrCreate(BASE_PATH, dir1.getAbsolutePath(), new ConcurrentHashMap<>(), false);
    RocksDBDAO dao2 = RocksDBDAOFactory.getOrCreate(BASE_PATH, dir2.getAbsolutePath(), new ConcurrentHashMap<>(), false);
    try {
      assertNotSame(dao1, dao2, "Different rocksDBBasePath values must produce distinct instances");
    } finally {
      RocksDBDAOFactory.release(BASE_PATH, dir1.getAbsolutePath());
      RocksDBDAOFactory.release(BASE_PATH, dir2.getAbsolutePath());
    }
  }

  @Test
  void testDifferentBasePathReturnDifferentInstances(@TempDir File tempDir) {
    String rocksDBBasePath = tempDir.getAbsolutePath();
    String basePath1 = "/hudi/table/a";
    String basePath2 = "/hudi/table/b";

    RocksDBDAO dao1 = RocksDBDAOFactory.getOrCreate(basePath1, rocksDBBasePath, new ConcurrentHashMap<>(), false);
    RocksDBDAO dao2 = RocksDBDAOFactory.getOrCreate(basePath2, rocksDBBasePath, new ConcurrentHashMap<>(), false);
    try {
      assertNotSame(dao1, dao2, "Different basePath values must produce distinct instances");
    } finally {
      RocksDBDAOFactory.release(basePath1, rocksDBBasePath);
      RocksDBDAOFactory.release(basePath2, rocksDBBasePath);
    }
  }

  // -------------------------------------------------------------------------
  // Reference counting
  // -------------------------------------------------------------------------

  @Test
  void testPartialReleasesDoNotCloseDAO(@TempDir File tempDir) {
    String rocksDBBasePath = tempDir.getAbsolutePath();

    RocksDBDAO dao1 = RocksDBDAOFactory.getOrCreate(BASE_PATH, rocksDBBasePath, new ConcurrentHashMap<>(), false);
    RocksDBDAO dao2 = RocksDBDAOFactory.getOrCreate(BASE_PATH, rocksDBBasePath, new ConcurrentHashMap<>(), false);
    RocksDBDAO dao3 = RocksDBDAOFactory.getOrCreate(BASE_PATH, rocksDBBasePath, new ConcurrentHashMap<>(), false);

    dao1.addColumnFamily(FAMILY);
    dao1.put(FAMILY, "key1", "value1");

    // Releasing two of the three references must not close the DAO.
    RocksDBDAOFactory.release(BASE_PATH, rocksDBBasePath);
    assertEquals("value1", dao2.<String>get(FAMILY, "key1"),
        "DAO must remain usable after first release");

    RocksDBDAOFactory.release(BASE_PATH, rocksDBBasePath);
    assertEquals("value1", dao3.<String>get(FAMILY, "key1"),
        "DAO must remain usable after second release");

    // Last release closes the DAO.
    RocksDBDAOFactory.release(BASE_PATH, rocksDBBasePath);
  }

  @Test
  void testLastReleaseClosesAndEvictsDAO(@TempDir File tempDir) {
    String rocksDBBasePath = tempDir.getAbsolutePath();

    RocksDBDAO dao = RocksDBDAOFactory.getOrCreate(BASE_PATH, rocksDBBasePath, new ConcurrentHashMap<>(), false);
    String physicalPath = dao.getRocksDBBasePath();
    assertTrue(new File(physicalPath).exists(), "RocksDB directory must exist after creation");

    RocksDBDAOFactory.release(BASE_PATH, rocksDBBasePath);

    // RocksDBDAO.close() deletes the physical directory; its absence confirms the DAO was closed.
    assertFalse(new File(physicalPath).exists(),
        "RocksDB directory must be deleted after the last release");
  }

  @Test
  void testReacquireAfterLastReleaseCreatesFreshInstance(@TempDir File tempDir) {
    String rocksDBBasePath = tempDir.getAbsolutePath();

    RocksDBDAO first = RocksDBDAOFactory.getOrCreate(BASE_PATH, rocksDBBasePath, new ConcurrentHashMap<>(), false);
    RocksDBDAOFactory.release(BASE_PATH, rocksDBBasePath);

    RocksDBDAO second = RocksDBDAOFactory.getOrCreate(BASE_PATH, rocksDBBasePath, new ConcurrentHashMap<>(), false);
    try {
      assertNotSame(first, second,
          "After the last release, getOrCreate must create a fresh RocksDBDAO instance");
    } finally {
      RocksDBDAOFactory.release(BASE_PATH, rocksDBBasePath);
    }
  }

  @Test
  void testReleaseWithoutAcquireIsNoop() {
    // Must not throw even if no matching entry exists in the factory.
    RocksDBDAOFactory.release("/nonexistent/base", "/nonexistent/rocks");
  }

  // -------------------------------------------------------------------------
  // Data isolation
  // -------------------------------------------------------------------------

  @Test
  void testDistinctInstancesDoNotShareData(@TempDir File dir1, @TempDir File dir2) {
    RocksDBDAO dao1 = RocksDBDAOFactory.getOrCreate(BASE_PATH, dir1.getAbsolutePath(), new ConcurrentHashMap<>(), false);
    RocksDBDAO dao2 = RocksDBDAOFactory.getOrCreate(BASE_PATH, dir2.getAbsolutePath(), new ConcurrentHashMap<>(), false);
    try {
      dao1.addColumnFamily(FAMILY);
      dao2.addColumnFamily(FAMILY);

      dao1.put(FAMILY, "key1", "value1");

      assertNull(dao2.<String>get(FAMILY, "key1"),
          "Distinct RocksDBDAO instances must not share data");
    } finally {
      RocksDBDAOFactory.release(BASE_PATH, dir1.getAbsolutePath());
      RocksDBDAOFactory.release(BASE_PATH, dir2.getAbsolutePath());
    }
  }

  @Test
  void testSharedInstanceSharesData(@TempDir File tempDir) {
    String rocksDBBasePath = tempDir.getAbsolutePath();

    RocksDBDAO dao1 = RocksDBDAOFactory.getOrCreate(BASE_PATH, rocksDBBasePath, new ConcurrentHashMap<>(), false);
    RocksDBDAO dao2 = RocksDBDAOFactory.getOrCreate(BASE_PATH, rocksDBBasePath, new ConcurrentHashMap<>(), false);
    try {
      dao1.addColumnFamily(FAMILY);
      dao1.put(FAMILY, "shared_key", "shared_value");

      assertEquals("shared_value", dao2.<String>get(FAMILY, "shared_key"),
          "Callers sharing the same DAO must observe each other's writes");
    } finally {
      RocksDBDAOFactory.release(BASE_PATH, rocksDBBasePath);
      RocksDBDAOFactory.release(BASE_PATH, rocksDBBasePath);
    }
  }

  // -------------------------------------------------------------------------
  // Thread safety
  // -------------------------------------------------------------------------

  @Test
  void testConcurrentGetOrCreateReturnsSameInstance(@TempDir File tempDir) throws InterruptedException {
    String rocksDBBasePath = tempDir.getAbsolutePath();
    int numThreads = 10;

    List<RocksDBDAO> results = new ArrayList<>();
    for (int i = 0; i < numThreads; i++) {
      results.add(null);
    }

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(numThreads);
    AtomicReference<Throwable> error = new AtomicReference<>();

    for (int i = 0; i < numThreads; i++) {
      final int idx = i;
      executor.submit(() -> {
        try {
          startLatch.await();
          results.set(idx,
              RocksDBDAOFactory.getOrCreate(BASE_PATH, rocksDBBasePath, new ConcurrentHashMap<>(), false));
        } catch (Throwable t) {
          error.compareAndSet(null, t);
        } finally {
          doneLatch.countDown();
        }
      });
    }

    startLatch.countDown();
    assertTrue(doneLatch.await(30, TimeUnit.SECONDS), "Threads did not finish in time");
    executor.shutdownNow();

    assertNull(error.get(), "Concurrent getOrCreate threw an exception: " + error.get());

    RocksDBDAO expected = results.get(0);
    for (int i = 1; i < numThreads; i++) {
      assertSame(expected, results.get(i),
          "All concurrent getOrCreate calls must return the same instance");
    }

    // Release all numThreads references acquired above.
    for (int i = 0; i < numThreads; i++) {
      RocksDBDAOFactory.release(BASE_PATH, rocksDBBasePath);
    }
  }
}
