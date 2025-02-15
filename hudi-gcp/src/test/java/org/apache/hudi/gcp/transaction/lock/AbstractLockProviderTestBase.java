/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.gcp.transaction.lock;

import org.apache.hudi.client.transaction.lock.ConditionalWriteLockConfig;
import org.apache.hudi.client.transaction.lock.ConditionalWriteLockProvider;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieLockException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractLockProviderTestBase {

  protected ConditionalWriteLockProvider lockProvider;
  protected static TypedProperties providerProperties;
  // A method that subclasses must implement to instantiate the correct provider.
  protected abstract ConditionalWriteLockProvider createLockProvider();

  protected void recreateLockProvider() {
    lockProvider = createLockProvider();
  }

  @BeforeEach
  void setUp() {
    // Create a fresh instance before each test
    // This will run first
    providerProperties = new TypedProperties();
  }

  @AfterEach
  void tearDown() {
    // Optionally unlock or close
    if (lockProvider != null) {
      lockProvider.unlock();
      lockProvider.close();
    }
  }

  // ----------------------------------------------------
  // Basic Tests
  // ----------------------------------------------------

  @Test
  void testTryLockSuccess() {
    boolean lockAcquired = lockProvider.tryLock(5, TimeUnit.SECONDS);
    assertTrue(lockAcquired, "Lock should be successfully acquired within the time limit.");
  }

  @Test
  void testTryLockReleasesLock() {
    assertTrue(lockProvider.tryLock(3, TimeUnit.SECONDS), "Lock should be successfully acquired.");
    lockProvider.unlock();
    assertTrue(lockProvider.tryLock(1, TimeUnit.SECONDS), "Lock should be reacquired after being released.");
  }

  @Test
  void testTryLockEdgeCaseZeroTimeout() {
    boolean lockAcquired = lockProvider.tryLock(0, TimeUnit.SECONDS);
    assertFalse(lockAcquired, "Lock should not be acquired with a zero timeout.");
  }

  @Test
  void testLockThreadKilledShouldNotCauseOrphanedHeartbeat() throws InterruptedException {
    providerProperties.put(ConditionalWriteLockConfig.LOCK_VALIDITY_TIMEOUT_MS.key(), 5000);
    providerProperties.put(ConditionalWriteLockConfig.HEARTBEAT_POLL_MS.key(), 1000);

    // Create a thread with a new lock provider to acquire the lock
    Thread lockingThread = new Thread(() -> {
      createLockProvider().tryLock();
    });
    lockingThread.start();

    // Wait for the thread to acquire the lock
    // Ensure the locking thread is dead.
    lockingThread.join(500);
    assertFalse(lockingThread.isAlive());

    // After the validity expires, should be able to reacquire
    ConditionalWriteLockProvider newLockProvider = createLockProvider();
    boolean lockAcquired = newLockProvider.tryLock(15, TimeUnit.SECONDS);
    assertTrue(lockAcquired, "Lock should be reacquired after expiration");
    assertNotNull(newLockProvider.getLock(), "Lock should be reacquired and getLock() should return non-null");
    newLockProvider.unlock();
    newLockProvider.close();
  }

  @Test
  void testUnlockWhenNoLockPresent() {
    lockProvider.unlock();
    assertNull(lockProvider.getLock());
  }

  @Test
  void testTryLockHappyPath() {
    assertTrue(lockProvider.tryLock(), "tryLock should succeed if lock not held");
    assertNotNull(lockProvider.getLock(), "Lock should be held after tryLock");
  }

  @Test
  void testTryLockReentrancy() {
    assertTrue(lockProvider.tryLock(), "tryLock should succeed if lock not held");
    assertTrue(lockProvider.tryLock(), "tryLock should succeed again");
  }

  @Test
  void testUnlockAndLock() {
    assertTrue(lockProvider.tryLock(), "tryLock should succeed if lock not held");
    assertNotNull(lockProvider.getLock());
    lockProvider.unlock();
    assertNull(lockProvider.getLock(), "Lock should be null after unlock");
    assertTrue(lockProvider.tryLock(), "tryLock should succeed if lock not held");
    assertNotNull(lockProvider.getLock());
  }

  @Test
  void testIdempotentUnlock() {
    assertTrue(lockProvider.tryLock(), "tryLock should succeed if lock not held");
    lockProvider.unlock();

    // Calling this again should no op, but will warn.
    lockProvider.unlock();
  }

  @Test
  void testLockReacquisitionInLoop() {
    for (int i = 0; i < 100; i++) {
      assertTrue(lockProvider.tryLock(), "tryLock should succeed if lock not held");
      assertNotNull(lockProvider.getLock(), "Lock should be held after acquisition in iteration " + i);
      lockProvider.unlock();
      assertNull(lockProvider.getLock(), "Lock should be null after unlock in iteration " + i);
    }
  }

  /// -----------
  /// Concurrency
  /// -----------

  @Test
  void testConcurrentAccessWithSeparateProviders() throws InterruptedException {
    final int NUM_THREADS = 20;
    boolean[] gotLock = new boolean[NUM_THREADS];

    // Create separate lock providers (one per thread).
    List<ConditionalWriteLockProvider> providers = new ArrayList<>();
    for (int i = 0; i < NUM_THREADS; i++) {
      providers.add(createLockProvider());
    }

    List<Thread> threads = getThreads(NUM_THREADS, gotLock, providers::get);
    for (Thread t : threads) {
      t.join();
    }

    int numLocksAcquired = 0;
    for (boolean lock : gotLock) {
      if (lock) {
        numLocksAcquired++;
      }
    }
    assertEquals(1, numLocksAcquired,
        "Expected one thread to acquire the lock, but got " + numLocksAcquired);

    // Close all providers.
    for (ConditionalWriteLockProvider provider : providers) {
      provider.unlock();
      provider.close();
    }
  }

  /// -----------
  /// Stress-testing
  /// -----------

  private List<Thread> getThreads(
      int numThreads,
      boolean[] gotLock,
      Function<Integer, ConditionalWriteLockProvider> providerSupplier
  ) {
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < numThreads; i++) {
      final int index = i;
      Thread t = new Thread(() -> {
        try {
          // Get the provider for this thread using the supplied function
          ConditionalWriteLockProvider provider = providerSupplier.apply(index);
          // Try to acquire the lock using the obtained provider
          gotLock[index] = provider.tryLock();
        } catch (Exception e) {
          if (!(e instanceof HoodieLockException)) {
            fail("Not supposed to throw any exception except HoodieLockException");
          }
        }


      });
      threads.add(t);
      t.start();
    }
    return threads;
  }
}