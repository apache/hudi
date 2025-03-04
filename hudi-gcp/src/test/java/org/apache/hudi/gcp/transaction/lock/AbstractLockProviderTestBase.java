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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
  void testTwoLockProvidersCloseAndUnlock() throws InterruptedException {
    // We had an issue with tryLock(30, TimeUnit.SECONDS) where it was not synchronized
    // and this created a race condition where closing it would result in improper
    // execution with the heartbeat manager.
    ConditionalWriteLockProvider provider1 = createLockProvider();
    assertTrue(provider1.tryLock(), "Provider1 should acquire the lock immediately");

    ConditionalWriteLockProvider provider2 = createLockProvider();

    CountDownLatch finishLatch = new CountDownLatch(1);

    Thread provider2Thread = new Thread(() -> {
      // We cannot Thread.sleep in a synchronized method, therefore
      // this will throw while waiting.
      assertThrows(HoodieLockException.class, () -> provider2.tryLock(10, TimeUnit.SECONDS));
      finishLatch.countDown();
    });
    provider2Thread.start();
    // We just need the tryLock code path to enter before close
    provider2.close();
    provider1.unlock();

    assertTrue(finishLatch.await(5000, TimeUnit.MILLISECONDS));
  }

  @Test
  void testLockAfterClosing() {
    ConditionalWriteLockProvider provider1 = createLockProvider();
    assertTrue(provider1.tryLock(), "Provider1 should acquire the lock immediately");
    provider1.unlock();
    provider1.close();
    assertThrows(HoodieLockException.class, provider1::tryLock);
  }

  @Test
  void testCloseBeforeUnlocking() {
    ConditionalWriteLockProvider provider1 = createLockProvider();
    assertTrue(provider1.tryLock(), "Provider1 should acquire the lock immediately");
    provider1.close();
    provider1.unlock();
    assertThrows(HoodieLockException.class, provider1::tryLock);
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
}