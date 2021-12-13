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

package org.apache.hudi.client.transaction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.transaction.lock.LocalProcessLockProvider;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieLockException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestLocalProcessLockProvider {

  private static final Logger LOG = LogManager.getLogger(TestLocalProcessLockProvider.class);
  private final Configuration hadoopConfiguration = new Configuration();
  private final LockConfiguration lockConfiguration = new LockConfiguration(new TypedProperties());

  @Test
  public void testLockAcquisition() {
    LocalProcessLockProvider localProcessLockProvider = new LocalProcessLockProvider(lockConfiguration, hadoopConfiguration);
    assertDoesNotThrow(() -> {
      localProcessLockProvider.lock();
    });
    assertDoesNotThrow(() -> {
      localProcessLockProvider.unlock();
    });
  }

  @Test
  public void testLockReAcquisitionBySameThread() {
    LocalProcessLockProvider localProcessLockProvider = new LocalProcessLockProvider(lockConfiguration, hadoopConfiguration);
    assertDoesNotThrow(() -> {
      localProcessLockProvider.lock();
    });
    assertThrows(HoodieLockException.class, () -> {
      localProcessLockProvider.lock();
    });
    assertDoesNotThrow(() -> {
      localProcessLockProvider.unlock();
    });
  }

  @Test
  public void testLockReAcquisitionByDifferentThread() {
    LocalProcessLockProvider localProcessLockProvider = new LocalProcessLockProvider(lockConfiguration, hadoopConfiguration);
    final AtomicBoolean writer2Completed = new AtomicBoolean(false);

    // Main test thread
    assertDoesNotThrow(() -> {
      localProcessLockProvider.lock();
    });

    // Another writer thread in parallel, should block
    // and later acquire the lock once it is released
    Thread writer2 = new Thread(new Runnable() {
      @Override
      public void run() {
        assertDoesNotThrow(() -> {
          localProcessLockProvider.lock();
        });
        assertDoesNotThrow(() -> {
          localProcessLockProvider.unlock();
        });
        writer2Completed.set(true);
      }
    });
    writer2.start();

    assertDoesNotThrow(() -> {
      localProcessLockProvider.unlock();
    });

    try {
      writer2.join();
    } catch (InterruptedException e) {
      //
    }
    Assertions.assertTrue(writer2Completed.get());
  }

  @Test
  public void testTryLockAcquisition() {
    LocalProcessLockProvider localProcessLockProvider = new LocalProcessLockProvider(lockConfiguration, hadoopConfiguration);
    Assertions.assertTrue(localProcessLockProvider.tryLock());
    assertDoesNotThrow(() -> {
      localProcessLockProvider.unlock();
    });
  }

  @Test
  public void testTryLockAcquisitionWithTimeout() {
    LocalProcessLockProvider localProcessLockProvider = new LocalProcessLockProvider(lockConfiguration, hadoopConfiguration);
    Assertions.assertTrue(localProcessLockProvider.tryLock(1, TimeUnit.MILLISECONDS));
    assertDoesNotThrow(() -> {
      localProcessLockProvider.unlock();
    });
  }

  @Test
  public void testTryLockReAcquisitionBySameThread() {
    LocalProcessLockProvider localProcessLockProvider = new LocalProcessLockProvider(lockConfiguration, hadoopConfiguration);
    Assertions.assertTrue(localProcessLockProvider.tryLock());
    assertThrows(HoodieLockException.class, () -> {
      localProcessLockProvider.tryLock(1, TimeUnit.MILLISECONDS);
    });
    assertDoesNotThrow(() -> {
      localProcessLockProvider.unlock();
    });
  }

  @Test
  public void testTryLockReAcquisitionByDifferentThread() {
    LocalProcessLockProvider localProcessLockProvider = new LocalProcessLockProvider(lockConfiguration, hadoopConfiguration);
    final AtomicBoolean writer2Completed = new AtomicBoolean(false);

    // Main test thread
    Assertions.assertTrue(localProcessLockProvider.tryLock());

    // Another writer thread
    Thread writer2 = new Thread(() -> {
      Assertions.assertFalse(localProcessLockProvider.tryLock(100L, TimeUnit.MILLISECONDS));
      writer2Completed.set(true);
    });
    writer2.start();
    try {
      writer2.join();
    } catch (InterruptedException e) {
      //
    }

    assertDoesNotThrow(() -> {
      localProcessLockProvider.unlock();
    });
    Assertions.assertTrue(writer2Completed.get());
  }

  @Test
  public void testTryLockAcquisitionBeforeTimeOutFromTwoThreads() {
    final LocalProcessLockProvider localProcessLockProvider = new LocalProcessLockProvider(lockConfiguration, hadoopConfiguration);
    final int threadCount = 3;
    final long awaitMaxTimeoutMs = 2000L;
    final CountDownLatch latch = new CountDownLatch(threadCount);
    final AtomicBoolean writer1Completed = new AtomicBoolean(false);
    final AtomicBoolean writer2Completed = new AtomicBoolean(false);

    // Let writer1 get the lock first, then wait for others
    // to join the sync up point.
    Thread writer1 = new Thread(() -> {
      Assertions.assertTrue(localProcessLockProvider.tryLock());
      latch.countDown();
      try {
        latch.await(awaitMaxTimeoutMs, TimeUnit.MILLISECONDS);
        // Following sleep is to make sure writer2 attempts
        // to try lock and to get bocked on the lock which
        // this thread is currently holding.
        Thread.sleep(50);
      } catch (InterruptedException e) {
        //
      }
      assertDoesNotThrow(() -> {
        localProcessLockProvider.unlock();
      });
      writer1Completed.set(true);
    });
    writer1.start();

    // Writer2 will block on trying to acquire the lock
    // and will eventually get the lock before the timeout.
    Thread writer2 = new Thread(() -> {
      latch.countDown();
      Assertions.assertTrue(localProcessLockProvider.tryLock(awaitMaxTimeoutMs, TimeUnit.MILLISECONDS));
      assertDoesNotThrow(() -> {
        localProcessLockProvider.unlock();
      });
      writer2Completed.set(true);
    });
    writer2.start();

    // Let writer1 and writer2 wait at the sync up
    // point to make sure they run in parallel and
    // one get blocked by the other.
    latch.countDown();
    try {
      writer1.join();
      writer2.join();
    } catch (InterruptedException e) {
      //
    }

    // Make sure both writers actually completed good
    Assertions.assertTrue(writer1Completed.get());
    Assertions.assertTrue(writer2Completed.get());
  }

  @Test
  public void testLockReleaseByClose() {
    LocalProcessLockProvider localProcessLockProvider = new LocalProcessLockProvider(lockConfiguration, hadoopConfiguration);
    assertDoesNotThrow(() -> {
      localProcessLockProvider.lock();
    });
    assertDoesNotThrow(() -> {
      localProcessLockProvider.close();
    });
  }

  @Test
  public void testRedundantUnlock() {
    LocalProcessLockProvider localProcessLockProvider = new LocalProcessLockProvider(lockConfiguration, hadoopConfiguration);
    assertDoesNotThrow(() -> {
      localProcessLockProvider.lock();
    });
    assertDoesNotThrow(() -> {
      localProcessLockProvider.unlock();
    });
    assertThrows(HoodieLockException.class, () -> {
      localProcessLockProvider.unlock();
    });
  }

  @Test
  public void testUnlockWithoutLock() {
    LocalProcessLockProvider localProcessLockProvider = new LocalProcessLockProvider(lockConfiguration, hadoopConfiguration);
    assertThrows(HoodieLockException.class, () -> {
      localProcessLockProvider.unlock();
    });
  }
}
