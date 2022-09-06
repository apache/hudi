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
import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
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

public class TestInProcessLockProvider {

  private static final Logger LOG = LogManager.getLogger(TestInProcessLockProvider.class);
  private final Configuration hadoopConfiguration = new Configuration();
  private final LockConfiguration lockConfiguration = new LockConfiguration(new TypedProperties());

  @Test
  public void testLockAcquisition() {
    InProcessLockProvider inProcessLockProvider = new InProcessLockProvider(lockConfiguration, hadoopConfiguration);
    assertDoesNotThrow(() -> {
      inProcessLockProvider.lock();
    });
    assertDoesNotThrow(() -> {
      inProcessLockProvider.unlock();
    });
  }

  @Test
  public void testLockReAcquisitionBySameThread() {
    InProcessLockProvider inProcessLockProvider = new InProcessLockProvider(lockConfiguration, hadoopConfiguration);
    assertDoesNotThrow(() -> {
      inProcessLockProvider.lock();
    });
    assertThrows(HoodieLockException.class, () -> {
      inProcessLockProvider.lock();
    });
    assertDoesNotThrow(() -> {
      inProcessLockProvider.unlock();
    });
  }

  @Test
  public void testLockReAcquisitionByDifferentThread() {
    InProcessLockProvider inProcessLockProvider = new InProcessLockProvider(lockConfiguration, hadoopConfiguration);
    final AtomicBoolean writer2Completed = new AtomicBoolean(false);

    // Main test thread
    assertDoesNotThrow(() -> {
      inProcessLockProvider.lock();
    });

    // Another writer thread in parallel, should block
    // and later acquire the lock once it is released
    Thread writer2 = new Thread(new Runnable() {
      @Override
      public void run() {
        assertDoesNotThrow(() -> {
          inProcessLockProvider.lock();
        });
        assertDoesNotThrow(() -> {
          inProcessLockProvider.unlock();
        });
        writer2Completed.set(true);
      }
    });
    writer2.start();

    assertDoesNotThrow(() -> {
      inProcessLockProvider.unlock();
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
    InProcessLockProvider inProcessLockProvider = new InProcessLockProvider(lockConfiguration, hadoopConfiguration);
    Assertions.assertTrue(inProcessLockProvider.tryLock());
    assertDoesNotThrow(() -> {
      inProcessLockProvider.unlock();
    });
  }

  @Test
  public void testTryLockAcquisitionWithTimeout() {
    InProcessLockProvider inProcessLockProvider = new InProcessLockProvider(lockConfiguration, hadoopConfiguration);
    Assertions.assertTrue(inProcessLockProvider.tryLock(1, TimeUnit.MILLISECONDS));
    assertDoesNotThrow(() -> {
      inProcessLockProvider.unlock();
    });
  }

  @Test
  public void testTryLockReAcquisitionBySameThread() {
    InProcessLockProvider inProcessLockProvider = new InProcessLockProvider(lockConfiguration, hadoopConfiguration);
    Assertions.assertTrue(inProcessLockProvider.tryLock());
    assertThrows(HoodieLockException.class, () -> {
      inProcessLockProvider.tryLock(1, TimeUnit.MILLISECONDS);
    });
    assertDoesNotThrow(() -> {
      inProcessLockProvider.unlock();
    });
  }

  @Test
  public void testTryLockReAcquisitionByDifferentThread() {
    InProcessLockProvider inProcessLockProvider = new InProcessLockProvider(lockConfiguration, hadoopConfiguration);
    final AtomicBoolean writer2Completed = new AtomicBoolean(false);

    // Main test thread
    Assertions.assertTrue(inProcessLockProvider.tryLock());

    // Another writer thread
    Thread writer2 = new Thread(() -> {
      Assertions.assertFalse(inProcessLockProvider.tryLock(100L, TimeUnit.MILLISECONDS));
      writer2Completed.set(true);
    });
    writer2.start();
    try {
      writer2.join();
    } catch (InterruptedException e) {
      //
    }

    Assertions.assertTrue(writer2Completed.get());
    assertDoesNotThrow(() -> {
      inProcessLockProvider.unlock();
    });
  }

  @Test
  public void testTryUnLockByDifferentThread() {
    InProcessLockProvider inProcessLockProvider = new InProcessLockProvider(lockConfiguration, hadoopConfiguration);
    final AtomicBoolean writer3Completed = new AtomicBoolean(false);

    // Main test thread
    Assertions.assertTrue(inProcessLockProvider.tryLock());

    // Another writer thread
    Thread writer2 = new Thread(() -> {
      assertDoesNotThrow(() -> {
        inProcessLockProvider.unlock();
      });
    });
    writer2.start();
    try {
      writer2.join();
    } catch (InterruptedException e) {
      //
    }

    // try acquiring by diff thread. should fail. since main thread still have acquired the lock. if previous unblock by a different thread would have succeeded, this lock
    // acquisition would succeed.
    Thread writer3 = new Thread(() -> {
      Assertions.assertFalse(inProcessLockProvider.tryLock(50, TimeUnit.MILLISECONDS));
      writer3Completed.set(true);
    });
    writer3.start();
    try {
      writer3.join();
    } catch (InterruptedException e) {
      //
    }

    Assertions.assertTrue(writer3Completed.get());
    assertDoesNotThrow(() -> {
      // unlock by main thread should succeed.
      inProcessLockProvider.unlock();
    });
  }

  @Test
  public void testTryLockAcquisitionBeforeTimeOutFromTwoThreads() {
    final InProcessLockProvider inProcessLockProvider = new InProcessLockProvider(lockConfiguration, hadoopConfiguration);
    final int threadCount = 3;
    final long awaitMaxTimeoutMs = 2000L;
    final CountDownLatch latch = new CountDownLatch(threadCount);
    final AtomicBoolean writer1Completed = new AtomicBoolean(false);
    final AtomicBoolean writer2Completed = new AtomicBoolean(false);

    // Let writer1 get the lock first, then wait for others
    // to join the sync up point.
    Thread writer1 = new Thread(() -> {
      Assertions.assertTrue(inProcessLockProvider.tryLock());
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
        inProcessLockProvider.unlock();
      });
      writer1Completed.set(true);
    });
    writer1.start();

    // Writer2 will block on trying to acquire the lock
    // and will eventually get the lock before the timeout.
    Thread writer2 = new Thread(() -> {
      latch.countDown();
      Assertions.assertTrue(inProcessLockProvider.tryLock(awaitMaxTimeoutMs, TimeUnit.MILLISECONDS));
      assertDoesNotThrow(() -> {
        inProcessLockProvider.unlock();
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
    InProcessLockProvider inProcessLockProvider = new InProcessLockProvider(lockConfiguration, hadoopConfiguration);
    assertDoesNotThrow(() -> {
      inProcessLockProvider.lock();
    });
    assertDoesNotThrow(() -> {
      inProcessLockProvider.close();
    });
  }

  @Test
  public void testRedundantUnlock() {
    InProcessLockProvider inProcessLockProvider = new InProcessLockProvider(lockConfiguration, hadoopConfiguration);
    assertDoesNotThrow(() -> {
      inProcessLockProvider.lock();
    });
    assertDoesNotThrow(() -> {
      inProcessLockProvider.unlock();
    });
    assertDoesNotThrow(() -> {
      inProcessLockProvider.unlock();
    });
  }

  @Test
  public void testUnlockWithoutLock() {
    InProcessLockProvider inProcessLockProvider = new InProcessLockProvider(lockConfiguration, hadoopConfiguration);
    assertDoesNotThrow(() -> {
      inProcessLockProvider.unlock();
    });
  }
}
