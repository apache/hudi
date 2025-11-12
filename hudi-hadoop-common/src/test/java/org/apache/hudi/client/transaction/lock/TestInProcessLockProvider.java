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

package org.apache.hudi.client.transaction.lock;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieLockException;
import org.apache.hudi.storage.StorageConfiguration;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestInProcessLockProvider {

  private static final Logger LOG = LoggerFactory.getLogger(TestInProcessLockProvider.class);
  private final StorageConfiguration<?> storageConf = getDefaultStorageConf();
  private final LockConfiguration lockConfiguration1;
  private final LockConfiguration lockConfiguration2;

  public TestInProcessLockProvider() {
    TypedProperties properties = new TypedProperties();
    properties.put(HoodieCommonConfig.BASE_PATH.key(), "table1");
    lockConfiguration1 = new LockConfiguration(properties);
    properties.put(HoodieCommonConfig.BASE_PATH.key(), "table2");
    lockConfiguration2 = new LockConfiguration(properties);
  }

  @Test
  public void testLockIdentity() throws InterruptedException {
    // The lifecycle of an InProcessLockProvider should not affect the singleton lock
    // for a single table, i.e., all three writers should hold the same underlying lock instance
    // on the same table.
    // Writer 1:   lock |----------------| unlock and close
    // Writer 2:   try lock   |      ...    lock |------| unlock and close
    // Writer 3:                          try lock  | ...  lock |------| unlock and close
    List<InProcessLockProvider> lockProviderList = new ArrayList<>();
    InProcessLockProvider lockProvider1 = new InProcessLockProvider(lockConfiguration1, storageConf);
    lockProviderList.add(lockProvider1);
    AtomicBoolean writer1Completed = new AtomicBoolean(false);
    AtomicBoolean writer2TryLock = new AtomicBoolean(false);
    AtomicBoolean writer2Locked = new AtomicBoolean(false);
    AtomicBoolean writer2Completed = new AtomicBoolean(false);
    AtomicBoolean writer3TryLock = new AtomicBoolean(false);
    AtomicBoolean writer3Completed = new AtomicBoolean(false);

    // Writer 1
    assertDoesNotThrow(() -> {
      LOG.info("Writer 1 tries to acquire the lock.");
      lockProvider1.lock();
      LOG.info("Writer 1 acquires the lock.");
    });
    // Writer 2 thread in parallel, should block
    // and later acquire the lock once it is released
    Thread writer2 = new Thread(() -> {
      InProcessLockProvider lockProvider2 = new InProcessLockProvider(lockConfiguration1, storageConf);
      lockProviderList.add(lockProvider2);
      assertDoesNotThrow(() -> {
        LOG.info("Writer 2 tries to acquire the lock.");
        writer2TryLock.set(true);
        lockProvider2.lock();
        LOG.info("Writer 2 acquires the lock.");
      });
      writer2Locked.set(true);

      while (!writer3TryLock.get()) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      assertDoesNotThrow(() -> {
        lockProvider2.unlock();
        LOG.info("Writer 2 releases the lock.");
      });
      lockProvider2.close();
      LOG.info("Writer 2 closes the lock provider.");
      writer2Completed.set(true);
    });

    Thread writer3 = new Thread(() -> {
      while (!writer2Locked.get() || !writer1Completed.get()) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      // Lock instance of Writer 3 should be held by Writer 2
      InProcessLockProvider lockProvider3 = new InProcessLockProvider(lockConfiguration1, storageConf);
      lockProviderList.add(lockProvider3);
      boolean isLocked = lockProvider3.getLock().isWriteLocked();
      if (!isLocked) {
        writer3TryLock.set(true);
        throw new RuntimeException("The lock instance in Writer 3 should be held by Writer 2: "
            + lockProvider3.getLock());
      }
      assertDoesNotThrow(() -> {
        LOG.info("Writer 3 tries to acquire the lock.");
        writer3TryLock.set(true);
        lockProvider3.lock();
        LOG.info("Writer 3 acquires the lock.");
      });

      assertDoesNotThrow(() -> {
        lockProvider3.unlock();
        LOG.info("Writer 3 releases the lock.");
      });
      lockProvider3.close();
      LOG.info("Writer 3 closes the lock provider.");
      writer3Completed.set(true);
    });

    writer2.start();
    writer3.start();

    while (!writer2TryLock.get()) {
      Thread.sleep(100);
    }

    assertDoesNotThrow(() -> {
      lockProvider1.unlock();
      LOG.info("Writer 1 releases the lock.");
      lockProvider1.close();
      LOG.info("Writer 1 closes the lock provider.");
      writer1Completed.set(true);
    });

    try {
      writer2.join();
      writer3.join();
    } catch (InterruptedException e) {
      // Ignore any exception
    }
    Assertions.assertTrue(writer2Completed.get());
    Assertions.assertTrue(writer3Completed.get());
    Assertions.assertEquals(lockProviderList.get(0).getLock(), lockProviderList.get(1).getLock());
    Assertions.assertEquals(lockProviderList.get(1).getLock(), lockProviderList.get(2).getLock());

    writer2.interrupt();
    writer3.interrupt();
  }

  @Test
  public void testLockAcquisition() {
    InProcessLockProvider inProcessLockProvider = new InProcessLockProvider(lockConfiguration1, storageConf);
    assertDoesNotThrow(() -> {
      inProcessLockProvider.lock();
    });
    assertDoesNotThrow(() -> {
      inProcessLockProvider.unlock();
    });
  }

  @Test
  public void testLockReAcquisitionBySameThread() {
    InProcessLockProvider inProcessLockProvider = new InProcessLockProvider(lockConfiguration1, storageConf);
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
  public void testLockReAcquisitionBySameThreadWithTwoTables() {
    InProcessLockProvider inProcessLockProvider1 = new InProcessLockProvider(lockConfiguration1, storageConf);
    InProcessLockProvider inProcessLockProvider2 = new InProcessLockProvider(lockConfiguration2, storageConf);

    assertDoesNotThrow(() -> {
      inProcessLockProvider1.lock();
    });
    assertDoesNotThrow(() -> {
      inProcessLockProvider2.lock();
    });
    assertThrows(HoodieLockException.class, () -> {
      inProcessLockProvider2.lock();
    });
    assertThrows(HoodieLockException.class, () -> {
      inProcessLockProvider1.lock();
    });
    assertDoesNotThrow(() -> {
      inProcessLockProvider1.unlock();
    });
    assertDoesNotThrow(() -> {
      inProcessLockProvider2.unlock();
    });
  }

  @Test
  public void testLockReAcquisitionByDifferentThread() {
    InProcessLockProvider inProcessLockProvider = new InProcessLockProvider(lockConfiguration1, storageConf);
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

    writer2.interrupt();
  }

  @Test
  public void testLockReAcquisitionByDifferentThreadWithTwoTables() {
    InProcessLockProvider inProcessLockProvider1 = new InProcessLockProvider(lockConfiguration1, storageConf);
    InProcessLockProvider inProcessLockProvider2 = new InProcessLockProvider(lockConfiguration2, storageConf);

    final AtomicBoolean writer2Stream1Completed = new AtomicBoolean(false);
    final AtomicBoolean writer2Stream2Completed = new AtomicBoolean(false);

    // Main test thread
    assertDoesNotThrow(() -> {
      inProcessLockProvider1.lock();
    });
    assertDoesNotThrow(() -> {
      inProcessLockProvider2.lock();
    });

    // Another writer thread in parallel, should block
    // and later acquire the lock once it is released
    Thread writer2Stream1 = new Thread(new Runnable() {
      @Override
      public void run() {
        assertDoesNotThrow(() -> {
          inProcessLockProvider1.lock();
        });
        assertDoesNotThrow(() -> {
          inProcessLockProvider1.unlock();
        });
        writer2Stream1Completed.set(true);
      }
    });
    Thread writer2Stream2 = new Thread(new Runnable() {
      @Override
      public void run() {
        assertDoesNotThrow(() -> {
          inProcessLockProvider2.lock();
        });
        assertDoesNotThrow(() -> {
          inProcessLockProvider2.unlock();
        });
        writer2Stream2Completed.set(true);
      }
    });

    writer2Stream1.start();
    writer2Stream2.start();

    assertDoesNotThrow(() -> {
      inProcessLockProvider1.unlock();
    });
    assertDoesNotThrow(() -> {
      inProcessLockProvider2.unlock();
    });

    try {
      writer2Stream1.join();
      writer2Stream2.join();
    } catch (InterruptedException e) {
      //
    }
    Assertions.assertTrue(writer2Stream1Completed.get());
    Assertions.assertTrue(writer2Stream2Completed.get());

    writer2Stream1.interrupt();
    writer2Stream2.interrupt();
  }

  @Test
  public void testTryLockAcquisition() {
    InProcessLockProvider inProcessLockProvider = new InProcessLockProvider(lockConfiguration1, storageConf);
    Assertions.assertTrue(inProcessLockProvider.tryLock());
    assertDoesNotThrow(() -> {
      inProcessLockProvider.unlock();
    });
  }

  @Test
  public void testTryLockAcquisitionWithTimeout() {
    InProcessLockProvider inProcessLockProvider = new InProcessLockProvider(lockConfiguration1, storageConf);
    Assertions.assertTrue(inProcessLockProvider.tryLock(1, TimeUnit.MILLISECONDS));
    assertDoesNotThrow(() -> {
      inProcessLockProvider.unlock();
    });
  }

  @Test
  public void testTryLockReAcquisitionBySameThread() {
    InProcessLockProvider inProcessLockProvider = new InProcessLockProvider(lockConfiguration1, storageConf);
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
    InProcessLockProvider inProcessLockProvider = new InProcessLockProvider(lockConfiguration1, storageConf);
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

    writer2.interrupt();
  }

  @Test
  public void testTryUnLockByDifferentThread() {
    InProcessLockProvider inProcessLockProvider = new InProcessLockProvider(lockConfiguration1, storageConf);
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

    writer2.interrupt();
    writer3.interrupt();
  }

  @Test
  public void testTryLockAcquisitionBeforeTimeOutFromTwoThreads() {
    final InProcessLockProvider inProcessLockProvider = new InProcessLockProvider(lockConfiguration1, storageConf);
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

    writer1.interrupt();
    writer2.interrupt();
  }

  @Test
  public void testLockReleaseByClose() {
    InProcessLockProvider inProcessLockProvider = new InProcessLockProvider(lockConfiguration1, storageConf);
    assertDoesNotThrow(() -> {
      inProcessLockProvider.lock();
    });
    assertDoesNotThrow(() -> {
      inProcessLockProvider.close();
    });
  }

  @Test
  public void testRedundantUnlock() {
    InProcessLockProvider inProcessLockProvider = new InProcessLockProvider(lockConfiguration1, storageConf);
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
    InProcessLockProvider inProcessLockProvider = new InProcessLockProvider(lockConfiguration1, storageConf);
    assertDoesNotThrow(() -> {
      inProcessLockProvider.unlock();
    });
  }
}
