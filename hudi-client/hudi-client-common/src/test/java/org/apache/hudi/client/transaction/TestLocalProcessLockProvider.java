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

import java.util.concurrent.TimeUnit;

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

    // Main test thread
    assertDoesNotThrow(() -> {
      localProcessLockProvider.lock();
    });

    // Another writer thread
    Thread writer2 = new Thread(new Runnable() {
      @Override
      public void run() {
        assertThrows(HoodieLockException.class, () -> {
          localProcessLockProvider.lock();
        });
      }
    });

    try {
      writer2.join();
    } catch (InterruptedException e) {
      //
    }

    assertDoesNotThrow(() -> {
      localProcessLockProvider.unlock();
    });
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

    // Main test thread
    Assertions.assertTrue(localProcessLockProvider.tryLock());

    // Another writer thread
    Thread writer2 = new Thread(new Runnable() {
      @Override
      public void run() {
        Assertions.assertFalse(localProcessLockProvider.tryLock());
      }
    });

    try {
      writer2.join();
    } catch (InterruptedException e) {
      //
    }

    assertDoesNotThrow(() -> {
      localProcessLockProvider.unlock();
    });
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
