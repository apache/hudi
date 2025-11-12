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
import org.apache.hudi.storage.StorageConfiguration;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Tests {@code NoopLockProvider}.
 */
public class TestNoopLockProvider {

  private static final Logger LOG = LoggerFactory.getLogger(TestNoopLockProvider.class);
  private final StorageConfiguration<?> storageConf = getDefaultStorageConf();
  private final LockConfiguration lockConfiguration1;
  private final LockConfiguration lockConfiguration2;

  public TestNoopLockProvider() {
    TypedProperties properties = new TypedProperties();
    properties.put(HoodieCommonConfig.BASE_PATH.key(), "table1");
    lockConfiguration1 = new LockConfiguration(properties);
    properties.put(HoodieCommonConfig.BASE_PATH.key(), "table2");
    lockConfiguration2 = new LockConfiguration(properties);
  }

  @Test
  public void testLockAcquisition() {
    NoopLockProvider noopLockProvider = new NoopLockProvider(lockConfiguration1, storageConf);
    assertDoesNotThrow(() -> {
      noopLockProvider.lock();
    });
    assertDoesNotThrow(() -> {
      noopLockProvider.unlock();
    });
  }

  @Test
  public void testLockReAcquisitionBySameThread() {
    NoopLockProvider noopLockProvider = new NoopLockProvider(lockConfiguration1, storageConf);
    assertDoesNotThrow(() -> {
      noopLockProvider.lock();
    });
    assertDoesNotThrow(() -> {
      noopLockProvider.lock();
    });
    assertDoesNotThrow(() -> {
      noopLockProvider.unlock();
    });
    assertDoesNotThrow(() -> {
      noopLockProvider.lock();
    });
  }

  @Test
  public void testLockReAcquisitionBySameThreadWithTwoTables() {
    NoopLockProvider noopLockProvider1 = new NoopLockProvider(lockConfiguration1, storageConf);
    NoopLockProvider noopLockProvider2 = new NoopLockProvider(lockConfiguration2, storageConf);

    assertDoesNotThrow(() -> {
      noopLockProvider1.lock();
    });
    assertDoesNotThrow(() -> {
      noopLockProvider2.lock();
    });
    assertDoesNotThrow(() -> {
      noopLockProvider1.lock();
    });
    assertDoesNotThrow(() -> {
      noopLockProvider1.lock();
    });
    assertDoesNotThrow(() -> {
      noopLockProvider1.unlock();
    });
    assertDoesNotThrow(() -> {
      noopLockProvider2.unlock();
    });
  }

  @Test
  public void testLockReAcquisitionByDifferentThread() {
    NoopLockProvider noopLockProvider = new NoopLockProvider(lockConfiguration1, storageConf);
    final AtomicBoolean writer2Completed = new AtomicBoolean(false);

    // Main test thread
    assertDoesNotThrow(() -> {
      noopLockProvider.lock();
    });

    // Another writer thread in parallel, should be able to acquire the lock instantly
    Thread writer2 = new Thread(new Runnable() {
      @Override
      public void run() {
        assertDoesNotThrow(() -> {
          noopLockProvider.lock();
        });
        assertDoesNotThrow(() -> {
          noopLockProvider.unlock();
        });
        writer2Completed.set(true);
      }
    });
    writer2.start();

    assertDoesNotThrow(() -> {
      noopLockProvider.unlock();
    });

    try {
      writer2.join();
    } catch (InterruptedException e) {
      //
    }
    Assertions.assertTrue(writer2Completed.get());

    writer2.interrupt();
  }
}
