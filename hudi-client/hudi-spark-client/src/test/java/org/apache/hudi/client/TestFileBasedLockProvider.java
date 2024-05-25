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

package org.apache.hudi.client;

import org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_EXPIRE_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_PATH_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY;
import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestFileBasedLockProvider {

  @TempDir
  Path tempDir;
  String basePath;
  LockConfiguration lockConfiguration;
  StorageConfiguration<Configuration> storageConf;

  @BeforeEach
  public void setUp() throws IOException {
    basePath = tempDir.toUri().getPath();
    Properties properties = new Properties();
    properties.setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, basePath);
    properties.setProperty(FILESYSTEM_LOCK_EXPIRE_PROP_KEY, "1");
    properties.setProperty(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "1000");
    properties.setProperty(LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY, "1000");
    properties.setProperty(LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY, "3");
    lockConfiguration = new LockConfiguration(properties);
    storageConf = getDefaultStorageConf();
  }

  @Test
  public void testAcquireLock() {
    FileSystemBasedLockProvider fileBasedLockProvider = new FileSystemBasedLockProvider(lockConfiguration, storageConf);
    assertTrue(fileBasedLockProvider.tryLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    fileBasedLockProvider.unlock();
  }

  @Test
  public void testAcquireLockWithDefaultPath() {
    lockConfiguration.getConfig().remove(FILESYSTEM_LOCK_PATH_PROP_KEY);
    lockConfiguration.getConfig().setProperty(HoodieWriteConfig.BASE_PATH.key(), basePath);
    FileSystemBasedLockProvider fileBasedLockProvider = new FileSystemBasedLockProvider(lockConfiguration, storageConf);
    assertTrue(fileBasedLockProvider.tryLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    fileBasedLockProvider.unlock();
    lockConfiguration.getConfig().setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, basePath);
  }

  @Test
  public void testUnLock() {
    FileSystemBasedLockProvider fileBasedLockProvider = new FileSystemBasedLockProvider(lockConfiguration, storageConf);
    assertTrue(fileBasedLockProvider.tryLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    fileBasedLockProvider.unlock();
    assertTrue(fileBasedLockProvider.tryLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
  }

  @Test
  public void testReentrantLock() {
    FileSystemBasedLockProvider fileBasedLockProvider = new FileSystemBasedLockProvider(lockConfiguration, storageConf);
    assertTrue(fileBasedLockProvider.tryLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    assertFalse(fileBasedLockProvider.tryLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    fileBasedLockProvider.unlock();
  }

  @Test
  public void testUnlockWithoutLock() {
    assertDoesNotThrow(() -> {
      FileSystemBasedLockProvider fileBasedLockProvider = new FileSystemBasedLockProvider(lockConfiguration, storageConf);
      fileBasedLockProvider.unlock();
    });
  }
}
