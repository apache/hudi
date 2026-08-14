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

import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_EXPIRE_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_PATH_PROP_KEY;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link FileSystemBasedLockProvider} against a local temp directory.
 */
public class TestFileSystemBasedLockProvider {

  @TempDir
  Path tempDir;

  private LockConfiguration lockConfiguration(String lockPath, int expireMinutes) {
    Properties props = new Properties();
    props.setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, lockPath);
    props.setProperty(FILESYSTEM_LOCK_EXPIRE_PROP_KEY, String.valueOf(expireMinutes));
    return new LockConfiguration(props);
  }

  private String lockDir(String name) {
    return tempDir.resolve(name).toString();
  }

  @Test
  public void testAcquireAndReleaseLock() {
    StorageConfiguration<?> storageConf = HoodieTestUtils.getDefaultStorageConf();
    FileSystemBasedLockProvider provider =
        new FileSystemBasedLockProvider(lockConfiguration(lockDir("acquire"), 0), storageConf);
    try {
      assertTrue(provider.tryLock(1, TimeUnit.SECONDS), "first acquisition should succeed");
      // getLock exposes the fully-qualified lock file path used on the backing storage.
      assertTrue(provider.getLock().endsWith("/lock"));
      provider.unlock();
      // After unlock the file is gone, so the lock is acquirable again.
      assertTrue(provider.tryLock(1, TimeUnit.SECONDS), "re-acquisition after unlock should succeed");
    } finally {
      provider.unlock();
      provider.close();
    }
  }

  @Test
  public void testConcurrentProvidersCannotBothHoldLock() {
    StorageConfiguration<?> storageConf = HoodieTestUtils.getDefaultStorageConf();
    LockConfiguration config = lockConfiguration(lockDir("contended"), 0);
    FileSystemBasedLockProvider holder = new FileSystemBasedLockProvider(config, storageConf);
    FileSystemBasedLockProvider contender = new FileSystemBasedLockProvider(config, storageConf);
    try {
      assertTrue(holder.tryLock(1, TimeUnit.SECONDS));
      // A non-expired lock owned by another provider blocks acquisition.
      assertFalse(contender.tryLock(1, TimeUnit.SECONDS), "second provider must not acquire a held lock");
      // The contender is able to read the current owner's lock info.
      assertTrue(contender.getCurrentOwnerLockInfo() != null && !contender.getCurrentOwnerLockInfo().isEmpty());
      holder.unlock();
      assertTrue(contender.tryLock(1, TimeUnit.SECONDS), "contender acquires after holder releases");
    } finally {
      holder.unlock();
      contender.unlock();
      holder.close();
    }
  }

  @Test
  public void testExpiredLockIsReclaimed() throws Exception {
    StorageConfiguration<?> storageConf = HoodieTestUtils.getDefaultStorageConf();
    String path = lockDir("expiry");
    // Expiry of 1 minute; we age the lock file deterministically rather than sleeping.
    LockConfiguration config = lockConfiguration(path, 1);
    FileSystemBasedLockProvider holder = new FileSystemBasedLockProvider(config, storageConf);
    FileSystemBasedLockProvider contender = new FileSystemBasedLockProvider(config, storageConf);
    try {
      assertTrue(holder.tryLock(1, TimeUnit.SECONDS));
      // Not yet expired: contender is blocked.
      assertFalse(contender.tryLock(1, TimeUnit.SECONDS));

      // Age the lock file well beyond the 1-minute expiry window.
      StoragePath lockFile = new StoragePath(path + StoragePath.SEPARATOR + "lock");
      HoodieStorage storage = HoodieStorageUtils.getStorage(lockFile.toString(), storageConf);
      storage.setModificationTime(lockFile, System.currentTimeMillis() - (5 * 60 * 1000L));

      // The expired lock file is deleted and the contender acquires it.
      assertTrue(contender.tryLock(1, TimeUnit.SECONDS), "expired lock should be reclaimable");
    } finally {
      holder.unlock();
      contender.unlock();
      contender.close();
    }
  }

  @Test
  public void testZeroExpiryNeverReclaims() throws Exception {
    StorageConfiguration<?> storageConf = HoodieTestUtils.getDefaultStorageConf();
    String path = lockDir("noexpiry");
    // Expiry of 0 disables reclamation entirely.
    LockConfiguration config = lockConfiguration(path, 0);
    FileSystemBasedLockProvider holder = new FileSystemBasedLockProvider(config, storageConf);
    FileSystemBasedLockProvider contender = new FileSystemBasedLockProvider(config, storageConf);
    try {
      assertTrue(holder.tryLock(1, TimeUnit.SECONDS));

      // Even a very old lock file must not be reclaimed when expiry is disabled.
      StoragePath lockFile = new StoragePath(path + StoragePath.SEPARATOR + "lock");
      HoodieStorage storage = HoodieStorageUtils.getStorage(lockFile.toString(), storageConf);
      storage.setModificationTime(lockFile, System.currentTimeMillis() - (60 * 60 * 1000L));

      assertFalse(contender.tryLock(1, TimeUnit.SECONDS), "zero expiry must never reclaim a lock");
    } finally {
      holder.unlock();
      contender.unlock();
      holder.close();
    }
  }

  @Test
  public void testUnlockWithoutLockIsNoOp() {
    StorageConfiguration<?> storageConf = HoodieTestUtils.getDefaultStorageConf();
    FileSystemBasedLockProvider provider =
        new FileSystemBasedLockProvider(lockConfiguration(lockDir("idempotent"), 0), storageConf);
    // Releasing when no lock is held must not raise.
    assertDoesNotThrow(provider::unlock);
    provider.close();
  }

  @Test
  public void testConstructorRejectsNegativeExpiry() {
    StorageConfiguration<?> storageConf = HoodieTestUtils.getDefaultStorageConf();
    LockConfiguration config = lockConfiguration(lockDir("bad"), -1);
    assertThrows(IllegalArgumentException.class,
        () -> new FileSystemBasedLockProvider(config, storageConf));
  }

  @Test
  public void testGetLockConfigProducesUsableProperties() {
    String tablePath = tempDir.resolve("table").toString();
    TypedProperties props = FileSystemBasedLockProvider.getLockConfig(tablePath);
    // The generated config points the lock provider at the table's auxiliary folder.
    assertTrue(props.getString(HoodieLockConfig.FILESYSTEM_LOCK_PATH.key()).startsWith(tablePath));
    assertEquals(FileSystemBasedLockProvider.class.getName(),
        props.getString(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key()));

    StorageConfiguration<?> storageConf = HoodieTestUtils.getDefaultStorageConf();
    FileSystemBasedLockProvider provider =
        new FileSystemBasedLockProvider(new LockConfiguration(props), storageConf);
    try {
      assertTrue(provider.tryLock(1, TimeUnit.SECONDS));
    } finally {
      provider.unlock();
      provider.close();
    }
  }

  @Test
  public void testLockPathDefaultsToMetafolderFromBasePath() {
    StorageConfiguration<?> storageConf = HoodieTestUtils.getDefaultStorageConf();
    Properties props = new Properties();
    props.setProperty(HoodieWriteConfig.BASE_PATH.key(), lockDir("defaultpath"));
    props.setProperty(FILESYSTEM_LOCK_EXPIRE_PROP_KEY, "0");
    FileSystemBasedLockProvider provider =
        new FileSystemBasedLockProvider(new LockConfiguration(props), storageConf);
    try {
      assertTrue(provider.tryLock(1, TimeUnit.SECONDS),
          "lock acquisition must work without an explicit lock path");
      // Without an explicit lock path the provider locks under the table metafolder.
      assertTrue(provider.getLock().endsWith(
          HoodieTableMetaClient.METAFOLDER_NAME + StoragePath.SEPARATOR + "lock"));
    } finally {
      provider.unlock();
      provider.close();
    }
  }

  @Test
  public void testSameProviderSecondTryLockFails() {
    StorageConfiguration<?> storageConf = HoodieTestUtils.getDefaultStorageConf();
    FileSystemBasedLockProvider provider =
        new FileSystemBasedLockProvider(lockConfiguration(lockDir("nonreentrant"), 0), storageConf);
    try {
      assertTrue(provider.tryLock(1, TimeUnit.SECONDS));
      // The file lock is not reentrant: a second tryLock by the same provider fails.
      assertFalse(provider.tryLock(1, TimeUnit.SECONDS));
    } finally {
      provider.unlock();
      provider.close();
    }
  }
}
