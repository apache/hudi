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
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_EXPIRE_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_PATH_PROP_KEY;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
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

  /**
   * The lock-file operations used to synchronize on the {@code "lock"} String constant, which is interned
   * and therefore shared JVM-wide with every other {@code "lock"} literal. Unrelated code holding that
   * monitor blocked lock acquisition outright. {@link
   * org.apache.hudi.client.transaction.FileSystemBasedLockProviderTestClass} aliased it the same way until
   * this change gave it a private monitor too, so the unrelated thread below stands in for any remaining
   * {@code synchronized ("lock")} anywhere in the JVM.
   */
  @Test
  public void testAcquisitionIsNotBlockedByTheInternedLockLiteral() throws Exception {
    StorageConfiguration<?> storageConf = HoodieTestUtils.getDefaultStorageConf();
    FileSystemBasedLockProvider provider =
        new FileSystemBasedLockProvider(lockConfiguration(lockDir("interned"), 0), storageConf);
    CountDownLatch holding = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    // stands in for any other class in the JVM doing synchronized ("lock")
    Thread unrelated = new Thread(() -> {
      synchronized ("lock") {
        holding.countDown();
        try {
          release.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    });
    unrelated.setDaemon(true);
    // Everything below has to sit inside the try: if an assertion fails before release.countDown() runs,
    // the daemon thread parks on release.await() forever holding the JVM-wide monitor, and surefire's
    // reuseForks would carry that into every later test in the fork.
    try {
      unrelated.start();
      assertTrue(holding.await(10, TimeUnit.SECONDS), "the unrelated thread should hold the interned monitor");

      boolean gotLock = assertTimeoutPreemptively(Duration.ofSeconds(10),
          () -> provider.tryLock(1, TimeUnit.SECONDS),
          "tryLock never returned - it is blocked on the monitor held by the unrelated thread, which means "
              + "the provider is synchronizing on the interned \"lock\" literal again rather than on a "
              + "private monitor");
      assertTrue(gotLock,
          "acquisition must not wait on a monitor held by code that has nothing to do with Hudi");

      // unlock() and close() take the same monitor, so exercise them while it is still held. Without this
      // the test passes with either of them reverted to synchronized (LOCK_FILE_NAME).
      assertTimeoutPreemptively(Duration.ofSeconds(10),
          () -> {
            provider.unlock();
            provider.close();
          },
          "unlock/close never returned - they are blocked on the monitor held by the unrelated thread, "
              + "which means unlock()/close() are synchronizing on the interned \"lock\" literal again "
              + "rather than on a private monitor");
    } finally {
      release.countDown();
      provider.unlock();
      provider.close();
    }
  }

  /**
   * {@code reloadCurrentOwnerLockInfo} opened the lock file in the try-with-resources header, before its own
   * existence check, so a vanished lock file threw {@link java.io.FileNotFoundException} instead of clearing
   * the field. The caller swallows that, so the previous owner's payload was then reported as the current
   * one. Reading it back after the file is gone must yield the empty string, not the stale payload.
   */
  @Test
  public void testReloadCurrentOwnerLockInfoClearsWhenLockFileIsGone() {
    StorageConfiguration<?> storageConf = HoodieTestUtils.getDefaultStorageConf();
    FileSystemBasedLockProvider provider =
        new FileSystemBasedLockProvider(lockConfiguration(lockDir("reload"), 0), storageConf);
    try {
      assertTrue(provider.tryLock(1, TimeUnit.SECONDS));
      provider.reloadCurrentOwnerLockInfo();
      assertFalse(provider.getCurrentOwnerLockInfo().isEmpty(),
          "the holder's payload should have been read back while the lock file exists");

      provider.unlock();
      assertDoesNotThrow(provider::reloadCurrentOwnerLockInfo,
          "a missing lock file must not throw out of the reload");
      assertEquals("", provider.getCurrentOwnerLockInfo(),
          "a missing lock file must clear the owner info rather than leave the previous owner's payload");
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
