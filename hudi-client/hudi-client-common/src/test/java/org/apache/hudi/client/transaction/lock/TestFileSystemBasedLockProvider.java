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

import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_EXPIRE_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_PATH_PROP_KEY;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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
  public void testAcquisitionIsNotBlockedByInternedLockLiteral() throws Exception {
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
    boolean closed = false;
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
      closed = true;
    } finally {
      release.countDown();
      // Clean up only if the in-try unlock/close did not run: closing twice is safe today solely
      // because HoodieHadoopStorage.close() is a no-op, which other backends need not honor.
      if (!closed) {
        provider.unlock();
        provider.close();
      }
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
      // Regression: the reload used to evaluate storage.open in its try-with-resources header, so a
      // vanished lock file threw FileNotFoundException out of the reload instead of clearing the field,
      // and the previous owner's payload kept being reported as the current one.
      assertDoesNotThrow(contender::reloadCurrentOwnerLockInfo,
          "a missing lock file must not throw out of the reload");
      assertEquals("", contender.getCurrentOwnerLockInfo(),
          "a missing lock file must clear the owner info rather than leave the previous owner's payload");
      assertTrue(contender.tryLock(1, TimeUnit.SECONDS), "contender acquires after holder releases");
    } finally {
      holder.unlock();
      contender.unlock();
      holder.close();
    }
  }

  /**
   * {@code storage.create(path, false)} in {@code acquireLock} is the provider's entire cross-process
   * mutual exclusion; the in-JVM monitor cannot serialize two processes, and {@code tryLock} returns
   * early on an existing file, so no public-API test can reach the already-exists arm. Pin the create
   * semantics directly: a second create must fail and must leave the winner's payload untouched.
   */
  @Test
  public void testAcquireLockRefusesToOverwriteExistingLockFile() {
    StorageConfiguration<?> storageConf = HoodieTestUtils.getDefaultStorageConf();
    LockConfiguration config = lockConfiguration(lockDir("atomiccreate"), 0);
    FileSystemBasedLockProvider winner = new FileSystemBasedLockProvider(config, storageConf);
    FileSystemBasedLockProvider loser = new FileSystemBasedLockProvider(config, storageConf);
    try {
      winner.acquireLock();
      winner.reloadCurrentOwnerLockInfo();
      String winnerPayload = winner.getCurrentOwnerLockInfo();
      assertFalse(winnerPayload.isEmpty(), "the winner's payload should be on storage");

      HoodieIOException thrown = assertThrows(HoodieIOException.class, loser::acquireLock,
          "a second create on an existing lock file must fail, not overwrite");
      assertInstanceOf(FileAlreadyExistsException.class, thrown.getCause(),
          "the loser must fail on the atomic create itself");

      loser.reloadCurrentOwnerLockInfo();
      assertEquals(winnerPayload, loser.getCurrentOwnerLockInfo(),
          "the losing attempt must leave the winner's payload untouched");
    } finally {
      winner.unlock();
      winner.close();
      loser.close();
    }
  }

  /**
   * The class declares {@link java.io.Serializable} but could never actually serialize: the
   * constructor built a non-serializable {@code LockInfo} eagerly (since HUDI-5377), so every
   * instance threw {@code NotSerializableException}. The mutable helpers are transient and lazy now;
   * a round trip of a lock-holding provider must work. Serializing while the lock is held is the
   * pin -- post-fix the field is null until first acquisition, and null serializes regardless of the
   * modifier. The copy's storage handles are transient and stay null, as they were before this change.
   */
  @Test
  public void testJavaSerializationRoundTrip() throws Exception {
    StorageConfiguration<?> storageConf = HoodieTestUtils.getDefaultStorageConf();
    FileSystemBasedLockProvider provider =
        new FileSystemBasedLockProvider(lockConfiguration(lockDir("serde"), 0), storageConf);
    try {
      assertTrue(provider.tryLock(1, TimeUnit.SECONDS), "the provider must hold the lock so lockInfo is populated");
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
        out.writeObject(provider);
      }
      try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
        FileSystemBasedLockProvider copy = assertInstanceOf(FileSystemBasedLockProvider.class, in.readObject());
        assertDoesNotThrow(copy::initLockInfo, "the transient helpers must rebuild on the deserialized copy");
      }
    } finally {
      provider.unlock();
      provider.close();
    }
  }

  /**
   * Pins the delete-after-check window in {@code reloadCurrentOwnerLockInfo}: a storage whose
   * {@code exists()} says true while {@code open()} reports the file missing reproduces a lock file
   * vanishing between an existence check and the open. With the earlier exists-precheck shape the
   * {@code FileNotFoundException} escaped as {@code HoodieIOException} and the previous owner's
   * payload survived; the catch must clear the field instead.
   */
  @Test
  public void testReloadClearsOwnerWhenLockFileVanishesAfterExistsCheck() {
    // Stub injection via hoodie.storage.class, per TestRequestHandler's precedent; the conf is
    // fresh per test, so the mutation cannot leak into other tests.
    StorageConfiguration<?> storageConf = HoodieTestUtils.getDefaultStorageConf();
    storageConf.set(HoodieStorageConfig.HOODIE_STORAGE_CLASS.key(), VanishingLockFileStorage.class.getName());
    FileSystemBasedLockProvider provider =
        new FileSystemBasedLockProvider(lockConfiguration(lockDir("vanish"), 0), storageConf);
    assertDoesNotThrow(provider::reloadCurrentOwnerLockInfo,
        "a lock file that vanishes between an existence check and the open must not throw");
    assertEquals("", provider.getCurrentOwnerLockInfo(),
        "a vanished lock file must clear the owner info rather than leave the previous owner's payload");
  }

  /**
   * A real IO failure must not be mistaken for a missing lock file: only {@code FileNotFoundException}
   * clears the owner info, anything else escapes as {@code HoodieIOException} and the caller keeps the
   * last known owner. Widening the reload's catch to plain {@code IOException} must fail this, and so
   * must clearing the field on the rethrow path.
   */
  @Test
  public void testReloadPropagatesNonMissingFileFailures() {
    StorageConfiguration<?> storageConf = HoodieTestUtils.getDefaultStorageConf();
    storageConf.set(HoodieStorageConfig.HOODIE_STORAGE_CLASS.key(), FailingOpenStorage.class.getName());
    FileSystemBasedLockProvider provider =
        new FileSystemBasedLockProvider(lockConfiguration(lockDir("ioerror"), 0), storageConf);
    try {
      FailingOpenStorage.failing = false;
      assertTrue(provider.tryLock(1, TimeUnit.SECONDS));
      provider.reloadCurrentOwnerLockInfo();
      String lastKnownOwner = provider.getCurrentOwnerLockInfo();
      assertFalse(lastKnownOwner.isEmpty(), "the seed reload should have read the holder's payload");

      FailingOpenStorage.failing = true;
      assertThrows(HoodieIOException.class, provider::reloadCurrentOwnerLockInfo,
          "an IO failure that is not a missing file must escape, not silently clear the owner info");
      assertEquals(lastKnownOwner, provider.getCurrentOwnerLockInfo(),
          "an IO failure must keep the last known owner, not clear it");
    } finally {
      FailingOpenStorage.failing = false;
      provider.unlock();
      provider.close();
    }
  }

  /**
   * A failing stat on the lock file must degrade to "not expired": the holder keeps the lock and the
   * contender loses cleanly while still reporting who holds it. Treating the failure as expired would
   * steal a live lock; rethrowing would skip the owner-info reload.
   */
  @Test
  public void testStatFailureDuringExpiryCheckDoesNotStealLock() throws Exception {
    String path = lockDir("statfail");
    FileSystemBasedLockProvider holder =
        new FileSystemBasedLockProvider(lockConfiguration(path, 1), HoodieTestUtils.getDefaultStorageConf());
    StorageConfiguration<?> stubConf = HoodieTestUtils.getDefaultStorageConf();
    stubConf.set(HoodieStorageConfig.HOODIE_STORAGE_CLASS.key(), FailingGetPathInfoStorage.class.getName());
    FileSystemBasedLockProvider contender =
        new FileSystemBasedLockProvider(lockConfiguration(path, 1), stubConf);
    try {
      assertTrue(holder.tryLock(1, TimeUnit.SECONDS));
      Path lockPath = Paths.get(holder.getLock());
      byte[] holderPayload = Files.readAllBytes(lockPath);
      assertFalse(contender.tryLock(1, TimeUnit.SECONDS),
          "a stat failure must not let the contender treat a live lock as expired");
      assertArrayEquals(holderPayload, Files.readAllBytes(lockPath),
          "the holder's lock file must survive the contender's failed expiry check untouched");
      assertFalse(contender.getCurrentOwnerLockInfo().isEmpty(),
          "the loser must still report the current owner");
    } finally {
      holder.unlock();
      holder.close();
      contender.close();
    }
  }

  /**
   * The catch in {@code tryLock} is the cross-process loser outcome: a failing create must surface as
   * a clean false, not as an exception. Pointing the lock directory at an existing regular file makes
   * {@code exists()} on {@code <file>/lock} false and the create fail deterministically.
   */
  @Test
  public void testTryLockReturnsFalseWhenCreateFails() throws Exception {
    StorageConfiguration<?> storageConf = HoodieTestUtils.getDefaultStorageConf();
    Path plainFile = tempDir.resolve("plainfile");
    Files.createFile(plainFile);
    FileSystemBasedLockProvider provider =
        new FileSystemBasedLockProvider(lockConfiguration(plainFile.toString(), 0), storageConf);
    try {
      assertFalse(provider.tryLock(1, TimeUnit.SECONDS),
          "a failing create must surface as tryLock returning false, not as an exception");
      assertEquals("", provider.getCurrentOwnerLockInfo(),
          "a failure before any reload must report an empty owner, not null");
    } finally {
      provider.close();
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
    assertEquals(tablePath + StoragePath.SEPARATOR + HoodieTableMetaClient.AUXILIARYFOLDER_NAME,
        props.getString(HoodieLockConfig.FILESYSTEM_LOCK_PATH.key()));
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
  public void testExplicitLockConfigAndBasePathFallbackResolveToSamePath() {
    // Regression guard for the cross-engine lock-path divergence: getLockConfig()'s explicit default
    // and the constructor's BASE_PATH-derived fallback must point at the very same lock file, or a
    // writer relying on one would not be mutually excluded from a writer relying on the other.
    StorageConfiguration<?> storageConf = HoodieTestUtils.getDefaultStorageConf();
    String tablePath = tempDir.resolve("sharedtable").toString();

    // 1) Provider built from getLockConfig() (explicit FILESYSTEM_LOCK_PATH).
    FileSystemBasedLockProvider fromLockConfig =
        new FileSystemBasedLockProvider(
            new LockConfiguration(FileSystemBasedLockProvider.getLockConfig(tablePath)), storageConf);

    // 2) Provider built with only BASE_PATH set, exercising the constructor fallback.
    Properties fallbackProps = new Properties();
    fallbackProps.setProperty(HoodieWriteConfig.BASE_PATH.key(), tablePath);
    fallbackProps.setProperty(FILESYSTEM_LOCK_EXPIRE_PROP_KEY, "0");
    FileSystemBasedLockProvider fromFallback =
        new FileSystemBasedLockProvider(new LockConfiguration(fallbackProps), storageConf);

    // getLock() is a pure accessor of the resolved lock-file path (no storage I/O), so comparing it
    // is enough and needs no acquire/release.
    assertEquals(fromLockConfig.getLock(), fromFallback.getLock(),
        "explicit default and BASE_PATH fallback must resolve to the same lock file");
    assertTrue(fromLockConfig.getLock()
        .endsWith(HoodieTableMetaClient.AUXILIARYFOLDER_NAME + StoragePath.SEPARATOR + "lock"));
  }

  @Test
  public void testLockPathDefaultsToAuxiliaryFolderFromBasePath() {
    StorageConfiguration<?> storageConf = HoodieTestUtils.getDefaultStorageConf();
    Properties props = new Properties();
    props.setProperty(HoodieWriteConfig.BASE_PATH.key(), lockDir("defaultpath"));
    props.setProperty(FILESYSTEM_LOCK_EXPIRE_PROP_KEY, "0");
    FileSystemBasedLockProvider provider =
        new FileSystemBasedLockProvider(new LockConfiguration(props), storageConf);
    try {
      assertTrue(provider.tryLock(1, TimeUnit.SECONDS),
          "lock acquisition must work without an explicit lock path");
      // Without an explicit lock path the provider locks under the table auxiliary folder.
      assertTrue(provider.getLock().endsWith(
          HoodieTableMetaClient.AUXILIARYFOLDER_NAME + StoragePath.SEPARATOR + "lock"));
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

  /**
   * Storage whose {@code exists()} always says true while {@code open()} reports the file missing,
   * simulating a lock file deleted between the two calls. The {@code exists()} override only matters
   * to regressed reload shapes that re-add an existence precheck; the fixed reload never calls it.
   * Loaded reflectively via {@code hoodie.storage.class}, hence public static with the
   * {@code (StoragePath, StorageConfiguration)} constructor.
   */
  public static class VanishingLockFileStorage extends HoodieHadoopStorage {
    public VanishingLockFileStorage(StoragePath path, StorageConfiguration<?> conf) {
      super(path, conf);
    }

    @Override
    public boolean exists(StoragePath path) {
      return true;
    }

    @Override
    public InputStream open(StoragePath path) throws IOException {
      throw new FileNotFoundException("vanished: " + path);
    }
  }

  /**
   * Storage whose {@code open()} fails with a plain IO error, never a missing-file signal, once the
   * flag is raised; real until then, so a test can seed state through it first.
   */
  public static class FailingOpenStorage extends HoodieHadoopStorage {
    static volatile boolean failing = false;

    public FailingOpenStorage(StoragePath path, StorageConfiguration<?> conf) {
      super(path, conf);
    }

    @Override
    public InputStream open(StoragePath path) throws IOException {
      if (failing) {
        throw new IOException("simulated IO failure: " + path);
      }
      return super.open(path);
    }
  }

  /** Storage whose {@code getPathInfo()} always fails, simulating a stat error during expiry checks. */
  public static class FailingGetPathInfoStorage extends HoodieHadoopStorage {
    public FailingGetPathInfoStorage(StoragePath path, StorageConfiguration<?> conf) {
      super(path, conf);
    }

    @Override
    public StoragePathInfo getPathInfo(StoragePath path) throws IOException {
      throw new IOException("simulated stat failure: " + path);
    }
  }
}
