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

package org.apache.hudi.client.transaction.lock;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.lock.LockProvider;
import org.apache.hudi.common.lock.LockState;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieLockException;
import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StorageSchemes;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_EXPIRE_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_PATH_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY;
import static org.apache.hudi.common.table.HoodieTableMetaClient.AUXILIARYFOLDER_NAME;

/**
 * A FileSystem based lock. This {@link LockProvider} implementation allows to lock table operations
 * using DFS. Users might need to manually clean the Locker's path if writeClient crash and never run again.
 * NOTE: This only works for DFS with atomic create/delete operation
 */
@Slf4j
public class FileSystemBasedLockProvider implements LockProvider<String>, Serializable {
  private static final long serialVersionUID = 1L;
  private static final String LOCK_FILE_NAME = "lock";
  /**
   * Guards this provider's lock-file operations.
   *
   * <p>These blocks used to synchronize on {@link #LOCK_FILE_NAME}. That is a compile-time String constant,
   * so it is interned: any class anywhere in the JVM that synchronizes on the same {@code "lock"} literal
   * contends on the very same monitor and silently couples itself to Hudi's lock acquisition. A private
   * object cannot be aliased that way.
   *
   * <p>Kept static so that, within a classloader, the mutual-exclusion scope is unchanged by this fix.
   * The interned literal was additionally shared across classloaders. Losing that wider scope matters
   * only for what the exclusive create does not protect: the expiry-reclaim sequence in
   * {@code tryLock} (exists, checkIfExpired, delete, create), the release path ({@code unlock}'s
   * exists-then-delete and {@code close}'s unconditional delete), plus the exists-then-create window
   * on a store whose create is not atomic. All are already unsafe across processes, the normal
   * multi-writer deployment; the monitor never made them safe. Cross-process mutual exclusion rests
   * entirely on the exclusive-mode {@code storage.create(path, false)} in {@code acquireLock} being
   * atomic on the underlying store, which HDFS guarantees and local FS does not (its create is a
   * check-then-act); this monitor is defense in depth for in-JVM callers, not the correctness
   * mechanism.
   */
  private static final Object LOCK_FILE_MONITOR = new Object();
  private final int lockTimeoutMinutes;
  private final transient HoodieStorage storage;
  private final transient StoragePath lockFile;
  protected LockConfiguration lockConfiguration;
  // Transient because LockInfo is not Serializable and the constructor used to build it eagerly, so
  // every instance since 0.13.0 (HUDI-5377) failed Spark closure capture with
  // NotSerializableException, the HUDI-7782 bug class. Pinning serialVersionUID is compat-safe:
  // 0.12.x, the only line that ever serialized this class, computed a UID no 0.13.0+ build could
  // read anyway, and no Hudi code persists a provider outside intra-job closure capture.
  // Null-guarded in initLockInfo() for the deserialized copy; such a copy still cannot lock, since
  // storage and lockFile are transient with no readObject to rebuild them, as before this change.
  private transient SimpleDateFormat sdf;
  private transient LockInfo lockInfo;
  /**
   * Written under {@link #LOCK_FILE_MONITOR} on the {@code tryLock} path, with no monitor at all when
   * {@code reloadCurrentOwnerLockInfo()} is called directly, and exposed through the generated getter
   * without synchronization. Every in-repo caller reads it on the thread that called {@code tryLock}, so they
   * would be safe either way; {@code getCurrentOwnerLockInfo()} is public {@link LockProvider} API, and a
   * plugged-in caller may read it from another thread, which is what the modifier is for.
   */
  @Getter
  private volatile String currentOwnerLockInfo = "";

  public FileSystemBasedLockProvider(final LockConfiguration lockConfiguration, final StorageConfiguration<?> configuration) {
    checkRequiredProps(lockConfiguration);
    this.lockConfiguration = lockConfiguration;
    String lockDirectory = lockConfiguration.getConfig().getString(FILESYSTEM_LOCK_PATH_PROP_KEY, null);
    if (StringUtils.isNullOrEmpty(lockDirectory)) {
      // Match getLockConfig() so writers using either default share the same lock.
      lockDirectory = defaultLockPath(lockConfiguration.getConfig().getString(HoodieWriteConfig.BASE_PATH.key()));
    }
    this.lockTimeoutMinutes = lockConfiguration.getConfig().getInteger(FILESYSTEM_LOCK_EXPIRE_PROP_KEY);
    this.lockFile = new StoragePath(lockDirectory + StoragePath.SEPARATOR + LOCK_FILE_NAME);
    this.storage = HoodieStorageUtils.getStorage(this.lockFile.toString(), configuration);
    List<String> customSupportedFSs = lockConfiguration.getConfig().getStringList(HoodieCommonConfig.HOODIE_FS_ATOMIC_CREATION_SUPPORT.key(), ",", new ArrayList<>());
    if (!customSupportedFSs.contains(this.storage.getScheme()) && !StorageSchemes.isAtomicCreationSupported(this.storage.getScheme())) {
      throw new HoodieLockException("Unsupported scheme :" + this.storage.getScheme() + ", since this fs can not support atomic creation");
    }
  }

  public FileSystemBasedLockProvider(final LockConfiguration lockConfiguration, final StorageConfiguration<?> configuration, final HoodieLockMetrics lockMetrics) {
    this(lockConfiguration, configuration);
  }

  @Override
  public void close() {
    synchronized (LOCK_FILE_MONITOR) {
      try {
        storage.deleteFile(this.lockFile);
      } catch (IOException e) {
        throw new HoodieLockException(generateLogStatement(LockState.FAILED_TO_RELEASE), e);
      } finally {
        try {
          // HoodieHadoopStorage.close() is currently a no-op since Hadoop FileSystem
          // instances are shared within the JVM process lifecycle and cannot be
          // individually closed. This call is retained for HoodieStorage interface
          // contract correctness and to support future storage backends that may
          // implement close().
          storage.close();
        } catch (IOException closeEx) {
          log.warn("Failed to close HoodieStorage", closeEx);
        }
      }
    }
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) {
    try {
      synchronized (LOCK_FILE_MONITOR) {
        // Check whether lock is already expired, if so try to delete lock file
        if (storage.exists(this.lockFile)) {
          if (checkIfExpired()) {
            storage.deleteFile(this.lockFile);
            log.warn("Delete expired lock file: {}", this.lockFile);
          } else {
            reloadCurrentOwnerLockInfo();
            return false;
          }
        }
        acquireLock();
        return storage.exists(this.lockFile);
      }
    } catch (IOException | HoodieIOException e) {
      log.info(generateLogStatement(LockState.FAILED_TO_ACQUIRE), e);
      return false;
    }
  }

  @Override
  public void unlock() {
    synchronized (LOCK_FILE_MONITOR) {
      try {
        if (storage.exists(this.lockFile)) {
          storage.deleteFile(this.lockFile);
        }
      } catch (IOException io) {
        throw new HoodieIOException(generateLogStatement(LockState.FAILED_TO_RELEASE), io);
      }
    }
  }

  @Override
  public String getLock() {
    return this.lockFile.toString();
  }

  /**
   * A stat failure degrades to "not expired", failing safe toward the current holder rather than
   * reclaiming a lock whose age is unknown. {@code StorageBasedLockProvider} makes the same choice
   * for transient errors via {@code LockGetResult.UNKNOWN_ERROR}.
   */
  private boolean checkIfExpired() {
    if (lockTimeoutMinutes == 0) {
      return false;
    }
    try {
      long modificationTime = storage.getPathInfo(this.lockFile).getModificationTime();
      if (System.currentTimeMillis() - modificationTime > lockTimeoutMinutes * 60 * 1000L) {
        return true;
      }
    } catch (IOException | HoodieIOException e) {
      log.error("{} failed to get lockFile's modification time", generateLogStatement(LockState.ACQUIRING), e);
    }
    return false;
  }

  /**
   * Creates the lock file, failing if it already exists. The {@code false} in
   * {@code storage.create(path, false)} requests an exclusive-mode create, the provider's entire
   * cross-process mutual exclusion on stores whose create is atomic; every other check in this
   * class is advisory. Package-private so a test can pin the exclusive mode.
   */
  @VisibleForTesting
  void acquireLock() {
    try (OutputStream os = storage.create(this.lockFile, false)) {
      initLockInfo();
      os.write(StringUtils.getUTF8Bytes(lockInfo.toString()));
    } catch (IOException e) {
      throw new HoodieIOException(generateLogStatement(LockState.FAILED_TO_ACQUIRE), e);
    }
  }

  public void initLockInfo() {
    // Guarded: sdf and lockInfo are lazily built, SimpleDateFormat is not thread safe, and the
    // fields lost the final modifier's free safe publication when they became lazy.
    synchronized (LOCK_FILE_MONITOR) {
      if (lockInfo == null) {
        lockInfo = new LockInfo();
      }
      if (sdf == null) {
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
      }
      lockInfo.setLockCreateTime(sdf.format(System.currentTimeMillis()));
      lockInfo.setLockThreadName(Thread.currentThread().getName());
      lockInfo.setLockStacksInfo(Thread.currentThread().getStackTrace());
    }
  }

  /**
   * Reloads the payload written by whoever currently holds the lock file.
   *
   * <p>A missing lock file must clear the field rather than throw: the caller swallows exceptions from
   * this method, so an escaping {@link FileNotFoundException} would leave the previous owner's payload
   * behind for {@link LockManager} to report as the current owner. The miss can surface at {@code open}
   * or, on a store opted in via {@code hoodie.fs.atomic_creation.support} that fetches lazily (S3A,
   * for example), at the first read, so the catch spans the whole read instead of an up-front
   * existence check that leaves the delete-after-check window open.
   */
  public void reloadCurrentOwnerLockInfo() {
    try (InputStream is = storage.open(this.lockFile)) {
      this.currentOwnerLockInfo = FileIOUtils.readAsUTFString(is);
    } catch (FileNotFoundException e) {
      this.currentOwnerLockInfo = "";
    } catch (IOException e) {
      throw new HoodieIOException(generateLogStatement(LockState.FAILED_TO_ACQUIRE), e);
    }
  }

  protected String generateLogStatement(LockState state) {
    return StringUtils.join(state.name(), " lock at: ", getLock());
  }

  private void checkRequiredProps(final LockConfiguration config) {
    ValidationUtils.checkArgument(config.getConfig().getString(FILESYSTEM_LOCK_PATH_PROP_KEY, null) != null
        || config.getConfig().getString(HoodieWriteConfig.BASE_PATH.key(), null) != null);
    ValidationUtils.checkArgument(config.getConfig().getInteger(FILESYSTEM_LOCK_EXPIRE_PROP_KEY) >= 0);
  }

  /**
   * Returns a filesystem based lock config with given table path.
   */
  public static TypedProperties getLockConfig(String tablePath) {
    TypedProperties props = new TypedProperties();
    props.put(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(), FileSystemBasedLockProvider.class.getName());
    props.put(HoodieLockConfig.LOCK_ACQUIRE_WAIT_TIMEOUT_MS.key(), "2000");
    props.put(LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY, "200");
    props.put(HoodieLockConfig.FILESYSTEM_LOCK_EXPIRE.key(), "1");
    props.put(HoodieLockConfig.LOCK_ACQUIRE_CLIENT_NUM_RETRIES.key(), "30");
    props.put(HoodieLockConfig.FILESYSTEM_LOCK_PATH.key(), defaultLockPath(tablePath));
    return props;
  }

  /**
   * Returns the default lock file root path.
   *
   * <p>IMPORTANT: this path should be shared especially when there is engine cooperation.
   */
  private static String defaultLockPath(String tablePath) {
    return tablePath + StoragePath.SEPARATOR + AUXILIARYFOLDER_NAME;
  }
}
