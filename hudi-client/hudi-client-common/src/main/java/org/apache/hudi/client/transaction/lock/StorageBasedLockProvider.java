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

import org.apache.hudi.client.transaction.lock.models.StorageLockData;
import org.apache.hudi.client.transaction.lock.models.StorageLockFile;
import org.apache.hudi.client.transaction.lock.models.HeartbeatManager;
import org.apache.hudi.client.transaction.lock.models.LockGetResult;
import org.apache.hudi.client.transaction.lock.models.LockProviderHeartbeatManager;
import org.apache.hudi.client.transaction.lock.models.LockUpdateResult;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.lock.LockProvider;
import org.apache.hudi.common.lock.LockState;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.hash.HashID;
import org.apache.hudi.config.StorageBasedLockConfig;
import org.apache.hudi.exception.HoodieLockException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StorageSchemes;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.lock.LockState.ACQUIRED;
import static org.apache.hudi.common.lock.LockState.ACQUIRING;
import static org.apache.hudi.common.lock.LockState.FAILED_TO_ACQUIRE;
import static org.apache.hudi.common.lock.LockState.FAILED_TO_RELEASE;
import static org.apache.hudi.common.lock.LockState.RELEASED;
import static org.apache.hudi.common.lock.LockState.RELEASING;
import static org.apache.hudi.common.table.HoodieTableMetaClient.LOCKS_FOLDER_NAME;

/**
 * A distributed filesystem storage based lock provider. This {@link LockProvider} implementation
 * leverages conditional writes to ensure transactional consistency for multi-writer scenarios.
 * The underlying storage client interface {@link StorageLock} is pluggable so it can be implemented for any
 * filesystem which supports conditional writes.
 */
@ThreadSafe
public class StorageBasedLockProvider implements LockProvider<StorageLockFile> {

  public static final String DEFAULT_TABLE_LOCK_FILE_NAME = "table_lock";
  // How long to wait before retrying lock acquisition in blocking calls.
  private static final long DEFAULT_LOCK_ACQUISITION_BUFFER_MS = 1000;
  // Maximum expected clock drift between two nodes.
  // This is similar idea as SkewAdjustingTimeGenerator.
  // In reality, within a single cloud provider all nodes share the same ntp
  // server
  // therefore we do not expect drift more than a few ms.
  // However, since our lock leases are pretty long, we can use a high buffer.
  private static final long CLOCK_DRIFT_BUFFER_MS = 500;

  // When we retry lock upserts, do so 5 times
  private static final long LOCK_UPSERT_RETRY_COUNT = 5;

  private static final Logger LOGGER = LoggerFactory.getLogger(StorageBasedLockProvider.class);

  Logger logger;

  // The lock service implementation which interacts with storage
  private final StorageLock lockService;

  private final long heartbeatInterval;
  private final long lockValidity;
  private final String ownerId;
  private final String lockFilePath;
  private final String bucketName;
  private final HeartbeatManager heartbeatManager;
  private transient Thread shutdownThread = null;

  @GuardedBy("this")
  private StorageLockFile currentLockObj = null;
  @GuardedBy("this")
  private boolean isClosed = false;

  private synchronized void setLock(StorageLockFile lockObj) {
    if (lockObj != null && !Objects.equals(lockObj.getOwner(), this.ownerId)) {
      throw new HoodieLockException("Owners do not match. Current lock owner: " + this.ownerId + " lock path: "
          + this.lockFilePath + " owner: " + lockObj.getOwner());
    }
    this.currentLockObj = lockObj;
  }

  /**
   * Default constructor for StorageBasedLockProvider, required by LockManager
   * to instantiate it using reflection.
   * 
   * @param lockConfiguration The lock configuration, should be transformable into
   *                          StorageBasedLockConfig
   * @param conf              Storage config, ignored.
   */
  public StorageBasedLockProvider(final LockConfiguration lockConfiguration, final StorageConfiguration<?> conf) {
    StorageBasedLockConfig config = new StorageBasedLockConfig.Builder()
        .fromProperties(lockConfiguration.getConfig()).build();
    heartbeatInterval = config.getHeartbeatPoll();
    lockValidity = config.getLockValidityTimeout();

    String configuredLocksLocation = config.getLocksLocation();

    // If not configured, recalculate the locks location as .hoodie/.locks;
    // otherwise (the lock location is configured), the configuration location is used as the folder
    // to which the lock file is written to, and the lock file name is determined by the table's base path
    String locksLocation = StringUtils.isNullOrEmpty(configuredLocksLocation)
        ? String.format("%s%s%s", config.getHudiTableBasePath(), StoragePath.SEPARATOR, LOCKS_FOLDER_NAME)
        : configuredLocksLocation;

    URI uri = parseURI(locksLocation);
    bucketName = uri.getHost(); // For most schemes, the bucket/container is the host.
    String folderName = uri.getPath(); // Path after the bucket/container.

    String fileName = StringUtils.isNullOrEmpty(configuredLocksLocation)
        ? DEFAULT_TABLE_LOCK_FILE_NAME
        : slugifyLockFolderFromBasePath(config.getHudiTableBasePath());

    lockFilePath = buildLockObjectPath(folderName, fileName);
    ownerId = UUID.randomUUID().toString();
    this.logger = LOGGER;
    this.heartbeatManager = new LockProviderHeartbeatManager(
        ownerId,
        heartbeatInterval,
        this::renewLock);

    try {
      this.lockService = (StorageLock) ReflectionUtils.loadClass(
          getLockServiceClassName(uri.getScheme()),
          new Class<?>[] { String.class, String.class, String.class, Properties.class },
          new Object[] { ownerId, bucketName, lockFilePath, lockConfiguration.getConfig() });
    } catch (Throwable e) {
      throw new HoodieLockException("Failed to load and initialize StorageLock", e);
    }
    shutdownThread = new Thread(() -> shutdown(true));
    Runtime.getRuntime().addShutdownHook(shutdownThread);
    logger.info("Instantiated new storage-based lock provider, owner: {}, lockfilePath: {}", ownerId, lockFilePath);
  }

  private URI parseURI(String location) {
    try {
      return new URI(location);
    } catch (URISyntaxException e) {
      throw new HoodieLockException("Unable to parse locks location as a URI: " + location, e);
    }
  }

  private static @NotNull String getLockServiceClassName(String scheme) {
    Option<StorageSchemes> schemeOptional = StorageSchemes.getStorageLockImplementationIfExists(scheme);
    if (schemeOptional.isPresent()) {
      return schemeOptional.get().getStorageLockClass();
    } else {
      throw new HoodieNotSupportedException("No implementation of StorageLock supports this scheme: " + scheme);
    }
  }

  @VisibleForTesting
  StorageBasedLockProvider(
      int heartbeatInterval,
      int lockValidity,
      String bucketName,
      String lockFilePath,
      String ownerId,
      HeartbeatManager heartbeatManager,
      StorageLock lockService,
      Logger logger) {
    this.heartbeatInterval = heartbeatInterval;
    this.lockValidity = lockValidity;
    this.bucketName = bucketName;
    this.lockFilePath = lockFilePath;
    this.heartbeatManager = heartbeatManager;
    this.lockService = lockService;
    this.ownerId = ownerId;
    this.logger = logger;
    logger.debug("Instantiating new Storage Based LP, owner: {}", ownerId);
    shutdownThread = new Thread(() -> shutdown(true));
    Runtime.getRuntime().addShutdownHook(shutdownThread);
  }

  // -----------------------------------------
  // BASE METHODS
  // -----------------------------------------

  @Override
  public synchronized StorageLockFile getLock() {
    return currentLockObj;
  }

  /**
   * Attempts to acquire the lock within the given timeout.
   */
  @Override
  public boolean tryLock(long time, TimeUnit unit) {
    long deadlineNanos = System.nanoTime() + unit.toNanos(time);

    while (System.nanoTime() < deadlineNanos) {
      try {
        if (tryLock()) {
          return true;
        }
        logDebugLockState(ACQUIRING);
        Thread.sleep(DEFAULT_LOCK_ACQUISITION_BUFFER_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new HoodieLockException(generateLockStateMessage(LockState.FAILED_TO_ACQUIRE), e);
      }
    }

    return false;
  }

  @Override
  public synchronized void close() {
    shutdown(false);
  }

  private synchronized void shutdown(boolean fromShutdownHook) {
    if (fromShutdownHook) {
      if (isClosed || !actuallyHoldsLock()) {
        return;
      }
      // Ensure we release the lock before shutdown
    } else {
      Runtime.getRuntime().removeShutdownHook(shutdownThread);
    }
    try {
      this.unlock();
    } catch (Exception e) {
      logger.error("Owner {}: Failed to unlock current lock.", ownerId, e);
    }
    try {
      this.lockService.close();
    } catch (Exception e) {
      logger.error("Owner {}: Lock service failed to close.", ownerId, e);
    }
    try {
      this.heartbeatManager.close();
    } catch (Exception e) {
      logger.error("Owner {}: Heartbeat manager failed to close.", ownerId, e);
    }

    this.isClosed = true;
  }

  private synchronized boolean isLockStillValid(StorageLockFile lock) {
    return !lock.isExpired() && !isCurrentTimeCertainlyOlderThanDistributedTime(lock.getValidUntil());
  }

  /**
   * Attempts a single pass to acquire the lock (non-blocking).
   * 
   * @return true if lock acquired, false otherwise
   */
  @Override
  public synchronized boolean tryLock() {
    assertHeartbeatManagerExists();
    assertUnclosed();
    logDebugLockState(ACQUIRING);
    if (actuallyHoldsLock()) {
      // Supports reentrant locks
      return true;
    }

    if (this.heartbeatManager.hasActiveHeartbeat()) {
      logger.error("Detected broken invariant: there is an active heartbeat without a lock being held.");
      // Breach of object invariant - we should never have an active heartbeat without
      // holding a lock.
      throw new HoodieLockException(generateLockStateMessage(FAILED_TO_ACQUIRE));
    }

    Pair<LockGetResult, StorageLockFile> latestLock = this.lockService.readCurrentLockFile();
    if (latestLock.getLeft() == LockGetResult.UNKNOWN_ERROR) {
      logInfoLockState(FAILED_TO_ACQUIRE, "Failed to get the latest lock status");
      // We were not able to determine whether a lock was present.
      return false;
    }

    if (latestLock.getLeft() == LockGetResult.SUCCESS && isLockStillValid(latestLock.getRight())) {
      String msg = String.format("Lock already held by %s", latestLock.getRight().getOwner());
      // Lock held by others.
      logInfoLockState(FAILED_TO_ACQUIRE, msg);
      return false;
    }

    // Try to acquire the lock
    StorageLockData newLockData = new StorageLockData(false, System.currentTimeMillis() + lockValidity, ownerId);
    Pair<LockUpdateResult, StorageLockFile> lockUpdateStatus = this.lockService.tryCreateOrUpdateLockFile(
        newLockData, latestLock.getLeft() == LockGetResult.NOT_EXISTS ? null : latestLock.getRight());
    if (lockUpdateStatus.getLeft() != LockUpdateResult.SUCCESS) {
      // failed to acquire the lock, indicates concurrent contention
      logInfoLockState(FAILED_TO_ACQUIRE);
      return false;
    }
    this.setLock(lockUpdateStatus.getRight());

    // There is a remote chance that
    // - after lock is acquired but before heartbeat starts the lock is expired.
    // - lock is acquired and heartbeat is up yet it does not run timely before the
    // lock is expired
    // It is mitigated by setting the lock validity period to a reasonably long
    // period to survive until heartbeat comes, plus
    // set the heartbeat interval relatively small enough.
    if (!this.heartbeatManager.startHeartbeatForThread(Thread.currentThread())) {
      // Precondition "no active heartbeat" is checked previously, so when
      // startHeartbeatForThread returns false,
      // we are confident no heartbeat thread is running.
      logErrorLockState(RELEASING, "We were unable to start the heartbeat!");
      tryExpireCurrentLock();
      return false;
    }

    logInfoLockState(ACQUIRED);
    return true;
  }

  /**
   * Determines whether this provider currently holds a valid lock.
   *
   * <p>
   * This method checks both the existence of a lock object and its validity. A
   * lock is considered
   * valid only if it exists and has not expired according to its timestamp.
   *
   * @return {@code true} if this provider holds a valid lock, {@code false}
   *         otherwise
   */
  private boolean actuallyHoldsLock() {
    return believesLockMightBeHeld() && isLockStillValid(getLock());
  }

  /**
   * Checks if this provider has a non-null lock object reference.
   *
   * <p>
   * A non-null lock object indicates that this provider has previously
   * **successfully** acquired a lock via
   * StorageBasedLockProvider##lock and has not yet **successfully** released
   * it via StorageBasedLockProvider#unlock().
   * It is merely an indicator that the lock might be held by this provider. To
   * truly certify we are the owner of the lock,
   * StorageBasedLockProvider#actuallyHoldsLock should be used.
   *
   * @return {@code true} if this provider has a non-null lock object,
   *         {@code false} otherwise
   * @see StorageBasedLockProvider#actuallyHoldsLock()
   */
  private boolean believesLockMightBeHeld() {
    return this.getLock() != null;
  }

  /**
   * Unlock by marking our current lock file "expired": true.
   */
  @Override
  public synchronized void unlock() {
    assertHeartbeatManagerExists();
    if (!believesLockMightBeHeld()) {
      return;
    }
    boolean believesNoLongerHoldsLock = true;

    // Try to stop the heartbeat first
    if (heartbeatManager.hasActiveHeartbeat()) {
      logger.debug("Owner {}: Gracefully shutting down heartbeat.", ownerId);
      believesNoLongerHoldsLock &= heartbeatManager.stopHeartbeat(true);
    }

    // Then expire the current lock.
    believesNoLongerHoldsLock &= tryExpireCurrentLock();
    if (!believesNoLongerHoldsLock) {
      throw new HoodieLockException(generateLockStateMessage(FAILED_TO_RELEASE));
    }
  }

  private void assertHeartbeatManagerExists() {
    if (heartbeatManager == null) {
      // broken function precondition.
      throw new HoodieLockException("Unexpected null heartbeatManager");
    }
  }

  private void assertUnclosed() {
    if (this.isClosed) {
      throw new HoodieLockException("Lock provider already closed");
    }
  }

  /**
   * Tries to expire the currently held lock.
   * 
   * @return True if we were successfully able to upload an expired lock.
   */
  private synchronized boolean tryExpireCurrentLock() {
    // It does not make sense to have heartbeat alive extending the lock lease while
    // here we are trying
    // to expire the lock.
    if (heartbeatManager.hasActiveHeartbeat()) {
      // broken function precondition.
      throw new HoodieLockException("Must stop heartbeat before expire lock file");
    }
    logDebugLockState(RELEASING);
    // Upload metadata that will unlock this lock.
    Pair<LockUpdateResult, StorageLockFile> result = this.lockService.tryCreateOrUpdateLockFileWithRetry(
        () -> new StorageLockData(true, this.getLock().getValidUntil(), ownerId),
        this.getLock(),
        // Keep retrying for the normal validity time.
        LOCK_UPSERT_RETRY_COUNT);
    switch (result.getLeft()) {
      case UNKNOWN_ERROR:
        // Here we do not know the state of the lock.
        logErrorLockState(FAILED_TO_RELEASE, "Lock state is unknown.");
        return false;
      case SUCCESS:
        logInfoLockState(RELEASED);
        setLock(null);
        return true;
      case ACQUIRED_BY_OTHERS:
        // As we are confident no lock is held by itself, clean up the cached lock object.
        // However this is an edge case, so warn.
        logWarnLockState(RELEASED, "lock should not have been acquired by others.");
        setLock(null);
        return true;
      default:
        throw new HoodieLockException("Unexpected lock update result: " + result.getLeft());
    }
  }

  /**
   * Renews (heartbeats) the current lock if we are the holder, it forcefully set
   * the expiration flag
   * to false and the lock expiration time to a later time in the future.
   * @return True if we successfully renewed the lock, false if not.
   */
  @VisibleForTesting
  protected synchronized boolean renewLock() {
    try {
      // If we don't hold the lock, no-op.
      if (!believesLockMightBeHeld()) {
        logger.warn("Owner {}: Cannot renew, no lock held by this process", ownerId);
        // No need to extend lock lease.
        return false;
      }

      long oldExpiration = getLock().getValidUntil();
      // Attempt conditional update, extend lock. There are 3 cases:
      // 1. Happy case: lock has not expired yet, we extend the lease to a longer
      // period.
      // 2. Corner case 1: lock is expired and is acquired by others, lock renewal
      // failed with ACQUIRED_BY_OTHERS.
      // 3. Corner case 2: lock is expired but no one has acquired it yet, lock
      // renewal "revived" the expired lock.
      // Please note we expect the corner cases almost never happens.
      // Action taken for corner case 2 is just a best effort mitigation. At least it
      // prevents further data corruption by
      // letting someone else acquire the lock.
      Pair<LockUpdateResult, StorageLockFile> currentLock = this.lockService.tryCreateOrUpdateLockFileWithRetry(
          () -> new StorageLockData(false, System.currentTimeMillis() + lockValidity, ownerId),
          getLock(),
          LOCK_UPSERT_RETRY_COUNT);
      switch (currentLock.getLeft()) {
        case ACQUIRED_BY_OTHERS:
          logger.error("Owner {}: Unable to renew lock as it is acquired by others.", ownerId);
          // No need to extend lock lease anymore.
          return false;
        case UNKNOWN_ERROR:
          // This could be transient, but unclear, we will let the heartbeat continue
          // normally.
          // If the next heartbeat run identifies our lock has expired we will error out.
          logger.warn("Owner {}: Unable to renew lock due to unknown error, could be transient.", ownerId);
          // Let heartbeat retry later.
          return true;
        case SUCCESS:
          // Only positive outcome
          this.setLock(currentLock.getRight());
          logger.info("Owner {}: Lock renewal successful. The renewal completes {} ms before expiration for lock {}.",
              ownerId, oldExpiration - System.currentTimeMillis(), lockFilePath);
          // Let heartbeat continue to renew lock lease again later.
          return true;
        default:
          throw new HoodieLockException("Unexpected lock update result: " + currentLock.getLeft());
      }
    } catch (Exception e) {
      logger.error("Owner {}: Exception occurred while renewing lock", ownerId, e);
      return false;
    }
  }

  // ---------
  // Utilities
  // ---------

  /**
   * Method to calculate whether a timestamp from a distributed source has
   * definitively occurred yet.
   */
  protected boolean isCurrentTimeCertainlyOlderThanDistributedTime(long epoch) {
    return System.currentTimeMillis() > epoch + CLOCK_DRIFT_BUFFER_MS;
  }

  private String buildLockObjectPath(String lockFolderName, String lockTableFileName) {
    // Normalize inputs by removing trailing slashes
    // We know lockTableFileName has already been parsed.
    if (lockFolderName.startsWith("/")) {
      lockFolderName = lockFolderName.substring(1);
    }

    // Append a slash only if one isn't already present.
    return lockFolderName + (lockFolderName.endsWith("/") ? "" : "/") + lockTableFileName + ".json";
  }

  private String slugifyLockFolderFromBasePath(String basePathKey) {
    // Remove the prefix once
    String cleanedPath = basePathKey.replaceFirst("^(gs://|s3://|s3a://)", "");

    // Generate the lock name
    return cleanedPath
        .replaceAll("[/\\\\]+", "-") // Replace slashes with dashes
        .replaceAll("[^0-9a-zA-Z_-]", "-") // Replace invalid characters
        .toLowerCase()
        .substring(Math.max(0, cleanedPath.length() - 40)) // Get last 40 characters
        + "_" + HashID.generateXXHashAsString(basePathKey, HashID.Size.BITS_64);
  }

  private String generateLockStateMessage(LockState state) {
    String threadName = Thread.currentThread().getName();
    return String.format("Owner %s: Lock file path %s, Thread %s, Storage based lock state %s", ownerId,
        lockFilePath, threadName, state.toString());
  }

  private static final String LOCK_STATE_LOGGER_MSG = "Owner {}: Lock file path {}, Thread {}, Storage based lock state {}";
  private static final String LOCK_STATE_LOGGER_MSG_WITH_INFO = "Owner {}: Lock file path {}, Thread {}, Storage based lock state {}, {}";

  private void logDebugLockState(LockState state) {
    logger.debug(LOCK_STATE_LOGGER_MSG, ownerId, lockFilePath, Thread.currentThread(), state);
  }

  private void logInfoLockState(LockState state) {
    logger.info(LOCK_STATE_LOGGER_MSG, ownerId, lockFilePath, Thread.currentThread(), state);
  }

  private void logInfoLockState(LockState state, String msg) {
    logger.info(LOCK_STATE_LOGGER_MSG_WITH_INFO, ownerId, lockFilePath, Thread.currentThread(), state, msg);
  }

  private void logWarnLockState(LockState state, String msg) {
    logger.warn(LOCK_STATE_LOGGER_MSG_WITH_INFO, ownerId, lockFilePath, Thread.currentThread(), state, msg);
  }

  private void logErrorLockState(LockState state, String msg) {
    logger.warn(LOCK_STATE_LOGGER_MSG_WITH_INFO, ownerId, lockFilePath, Thread.currentThread(), state, msg);

  }
}