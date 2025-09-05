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

import org.apache.hudi.client.transaction.lock.audit.AuditOperationState;
import org.apache.hudi.client.transaction.lock.audit.AuditService;
import org.apache.hudi.client.transaction.lock.audit.AuditServiceFactory;
import org.apache.hudi.client.transaction.lock.metrics.HoodieLockMetrics;
import org.apache.hudi.client.transaction.lock.models.HeartbeatManager;
import org.apache.hudi.client.transaction.lock.models.LockGetResult;
import org.apache.hudi.client.transaction.lock.models.LockProviderHeartbeatManager;
import org.apache.hudi.client.transaction.lock.models.LockUpsertResult;
import org.apache.hudi.client.transaction.lock.models.StorageLockData;
import org.apache.hudi.client.transaction.lock.models.StorageLockFile;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.lock.LockProvider;
import org.apache.hudi.common.lock.LockState;
import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
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
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.hudi.common.config.LockConfiguration.DEFAULT_LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS;
import static org.apache.hudi.common.lock.LockState.ACQUIRED;
import static org.apache.hudi.common.lock.LockState.ACQUIRING;
import static org.apache.hudi.common.lock.LockState.FAILED_TO_ACQUIRE;
import static org.apache.hudi.common.lock.LockState.FAILED_TO_RELEASE;
import static org.apache.hudi.common.lock.LockState.RELEASED;
import static org.apache.hudi.common.lock.LockState.RELEASING;

/**
 * A distributed filesystem storage based lock provider. This {@link LockProvider} implementation
 * leverages conditional writes to ensure transactional consistency for multi-writer scenarios.
 * The underlying storage client interface {@link StorageLockClient} is pluggable so it can be implemented for any
 * filesystem which supports conditional writes.
 */
@ThreadSafe
public class StorageBasedLockProvider implements LockProvider<StorageLockFile> {

  public static final String DEFAULT_TABLE_LOCK_FILE_NAME = "table_lock.json";
  // How long to wait before retrying lock acquisition in blocking calls.
  // Maximum expected clock drift between two nodes.
  // This is similar idea as SkewAdjustingTimeGenerator.
  // In reality, within a single cloud provider all nodes share the same ntp
  // server
  // therefore we do not expect drift more than a few ms.
  // However, since our lock leases are pretty long, we can use a high buffer.
  private static final long CLOCK_DRIFT_BUFFER_MS = 500;

  private static final Logger LOGGER = LoggerFactory.getLogger(StorageBasedLockProvider.class);

  // Use for testing
  private final Logger logger;

  // The lock service implementation which interacts with storage
  private final StorageLockClient storageLockClient;

  private final long lockValiditySecs;
  private final String ownerId;
  private final String lockFilePath;
  private final HeartbeatManager heartbeatManager;
  private final transient Thread shutdownThread;
  private final Option<HoodieLockMetrics> hoodieLockMetrics;
  private final Option<AuditService> auditService;

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
    this(
            UUID.randomUUID().toString(),
            lockConfiguration.getConfig(),
            LockProviderHeartbeatManager::new,
            getStorageLockClientClassName(),
            LOGGER,
        null);
  }

  /**
   * Constructor for StorageBasedLockProvider with HoodieLockMetrics support.
   * This constructor allows lock providers to access metrics for fine-grained metrics collection.
   * 
   * @param lockConfiguration The lock configuration, should be transformable into
   *                          StorageBasedLockConfig
   * @param conf              Storage config, ignored.
   * @param metrics           HoodieLockMetrics instance for metrics collection
   */
  public StorageBasedLockProvider(final LockConfiguration lockConfiguration, final StorageConfiguration<?> conf, final HoodieLockMetrics metrics) {
    this(
            UUID.randomUUID().toString(),
            lockConfiguration.getConfig(),
            LockProviderHeartbeatManager::new,
            getStorageLockClientClassName(),
            LOGGER,
            metrics);
  }

  private static Functions.Function3<String, String, TypedProperties, StorageLockClient> getStorageLockClientClassName() {
    return (ownerId, lockFilePath, lockConfig) -> {
      try {
        return (StorageLockClient) ReflectionUtils.loadClass(
                getLockServiceClassName(new URI(lockFilePath).getScheme()),
                new Class<?>[]{String.class, String.class, Properties.class},
                new Object[]{ownerId, lockFilePath, lockConfig});
      } catch (Throwable e) {
        throw new HoodieLockException("Failed to load and initialize StorageLock", e);
      }
    };
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
      String ownerId,
      TypedProperties properties,
      Functions.Function3<String, Long, Supplier<Boolean>, HeartbeatManager> heartbeatManagerLoader,
      Functions.Function3<String, String, TypedProperties, StorageLockClient> storageLockClientLoader,
      Logger logger,
      HoodieLockMetrics hoodieLockMetrics) {
    StorageBasedLockConfig config = new StorageBasedLockConfig.Builder().fromProperties(properties).build();
    long heartbeatPollSeconds = config.getHeartbeatPollSeconds();
    this.lockValiditySecs = config.getValiditySeconds();
    String lockFolderPath = StorageLockClient.getLockFolderPath(config.getHudiTableBasePath());
    this.lockFilePath = String.format("%s%s%s", lockFolderPath, StoragePath.SEPARATOR, DEFAULT_TABLE_LOCK_FILE_NAME);
    this.heartbeatManager = heartbeatManagerLoader.apply(ownerId, TimeUnit.SECONDS.toMillis(heartbeatPollSeconds), this::renewLock);
    this.storageLockClient = storageLockClientLoader.apply(ownerId, lockFilePath, properties);
    this.ownerId = ownerId;
    this.logger = logger;
    this.hoodieLockMetrics = Option.ofNullable(hoodieLockMetrics);
    this.auditService = AuditServiceFactory.createLockProviderAuditService(
        ownerId, config.getHudiTableBasePath(), storageLockClient,
        this::calculateLockExpiration, this::actuallyHoldsLock);
    shutdownThread = new Thread(() -> shutdown(true));
    Runtime.getRuntime().addShutdownHook(shutdownThread);
    logger.info("Instantiated new storage-based lock provider, owner: {}, lockfilePath: {}", ownerId, lockFilePath);
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
        logDebugLockState(ACQUIRING);
        if (tryLock()) {
          return true;
        }
        Thread.sleep(Long.parseLong(DEFAULT_LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        hoodieLockMetrics.ifPresent(HoodieLockMetrics::updateLockInterruptedMetric);
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
      // Try to expire the lock from the shutdown hook.
      if (!isClosed && actuallyHoldsLock()) {
        tryExpireCurrentLock(true);
      }
      // Do not execute any further actions, mark closed
      this.isClosed = true;
      return;
    } else {
      try {
        tryRemoveShutdownHook();
      } catch (IllegalStateException e) {
        logger.warn("Owner {}: Failed to remove shutdown hook, JVM is already shutting down.", ownerId, e);
      }
    }
    try {
      this.unlock();
    } catch (Exception e) {
      logger.error("Owner {}: Failed to unlock current lock.", ownerId, e);
    }
    try {
      this.storageLockClient.close();
    } catch (Exception e) {
      logger.error("Owner {}: Lock service failed to close.", ownerId, e);
    }
    try {
      this.heartbeatManager.close();
    } catch (Exception e) {
      logger.error("Owner {}: Heartbeat manager failed to close.", ownerId, e);
    }
    try {
      this.auditService.ifPresent(auditService -> {
        try {
          auditService.close();
        } catch (Exception e) {
          logger.error("Owner {}: Audit service failed to close.", ownerId, e);
        }
      });
    } catch (Exception e) {
      logger.error("Owner {}: Failed to close audit service.", ownerId, e);
    }

    this.isClosed = true;
  }

  @VisibleForTesting
  void tryRemoveShutdownHook() {
    Runtime.getRuntime().removeShutdownHook(shutdownThread);
  }

  private synchronized boolean isLockStillValid(StorageLockFile lock) {
    if (lock.isExpired()) {
      return false;
    } else if (isCurrentTimeCertainlyOlderThanDistributedTime(lock.getValidUntilMs())) {
      hoodieLockMetrics.ifPresent(HoodieLockMetrics::updateLockDanglingMetric);
      logWarnLockState(ACQUIRING, "Found a dangling expired lock with owner " + lock.getOwner());
      return false;
    }

    return true;
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
      hoodieLockMetrics.ifPresent(HoodieLockMetrics::updateLockProviderFatalErrorMetric);
      throw new HoodieLockException(generateLockStateMessage(FAILED_TO_ACQUIRE));
    }

    Pair<LockGetResult, Option<StorageLockFile>> latestLock = this.storageLockClient.readCurrentLockFile();
    if (latestLock.getLeft() == LockGetResult.UNKNOWN_ERROR) {
      logInfoLockState(FAILED_TO_ACQUIRE, "Failed to get the latest lock status");
      hoodieLockMetrics.ifPresent(HoodieLockMetrics::updateLockStateUnknownMetric);
      // We were not able to determine whether a lock was present.
      return false;
    }

    if (latestLock.getLeft() == LockGetResult.SUCCESS && isLockStillValid(latestLock.getRight().get())) {
      String msg = String.format("Lock already held by %s", latestLock.getRight().get().getOwner());
      // Lock held by others.
      logInfoLockState(FAILED_TO_ACQUIRE, msg);
      return false;
    }

    // Try to acquire the lock
    long acquisitionTimestamp = getCurrentEpochMs();
    long lockExpirationMs = calculateLockExpiration();
    StorageLockData newLockData = new StorageLockData(false, lockExpirationMs, ownerId);
    Pair<LockUpsertResult, Option<StorageLockFile>> lockUpdateStatus = this.storageLockClient.tryUpsertLockFile(
        newLockData,
        latestLock.getRight());
    if (lockUpdateStatus.getLeft() != LockUpsertResult.SUCCESS) {
      // failed to acquire the lock, indicates concurrent contention
      logInfoLockState(FAILED_TO_ACQUIRE);
      hoodieLockMetrics.ifPresent(HoodieLockMetrics::updateLockAcquirePreconditionFailureMetric);
      return false;
    }
    this.setLock(lockUpdateStatus.getRight().get());
    hoodieLockMetrics.ifPresent(metrics -> metrics.updateLockExpirationDeadlineMetric(
        (int) (lockUpdateStatus.getRight().get().getValidUntilMs() - getCurrentEpochMs())));

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
      hoodieLockMetrics.ifPresent(HoodieLockMetrics::updateLockProviderFatalErrorMetric);
      tryExpireCurrentLock(false);
      return false;
    }

    logInfoLockState(ACQUIRED);
    recordAuditOperation(AuditOperationState.START, acquisitionTimestamp);
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
    believesNoLongerHoldsLock &= tryExpireCurrentLock(false);
    if (!believesNoLongerHoldsLock) {
      hoodieLockMetrics.ifPresent(HoodieLockMetrics::updateLockReleaseFailureMetric);
      throw new HoodieLockException(generateLockStateMessage(FAILED_TO_RELEASE));
    }
  }

  private void assertHeartbeatManagerExists() {
    if (heartbeatManager == null) {
      // broken function precondition.
      hoodieLockMetrics.ifPresent(HoodieLockMetrics::updateLockProviderFatalErrorMetric);
      throw new HoodieLockException("Unexpected null heartbeatManager");
    }
  }

  private void assertUnclosed() {
    if (this.isClosed) {
      hoodieLockMetrics.ifPresent(HoodieLockMetrics::updateLockProviderFatalErrorMetric);
      throw new HoodieLockException("Lock provider already closed");
    }
  }

  /**
   * Tries to expire the currently held lock.
   * @param fromShutdownHook Whether we are attempting best effort quick unlock from shutdown hook.
   * @return True if we were successfully able to upload an expired lock.
   */
  private synchronized boolean tryExpireCurrentLock(boolean fromShutdownHook) {
    // It does not make sense to have heartbeat alive extending the lock lease while
    // here we are trying
    // to expire the lock.
    if (!fromShutdownHook && heartbeatManager.hasActiveHeartbeat()) {
      // broken function precondition.
      throw new HoodieLockException("Must stop heartbeat before expire lock file");
    }
    logDebugLockState(RELEASING);
    // Upload metadata that will unlock this lock.
    StorageLockData expiredLockData = new StorageLockData(true, this.getLock().getValidUntilMs(), ownerId);
    Pair<LockUpsertResult, Option<StorageLockFile>> result;
    result = this.storageLockClient.tryUpsertLockFile(expiredLockData, Option.of(this.getLock()));
    switch (result.getLeft()) {
      case UNKNOWN_ERROR:
        // Here we do not know the state of the lock.
        logErrorLockState(FAILED_TO_RELEASE, "Lock state is unknown.");
        hoodieLockMetrics.ifPresent(HoodieLockMetrics::updateLockStateUnknownMetric);
        return false;
      case SUCCESS:
        logInfoLockState(RELEASED);
        recordAuditOperation(AuditOperationState.END, System.currentTimeMillis());
        setLock(null);
        return true;
      case ACQUIRED_BY_OTHERS:
        // As we are confident no lock is held by itself, clean up the cached lock object.
        logErrorLockState(RELEASED, "lock should not have been acquired by others.");
        setLock(null);
        hoodieLockMetrics.ifPresent(HoodieLockMetrics::updateLockAcquiredByOthersErrorMetric);
        return true;
      default:
        hoodieLockMetrics.ifPresent(HoodieLockMetrics::updateLockReleaseFailureMetric);
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

      long oldExpirationMs = getLock().getValidUntilMs();
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
      long acquisitionTimestamp = getCurrentEpochMs();
      long lockExpirationMs = calculateLockExpiration();
      Pair<LockUpsertResult, Option<StorageLockFile>> currentLock = this.storageLockClient.tryUpsertLockFile(
          new StorageLockData(false, lockExpirationMs, ownerId),
          Option.of(getLock()));
      switch (currentLock.getLeft()) {
        case ACQUIRED_BY_OTHERS:
          logger.error("Owner {}: Unable to renew lock as it is acquired by others.", ownerId);
          hoodieLockMetrics.ifPresent(HoodieLockMetrics::updateLockAcquiredByOthersErrorMetric);
          // No need to extend lock lease anymore.
          return false;
        case UNKNOWN_ERROR:
          // This could be transient, but unclear, we will let the heartbeat continue
          // normally.
          // If the next heartbeat run identifies our lock has expired we will error out.
          logger.warn("Owner {}: Unable to renew lock due to unknown error, could be transient.", ownerId);
          hoodieLockMetrics.ifPresent(HoodieLockMetrics::updateLockStateUnknownMetric);
          // Let heartbeat retry later.
          return true;
        case SUCCESS:
          // Only positive outcome
          this.setLock(currentLock.getRight().get());
          hoodieLockMetrics.ifPresent(metrics -> metrics.updateLockExpirationDeadlineMetric(
              (int) (oldExpirationMs - getCurrentEpochMs())));
          logger.info("Owner {}: Lock renewal successful. The renewal completes {} ms before expiration for lock {}.",
              ownerId, oldExpirationMs - getCurrentEpochMs(), lockFilePath);
          recordAuditOperation(AuditOperationState.RENEW, acquisitionTimestamp);
          // Let heartbeat continue to renew lock lease again later.
          return true;
        default:
          throw new HoodieLockException("Unexpected lock update result: " + currentLock.getLeft());
      }
    } catch (Exception e) {
      logger.error("Owner {}: Exception occurred while renewing lock", ownerId, e);
      hoodieLockMetrics.ifPresent(HoodieLockMetrics::updateLockProviderFatalErrorMetric);
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
  protected boolean isCurrentTimeCertainlyOlderThanDistributedTime(long epochMs) {
    return getCurrentEpochMs() > epochMs + CLOCK_DRIFT_BUFFER_MS;
  }

  private String generateLockStateMessage(LockState state) {
    String threadName = Thread.currentThread().getName();
    return String.format(
            "Owner %s: Lock file path %s, Thread %s, Storage based lock state %s",
            ownerId,
            lockFilePath,
            threadName,
            state.toString());
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
    logger.error(LOCK_STATE_LOGGER_MSG_WITH_INFO, ownerId, lockFilePath, Thread.currentThread(), state, msg);
  }

  @VisibleForTesting
  long getCurrentEpochMs() {
    return System.currentTimeMillis();
  }

  /**
   * Calculates the lock expiration time based on current time and validity period.
   * 
   * @return Lock expiration time in milliseconds
   */
  @VisibleForTesting
  long calculateLockExpiration() {
    return getCurrentEpochMs() + TimeUnit.SECONDS.toMillis(lockValiditySecs);
  }

  /**
   * Helper method to record audit operations.
   */
  private void recordAuditOperation(AuditOperationState state, long timestamp) {
    auditService.ifPresent(service -> {
      try {
        service.recordOperation(state, timestamp);
      } catch (Exception e) {
        // Log but don't fail the lock operation due to recording failures
        logger.warn("Owner {}: Failed to record audit operation {}: {}", ownerId, state, e.getMessage());
      }
    });
  }
}
