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

import org.apache.hudi.client.transaction.lock.models.ConditionalWriteLockData;
import org.apache.hudi.client.transaction.lock.models.ConditionalWriteLockFile;
import org.apache.hudi.client.transaction.lock.models.HeartbeatManager;
import org.apache.hudi.client.transaction.lock.models.LockProviderHeartbeatManager;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.lock.LockProvider;
import org.apache.hudi.common.lock.LockState;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.hash.HashID;
import org.apache.hudi.exception.HoodieLockException;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.lock.LockState.ACQUIRED;
import static org.apache.hudi.common.lock.LockState.ACQUIRING;
import static org.apache.hudi.common.lock.LockState.FAILED_TO_ACQUIRE;
import static org.apache.hudi.common.lock.LockState.FAILED_TO_RELEASE;
import static org.apache.hudi.common.lock.LockState.RELEASED;
import static org.apache.hudi.common.lock.LockState.RELEASING;

@ThreadSafe
public class ConditionalWriteLockProvider
        implements LockProvider<ConditionalWriteLockFile> {

  // How long to wait before retrying lock acquisition in blocking calls.
  private static final long DEFAULT_LOCK_ACQUISITION_BUFFER = 1000;
  // Maximum expected clock drift between two nodes.
  // This is similar idea as SkewAdjustingTimeGenerator.
  // In reality, within a single cloud provider all nodes share the same ntp server
  // therefore we do not expect drift more than a few ms.
  // However, since our lock leases are pretty long, we can use a high buffer.
  private static final long CLOCK_DRIFT_BUFFER_MS = 500;

  // When we retry lock upserts, do so 5 times
  private static final long LOCK_UPSERT_RETRY_COUNT = 5;

  private static final String GCS_LOCK_SERVICE_CLASS_NAME = "org.apache.hudi.gcp.transaction.lock.GCSConditionalWriteLockService";

  // The static logger to be shared across contexts
  private static final Logger DEFAULT_LOGGER = LoggerFactory.getLogger(ConditionalWriteLockProvider.class);
  private static final String LOCK_STATE_LOGGER_MSG = "Owner {}: Lock file path {}, Thread {}, Conditional Write lock state {}";
  private static final String LOCK_STATE_LOGGER_MSG_WITH_INFO = "Owner {}: Lock file path {}, Thread {}, Conditional Write lock state {}, {}";

  @VisibleForTesting
  Logger logger;

  // Concurrency safety, currently locking all the important parts.
  private final Object localMutex = new Object();

  // The lock service implementation which interacts with cloud storage.
  private final ConditionalWriteLockService lockService;

  // Local variables
  private final long heartbeatIntervalMs;
  private final long lockValidityMs;
  private final String ownerId;
  private final String lockFilePath;
  private final String bucketName;
  private final HeartbeatManager heartbeatManager;

  // Provide a place to store the "current lock object"
  @GuardedBy("localMutex")
  private ConditionalWriteLockFile currentLockObj = null;

  private void setLock(ConditionalWriteLockFile lockObj) {
    if (lockObj != null && !Objects.equals(lockObj.getOwner(), this.ownerId)) {
      throw new HoodieLockException("Owners do not match! Current LP owner: " + this.ownerId + " lock path: " + this.lockFilePath + " owner: " + lockObj.getOwner());
    }
    this.currentLockObj = lockObj;
  }

  /**
   * Default constructor for ConditionalWriteLockProvider, required by LockManager to instantiate it using reflection.
   * @param lockConfiguration The lock configuration, should be transformable into ConditionalWriteLockConfig
   * @param conf Hadoop config, ignored.
   */
  public ConditionalWriteLockProvider(final LockConfiguration lockConfiguration, final Configuration conf) {
    ConditionalWriteLockConfig config = new ConditionalWriteLockConfig.Builder().fromProperties(lockConfiguration.getConfig()).build();
    heartbeatIntervalMs = config.getHeartbeatPollMs();
    lockValidityMs = config.getLockValidityTimeoutMs();

    // Extract and print the bucket (or container) and path
    try {
      URI uri = new URI(config.getLocksLocation());
      bucketName = uri.getHost(); // For most schemes, the bucket/container is the host
      String pathName = uri.getPath(); // Path after the bucket/container
      lockFilePath = buildLockObjectPath(pathName, slugifyLockFolderFromBasePath(config.getHudiTableBasePath()));
    } catch (URISyntaxException e) {
      throw new HoodieLockException("Unable to parse locks location as a URI:" + config.getLocksLocation(), e);
    }

    ownerId = UUID.randomUUID().toString();
    this.logger = DEFAULT_LOGGER;
    this.heartbeatManager = new LockProviderHeartbeatManager(
        ownerId,
        heartbeatIntervalMs,
        this::renewLock
    );

    String lockServiceClassName;
    if (config.getLocksLocation().startsWith("gs://") || config.getLocksLocation().startsWith("https://storage.googleapis.com")) {
      // Create GCS service
      lockServiceClassName = GCS_LOCK_SERVICE_CLASS_NAME;
    } else {
      throw new HoodieLockException("No implementation of ConditionalWriteLockService supports this lock storage location");
    }

    try {
      this.lockService = ReflectionUtils.loadClass(
          lockServiceClassName,
          new Class<?>[] {String.class, String.class, String.class},
          new Object[] {ownerId, bucketName, lockFilePath});
    } catch (Throwable e) {
      throw new HoodieLockException("Failed to load and initialize ConditionalWriteLockService", e);
    }

    logger.info("Instantiated new Conditional Write LP, owner: {}", ownerId);
  }

  @VisibleForTesting
  ConditionalWriteLockProvider(
      int heartbeatIntervalMs,
      int lockValidityMs,
      String bucketName,
      String lockFilePath,
      String ownerId,
      HeartbeatManager heartbeatManager,
      ConditionalWriteLockService lockService,
      Logger logger) {
    this.heartbeatIntervalMs = heartbeatIntervalMs;
    this.lockValidityMs = lockValidityMs;
    this.bucketName = bucketName;
    this.lockFilePath = lockFilePath;
    this.heartbeatManager = heartbeatManager;
    this.lockService = lockService;
    this.ownerId = ownerId;
    this.logger = logger;
    logger.info("Instantiating new Conditional Write LP, owner: {}", ownerId);
  }

  // -----------------------------------------
  // BASE METHODS
  // -----------------------------------------

  @Override
  public ConditionalWriteLockFile getLock() {
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
        logger.debug(
            LOCK_STATE_LOGGER_MSG,
            ownerId,
            lockFilePath,
            Thread.currentThread(),
            ACQUIRING);
        Thread.sleep(DEFAULT_LOCK_ACQUISITION_BUFFER);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new HoodieLockException(generateLockStateMessage(LockState.FAILED_TO_ACQUIRE), e);
      }
    }

    return false;
  }

  @Override
  public void close() {
    synchronized (localMutex) {
      try {
        this.lockService.close();
      } catch (Exception e) {
        logger.error("Owner {}: Lock service failed to close.", this.ownerId, e);
      }
      try {
        this.heartbeatManager.close();
      } catch (Exception e) {
        logger.error("Owner {}: Heartbeat manager failed to close.", this.ownerId, e);
      }
    }
  }

  /**
   * Attempts a single pass to acquire the lock (non-blocking).
   * @return true if lock acquired, false otherwise
   */
  @Override
  public boolean tryLock() {
    synchronized (localMutex) {
      logger.debug(
          LOCK_STATE_LOGGER_MSG,
          ownerId,
          lockFilePath,
          Thread.currentThread(),
          ACQUIRING);
      if (this.getLock() != null && !this.getLock().isExpired() && !isCurrentTimeCertainlyOlderThanDistributedTime(this.getLock().getValidUntil())) {
        // Supports reentrant locks
        return true;
      }

      if (this.heartbeatManager != null && this.heartbeatManager.hasActiveHeartbeat()) {
        throw new HoodieLockException(generateLockStateMessage(FAILED_TO_ACQUIRE));
      }

      Option<ConditionalWriteLockFile> latestLock = this.lockService.getCurrentLockFile();
      if (latestLock == null) {
        // We were not able to determine whether a lock was present.
        return false;
      }

      if (latestLock.isPresent() && !latestLock.get().isExpired() && !isCurrentTimeCertainlyOlderThanDistributedTime(latestLock.get().getValidUntil())) {
        // An unexpired lock file exists, we can't acquire
        logger.info(
            LOCK_STATE_LOGGER_MSG_WITH_INFO,
            ownerId,
            lockFilePath,
            Thread.currentThread(),
            FAILED_TO_ACQUIRE,
            String.format("Lock already held by %s", latestLock.get().getOwner()));
        return false;
      }

      // This request should have preconditions that ensure the request fails if the lock file
      // already exists. If we get null, then someone beat us to the lock
      ConditionalWriteLockData newLockData = new ConditionalWriteLockData(false, System.currentTimeMillis() + lockValidityMs, ownerId);
      ConditionalWriteLockFile lockObject = this.lockService.tryCreateOrUpdateLockFile(newLockData, latestLock.orElse(null));
      if (lockObject == null) {
        // failed to acquire the lock, indicates concurrent contention
        logger.info(
            LOCK_STATE_LOGGER_MSG,
            ownerId,
            lockFilePath,
            Thread.currentThread(),
            FAILED_TO_ACQUIRE);
        return false;
      }
      this.setLock(lockObject);

      if (this.heartbeatManager == null || !this.heartbeatManager.startHeartbeatForThread(Thread.currentThread())) {
        logger.error(
            LOCK_STATE_LOGGER_MSG_WITH_INFO,
            ownerId,
            lockFilePath,
            Thread.currentThread(),
            RELEASING,
            "We were unable to start the heartbeat!");
        tryExpireCurrentLock();
        return false;
      }

      logger.info(
          LOCK_STATE_LOGGER_MSG,
          ownerId,
          lockFilePath,
          Thread.currentThread(),
          ACQUIRED);
      return true;
    }
  }

  /**
   * Unlock by marking our current lock file "expired": true.
   */
  @Override
  public void unlock() {
    synchronized (localMutex) {
      if (this.getLock() == null) {
        return;
      }
      // The goal is to setLock(null)
      // This indicates to future callers that we do not have the lock and our heartbeat task is expired.
      // If either of our actions to perform this work fail, then we cannot setLock to null.
      boolean canSetCurrentLockNull = true;

      // Try to stop the heartbeat first
      if (heartbeatManager != null && heartbeatManager.hasActiveHeartbeat()) {
        logger.info("Owner {}: Gracefully shutting down heartbeat.", this.ownerId);
        canSetCurrentLockNull &= heartbeatManager.stopHeartbeat(true);
      }

      // Then expire the current lock.
      canSetCurrentLockNull &= tryExpireCurrentLock();
      if (canSetCurrentLockNull) {
        this.setLock(null);
      } else {
        throw new HoodieLockException(generateLockStateMessage(FAILED_TO_RELEASE));
      }
    }
  }

  /**
   * Tries to expire the currently held lock.
   * @return True if we were successfully able to upload an expired lock.
   */
  private boolean tryExpireCurrentLock() {
    synchronized (localMutex) {
      logger.debug(
          LOCK_STATE_LOGGER_MSG,
          ownerId,
          lockFilePath,
          Thread.currentThread(),
          RELEASING);

      // Upload metadata that will unlock this lock.
      Option<ConditionalWriteLockFile> result = this.lockService.tryCreateOrUpdateLockFileWithRetry(
          () -> new ConditionalWriteLockData(true, this.getLock().getValidUntil(), ownerId),
          this.getLock(),
          // Keep retrying for the normal validity time.
          LOCK_UPSERT_RETRY_COUNT);
      if (result == null) {
        // Precondition failure, this can happen if our lock expires and someone else acquires the lock.
        logger.warn(
            LOCK_STATE_LOGGER_MSG,
            ownerId,
            lockFilePath,
            Thread.currentThread(),
            FAILED_TO_RELEASE);
      } else if (!result.isPresent()) {
        // Here we do not know the state of the lock.
        logger.error(
            LOCK_STATE_LOGGER_MSG,
            ownerId,
            lockFilePath,
            Thread.currentThread(),
            FAILED_TO_RELEASE);
      } else {
        logger.debug(
            LOCK_STATE_LOGGER_MSG,
            ownerId,
            lockFilePath,
            Thread.currentThread(),
            RELEASED);
        this.setLock(result.get());
        return true;
      }

      return false;
    }
  }

  /**
   * Renews (heartbeats) the current lock if we are the holder,
   * extending its last modified time in cloud storage so it won't be considered expired.
   * Called by the HeartbeatManager
   *
   * @return whether we were able to renew the lock.
   * If false, the caller of this (the heartbeat manager) will end the heartbeat task.
   */
  @VisibleForTesting
  protected boolean renewLock() {
    synchronized (localMutex) {
      try {
        // Note: we do not need to compare to the current latest lockfile, as long as our clock drift buffer
        // is accurate. We can guarantee that we are last process to modify this lockfile as long as the generation
        // stored here matches the previous.
        if (this.getLock() == null) {
          logger.warn("Owner {}: Cannot renew, no lock held by this process", this.ownerId);
          if (this.heartbeatManager == null || !this.heartbeatManager.hasActiveHeartbeat()) {
            logger.warn("Owner {}: Another process has already shutdown the heartbeat after this task fired.", this.ownerId);
          }
          return false;
        }

        long oldExpirationMs = this.getLock().getValidUntil();
        if (isCurrentTimePossiblyOlderThanDistributedTime(oldExpirationMs) && !this.getLock().isExpired()) {
          logger.error("Owner {}: Cannot renew unexpired lock which is within clock drift of expiration!", this.ownerId);
        } else {
          // Attempt conditional update, extend lock.
          ConditionalWriteLockData extendedExpirationLockData = new ConditionalWriteLockData(
              false, System.currentTimeMillis() + lockValidityMs, ownerId);
          Option<ConditionalWriteLockFile> currentLock = this.lockService.tryCreateOrUpdateLockFileWithRetry(
              () -> extendedExpirationLockData,
              this.getLock(),
              // Keep retrying lock renewal until we are 1 heartbeat interval away from expiration.
              LOCK_UPSERT_RETRY_COUNT);
          if (currentLock == null) {
            logger.error("Owner {}: Unable to upload metadata to renew lock due to fatal error.", this.ownerId);
          } else if (!currentLock.isPresent()) {
            // This could be transient, but unclear, we will let the heartbeat continue normally.
            // If the next heartbeat run identifies our lock has expired we will error out.
            logger.warn("Owner {}: Unable to upload metadata to renew lock due to unknown issue, could be transient.", this.ownerId);
            return true;
          } else {
            // Only positive outcome
            this.setLock(currentLock.get());
            logger.info("Owner {}: renewal succeeded for lock: {} with {} ms left until expiration.", this.ownerId, this.lockFilePath, oldExpirationMs - System.currentTimeMillis());
            return true;
          }
        }
      } catch (Exception e) {
        logger.error("Owner {}: Exception occurred while renewing lock", this.ownerId, e);
      }

      // Any code path which returns false will trigger heartbeat to stop.
      return false;
    }
  }

  // ---------
  // Utilities
  // ---------

  /**
   * Method to calculate whether a timestamp from a distributed source has definitively occurred yet.
   */
  protected boolean isCurrentTimeCertainlyOlderThanDistributedTime(long epochMs) {
    return System.currentTimeMillis() > epochMs + CLOCK_DRIFT_BUFFER_MS;
  }

  /**
   * Method to calculate whether a timestamp from a distributed source has potentially occurred yet.
   */
  protected boolean isCurrentTimePossiblyOlderThanDistributedTime(long epochMs) {
    return System.currentTimeMillis() + CLOCK_DRIFT_BUFFER_MS > epochMs;
  }

  private String buildLockObjectPath(String lockFolderName, String lockTableFileName) {
    // Normalize inputs by removing trailing slashes
    // We know lockTableFileName has already been parsed.
    if (lockFolderName.startsWith("/")) {
      lockFolderName = lockFolderName.substring(1);
    }

    // Append a slash only if one isn’t already present.
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
    return String.format("Owner %s: Lock file path %s, Thread %s, Conditional Write lock state %s", this.ownerId, this.lockFilePath, threadName, state.toString());
  }
}