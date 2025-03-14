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

package org.apache.hudi.gcp.transaction.lock;

import org.apache.hudi.client.transaction.lock.ConditionalWriteLockService;
import org.apache.hudi.client.transaction.lock.LockGetResult;
import org.apache.hudi.client.transaction.lock.models.ConditionalWriteLockData;
import org.apache.hudi.client.transaction.lock.models.ConditionalWriteLockFile;
import org.apache.hudi.common.util.VisibleForTesting;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.util.Properties;
import java.util.function.Supplier;

import org.apache.hudi.client.transaction.lock.LockUpdateResult;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A GCS-based implementation of a distributed lock provider using conditional writes
 * with generationMatch, plus local concurrency safety, heartbeat/renew, and pruning old locks.
 */
@ThreadSafe
public class GCSConditionalWriteLockService implements ConditionalWriteLockService {
  private static final Logger DEFAULT_LOGGER = LoggerFactory.getLogger(GCSConditionalWriteLockService.class);
  private static final long WAIT_TIME_FOR_RETRY_MS = 1000L;
  private static final long PRECONDITION_FAILURE_ERROR_CODE = 412;
  private static final long NOT_FOUND_ERROR_CODE = 404;
  private static final long RATE_LIMIT_ERROR_CODE = 429;
  private static final long INTERNAL_SERVER_ERROR_CODE_MIN = 500;
  private final Logger logger;
  private final Storage gcsClient;
  private final String bucketName;
  private final String lockFilePath;
  private final String ownerId;

  /** Constructor that is used by reflection to instantiate a GCS-based locking service.
   * @param ownerId The owner id.
   * @param bucketName The name of the bucket.
   * @param lockFilePath The path within the bucket where to write lock files.
   * @param props The properties for the lock config, can be used to customize client.
   */
  public GCSConditionalWriteLockService(
      String ownerId,
      String bucketName,
      String lockFilePath,
      Properties props) {
    this(ownerId, bucketName, lockFilePath, StorageOptions.newBuilder()
        .build()
        .getService(), DEFAULT_LOGGER);
  }

  @VisibleForTesting
  GCSConditionalWriteLockService(
      String ownerId,
      String bucketName,
      String lockFilePath,
      Storage gcsClient,
      Logger logger) {
    this.gcsClient = gcsClient;
    this.lockFilePath = lockFilePath;
    this.bucketName = bucketName;
    this.ownerId = ownerId;
    this.logger = logger;
  }

  /**
   * Attempts to create or update the lock file using the given lock data and generation number.
   *
   * @param lockData the new lock data to use.
   * @param generationNumber the expected generation number (0 for creation).
   * @return the updated ConditionalWriteLockFile instance.
   * @throws StorageException if the update fails.
   */
  private ConditionalWriteLockFile createOrUpdateLockFileInternal(ConditionalWriteLockData lockData, long generationNumber)
      throws StorageException {
    BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of(bucketName, lockFilePath)).build();
    Blob updatedBlob = gcsClient.create(
        blobInfo,
        ConditionalWriteLockFile.toByteArray(lockData),
        Storage.BlobTargetOption.generationMatch(generationNumber));
    return new ConditionalWriteLockFile(
        lockData,
        String.valueOf(updatedBlob.getGeneration()));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Pair<LockUpdateResult, ConditionalWriteLockFile> tryCreateOrUpdateLockFile(
      ConditionalWriteLockData newLockData, 
      ConditionalWriteLockFile previousLockFile) {
    long generationNumber = getGenerationNumber(previousLockFile);
    try {
      ConditionalWriteLockFile updatedFile = createOrUpdateLockFileInternal(newLockData, generationNumber);
      return Pair.of(LockUpdateResult.SUCCESS, updatedFile);
    } catch (StorageException e) {
      if (e.getCode() == PRECONDITION_FAILURE_ERROR_CODE) {
        logger.info("OwnerId: {}, Unable to write new lock file. Another process has modified this lockfile {} already.", 
            ownerId, lockFilePath);
        return Pair.of(LockUpdateResult.ACQUIRED_BY_OTHERS, null);
      } else if (e.getCode() == RATE_LIMIT_ERROR_CODE) {
        logger.warn("OwnerId: {}, Rate limit exceeded for lock file: {}", ownerId, lockFilePath);
      } else if (e.getCode() >= INTERNAL_SERVER_ERROR_CODE_MIN) {
        logger.warn("OwnerId: {}, GCS returned internal server error code for lock file: {}", 
            ownerId, lockFilePath, e);
      } else {
        throw e;
      }
      return Pair.of(LockUpdateResult.UNKNOWN_ERROR, null);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Pair<LockUpdateResult, ConditionalWriteLockFile> tryCreateOrUpdateLockFileWithRetry(
      Supplier<ConditionalWriteLockData> newLockDataSupplier,
      ConditionalWriteLockFile previousLockFile,
      long maxAttempts) {
    long generationNumber = getGenerationNumber(previousLockFile);
    long attempts = 0;

    while (attempts < maxAttempts) {
      try {
        attempts++;
        logger.debug("OwnerId: {}, Attempt {} to create lock file {}.", ownerId, attempts, lockFilePath);

        ConditionalWriteLockFile updatedLockFile = createOrUpdateLockFileInternal(
            newLockDataSupplier.get(), generationNumber);
        return Pair.of(LockUpdateResult.SUCCESS, updatedLockFile);

      } catch (StorageException e) {
        if (e.getCode() == PRECONDITION_FAILURE_ERROR_CODE) {
          logger.warn("OwnerId: {}, Unable to write new lock file. Another process has modified this lock file {} already. This error is not retriable.", 
              ownerId, lockFilePath);
          return Pair.of(LockUpdateResult.ACQUIRED_BY_OTHERS, null);
        } else if (e.getCode() == RATE_LIMIT_ERROR_CODE) {
          logger.warn("OwnerId: {}, Rate limit exceeded for writing lock file: {} with retry", ownerId, lockFilePath);
        } else if (e.getCode() >= INTERNAL_SERVER_ERROR_CODE_MIN) {
          logger.warn("OwnerId: {}, GCS returned internal server error code for writing lock file: {} with retry", ownerId, lockFilePath, e);
        } else {
          logger.warn("OwnerId: {}, Unknown error encountered while writing lock file: {} with retry", ownerId, lockFilePath, e);
        }
      }

      try {
        Thread.sleep(WAIT_TIME_FOR_RETRY_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }

    logger.warn("OwnerId: {}, Upsert for lockfile {} did not succeed after {} attempts.", 
        ownerId, lockFilePath, attempts);
    return Pair.of(LockUpdateResult.UNKNOWN_ERROR, null);
  }

  /**
   * Handling storage exception for GET request
   * @param e The error to handle.
   * @param ignore404 Whether to ignore 404 as a valid exception.
   *                  When we read from stream we might see this, and
   *                  it should not be counted as NOT_EXISTS.
   * @return A pair of the result type and the file, if we were able to create it.
   */
  private Pair<LockGetResult, ConditionalWriteLockFile> handleGetStorageException(StorageException e, boolean ignore404) {
    if (e.getCode() == NOT_FOUND_ERROR_CODE) {
      if (ignore404) {
        logger.info("OwnerId: {}, GCS stream read failure detected: {}", ownerId, lockFilePath);
        return Pair.of(LockGetResult.UNKNOWN_ERROR, null);
      }
      logger.info("OwnerId: {}, Object not found in the path: {}", ownerId, lockFilePath);
      return Pair.of(LockGetResult.NOT_EXISTS, null);
    } else if (e.getCode() == RATE_LIMIT_ERROR_CODE) {
      logger.warn("OwnerId: {}, Rate limit exceeded for lock file: {}", ownerId, lockFilePath);
    } else if (e.getCode() >= INTERNAL_SERVER_ERROR_CODE_MIN) {
      logger.warn("OwnerId: {}, GCS returned internal server error code for lock file: {}", ownerId, lockFilePath, e);
    } else {
      throw e;
    }
    return Pair.of(LockGetResult.UNKNOWN_ERROR, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Pair<LockGetResult, ConditionalWriteLockFile> getCurrentLockFile() {
    try {
      Blob blob = gcsClient.get(BlobId.of(bucketName, lockFilePath));
      if (blob == null) {
        return Pair.of(LockGetResult.NOT_EXISTS, null);
      }
      return getLockFileFromBlob(blob);
    } catch (StorageException e) {
      return handleGetStorageException(e, false);
    } catch (HoodieIOException e) {
      // GCS will throw IOException wrapping 404 when reading from stream for file that has been modified in between calling gcsClient.get
      // and ConditionalWriteLockFile.createFromStream. People have complained that this is not being strongly consistent, however
      // we have to handle this case. https://stackoverflow.com/q/66759993
      Throwable cause = e.getCause();
      if (cause instanceof IOException && cause.getCause() instanceof StorageException) {
        return handleGetStorageException((StorageException) cause.getCause(), true);
      }
      throw e;
    }
  }

  private @NotNull Pair<LockGetResult, ConditionalWriteLockFile> getLockFileFromBlob(Blob blob) {
    try (InputStream inputStream = Channels.newInputStream(blob.reader())) {
      return Pair.of(LockGetResult.SUCCESS,
          ConditionalWriteLockFile.createFromStream(inputStream, String.valueOf(blob.getGeneration()))
      );
    } catch (IOException e) {
      // Our createFromStream method does not throw IOExceptions, it wraps in HoodieIOException, however Sonar requires handling this.
      throw new UncheckedIOException("Failed reading blob: " + lockFilePath, e);
    }
  }

  @Override
  public void close() throws Exception {
    this.gcsClient.close();
  }

  private long getGenerationNumber(ConditionalWriteLockFile file) {
    return (file != null)
        ? Long.parseLong(file.getVersionId())
        : 0;
  }
}