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

import org.apache.hudi.client.transaction.lock.StorageLockClient;
import org.apache.hudi.client.transaction.lock.models.LockGetResult;
import org.apache.hudi.client.transaction.lock.models.LockUpsertResult;
import org.apache.hudi.client.transaction.lock.models.StorageLockData;
import org.apache.hudi.client.transaction.lock.models.StorageLockFile;
import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A GCS-based implementation of a distributed lock provider using conditional writes
 * with generationMatch, plus local concurrency safety, heartbeat/renew, and pruning old locks.
 */
@Slf4j
@ThreadSafe
public class GCSStorageLockClient implements StorageLockClient {

  private static final long PRECONDITION_FAILURE_ERROR_CODE = 412;
  private static final long NOT_FOUND_ERROR_CODE = 404;
  private static final long RATE_LIMIT_ERROR_CODE = 429;
  private static final long INTERNAL_SERVER_ERROR_CODE_MIN = 500;
  private final Logger logger;
  private final Storage gcsClient;
  private final String bucketName;
  private final String lockFilePath;
  private final String ownerId;

  /**
   * Constructor that is used by reflection to instantiate a GCS-based locking service.
   *
   * @param ownerId     The owner id.
   * @param lockFileUri The path within the bucket where to write lock files.
   * @param props       The properties for the lock config, can be used to customize client.
   */
  public GCSStorageLockClient(
      String ownerId,
      String lockFileUri,
      Properties props) {
    this(ownerId, lockFileUri, props, createDefaultGcsClient(), log);
  }

  @VisibleForTesting
  GCSStorageLockClient(
      String ownerId,
      String lockFileUri,
      Properties properties,
      Functions.Function1<Properties, Storage> gcsClientSupplier,
      Logger logger) {
    Pair<String, String> bucketAndPath = StorageLockClient.parseBucketAndPath(lockFileUri);
    this.bucketName = bucketAndPath.getLeft();
    this.lockFilePath = bucketAndPath.getRight();
    this.gcsClient = gcsClientSupplier.apply(properties);
    this.ownerId = ownerId;
    this.logger = logger;
  }

  private static Functions.Function1<Properties, Storage> createDefaultGcsClient() {
    return (props) -> {
      // Provide the option to customize the timeouts later on.
      // For now, defaults suffice
      return StorageOptions.newBuilder().build().getService();
    };
  }

  /**
   * Attempts to create or update the lock file using the given lock data and generation number.
   *
   * @param lockData         the new lock data to use.
   * @param generationNumber the expected generation number (0 for creation).
   * @return the updated StorageLockFile instance.
   * @throws StorageException if the update fails.
   */
  private StorageLockFile createOrUpdateLockFileInternal(StorageLockData lockData, long generationNumber)
      throws StorageException {
    BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of(bucketName, lockFilePath)).build();
    Blob updatedBlob = gcsClient.create(
        blobInfo,
        StorageLockFile.toByteArray(lockData),
        Storage.BlobTargetOption.generationMatch(generationNumber));
    return new StorageLockFile(
        lockData,
        String.valueOf(updatedBlob.getGeneration()));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Pair<LockUpsertResult, Option<StorageLockFile>> tryUpsertLockFile(
      StorageLockData newLockData,
      Option<StorageLockFile> previousLockFile) {
    long generationNumber = getGenerationNumber(previousLockFile);
    try {
      StorageLockFile updatedFile = createOrUpdateLockFileInternal(newLockData, generationNumber);
      return Pair.of(LockUpsertResult.SUCCESS, Option.of(updatedFile));
    } catch (StorageException e) {
      if (e.getCode() == PRECONDITION_FAILURE_ERROR_CODE) {
        logger.info("OwnerId: {}, Unable to write new lock file. Another process has modified this lockfile {} already.",
            ownerId, lockFilePath);
        return Pair.of(LockUpsertResult.ACQUIRED_BY_OTHERS, Option.empty());
      } else if (e.getCode() == RATE_LIMIT_ERROR_CODE) {
        logger.warn("OwnerId: {}, Rate limit exceeded for lock file: {}", ownerId, lockFilePath);
      } else if (e.getCode() >= INTERNAL_SERVER_ERROR_CODE_MIN) {
        logger.warn("OwnerId: {}, GCS returned internal server error code for lock file: {}",
            ownerId, lockFilePath, e);
      } else {
        throw e;
      }
      return Pair.of(LockUpsertResult.UNKNOWN_ERROR, Option.empty());
    }
  }

  /**
   * Handling storage exception for GET request
   *
   * @param e         The error to handle.
   * @param ignore404 Whether to ignore 404 as a valid exception.
   *                  When we read from stream we might see this, and
   *                  it should not be counted as NOT_EXISTS.
   * @return The type of getResult error
   */
  private LockGetResult handleGetStorageException(StorageException e, boolean ignore404) {
    if (e.getCode() == NOT_FOUND_ERROR_CODE) {
      if (ignore404) {
        logger.info("OwnerId: {}, GCS stream read failure detected: {}", ownerId, lockFilePath);
      } else {
        logger.info("OwnerId: {}, Object not found in the path: {}", ownerId, lockFilePath);
        return LockGetResult.NOT_EXISTS;
      }
    } else if (e.getCode() == RATE_LIMIT_ERROR_CODE) {
      logger.warn("OwnerId: {}, Rate limit exceeded for lock file: {}", ownerId, lockFilePath);
    } else if (e.getCode() >= INTERNAL_SERVER_ERROR_CODE_MIN) {
      logger.warn("OwnerId: {}, GCS returned internal server error code for lock file: {}", ownerId, lockFilePath, e);
    } else {
      throw e;
    }
    return LockGetResult.UNKNOWN_ERROR;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Pair<LockGetResult, Option<StorageLockFile>> readCurrentLockFile() {
    try {
      Blob blob = gcsClient.get(BlobId.of(bucketName, lockFilePath));
      if (blob == null) {
        return Pair.of(LockGetResult.NOT_EXISTS, Option.empty());
      }
      return getLockFileFromBlob(blob);
    } catch (StorageException e) {
      return Pair.of(handleGetStorageException(e, false), Option.empty());
    } catch (HoodieIOException e) {
      // GCS will throw IOException wrapping 404 when reading from stream for file that has been modified in between calling gcsClient.get
      // and StorageLockFile.createFromStream. People have complained that this is not being strongly consistent, however
      // we have to handle this case. https://stackoverflow.com/q/66759993
      Throwable cause = e.getCause();
      if (cause instanceof IOException && cause.getCause() instanceof StorageException) {
        return Pair.of(handleGetStorageException((StorageException) cause.getCause(), true), Option.empty());
      }
      throw e;
    }
  }

  private @Nonnull Pair<LockGetResult, Option<StorageLockFile>> getLockFileFromBlob(Blob blob) {
    try (InputStream inputStream = Channels.newInputStream(blob.reader())) {
      return Pair.of(LockGetResult.SUCCESS,
          Option.of(StorageLockFile.createFromStream(inputStream, String.valueOf(blob.getGeneration()))));
    } catch (IOException e) {
      // Our createFromStream method does not throw IOExceptions, it wraps in HoodieIOException, however Sonar requires handling this.
      throw new UncheckedIOException("Failed reading blob: " + lockFilePath, e);
    }
  }

  @Override
  public Option<String> readObject(String filePath, boolean checkExistsFirst) {
    try {
      // Parse the file path to get bucket and object path
      Pair<String, String> bucketAndPath = StorageLockClient.parseBucketAndPath(filePath);
      String bucket = bucketAndPath.getLeft();
      String objectPath = bucketAndPath.getRight();

      BlobId blobId = BlobId.of(bucket, objectPath);

      if (checkExistsFirst) {
        // First check if the file exists (lightweight metadata check)
        Blob blob = gcsClient.get(blobId);

        if (blob == null || !blob.exists()) {
          // File doesn't exist - this is the common case for optional configs
          logger.debug("JSON config file not found: {}", filePath);
          return Option.empty();
        }

        // File exists, read its content
        byte[] content = blob.getContent();
        return Option.of(new String(content, UTF_8));
      } else {
        // Direct read without existence check
        byte[] content = gcsClient.readAllBytes(blobId);
        return Option.of(new String(content, UTF_8));
      }
    } catch (StorageException e) {
      if (e.getCode() == NOT_FOUND_ERROR_CODE) {
        logger.debug("JSON config file not found: {}", filePath);
      } else {
        logger.warn("Error reading JSON config file: {}", filePath, e);
      }
      return Option.empty();
    } catch (Exception e) {
      logger.warn("Error reading JSON config file: {}", filePath, e);
      return Option.empty();
    }
  }

  @Override
  public boolean writeObject(String filePath, String content) {
    try {
      // Parse the file path to get bucket and object path
      Pair<String, String> bucketAndPath = StorageLockClient.parseBucketAndPath(filePath);
      String bucket = bucketAndPath.getLeft();
      String objectPath = bucketAndPath.getRight();

      BlobId blobId = BlobId.of(bucket, objectPath);
      BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();

      // Write the content to GCS
      gcsClient.create(blobInfo, content.getBytes(UTF_8));

      logger.debug("Successfully wrote object to: {}", filePath);
      return true;
    } catch (Exception e) {
      logger.error("Error writing object to: {}", filePath, e);
      return false;
    }
  }

  @Override
  public void close() throws Exception {
    this.gcsClient.close();
  }

  private long getGenerationNumber(Option<StorageLockFile> file) {
    return (file.isPresent())
        ? Long.parseLong(file.get().getVersionId())
        : 0;
  }
}