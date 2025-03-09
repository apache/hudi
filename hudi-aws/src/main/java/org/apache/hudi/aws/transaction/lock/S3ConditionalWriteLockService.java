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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.
 * See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.hudi.aws.transaction.lock;

import org.apache.hudi.aws.credentials.HoodieAWSCredentialsProviderFactory;
import org.apache.hudi.client.transaction.lock.ConditionalWriteLockService;
import org.apache.hudi.client.transaction.lock.LockGetResult;
import org.apache.hudi.client.transaction.lock.LockUpdateResult;
import org.apache.hudi.client.transaction.lock.models.ConditionalWriteLockData;
import org.apache.hudi.client.transaction.lock.models.ConditionalWriteLockFile;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Properties;
import java.util.function.Supplier;

/**
 * S3-based distributed lock service using ETag checks (AWS SDK v2).
 * TODO: Replace with real RFC link.
 * See RFC: <a href="https://github.com/apache/hudi/pull/12927/files#diff-90ff2406fb2db425551f60038ab1c87e0aaa65d6e7d0bc2475dcecc4b9764521R83">RFC-91</a>
 */
@ThreadSafe
public class S3ConditionalWriteLockService implements ConditionalWriteLockService {

  private static final Logger LOG = LoggerFactory.getLogger(S3ConditionalWriteLockService.class);
  private static final long WAIT_TIME_FOR_RETRY_MS = 1000L;
  private static final int PRECONDITION_FAILURE_ERROR_CODE = 412;
  private static final int NOT_FOUND_ERROR_CODE = 404;
  private static final int CONDITIONAL_REQUEST_CONFLICT_ERROR_CODE = 409;
  private static final int RATE_LIMIT_ERROR_CODE = 429;
  private static final int INTERNAL_SERVER_ERROR_CODE_MIN = 500;

  private final Logger logger;
  private final S3Client s3Client;
  private final String bucketName;
  private final String lockFilePath;
  private final String ownerId;

  /** Constructor that is used by reflection to instantiate an S3-based locking service.
   * @param ownerId The owner id.
   * @param bucketName The name of the bucket.
   * @param lockFilePath The path within the bucket where to write lock files.
   * @param props The properties for the lock config, can be used to customize client.
   */
  public S3ConditionalWriteLockService(String ownerId, String bucketName, String lockFilePath, Properties props) {
    this(
        ownerId,
        bucketName,
        lockFilePath,
        createDefaultS3Client(props),
        LOG);
  }

  @VisibleForTesting
  S3ConditionalWriteLockService(String ownerId,
                                String bucketName,
                                String lockFilePath,
                                S3Client s3Client,
                                Logger logger) {
    this.s3Client = s3Client;
    this.lockFilePath = lockFilePath;
    this.bucketName = bucketName;
    this.ownerId = ownerId;
    this.logger = logger;
  }

  private static S3Client createDefaultS3Client(Properties props) {
    Region region;
    try {
      region = DefaultAwsRegionProviderChain.builder().build().getRegion();
    } catch (SdkClientException e) {
      // Fallback to us-east-1 if no region is found
      region = Region.US_EAST_1;
    }
    return S3Client.builder().credentialsProvider(HoodieAWSCredentialsProviderFactory.getAwsCredentialsProvider(props)).region(region).build();
  }

  /**
   * Internal helper to create or update the lock file with optional ETag precondition.
   */
  private ConditionalWriteLockFile createOrUpdateLockFileInternal(ConditionalWriteLockData lockData, String expectedEtag) {
    byte[] bytes = ConditionalWriteLockFile.toByteArray(lockData);
    PutObjectRequest.Builder putRequestBuilder = PutObjectRequest.builder()
        .bucket(bucketName)
        .key(lockFilePath);

    // ETag-based constraints:
    // - If expectedEtag is not null:
    //    We assume that the file already exists on S3 with the ETag "expectedEtag".
    //    The update operation will include an ifMatch(expectedEtag) condition, meaning the update will only
    //    succeed if the current file's ETag exactly matches expectedEtag.
    //    If the actual ETag of the file on S3 differs from expectedEtag, the update attempt will fail.
    // - If expectedEtag is null:
    //    We assume that the file does not currently exist on S3.
    //    The operation will use ifNoneMatch("*"), which instructs S3 to create the file only if it doesn't already exist.
    //    If a file with the same name is present (i.e., there is an existing ETag), the creation attempt will fail.
    if (expectedEtag == null) {
      putRequestBuilder.ifNoneMatch("*");
    } else {
      putRequestBuilder.ifMatch(expectedEtag);
    }

    PutObjectResponse response = s3Client.putObject(putRequestBuilder.build(), RequestBody.fromBytes(bytes));
    String newEtag = response.eTag();

    return new ConditionalWriteLockFile(lockData, newEtag);
  }

  @Override
  public Pair<LockUpdateResult, ConditionalWriteLockFile> tryCreateOrUpdateLockFile(
      ConditionalWriteLockData newLockData,
      ConditionalWriteLockFile previousLockFile
  ) {
    String currentEtag = (previousLockFile != null) ? previousLockFile.getVersionId() : null;
    try {
      ConditionalWriteLockFile updatedFile = createOrUpdateLockFileInternal(newLockData, currentEtag);
      return Pair.of(LockUpdateResult.SUCCESS, updatedFile);
    } catch (S3Exception e) {
      int status = e.statusCode();
      if (status == PRECONDITION_FAILURE_ERROR_CODE || status == CONDITIONAL_REQUEST_CONFLICT_ERROR_CODE) {
        logger.info("OwnerId: {}, Lockfile modified by another process: {}", ownerId, lockFilePath);
        return Pair.of(LockUpdateResult.ACQUIRED_BY_OTHERS, null);
      } else if (status == RATE_LIMIT_ERROR_CODE) {
        logger.warn("OwnerId: {}, Rate limit exceeded for: {}", ownerId, lockFilePath);
      } else if (status >= INTERNAL_SERVER_ERROR_CODE_MIN) {
        logger.warn("OwnerId: {}, S3 internal server error for: {}", ownerId, lockFilePath, e);
      } else {
        throw e;
      }
      return Pair.of(LockUpdateResult.UNKNOWN_ERROR, null);
    }
  }

  @Override
  public Pair<LockUpdateResult, ConditionalWriteLockFile> tryCreateOrUpdateLockFileWithRetry(
      Supplier<ConditionalWriteLockData> newLockDataSupplier,
      ConditionalWriteLockFile previousLockFile,
      long maxAttempts
  ) {
    String currentEtag = (previousLockFile != null) ? previousLockFile.getVersionId() : null;
    long attempts = 0;

    while (attempts < maxAttempts) {
      attempts++;
      try {
        ConditionalWriteLockFile updated =
            createOrUpdateLockFileInternal(newLockDataSupplier.get(), currentEtag);
        return Pair.of(LockUpdateResult.SUCCESS, updated);
      } catch (S3Exception e) {
        int status = e.statusCode();
        if (status == PRECONDITION_FAILURE_ERROR_CODE) {
          logger.warn("OwnerId: {}, Lockfile modified by another process: {}", ownerId, lockFilePath);
          return Pair.of(LockUpdateResult.ACQUIRED_BY_OTHERS, null);
        } else if (status == RATE_LIMIT_ERROR_CODE) {
          logger.warn("OwnerId: {}, Rate limit exceeded for: {}", ownerId, lockFilePath);
        } else if (status >= INTERNAL_SERVER_ERROR_CODE_MIN) {
          logger.warn("OwnerId: {}, S3 internal server error for: {}", ownerId, lockFilePath, e);
        } else {
          logger.warn("OwnerId: {}, Error writing lock file: {}", ownerId, lockFilePath, e);
        }
      } catch (AwsServiceException | SdkClientException e) {
        logger.warn("OwnerId: {}, Unexpected SDK error while writing lock file: {}", ownerId, lockFilePath, e);
      }

      try {
        Thread.sleep(WAIT_TIME_FOR_RETRY_MS);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        break;
      }
    }

    logger.warn("OwnerId: {}, Upsert for lockfile {} failed after {} attempts.", ownerId, lockFilePath, attempts);
    return Pair.of(LockUpdateResult.UNKNOWN_ERROR, null);
  }

  @Override
  public Pair<LockGetResult, ConditionalWriteLockFile> getCurrentLockFile() {
    try (ResponseInputStream<GetObjectResponse> in = s3Client.getObject(
        GetObjectRequest.builder()
            .bucket(bucketName)
            .key(lockFilePath)
            .build())) {
      String eTag = in.response().eTag();
      return Pair.of(LockGetResult.SUCCESS, ConditionalWriteLockFile.createFromStream(in, eTag));
    } catch (S3Exception e) {
      int status = e.statusCode();
      if (status == NOT_FOUND_ERROR_CODE) {
        logger.info("OwnerId: {}, Object not found: {}", ownerId, lockFilePath);
        return Pair.of(LockGetResult.NOT_EXISTS, null);
      } else if (status == CONDITIONAL_REQUEST_CONFLICT_ERROR_CODE) {
        logger.info("OwnerId: {}, Conflicting operation has occurred: {}", ownerId, lockFilePath);
        return Pair.of(LockGetResult.UNKNOWN_ERROR, null);
      } else if (status == RATE_LIMIT_ERROR_CODE) {
        logger.warn("OwnerId: {}, Rate limit exceeded: {}", ownerId, lockFilePath);
        return Pair.of(LockGetResult.UNKNOWN_ERROR, null);
      } else if (status >= INTERNAL_SERVER_ERROR_CODE_MIN) {
        logger.warn("OwnerId: {}, S3 internal server error: {}", ownerId, lockFilePath, e);
        return Pair.of(LockGetResult.UNKNOWN_ERROR, null);
      } else {
        throw e;
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed reading lock file from S3: " + lockFilePath, e);
    }
  }

  @Override
  public void close() {
    s3Client.close();
  }
}
