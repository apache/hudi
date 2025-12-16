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
import org.apache.hudi.client.transaction.lock.StorageLockClient;
import org.apache.hudi.client.transaction.lock.models.LockGetResult;
import org.apache.hudi.client.transaction.lock.models.LockUpsertResult;
import org.apache.hudi.client.transaction.lock.models.StorageLockData;
import org.apache.hudi.client.transaction.lock.models.StorageLockFile;
import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetBucketLocationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketLocationResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Properties;

import static org.apache.hudi.config.StorageBasedLockConfig.VALIDITY_TIMEOUT_SECONDS;

/**
 * S3-based distributed lock client using ETag checks (AWS SDK v2).
 * See RFC: <a href="https://github.com/apache/hudi/blob/master/rfc/rfc-91/rfc-91.md">RFC-91</a>
 */
@Slf4j
@ThreadSafe
public class S3StorageLockClient implements StorageLockClient {
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

  /**
   * Constructor that is used by reflection to instantiate an S3-based storage locking client.
   *
   * @param ownerId     The owner id.
   * @param lockFileUri The full table base path where the lock will be written.
   * @param props       The properties for the lock config, can be used to customize client.
   */
  public S3StorageLockClient(String ownerId, String lockFileUri, Properties props) {
    this(ownerId, lockFileUri, props, createDefaultS3Client(), log);
  }

  @VisibleForTesting
  S3StorageLockClient(String ownerId, String lockFileUri, Properties props, Functions.Function2<String, Properties, S3Client> s3ClientSupplier, Logger logger) {
    Pair<String, String> bucketAndPath = StorageLockClient.parseBucketAndPath(lockFileUri);
    this.bucketName = bucketAndPath.getLeft();
    this.lockFilePath = bucketAndPath.getRight();

    this.s3Client = s3ClientSupplier.apply(bucketName, props);
    this.ownerId = ownerId;
    this.logger = logger;
  }

  @Override
  public Pair<LockGetResult, Option<StorageLockFile>> readCurrentLockFile() {
    try (ResponseInputStream<GetObjectResponse> in = s3Client.getObject(
        GetObjectRequest.builder()
            .bucket(bucketName)
            .key(lockFilePath)
            .build())) {
      String eTag = in.response().eTag();
      return Pair.of(LockGetResult.SUCCESS, Option.of(StorageLockFile.createFromStream(in, eTag)));
    } catch (S3Exception e) {
      int status = e.statusCode();
      // Default to unknown error
      LockGetResult result = LockGetResult.UNKNOWN_ERROR;
      if (status == NOT_FOUND_ERROR_CODE) {
        logger.info("OwnerId: {}, Object not found: {}", ownerId, lockFilePath);
        result = LockGetResult.NOT_EXISTS;
      } else if (status == CONDITIONAL_REQUEST_CONFLICT_ERROR_CODE) {
        logger.info("OwnerId: {}, Conflicting operation has occurred: {}", ownerId, lockFilePath);
      } else if (status == RATE_LIMIT_ERROR_CODE) {
        logger.warn("OwnerId: {}, Rate limit exceeded: {}", ownerId, lockFilePath);
      } else if (status >= INTERNAL_SERVER_ERROR_CODE_MIN) {
        logger.warn("OwnerId: {}, S3 internal server error: {}", ownerId, lockFilePath, e);
      } else {
        throw e;
      }
      return Pair.of(result, Option.empty());
    } catch (IOException e) {
      throw new UncheckedIOException("Failed reading lock file from S3: " + lockFilePath, e);
    }
  }

  @Override
  public Pair<LockUpsertResult, Option<StorageLockFile>> tryUpsertLockFile(StorageLockData newLockData, Option<StorageLockFile> previousLockFile) {
    boolean isLockRenewal = previousLockFile.isPresent();
    String currentEtag = isLockRenewal ? previousLockFile.get().getVersionId() : null;
    LockUpsertResult result = LockUpsertResult.UNKNOWN_ERROR;
    try {
      StorageLockFile updated = createOrUpdateLockFileInternal(newLockData, currentEtag);
      return Pair.of(LockUpsertResult.SUCCESS, Option.of(updated));
    } catch (S3Exception e) {
      result = handleUpsertS3Exception(e);
    } catch (AwsServiceException | SdkClientException e) {
      logger.error("OwnerId: {}, Unexpected SDK error while writing lock file: {}", ownerId, lockFilePath, e);
      if (!isLockRenewal) {
        // We should always throw errors early when we are creating the lock file.
        // This is likely indicative of a larger issue that should bubble up sooner.
        throw e;
      }
    }

    return Pair.of(result, Option.empty());
  }

  /**
   * Internal helper to create or update the lock file with optional ETag precondition.
   */
  private StorageLockFile createOrUpdateLockFileInternal(StorageLockData lockData, String expectedEtag) {
    byte[] bytes = StorageLockFile.toByteArray(lockData);
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

    return new StorageLockFile(lockData, newEtag);
  }

  private LockUpsertResult handleUpsertS3Exception(S3Exception e) {
    int status = e.statusCode();
    if (status == PRECONDITION_FAILURE_ERROR_CODE) {
      logger.info("OwnerId: {}, Lockfile modified by another process: {}", ownerId, lockFilePath);
      return LockUpsertResult.ACQUIRED_BY_OTHERS;
    } else if (status == CONDITIONAL_REQUEST_CONFLICT_ERROR_CODE) {
      logger.info("OwnerId: {}, Retriable conditional request conflict error: {}", ownerId, lockFilePath);
    } else if (status == RATE_LIMIT_ERROR_CODE) {
      logger.warn("OwnerId: {}, Rate limit exceeded for: {}", ownerId, lockFilePath);
    } else if (status >= INTERNAL_SERVER_ERROR_CODE_MIN) {
      logger.warn("OwnerId: {}, internal server error for: {}", ownerId, lockFilePath, e);
    } else {
      logger.warn("OwnerId: {}, Error writing lock file: {}", ownerId, lockFilePath, e);
    }

    return LockUpsertResult.UNKNOWN_ERROR;
  }

  private static Functions.Function2<String, Properties, S3Client> createDefaultS3Client() {
    return (bucketName, props) -> {
      Region region;
      boolean requiredFallbackRegion = false;
      try {
        region = DefaultAwsRegionProviderChain.builder().build().getRegion();
      } catch (SdkClientException e) {
        // Fallback to us-east-1 if no region is found
        region = Region.US_EAST_1;
        requiredFallbackRegion = true;
      }

      // Set all request timeouts to be 1/5 of the default validity.
      // Each call to acquire a lock requires 2 requests.
      // Each renewal requires 1 request.
      long validityTimeoutSecs =
          ((Number) props.getOrDefault(
              VALIDITY_TIMEOUT_SECONDS.key(),
              VALIDITY_TIMEOUT_SECONDS.defaultValue()
          )).longValue();
      long s3CallTimeoutSecs = validityTimeoutSecs / 5;
      S3Client s3Client = createS3Client(region, s3CallTimeoutSecs, props);
      if (requiredFallbackRegion) {
        GetBucketLocationResponse bucketLocationResponse = s3Client.getBucketLocation(
            GetBucketLocationRequest.builder().bucket(bucketName).build());
        // This is null when the region is US_EAST_1, so we do not need to worry about duplicate logic.
        String regionString = bucketLocationResponse.locationConstraintAsString();
        if (!StringUtils.isNullOrEmpty(regionString)) {
          // Close existing client and create another.
          s3Client.close();
          return createS3Client(
              Region.of(regionString),
              s3CallTimeoutSecs,
              props);
        }
      }

      return s3Client;
    };
  }

  private static S3Client createS3Client(Region region, long timeoutSecs, Properties props) {
    // Set the timeout, credentials, and region
    return S3Client.builder()
        .overrideConfiguration(
            b -> b.apiCallTimeout(Duration.ofSeconds(timeoutSecs)))
        .credentialsProvider(HoodieAWSCredentialsProviderFactory.getAwsCredentialsProvider(props))
        .region(region).build();
  }

  @Override
  public Option<String> readObject(String filePath, boolean checkExistsFirst) {
    try {
      // Parse the file path to get bucket and key
      Pair<String, String> bucketAndKey = StorageLockClient.parseBucketAndPath(filePath);
      String bucket = bucketAndKey.getLeft();
      String key = bucketAndKey.getRight();

      if (checkExistsFirst) {
        // First check if the file exists (lightweight HEAD request)
        try {
          s3Client.headObject(HeadObjectRequest.builder()
              .bucket(bucket)
              .key(key)
              .build());
        } catch (S3Exception e) {
          if (e.statusCode() == NOT_FOUND_ERROR_CODE) {
            // File doesn't exist - this is the common case for optional configs
            logger.debug("JSON config file not found: {}", filePath);
            return Option.empty();
          }
          throw e; // Re-throw other errors
        }
      }

      // Read the file (either after existence check or directly)
      String content = s3Client.getObjectAsBytes(
          GetObjectRequest.builder()
              .bucket(bucket)
              .key(key)
              .build()).asUtf8String();

      return Option.of(content);
    } catch (S3Exception e) {
      if (e.statusCode() == NOT_FOUND_ERROR_CODE) {
        logger.debug("JSON config file not found: {}", filePath);
        return Option.empty();
      }
      logger.error("Error reading JSON config file: {}", filePath, e);
      return Option.empty();
    } catch (Exception e) {
      logger.error("Error reading JSON config file: {}", filePath, e);
      return Option.empty();
    }
  }

  @Override
  public boolean writeObject(String filePath, String content) {
    try {
      // Parse the file path to get bucket and key
      Pair<String, String> bucketAndPath = StorageLockClient.parseBucketAndPath(filePath);
      String bucket = bucketAndPath.getLeft();
      String key = bucketAndPath.getRight();

      // Write the content to S3
      s3Client.putObject(
          PutObjectRequest.builder()
              .bucket(bucket)
              .key(key)
              .build(),
          RequestBody.fromString(content));

      logger.debug("Successfully wrote object to: {}", filePath);
      return true;
    } catch (Exception e) {
      logger.error("Error writing object to: {}", filePath, e);
      return false;
    }
  }

  @Override
  public void close() {
    s3Client.close();
  }
}
