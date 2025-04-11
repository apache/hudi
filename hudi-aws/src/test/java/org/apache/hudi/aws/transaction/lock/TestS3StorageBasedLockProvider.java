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
 * "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.aws.transaction.lock;

import org.apache.hudi.client.transaction.lock.StorageBasedLockProviderTestBase;
import org.apache.hudi.client.transaction.lock.StorageBasedLockProvider;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.config.HoodieAWSConfig;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

import static org.apache.hudi.common.config.HoodieCommonConfig.BASE_PATH;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mockStatic;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

/**
 * Tests S3-based StorageBasedLockProvider using a LocalStack container
 * to emulate S3.
 */
@Disabled("HUDI-9159 The tests do not work. Disabling them to unblock Azure CI")
public class TestS3StorageBasedLockProvider extends StorageBasedLockProviderTestBase {

  private static final DockerImageName LOCALSTACK_IMAGE =
          DockerImageName.parse("localstack/localstack:4.1.0");

  private static LocalStackContainer S3_CONTAINER;

  private static final String TEST_BUCKET = "test-bucket";
  private static S3Client s3Client;

  @BeforeAll
  static void initContainer() {
    // Spin up LocalStack with S3 service
    S3_CONTAINER = new LocalStackContainer(LOCALSTACK_IMAGE)
            .withServices(S3);
    S3_CONTAINER.start();

    s3Client = S3Client.builder()
            .endpointOverride(S3_CONTAINER.getEndpointOverride(S3))
            .credentialsProvider(
                    StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(
                                    S3_CONTAINER.getAccessKey(),
                                    S3_CONTAINER.getSecretKey()
                            )
                    )
            )
            .region(Region.of(S3_CONTAINER.getRegion()))
            .build();
    s3Client.createBucket(CreateBucketRequest.builder()
            .bucket(TEST_BUCKET)
            .build());
  }

  @AfterAll
  static void stopContainer() {
    if (S3_CONTAINER != null) {
      S3_CONTAINER.stop();
    }
  }

  /**
   * Creates a real S3-based StorageBasedLockProvider for testing.
   */
  @Override
  protected StorageBasedLockProvider createLockProvider() {
    LockConfiguration lockConf = new LockConfiguration(providerProperties);

    try (MockedStatic<S3Client> s3ClientStaticMock =
                 mockStatic(S3Client.class, Mockito.CALLS_REAL_METHODS)) {
      s3ClientStaticMock.when(S3Client::builder).thenAnswer(invocation -> {
        S3ClientBuilder builder = (S3ClientBuilder) invocation.callRealMethod();
        // Always add the endpoint override from S3_CONTAINER
        builder.endpointOverride(S3_CONTAINER.getEndpointOverride(S3));
        return builder;
      });
      return new StorageBasedLockProvider(
              lockConf,
              null);
    }
  }

  @BeforeEach
  void setupLockProvider() {
    // Provide base path for the test
    providerProperties.put(BASE_PATH.key(), String.format("s3://%s/lake/db/tbl-default", TEST_BUCKET));
    providerProperties.put(HoodieAWSConfig.AWS_SECRET_KEY.key(), S3_CONTAINER.getSecretKey());
    providerProperties.put(HoodieAWSConfig.AWS_ACCESS_KEY.key(), S3_CONTAINER.getAccessKey());
    lockProvider = createLockProvider();
  }

  @Test
  void testS3Preconditions() {
    // Similar to GCS test, just to show we can put and head objects
    String key = "my-test-object";
    // Put an object
    PutObjectResponse put1 = s3Client.putObject(
            PutObjectRequest.builder()
                    .bucket(TEST_BUCKET)
                    .key(key)
                    .serverSideEncryption(ServerSideEncryption.AES256)
                    .ifNoneMatch("*")
                    .build(),
            RequestBody.fromString("Hello1"));

    // Put a second object with different contents
    PutObjectResponse put2 = s3Client.putObject(
            PutObjectRequest.builder()
                    .bucket(TEST_BUCKET)
                    .key(key)
                    .serverSideEncryption(ServerSideEncryption.AES256)
                    .ifMatch(put1.eTag())
                    .build(),
            RequestBody.fromString("Hello2"));
    s3Client.close();
    // ETags should differ
    assertNotNull(put1.eTag());
    assertNotNull(put2.eTag());
    assertNotEquals(put1.eTag(), put2.eTag(), "ETags should differ if the content changed");
  }
}
