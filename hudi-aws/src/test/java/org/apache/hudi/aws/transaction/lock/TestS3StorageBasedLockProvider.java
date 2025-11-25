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
import org.apache.hudi.client.transaction.lock.models.StorageLockFile;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.config.HoodieAWSConfig;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

import static org.apache.hudi.common.config.HoodieCommonConfig.BASE_PATH;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mockStatic;
import java.net.URI;

import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

/**
 * Tests S3-based StorageBasedLockProvider using a LocalStack container
 * to emulate S3, with Toxiproxy for network failure injection.
 */
public class TestS3StorageBasedLockProvider extends StorageBasedLockProviderTestBase {

  private static final DockerImageName LOCALSTACK_IMAGE =
          DockerImageName.parse("localstack/localstack:4.1.0");
  private static final DockerImageName TOXIPROXY_IMAGE =
          DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.5.0");

  private static Network network;
  private static LocalStackContainer S3_CONTAINER;
  private static ToxiproxyContainer TOXIPROXY_CONTAINER;
  private static ToxiproxyContainer.ContainerProxy s3Proxy;
  private static MockedStatic<S3Client> s3ClientStaticMock;

  private static final String TEST_BUCKET = "test-bucket";
  private static S3Client s3Client;

  @BeforeAll
  static void initContainer() {
    // Create a shared network for containers to communicate
    network = Network.newNetwork();

    // Spin up LocalStack with S3 service on the shared network
    S3_CONTAINER = new LocalStackContainer(LOCALSTACK_IMAGE)
            .withNetwork(network)
            .withNetworkAliases("localstack")
            .withServices(S3);
    S3_CONTAINER.start();

    // Spin up Toxiproxy on the same network
    TOXIPROXY_CONTAINER = new ToxiproxyContainer(TOXIPROXY_IMAGE)
            .withNetwork(network);
    TOXIPROXY_CONTAINER.start();

    // Create proxy: S3Client → Toxiproxy → LocalStack
    // Toxiproxy will forward requests to LocalStack and allow us to inject failures
    s3Proxy = TOXIPROXY_CONTAINER.getProxy(S3_CONTAINER, 4566);

    // Create S3 client for setup (uses direct connection to LocalStack, not through proxy)
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

    // Setup persistent MockedStatic (not try-with-resources)
    // This intercepts S3Client.builder() calls to route through Toxiproxy
    s3ClientStaticMock = mockStatic(S3Client.class, Mockito.CALLS_REAL_METHODS);
    s3ClientStaticMock.when(S3Client::builder).thenAnswer(invocation -> {
      S3ClientBuilder builder = (S3ClientBuilder) invocation.callRealMethod();
      // Point to Toxiproxy instead of LocalStack directly
      URI proxyUri = URI.create(String.format("http://%s:%d",
              s3Proxy.getContainerIpAddress(),
              s3Proxy.getProxyPort()));
      builder.endpointOverride(proxyUri)
             .forcePathStyle(true);  // Required for LocalStack to avoid virtual-hosted-style URLs
      return builder;
    });
  }

  @AfterAll
  static void stopContainer() {
    // Close the mocked static
    if (s3ClientStaticMock != null) {
      s3ClientStaticMock.close();
    }
    // Close the S3 client
    if (s3Client != null) {
      s3Client.close();
    }
    // Stop Toxiproxy container
    if (TOXIPROXY_CONTAINER != null) {
      TOXIPROXY_CONTAINER.stop();
    }
    // Stop LocalStack container
    if (S3_CONTAINER != null) {
      S3_CONTAINER.stop();
    }
    // Close the network
    if (network != null) {
      network.close();
    }
  }

  /**
   * Creates a real S3-based StorageBasedLockProvider for testing.
   * The S3Client created by the provider will route through Toxiproxy (configured in initContainer).
   */
  @Override
  protected StorageBasedLockProvider createLockProvider() {
    LockConfiguration lockConf = new LockConfiguration(providerProperties);
    // No try-with-resources - using the persistent mock from initContainer
    return new StorageBasedLockProvider(lockConf, null);
  }

  @BeforeEach
  void setupLockProvider() {
    // Clear any toxics from previous tests
    try {
      s3Proxy.toxics().getAll().forEach(toxic -> {
        try {
          toxic.remove();
        } catch (Exception e) {
          // Ignore removal errors
        }
      });
    } catch (Exception e) {
      // Ignore errors getting toxics
    }

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

  /**
   * Test that reproduces the bug where a conditional PUT succeeds on S3 but returns 412 due to SDK retry.
   *
   * Scenario:
   * 1. Client sends conditional PUT with ETag precondition
   * 2. Request reaches LocalStack and succeeds (lock file is updated)
   * 3. Toxiproxy delays the response, causing client timeout
   * 4. AWS SDK retries the same conditional PUT
   * 5. Retry gets 412 because ETag changed (first request succeeded)
   * 6. Client thinks operation failed, but lock is actually acquired on S3
   *
   * This test demonstrates the inconsistent state: tryLock() returns false but S3 has our lock.
   */
  @Test
  void testConditionalPutSucceedsButReturns412DueToSDKRetry() throws Exception {
    // Step 1: Configure shorter timeouts for this test
    // The S3StorageLockClient sets apiCallTimeout to validity/5, so validity=5 means timeout=1s
    providerProperties.put("hoodie.write.lock.storage_based.storage.validity.timeout.seconds", "5");

    // Step 2: Create initial lock to establish baseline state
    StorageBasedLockProvider initialProvider = createLockProvider();
    boolean initialLockAcquired = initialProvider.tryLock();
    assertNotNull(initialProvider, "Initial lock provider should be created");

    // Track the initial lock owner ID so we can verify it changes
    String initialOwnerId = null;
    if (initialLockAcquired) {
      // Read the initial lock file to get the owner ID
      try (S3Client directClient = S3Client.builder()
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
          .forcePathStyle(true)
          .build()) {

        String lockKey = "lake/db/tbl-default/.hoodie/.locks/table_lock.json";
        ResponseInputStream<GetObjectResponse> response = directClient.getObject(
            GetObjectRequest.builder()
                .bucket(TEST_BUCKET)
                .key(lockKey)
                .build()
        );
        StorageLockFile initialLock = StorageLockFile.createFromStream(response, response.response().eTag());
        initialOwnerId = initialLock.getOwner();
        System.out.println("Initial provider owner ID: " + initialOwnerId);
      }

      // Release and close the initial lock
      initialProvider.unlock();
    }
    initialProvider.close();
    assertNotNull(initialOwnerId, "Should have captured initial owner ID");

    // Step 3: Create lock provider BEFORE adding toxic
    // This ensures provider initialization succeeds
    StorageBasedLockProvider provider = createLockProvider();

    // Step 4: Simulate network failure using very long latency toxic
    // Add 15s latency to all downstream traffic (responses from S3 to client)
    // This exceeds the total SDK timeout budget including retries (~5s total)
    // Requests (upstream) go through normally, so PUT succeeds on S3
    // But client times out before receiving any response
    s3Proxy.toxics()
        .latency("slow_response", ToxicDirection.DOWNSTREAM, 15000);  // 15 second delay

    // Step 5: Attempt to acquire lock
    // The S3Client will timeout and retry during this call
    boolean lockAcquired = false;
    Exception caughtException = null;
    try {
      lockAcquired = provider.tryLock();
    } catch (Exception e) {
      // May throw exception due to timeout
      caughtException = e;
      System.out.println("Exception during lock acquisition: " + e.getMessage());
    }

    // Step 6: Remove toxic so verification can succeed
    try {
      s3Proxy.toxics().get("slow_response").remove();
    } catch (Exception e) {
      // Ignore if already removed
    }

    // Step 7: Check results
    // The bug occurs when: tryLock() returns false OR throws exception, but lock was actually acquired
    System.out.println("Lock acquired result: " + lockAcquired);
    System.out.println("Exception caught: " + (caughtException != null ? caughtException.getClass().getSimpleName() : "none"));

    // Step 8: Verify actual lock state by reading directly from LocalStack (bypassing Toxiproxy)
    // Create a direct client to LocalStack for verification
    S3Client directClient = S3Client.builder()
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
        .forcePathStyle(true)  // Required for LocalStack
        .build();

    try {
      // Read the lock file from S3
      String lockKey = "lake/db/tbl-default/.hoodie/.locks/table_lock.json";
      ResponseInputStream<GetObjectResponse> response = directClient.getObject(
          GetObjectRequest.builder()
              .bucket(TEST_BUCKET)
              .key(lockKey)
              .build()
      );

      String eTag = response.response().eTag();
      StorageLockFile actualLockOnS3 = StorageLockFile.createFromStream(response, eTag);

      // Step 9: Validate that bug occurred - the INCONSISTENCY is the bug
      // Bug = Client thinks it failed (lockAcquired=false OR exception thrown)
      //       BUT lock file exists on S3 (operation actually succeeded)

      // Assert client-side failure: if tryLock() succeeded, this is NOT the bug scenario
      if (lockAcquired && caughtException == null) {
        fail("Bug NOT reproduced: tryLock() succeeded (returned true and no exception). " +
            "Expected: tryLock() should fail but lock should exist on S3.");
      }

      // At this point, we know client thinks it failed
      // Now verify server succeeded (lock file exists on S3)
      assertNotNull(actualLockOnS3,
          "Bug NOT reproduced: lock file should exist on S3 despite client failure");
      assertNotNull(actualLockOnS3.getOwner(),
          "Bug NOT reproduced: lock file should have owner despite client failure");

      // CRITICAL: Verify that the lock owner on S3 is NOT the initial provider
      // This proves that the second provider's PUT succeeded on S3 even though the client thinks it failed
      System.out.println("DEBUG: Initial owner ID (first provider): " + initialOwnerId);
      System.out.println("DEBUG: Actual owner ID on S3: " + actualLockOnS3.getOwner());

      assertNotEquals(initialOwnerId, actualLockOnS3.getOwner(),
          "Bug NOT reproduced correctly: Lock owner on S3 is still the initial provider (" + initialOwnerId + "). " +
          "Expected it to change to the second provider. " +
          "This means the second provider's PUT never succeeded on S3.");

      // Bug is confirmed! Client thinks it failed but S3 has the lock
      System.out.println("=== BUG REPRODUCED ===");
      System.out.println("Inconsistency confirmed:");
      System.out.println("  Client: tryLock() = " + lockAcquired +
          ", exception = " + (caughtException != null ? caughtException.getMessage() : "none"));
      System.out.println("  Server: Lock exists with owner " + actualLockOnS3.getOwner());

      // Test succeeded in reproducing the bug!

    } finally {
      directClient.close();
      provider.close();
    }
  }
}
