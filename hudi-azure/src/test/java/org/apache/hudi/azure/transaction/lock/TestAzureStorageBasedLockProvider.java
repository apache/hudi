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

package org.apache.hudi.azure.transaction.lock;

import org.apache.hudi.client.transaction.lock.StorageBasedLockProvider;
import org.apache.hudi.client.transaction.lock.StorageBasedLockProviderTestBase;
import org.apache.hudi.common.config.LockConfiguration;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import static org.apache.hudi.common.config.HoodieCommonConfig.BASE_PATH;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests Azure Blob Storage-based StorageBasedLockProvider using Azurite container
 * to emulate Azure Blob Storage.
 */
@Disabled("Integration test requiring Docker - enable for manual testing")
public class TestAzureStorageBasedLockProvider extends StorageBasedLockProviderTestBase {

  private static final DockerImageName AZURITE_IMAGE =
      DockerImageName.parse("mcr.microsoft.com/azure-storage/azurite:latest");

  private static GenericContainer<?> AZURITE_CONTAINER;

  private static final String TEST_CONTAINER = "test-container";
  private static final String AZURITE_ACCOUNT_NAME = "devstoreaccount1";
  private static final String AZURITE_ACCOUNT_KEY =
      "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

  private static BlobServiceClient blobServiceClient;
  private static String connectionString;

  @BeforeAll
  static void initContainer() {
    // Spin up Azurite container with Blob service
    AZURITE_CONTAINER = new GenericContainer<>(AZURITE_IMAGE)
        .withExposedPorts(10000)  // Blob service port
        .withCommand("azurite-blob", "--blobHost", "0.0.0.0");
    AZURITE_CONTAINER.start();

    String blobEndpoint = String.format("http://%s:%d/%s",
        AZURITE_CONTAINER.getHost(),
        AZURITE_CONTAINER.getMappedPort(10000),
        AZURITE_ACCOUNT_NAME);

    connectionString = String.format(
        "DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=%s;",
        AZURITE_ACCOUNT_NAME,
        AZURITE_ACCOUNT_KEY,
        blobEndpoint);

    blobServiceClient = new BlobServiceClientBuilder()
        .connectionString(connectionString)
        .buildClient();

    BlobContainerClient containerClient = blobServiceClient.createBlobContainer(TEST_CONTAINER);
    assertNotNull(containerClient);
  }

  @AfterAll
  static void stopContainer() {
    if (AZURITE_CONTAINER != null) {
      AZURITE_CONTAINER.stop();
    }
  }

  /**
   * Creates a real Azure-based StorageBasedLockProvider for testing.
   */
  @Override
  protected StorageBasedLockProvider createLockProvider() {
    LockConfiguration lockConf = new LockConfiguration(providerProperties);
    return new StorageBasedLockProvider(lockConf, null);
  }

  @BeforeEach
  void setupLockProvider() {
    // Provide base path for the test using WASB format
    String basePath = String.format("wasbs://%s@%s.blob.core.windows.net/lake/db/tbl-default",
        TEST_CONTAINER, AZURITE_ACCOUNT_NAME);
    providerProperties.put(BASE_PATH.key(), basePath);
    // Note: The lock client will use DefaultAzureCredential which will work with
    // the Azurite endpoint. For real Azure environments, no configuration is needed.
    lockProvider = createLockProvider();
  }

  @Test
  void testAzurePreconditions() {
    String blobName = "test-preconditions-blob";
    BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(TEST_CONTAINER);

    BlobParallelUploadOptions options1 = new BlobParallelUploadOptions(BinaryData.fromString("Content1"))
        .setRequestConditions(new BlobRequestConditions().setIfNoneMatch("*"));
    var response1 = containerClient.getBlobClient(blobName)
        .uploadWithResponse(options1, null, null);
    assertNotNull(response1.getValue().getETag());
    String etag1 = response1.getValue().getETag();

    BlobParallelUploadOptions options2 = new BlobParallelUploadOptions(BinaryData.fromString("Content2"))
        .setRequestConditions(new BlobRequestConditions().setIfMatch(etag1));
    var response2 = containerClient.getBlobClient(blobName)
        .uploadWithResponse(options2, null, null);
    assertNotNull(response2.getValue().getETag());
    String etag2 = response2.getValue().getETag();

    assertNotEquals(etag1, etag2);
  }
}
