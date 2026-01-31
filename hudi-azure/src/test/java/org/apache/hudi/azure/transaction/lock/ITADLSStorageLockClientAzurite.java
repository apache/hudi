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

import org.apache.hudi.client.transaction.lock.models.LockGetResult;
import org.apache.hudi.client.transaction.lock.models.LockUpsertResult;
import org.apache.hudi.client.transaction.lock.models.StorageLockData;
import org.apache.hudi.client.transaction.lock.models.StorageLockFile;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for {@link ADLSStorageLockClient} using Azurite (Azure Storage emulator).
 *
 * <p>Run with: {@code mvn -Pazure-integration-tests -pl hudi-azure verify}
 */
@Testcontainers(disabledWithoutDocker = true)
@DisabledIfEnvironmentVariable(named = "SKIP_AZURITE_IT", matches = "true")
public class ITADLSStorageLockClientAzurite {

  private static final DockerImageName AZURITE_IMAGE =
      DockerImageName.parse("mcr.microsoft.com/azure-storage/azurite");

  // Standard Azurite defaults (documented by Microsoft)
  private static final String ACCOUNT_NAME = "devstoreaccount1";
  // Standard Azurite dev account key (NOT a secret; used by the emulator by default)
  // See: https://learn.microsoft.com/azure/storage/common/storage-use-azurite?tabs=visual-studio#connection-strings
  private static final String ACCOUNT_KEY =
      "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

  @Container
  public static final GenericContainer<?> AZURITE =
      new GenericContainer<>(AZURITE_IMAGE)
          .withExposedPorts(10000)
          .withCommand("azurite-blob", "--blobHost", "0.0.0.0", "--blobPort", "10000", "--loose")
          .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(60)));

  private static String blobEndpoint() {
    // Azurite expects /<accountName> in the endpoint URL path
    return "http://" + AZURITE.getHost() + ":" + AZURITE.getMappedPort(10000) + "/" + ACCOUNT_NAME;
  }

  private static String connectionString() {
    String key = System.getenv("AZURITE_ACCOUNT_KEY");
    if (key == null || key.trim().isEmpty()) {
      key = ACCOUNT_KEY;
    }
    return "DefaultEndpointsProtocol=http;"
        + "AccountName=" + ACCOUNT_NAME + ";"
        + "AccountKey=" + key + ";"
        + "BlobEndpoint=" + blobEndpoint() + ";";
  }

  @Test
  void testCreateUpdateAndReadLockFileWithAzurite() {
    String container = "container";
    String blobPath = "locks/table_lock.json";

    BlobServiceClient svc = new BlobServiceClientBuilder()
        .connectionString(connectionString())
        .buildClient();
    BlobContainerClient containerClient = svc.getBlobContainerClient(container);
    containerClient.createIfNotExists();

    // NOTE: lockFileUri only needs to be parseable for container/blobPath extraction.
    // The actual endpoint comes from the connection string.
    String lockFileUri = "https://localhost:10000/" + container + "/" + blobPath;
    Properties props = new Properties();
    props.setProperty(ADLSStorageLockClient.AZURE_CONNECTION_STRING, connectionString());

    ADLSStorageLockClient owner1 = new ADLSStorageLockClient("owner1", lockFileUri, props);
    StorageLockData lockData1 = new StorageLockData(false, System.currentTimeMillis() + 60_000, "owner1");
    Pair<LockUpsertResult, Option<StorageLockFile>> upsert1 = owner1.tryUpsertLockFile(lockData1, Option.empty());
    assertEquals(LockUpsertResult.SUCCESS, upsert1.getLeft());
    assertTrue(upsert1.getRight().isPresent());

    // Read back
    Pair<LockGetResult, Option<StorageLockFile>> read = owner1.readCurrentLockFile();
    assertEquals(LockGetResult.SUCCESS, read.getLeft());
    assertTrue(read.getRight().isPresent());

    // Wrong ETag should fail with precondition and be mapped to ACQUIRED_BY_OTHERS
    ADLSStorageLockClient owner2 = new ADLSStorageLockClient("owner2", lockFileUri, props);
    StorageLockFile wrongPrev = new StorageLockFile(lockData1, "\"etag-does-not-match\"");
    StorageLockData lockData2 = new StorageLockData(false, System.currentTimeMillis() + 60_000, "owner2");
    Pair<LockUpsertResult, Option<StorageLockFile>> upsert2 =
        owner2.tryUpsertLockFile(lockData2, Option.of(wrongPrev));
    assertEquals(LockUpsertResult.ACQUIRED_BY_OTHERS, upsert2.getLeft());
  }
}
