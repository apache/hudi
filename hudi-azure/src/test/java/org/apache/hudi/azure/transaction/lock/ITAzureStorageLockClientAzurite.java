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

import org.apache.hudi.client.transaction.lock.LockGetResult;
import org.apache.hudi.client.transaction.lock.LockUpsertResult;
import org.apache.hudi.client.transaction.lock.StorageLockData;
import org.apache.hudi.client.transaction.lock.StorageLockFile;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.AzureStorageLockConfig;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.testcontainers.containers.ContainerFetchException;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.abort;

/**
 * Integration tests for {@link AzureStorageLockClient} using Azurite (Azure Storage emulator).
 *
 * <p>Run with: {@code mvn -Pazure-integration-tests -pl hudi-azure verify}
 *
 * <p>If the Azurite Docker image cannot be pulled (Microsoft Container Registry blocked,
 * rate-limited, or network unavailable), the test class is skipped via {@link
 * org.junit.jupiter.api.Assumptions#abort(String)} rather than failing the CI run. Integration
 * tests against external container images should not gate the overall build on registry-side
 * outages that are outside the project's control.
 */
@Slf4j
@DisabledIfEnvironmentVariable(named = "SKIP_AZURITE_IT", matches = "true")
public class ITAzureStorageLockClientAzurite {

  // Pin the Azurite version. The previous unpinned reference (implicit ":latest") was more
  // susceptible to MCR rate-limiting / WAF blocks because :latest is queried more aggressively
  // and is not cached as effectively as a tagged manifest. Bump deliberately when needed.
  private static final DockerImageName AZURITE_IMAGE =
      DockerImageName.parse("mcr.microsoft.com/azure-storage/azurite:3.34.0");

  // Standard Azurite defaults (documented by Microsoft)
  private static final String ACCOUNT_NAME = "devstoreaccount1";
  // Standard Azurite dev account key (NOT a secret; used by the emulator by default)
  // See: https://learn.microsoft.com/azure/storage/common/storage-use-azurite?tabs=visual-studio#connection-strings
  private static final String ACCOUNT_KEY =
      "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

  // Manual container lifecycle (instead of @Testcontainers + @Container) so the test class is
  // skipped — not failed — when the image cannot be pulled. The @Testcontainers extension
  // surfaces image-pull errors as test errors, which is the wrong outcome for an external
  // infrastructure failure (e.g. MCR returning HTTP 403 "request blocked" via Azure WAF).
  private static GenericContainer<?> azurite;

  @BeforeAll
  static void startAzurite() {
    GenericContainer<?> container = new GenericContainer<>(AZURITE_IMAGE)
        .withExposedPorts(10000)
        .withCommand("azurite-blob", "--blobHost", "0.0.0.0", "--blobPort", "10000", "--loose")
        .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(60)));
    try {
      container.start();
      azurite = container;
    } catch (ContainerFetchException | ContainerLaunchException e) {
      // Image pull / container start failure. Most commonly caused by MCR being unreachable
      // or rate-limited from CI runners. Skip the suite rather than failing the build.
      log.warn("Azurite container unavailable; skipping suite.", e);
      abort("Azurite container could not be started (image pull or launch failed): " + e.getMessage());
    } catch (RuntimeException e) {
      // Testcontainers wraps a variety of Docker / network failures in plain RuntimeExceptions
      // during start(). Treat any "could not start container" failure as a skip rather than
      // an error for the same reason — these are infrastructure flakes, not test failures.
      // Re-throw if Docker itself is missing so the developer gets a clear diagnosis locally.
      // Known messages we want to surface: "Could not find a valid Docker environment",
      // "Docker not found", "docker: command not found".
      String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
      if (msg.contains("docker") && (msg.contains("not found") || msg.contains("no such")
          || msg.contains("could not find") || msg.contains("not running"))) {
        throw e;
      }
      log.warn("Azurite container unavailable; skipping suite.", e);
      abort("Azurite container could not be started: " + e.getMessage());
    }
  }

  @AfterAll
  static void stopAzurite() {
    if (azurite != null && azurite.isRunning()) {
      azurite.stop();
    }
  }

  private static String blobEndpoint() {
    // Azurite expects /<accountName> in the endpoint URL path
    return "http://" + azurite.getHost() + ":" + azurite.getMappedPort(10000) + "/" + ACCOUNT_NAME;
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
    String lockFileUri = "https://localhost:10000/" + ACCOUNT_NAME + "/" + container + "/" + blobPath;
    Properties props = new Properties();
    props.setProperty(AzureStorageLockConfig.AZURE_CONNECTION_STRING.key(), connectionString());

    AzureStorageLockClient owner1 = new AzureStorageLockClient("owner1", lockFileUri, props);
    StorageLockData lockData1 = new StorageLockData(false, System.currentTimeMillis() + 60_000, "owner1");
    Pair<LockUpsertResult, Option<StorageLockFile>> upsert1 = owner1.tryUpsertLockFile(lockData1, Option.empty());
    assertEquals(LockUpsertResult.SUCCESS, upsert1.getLeft());
    assertTrue(upsert1.getRight().isPresent());

    // Read back
    Pair<LockGetResult, Option<StorageLockFile>> read = owner1.readCurrentLockFile();
    assertEquals(LockGetResult.SUCCESS, read.getLeft());
    assertTrue(read.getRight().isPresent());

    // Wrong ETag should fail with precondition and be mapped to ACQUIRED_BY_OTHERS
    AzureStorageLockClient owner2 = new AzureStorageLockClient("owner2", lockFileUri, props);
    StorageLockFile wrongPrev = new StorageLockFile(lockData1, "\"etag-does-not-match\"");
    StorageLockData lockData2 = new StorageLockData(false, System.currentTimeMillis() + 60_000, "owner2");
    Pair<LockUpsertResult, Option<StorageLockFile>> upsert2 =
        owner2.tryUpsertLockFile(lockData2, Option.of(wrongPrev));
    assertEquals(LockUpsertResult.ACQUIRED_BY_OTHERS, upsert2.getLeft());
  }
}
