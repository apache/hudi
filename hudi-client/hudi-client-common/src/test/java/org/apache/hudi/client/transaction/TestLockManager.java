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

package org.apache.hudi.client.transaction;

import org.apache.hudi.client.transaction.lock.LockManager;
import org.apache.hudi.client.transaction.lock.StorageBasedLockProvider;
import org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;

import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestLockManager extends HoodieCommonTestHarness {

  private static final Logger LOG = LoggerFactory.getLogger(TestLockManager.class);

  private static TestingServer server;
  private static final String ZK_BASE_PATH = "/hudi/test/lock";
  private static final String KEY = "table1";

  @BeforeAll
  public static void setup() {
    while (server == null) {
      try {
        server = new TestingServer();
      } catch (Exception e) {
        LOG.error("Getting bind exception - retrying to allocate server");
        server = null;
      }
    }
  }

  @AfterAll
  public static void tearDown() throws IOException {
    if (server != null) {
      server.close();
    }
  }

  @BeforeEach
  void init() throws IOException {
    initPath();
    initMetaClient();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testLockAndUnlock(boolean multiWriter) {
    HoodieWriteConfig writeConfig = multiWriter ? getMultiWriterWriteConfig() : getSingleWriterWriteConfig();
    LockManager lockManager = new LockManager(writeConfig, this.metaClient.getStorage());
    LockManager mockLockManager = Mockito.spy(lockManager);

    assertDoesNotThrow(() -> {
      mockLockManager.lock();
    });

    assertDoesNotThrow(() -> {
      mockLockManager.unlock();
    });

    Mockito.verify(mockLockManager).close();
  }

  private HoodieWriteConfig getMultiWriterWriteConfig() {
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
            .build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(ZookeeperBasedLockProvider.class)
            .withZkBasePath(ZK_BASE_PATH)
            .withZkLockKey(KEY)
            .withZkQuorum(server.getConnectString())
            .build())
        .build();
  }

  private HoodieWriteConfig getSingleWriterWriteConfig() {
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(ZookeeperBasedLockProvider.class)
            .withZkBasePath(ZK_BASE_PATH)
            .withZkLockKey(KEY)
            .withZkQuorum(server.getConnectString())
            .build())
        .build();
  }

  @Test
  void testLockManagerTriesMetricsConstructorFirst() {
    HoodieWriteConfig writeConfig = getStorageBasedLockWriteConfig();
    LockManager lockManager = new LockManager(writeConfig, this.metaClient.getStorage());

    // The getLockProvider() call should fail due to invalid scheme in basePath, 
    // but this validates that we successfully found and tried the metrics constructor
    // (rather than falling back to the standard constructor due to reflection issues)
    HoodieException exception = assertThrows(HoodieException.class, lockManager::getLockProvider,
        "StorageBasedLockProvider should fail with HoodieLockException due to invalid scheme, but this confirms metrics constructor was found");
    
    // Assert that the exception message references the scheme failure, confirming we got to the 
    // StorageBasedLockProvider instantiation (not a reflection failure)
    assertTrue(exception.getCause().getCause().getCause().getMessage().startsWith("Unsupported scheme"),
        "Exception should mention scheme failure, confirming metrics constructor was successfully found and used");

    lockManager.close();
  }

  @Test
  void testLockManagerFallbackToStandardConstructor() {
    // Use ZookeeperBasedLockProvider which doesn't have the metrics constructor
    HoodieWriteConfig writeConfig = getSingleWriterWriteConfig();
    LockManager lockManager = new LockManager(writeConfig, this.metaClient.getStorage());

    // Get the lock provider to trigger instantiation and fallback
    assertDoesNotThrow(() -> {
      lockManager.getLockProvider();
    });

    lockManager.close();
  }

  private HoodieWriteConfig getStorageBasedLockWriteConfig() {
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(StorageBasedLockProvider.class)
            .build())
        .build();
  }
}
