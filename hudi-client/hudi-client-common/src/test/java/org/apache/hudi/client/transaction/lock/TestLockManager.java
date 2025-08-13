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

package org.apache.hudi.client.transaction.lock;

import org.apache.hudi.client.transaction.lock.metrics.HoodieLockMetrics;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
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
import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
    LockManager lockManager = new LockManager(writeConfig, this.metaClient.getFs());
    LockManager mockLockManager = Mockito.spy(lockManager);

    assertDoesNotThrow(() -> {
      mockLockManager.lock();
    });

    assertDoesNotThrow(() -> {
      mockLockManager.unlock();
    });

    verify(mockLockManager, times(1)).closeQuietly(false);
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
    LockManager lockManager = new LockManager(writeConfig, this.metaClient.getFs());

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
    LockManager lockManager = new LockManager(writeConfig, this.metaClient.getFs());

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

  @Test
  void testMetricsReportedOnceWhenUnlockCalled() throws Exception {
    // Create write config with metrics enabled
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withMetricsConfig(HoodieMetricsConfig.newBuilder()
            .withLockingMetrics(true)
            .build())
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(ZookeeperBasedLockProvider.class)
            .withZkBasePath(ZK_BASE_PATH)
            .withZkLockKey(KEY)
            .withZkQuorum(server.getConnectString())
            .build())
        .build();

    LockManager lockManager = new LockManager(writeConfig, this.metaClient.getFs());
    
    // Create a mock HoodieLockMetrics and inject it
    HoodieLockMetrics mockMetrics = Mockito.mock(HoodieLockMetrics.class);
    Field metricsField = LockManager.class.getDeclaredField("metrics");
    metricsField.setAccessible(true);
    metricsField.set(lockManager, mockMetrics);

    // Perform lock and unlock
    lockManager.lock();
    lockManager.unlock();

    // Verify updateLockHeldTimerMetrics is called exactly once (only in unlock)
    verify(mockMetrics, times(1)).updateLockHeldTimerMetrics();
  }

  @Test
  void testMetricsReportedOnceWhenCloseCalledWithoutUnlock() throws Exception {
    // Create write config with metrics enabled
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withMetricsConfig(HoodieMetricsConfig.newBuilder()
            .withLockingMetrics(true)
            .build())
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(ZookeeperBasedLockProvider.class)
            .withZkBasePath(ZK_BASE_PATH)
            .withZkLockKey(KEY)
            .withZkQuorum(server.getConnectString())
            .build())
        .build();

    LockManager lockManager = new LockManager(writeConfig, this.metaClient.getFs());
    
    // Create a mock HoodieLockMetrics and inject it
    HoodieLockMetrics mockMetrics = Mockito.mock(HoodieLockMetrics.class);
    Field metricsField = LockManager.class.getDeclaredField("metrics");
    metricsField.setAccessible(true);
    metricsField.set(lockManager, mockMetrics);

    // Perform lock and then close directly (simulating error scenario or shutdown)
    lockManager.lock();
    lockManager.close();

    // Verify updateLockHeldTimerMetrics is called exactly once (only in close)
    verify(mockMetrics, times(1)).updateLockHeldTimerMetrics();
  }

  @Test
  void testMetricsIdempotencyWithMultipleCalls() throws Exception {
    // Create write config with metrics enabled
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withMetricsConfig(HoodieMetricsConfig.newBuilder()
            .withLockingMetrics(true)
            .build())
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(ZookeeperBasedLockProvider.class)
            .withZkBasePath(ZK_BASE_PATH)
            .withZkLockKey(KEY)
            .withZkQuorum(server.getConnectString())
            .build())
        .build();

    LockManager lockManager = new LockManager(writeConfig, this.metaClient.getFs());
    
    // Create a mock HoodieLockMetrics and inject it
    HoodieLockMetrics mockMetrics = Mockito.mock(HoodieLockMetrics.class);
    Field metricsField = LockManager.class.getDeclaredField("metrics");
    metricsField.setAccessible(true);
    metricsField.set(lockManager, mockMetrics);

    // Perform lock and unlock
    lockManager.lock();
    lockManager.unlock();
    
    // Try to close again (should not call updateLockHeldTimerMetrics again)
    lockManager.close();
    
    // Verify updateLockHeldTimerMetrics is still called exactly once
    verify(mockMetrics, times(1)).updateLockHeldTimerMetrics();
  }

  @Test
  void testMetricsCalledWhenLockNotAcquired() throws Exception {
    // Create write config with metrics enabled
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withMetricsConfig(HoodieMetricsConfig.newBuilder()
            .withLockingMetrics(true)
            .build())
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(ZookeeperBasedLockProvider.class)
            .withZkBasePath(ZK_BASE_PATH)
            .withZkLockKey(KEY)
            .withZkQuorum(server.getConnectString())
            .build())
        .build();

    LockManager lockManager = new LockManager(writeConfig, this.metaClient.getFs());
    
    // Initialize the lock provider by calling getLockProvider (but don't acquire lock)
    lockManager.getLockProvider();
    
    // Create a mock HoodieLockMetrics and inject it
    HoodieLockMetrics mockMetrics = Mockito.mock(HoodieLockMetrics.class);
    Field metricsField = LockManager.class.getDeclaredField("metrics");
    metricsField.setAccessible(true);
    metricsField.set(lockManager, mockMetrics);

    // Close without acquiring lock (but lockProvider is initialized)
    lockManager.close();
    
    // Verify updateLockHeldTimerMetrics is called (but will be a no-op internally
    // since no timer was started - lockDurationTimer.tryEndTimer() returns Option.empty())
    verify(mockMetrics, times(1)).updateLockHeldTimerMetrics();
  }
}
