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
import org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

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
    LockManager lockManager = new LockManager(writeConfig,
        (FileSystem) this.metaClient.getStorage().getFileSystem());
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
}
