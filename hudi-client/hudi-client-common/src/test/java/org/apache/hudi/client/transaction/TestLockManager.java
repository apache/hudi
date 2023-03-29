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
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class TestLockManager extends HoodieCommonTestHarness {

  private static final Logger LOG = LogManager.getLogger(TestLockManager.class);

  private static TestingServer server;
  private static String zk_basePath = "/hudi/test/lock";
  private static String key = "table1";

  HoodieWriteConfig writeConfig;
  LockManager lockManager;

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

  private HoodieWriteConfig getWriteConfig() {
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
            .build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(ZookeeperBasedLockProvider.class)
            .withZkBasePath(zk_basePath)
            .withZkLockKey(key)
            .withZkQuorum(server.getConnectString())
            .build())
        .build();
  }

  @BeforeEach
  private void init() throws IOException {
    initPath();
    initMetaClient();
    this.writeConfig = getWriteConfig();
    this.lockManager = new LockManager(this.writeConfig, this.metaClient.getFs());
  }

  @Test
  public void testLockAndUnlock() {
    LockManager mockLockManager = Mockito.spy(lockManager);

    assertDoesNotThrow(() -> {
      mockLockManager.lock();
    });

    assertDoesNotThrow(() -> {
      mockLockManager.unlock();
    });

    Mockito.verify(mockLockManager).close();
  }
}
