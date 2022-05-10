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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.exception.HoodieLockException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_BASE_PATH_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_CONNECTION_TIMEOUT_MS_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_CONNECT_URL_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_LOCK_KEY_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_SESSION_TIMEOUT_MS_PROP_KEY;

public class TestZookeeperBasedLockProvider {

  private static final Logger LOG = LogManager.getLogger(TestZookeeperBasedLockProvider.class);

  private static TestingServer server;
  private static CuratorFramework client;
  private static String basePath = "/hudi/test/lock";
  private static String key = "table1";
  private static LockConfiguration lockConfiguration;

  @BeforeAll
  public static void setup() {
    while (server == null) {
      try {
        server = new TestingServer();
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        client = builder.connectString(server.getConnectString()).retryPolicy(new RetryOneTime(1000)).build();
      } catch (Exception e) {
        LOG.error("Getting bind exception - retrying to allocate server");
        server = null;
      }
    }
    Properties properties = new Properties();
    properties.setProperty(ZK_BASE_PATH_PROP_KEY, basePath);
    properties.setProperty(ZK_LOCK_KEY_PROP_KEY, key);
    properties.setProperty(ZK_CONNECT_URL_PROP_KEY, server.getConnectString());
    properties.setProperty(ZK_BASE_PATH_PROP_KEY, server.getTempDirectory().getAbsolutePath());
    properties.setProperty(ZK_SESSION_TIMEOUT_MS_PROP_KEY, "10000");
    properties.setProperty(ZK_CONNECTION_TIMEOUT_MS_PROP_KEY, "10000");
    properties.setProperty(ZK_LOCK_KEY_PROP_KEY, "key");
    properties.setProperty(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "1000");
    lockConfiguration = new LockConfiguration(properties);
  }

  @Test
  public void testAcquireLock() {
    ZookeeperBasedLockProvider zookeeperBasedLockProvider = new ZookeeperBasedLockProvider(lockConfiguration, client);
    Assertions.assertTrue(zookeeperBasedLockProvider.tryLockWithInstant(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS, "123"));
    zookeeperBasedLockProvider.unlock();
  }

  @Test
  public void testUnLock() {
    ZookeeperBasedLockProvider zookeeperBasedLockProvider = new ZookeeperBasedLockProvider(lockConfiguration, client);
    Assertions.assertTrue(zookeeperBasedLockProvider.tryLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    zookeeperBasedLockProvider.unlock();
    zookeeperBasedLockProvider.tryLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS);
  }

  @Test
  public void testReentrantLock() {
    ZookeeperBasedLockProvider zookeeperBasedLockProvider = new ZookeeperBasedLockProvider(lockConfiguration, client);
    Assertions.assertTrue(zookeeperBasedLockProvider.tryLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    try {
      zookeeperBasedLockProvider.tryLock(lockConfiguration.getConfig()
          .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS);
      Assertions.fail();
    } catch (HoodieLockException e) {
      // expected
    }
    zookeeperBasedLockProvider.unlock();
  }

  @Test
  public void testUnlockWithoutLock() {
    ZookeeperBasedLockProvider zookeeperBasedLockProvider = new ZookeeperBasedLockProvider(lockConfiguration, client);
    zookeeperBasedLockProvider.unlock();
  }
}
