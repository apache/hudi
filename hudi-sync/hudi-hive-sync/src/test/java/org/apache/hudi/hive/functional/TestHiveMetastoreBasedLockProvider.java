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

package org.apache.hudi.hive.functional;

import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.hive.transaction.lock.HiveMetastoreBasedLockProvider;
import org.apache.hudi.hive.testutils.HiveSyncFunctionalTestHarness;

import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.config.LockConfiguration.DEFAULT_LOCK_ACQUIRE_NUM_RETRIES;
import static org.apache.hudi.common.config.LockConfiguration.DEFAULT_LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS;
import static org.apache.hudi.common.config.LockConfiguration.DEFAULT_ZK_CONNECTION_TIMEOUT_MS;
import static org.apache.hudi.common.config.LockConfiguration.HIVE_DATABASE_NAME_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.HIVE_TABLE_NAME_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_CONNECTION_TIMEOUT_MS_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_CONNECT_URL_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_PORT_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_SESSION_TIMEOUT_MS_PROP_KEY;

/**
 * For all tests, we need to set LockComponent.setOperationType(DataOperationType.NO_TXN).
 * This is needed because of this -> https://github.com/apache/hive/blob/master/standalone-metastore
 * /metastore-server/src/main/java/org/apache/hadoop/hive/metastore/txn/TxnHandler.java#L2892
 * Unless this is set, we cannot use HiveMetastore server in tests for locking use-cases.
 */
@Tag("functional")
public class TestHiveMetastoreBasedLockProvider extends HiveSyncFunctionalTestHarness {

  private static final String TEST_DB_NAME = "testdb";
  private static final String TEST_TABLE_NAME = "testtable";
  private LockComponent lockComponent = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, TEST_DB_NAME);
  private LockConfiguration lockConfiguration;

  @BeforeEach
  public void init() throws Exception {
    TypedProperties properties = new TypedProperties();
    properties.setProperty(HIVE_DATABASE_NAME_PROP_KEY, TEST_DB_NAME);
    properties.setProperty(HIVE_TABLE_NAME_PROP_KEY, TEST_TABLE_NAME);
    properties.setProperty(LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY, DEFAULT_LOCK_ACQUIRE_NUM_RETRIES);
    properties.setProperty(LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY, DEFAULT_LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS);
    properties.setProperty(ZK_CONNECT_URL_PROP_KEY, zkService().connectString());
    properties.setProperty(ZK_PORT_PROP_KEY, hiveConf().get("hive.zookeeper.client.port"));
    properties.setProperty(ZK_SESSION_TIMEOUT_MS_PROP_KEY, hiveConf().get("hive.zookeeper.session.timeout"));
    properties.setProperty(ZK_CONNECTION_TIMEOUT_MS_PROP_KEY, String.valueOf(DEFAULT_ZK_CONNECTION_TIMEOUT_MS));
    properties.setProperty(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, String.valueOf(1000));
    lockConfiguration = new LockConfiguration(properties);
    lockComponent.setTablename(TEST_TABLE_NAME);
  }

  @Test
  public void testAcquireLock() throws Exception {
    HiveMetastoreBasedLockProvider lockProvider = new HiveMetastoreBasedLockProvider(lockConfiguration, hiveConf());
    lockComponent.setOperationType(DataOperationType.NO_TXN);
    Assertions.assertTrue(lockProvider.acquireLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS, lockComponent));
    try {
      Assertions.assertTrue(lockProvider.acquireLock(lockConfiguration.getConfig()
          .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS, lockComponent));
      Assertions.fail();
    } catch (Exception e) {
      // Expected since lock is already acquired
    }
    lockProvider.unlock();
    // try to lock again after unlocking
    Assertions.assertTrue(lockProvider.acquireLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS, lockComponent));
    lockProvider.close();
  }

  @Test
  public void testUnlock() throws Exception {
    HiveMetastoreBasedLockProvider lockProvider = new HiveMetastoreBasedLockProvider(lockConfiguration, hiveConf());
    lockComponent.setOperationType(DataOperationType.NO_TXN);
    Assertions.assertTrue(lockProvider.acquireLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS, lockComponent));
    lockProvider.unlock();
    // try to lock again after unlocking
    Assertions.assertTrue(lockProvider.acquireLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS, lockComponent));
    lockProvider.close();
  }

  @Test
  public void testReentrantLock() throws Exception {
    HiveMetastoreBasedLockProvider lockProvider = new HiveMetastoreBasedLockProvider(lockConfiguration, hiveConf());
    lockComponent.setOperationType(DataOperationType.NO_TXN);
    Assertions.assertTrue(lockProvider.acquireLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS, lockComponent));
    try {
      lockProvider.acquireLock(lockConfiguration.getConfig()
          .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS, lockComponent);
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
    lockProvider.unlock();

    // not acquired in the beginning
    HiveMetastoreBasedLockProvider lockProvider1 = new HiveMetastoreBasedLockProvider(lockConfiguration, hiveConf());
    HiveMetastoreBasedLockProvider lockProvider2 = new HiveMetastoreBasedLockProvider(lockConfiguration, hiveConf());
    lockComponent.setOperationType(DataOperationType.NO_TXN);
    Assertions.assertTrue(lockProvider1.acquireLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS, lockComponent));
    try {
      boolean acquireStatus = lockProvider2.acquireLock(lockConfiguration.getConfig()
          .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS, lockComponent);
      Assertions.assertFalse(acquireStatus);
    } catch (IllegalArgumentException e) {
      // expected
    }
    lockProvider1.unlock();
    Assertions.assertTrue(lockProvider2.acquireLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS, lockComponent));
    lockProvider2.unlock();

    lockProvider.close();
    lockProvider1.close();
    lockProvider2.close();
  }

  @Test
  public void testWaitingLock() throws Exception {
    // create different HiveMetastoreBasedLockProvider to simulate different applications
    HiveMetastoreBasedLockProvider lockProvider1 = new HiveMetastoreBasedLockProvider(lockConfiguration, hiveConf());
    HiveMetastoreBasedLockProvider lockProvider2 = new HiveMetastoreBasedLockProvider(lockConfiguration, hiveConf());
    lockComponent.setOperationType(DataOperationType.NO_TXN);
    Assertions.assertTrue(lockProvider1.acquireLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS, lockComponent));
    try {
      boolean acquireStatus = lockProvider2.acquireLock(lockConfiguration.getConfig()
          .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS, lockComponent);
      Assertions.assertFalse(acquireStatus);
    } catch (IllegalArgumentException e) {
      // expected
    }
    lockProvider1.unlock();
    // create the third HiveMetastoreBasedLockProvider to acquire lock
    HiveMetastoreBasedLockProvider lockProvider3 = new HiveMetastoreBasedLockProvider(lockConfiguration, hiveConf());
    boolean acquireStatus = lockProvider3.acquireLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS, lockComponent);
    // we should acquired lock, since lockProvider1 has already released lock
    Assertions.assertTrue(acquireStatus);
    lockProvider3.unlock();
    // close all HiveMetastoreBasedLockProvider
    lockProvider1.close();
    lockProvider2.close();
    lockProvider3.close();
  }

  @Test
  public void testUnlockWithoutLock() {
    HiveMetastoreBasedLockProvider lockProvider = new HiveMetastoreBasedLockProvider(lockConfiguration, hiveConf());
    lockComponent.setOperationType(DataOperationType.NO_TXN);
    lockProvider.unlock();
  }

}
