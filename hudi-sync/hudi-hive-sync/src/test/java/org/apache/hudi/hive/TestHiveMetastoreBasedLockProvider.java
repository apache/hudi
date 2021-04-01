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

package org.apache.hudi.hive;

import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.hive.testutils.HiveTestUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.config.LockConfiguration.DEFAULT_LOCK_ACQUIRE_NUM_RETRIES;
import static org.apache.hudi.common.config.LockConfiguration.DEFAULT_LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS;
import static org.apache.hudi.common.config.LockConfiguration.DEFAULT_ZK_CONNECTION_TIMEOUT_MS;
import static org.apache.hudi.common.config.LockConfiguration.HIVE_DATABASE_NAME_PROP;
import static org.apache.hudi.common.config.LockConfiguration.HIVE_TABLE_NAME_PROP;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_NUM_RETRIES_PROP;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP;
import static org.apache.hudi.common.config.LockConfiguration.ZK_CONNECTION_TIMEOUT_MS_PROP;
import static org.apache.hudi.common.config.LockConfiguration.ZK_CONNECT_URL_PROP;
import static org.apache.hudi.common.config.LockConfiguration.ZK_PORT_PROP;
import static org.apache.hudi.common.config.LockConfiguration.ZK_SESSION_TIMEOUT_MS_PROP;

/**
 * For all tests, we need to set LockComponent.setOperationType(DataOperationType.NO_TXN).
 * This is needed because of this -> https://github.com/apache/hive/blob/master/standalone-metastore
 * /metastore-server/src/main/java/org/apache/hadoop/hive/metastore/txn/TxnHandler.java#L2892
 * Unless this is set, we cannot use HiveMetastore server in tests for locking use-cases.
 */
public class TestHiveMetastoreBasedLockProvider {

  private static Connection connection;
  private static LockComponent lockComponent = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, "testdb");
  private static LockConfiguration lockConfiguration;

  @BeforeAll
  public static void init() throws Exception {
    HiveTestUtil.setUp();
    createHiveConnection();
    connection.createStatement().execute("create database if not exists testdb");
    TypedProperties properties = new TypedProperties();
    properties.setProperty(HIVE_DATABASE_NAME_PROP, "testdb");
    properties.setProperty(HIVE_TABLE_NAME_PROP, "testtable");
    properties.setProperty(LOCK_ACQUIRE_NUM_RETRIES_PROP, DEFAULT_LOCK_ACQUIRE_NUM_RETRIES);
    properties.setProperty(LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP, DEFAULT_LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS);
    properties.setProperty(ZK_CONNECT_URL_PROP, HiveTestUtil.getZkService().connectString());
    properties.setProperty(ZK_PORT_PROP, HiveTestUtil.getHiveConf().get("hive.zookeeper.client.port"));
    properties.setProperty(ZK_SESSION_TIMEOUT_MS_PROP, HiveTestUtil.getHiveConf().get("hive.zookeeper.session.timeout"));
    properties.setProperty(ZK_CONNECTION_TIMEOUT_MS_PROP, String.valueOf(DEFAULT_ZK_CONNECTION_TIMEOUT_MS));
    properties.setProperty(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP, String.valueOf(1000));
    lockConfiguration = new LockConfiguration(properties);
    lockComponent.setTablename("testtable");
  }

  @AfterAll
  public static void cleanUpClass() {
    HiveTestUtil.shutdown();
  }

  @Test
  public void testAcquireLock() throws Exception {
    HiveMetastoreBasedLockProvider lockProvider = new HiveMetastoreBasedLockProvider(lockConfiguration, HiveTestUtil.getHiveConf());
    lockComponent.setOperationType(DataOperationType.NO_TXN);
    Assertions.assertTrue(lockProvider.acquireLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP), TimeUnit.MILLISECONDS, lockComponent));
    try {
      Assertions.assertTrue(lockProvider.acquireLock(lockConfiguration.getConfig()
          .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP), TimeUnit.MILLISECONDS, lockComponent));
      Assertions.fail();
    } catch (Exception e) {
      // Expected since lock is already acquired
    }
    lockProvider.unlock();
    // try to lock again after unlocking
    Assertions.assertTrue(lockProvider.acquireLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP), TimeUnit.MILLISECONDS, lockComponent));
    lockProvider.close();
  }

  @Test
  public void testUnlock() throws Exception {
    HiveMetastoreBasedLockProvider lockProvider = new HiveMetastoreBasedLockProvider(lockConfiguration, HiveTestUtil.getHiveConf());
    lockComponent.setOperationType(DataOperationType.NO_TXN);
    Assertions.assertTrue(lockProvider.acquireLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP), TimeUnit.MILLISECONDS, lockComponent));
    lockProvider.unlock();
    // try to lock again after unlocking
    Assertions.assertTrue(lockProvider.acquireLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP), TimeUnit.MILLISECONDS, lockComponent));
    lockProvider.close();
  }

  @Test
  public void testReentrantLock() throws Exception {
    HiveMetastoreBasedLockProvider lockProvider = new HiveMetastoreBasedLockProvider(lockConfiguration, HiveTestUtil.getHiveConf());
    lockComponent.setOperationType(DataOperationType.NO_TXN);
    Assertions.assertTrue(lockProvider.acquireLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP), TimeUnit.MILLISECONDS, lockComponent));
    try {
      lockProvider.acquireLock(lockConfiguration.getConfig()
          .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP), TimeUnit.MILLISECONDS, lockComponent);
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
    lockProvider.unlock();
  }

  @Test
  public void testUnlockWithoutLock() {
    HiveMetastoreBasedLockProvider lockProvider = new HiveMetastoreBasedLockProvider(lockConfiguration, HiveTestUtil.getHiveConf());
    lockComponent.setOperationType(DataOperationType.NO_TXN);
    lockProvider.unlock();
  }

  private static void createHiveConnection() {
    if (connection == null) {
      try {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
      } catch (ClassNotFoundException e) {
        throw new RuntimeException();
      }
      try {
        connection = DriverManager.getConnection("jdbc:hive2://127.0.0.1:9999/");
      } catch (SQLException e) {
        throw new HoodieHiveSyncException("Cannot create hive connection ", e);
      }
    }
  }

}
