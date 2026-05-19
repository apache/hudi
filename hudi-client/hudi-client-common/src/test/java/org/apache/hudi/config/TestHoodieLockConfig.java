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

package org.apache.hudi.config;

import org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider;
import org.apache.hudi.client.transaction.lock.StorageBasedLockProvider;
import org.apache.hudi.client.transaction.lock.ZookeeperBasedImplicitBasePathLockProvider;
import org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.exception.HoodieException;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieLockConfig {

  @Test
  public void testGetLockConfigForFileSystemLockProvider() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path/")
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(FileSystemBasedLockProvider.class)
            .withFileSystemLockPath("/tmp/lock_dir")
            .withFileSystemLockExpire(10)
            .build())
        .build();

    HoodieLockConfig lockConfig = HoodieLockConfig.deriveLockConfigForDifferentTable(
        FileSystemBasedLockProvider.class.getCanonicalName(), writeConfig);
    assertEquals(FileSystemBasedLockProvider.class.getCanonicalName(),
        lockConfig.getString(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME));
    assertEquals("/tmp/lock_dir", lockConfig.getString(HoodieLockConfig.FILESYSTEM_LOCK_PATH));
    assertEquals("10", lockConfig.getString(HoodieLockConfig.FILESYSTEM_LOCK_EXPIRE));
  }

  @Test
  public void testGetLockConfigForZookeeperLockProvider() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path/")
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(ZookeeperBasedLockProvider.class)
            .withZkQuorum("zk-host:2181")
            .withZkBasePath("/hudi/locks")
            .withZkLockKey("test_table")
            .withZkPort("2181")
            .withZkSessionTimeoutInMs(30000L)
            .withZkConnectionTimeoutInMs(15000L)
            .build())
        .build();

    HoodieLockConfig lockConfig = HoodieLockConfig.deriveLockConfigForDifferentTable(
        ZookeeperBasedLockProvider.class.getCanonicalName(), writeConfig);
    assertEquals(ZookeeperBasedLockProvider.class.getCanonicalName(),
        lockConfig.getString(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME));
    assertEquals("zk-host:2181", lockConfig.getString(HoodieLockConfig.ZK_CONNECT_URL));
    assertEquals("/hudi/locks", lockConfig.getString(HoodieLockConfig.ZK_BASE_PATH));
    assertEquals("test_table", lockConfig.getString(HoodieLockConfig.ZK_LOCK_KEY));
    assertEquals("2181", lockConfig.getString(HoodieLockConfig.ZK_PORT));
    assertEquals("30000", lockConfig.getString(HoodieLockConfig.ZK_SESSION_TIMEOUT_MS));
    assertEquals("15000", lockConfig.getString(HoodieLockConfig.ZK_CONNECTION_TIMEOUT_MS));
  }

  @Test
  public void testGetLockConfigRejectsZookeeperImplicitBasePathLockProvider() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path/")
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(ZookeeperBasedImplicitBasePathLockProvider.class)
            .withZkQuorum("zk-host:2181")
            .withZkPort("2181")
            .build())
        .build();

    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
        HoodieLockConfig.deriveLockConfigForDifferentTable(
            ZookeeperBasedImplicitBasePathLockProvider.class.getCanonicalName(), writeConfig));
    assertTrue(ex.getMessage().contains("derives its lock identity from the table's base path"));
    assertTrue(ex.getMessage().contains(ZookeeperBasedLockProvider.class.getCanonicalName()));
  }

  @Test
  public void testGetLockConfigForHiveMetastoreLockProvider() {
    Properties lockProps = new Properties();
    lockProps.put(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(),
        HoodieLockConfig.HIVE_METASTORE_BASED_LOCK_PROVIDER_CLASS);
    lockProps.put(HoodieLockConfig.HIVE_DATABASE_NAME.key(), "my_database");
    lockProps.put(HoodieLockConfig.HIVE_TABLE_NAME.key(), "my_table");
    lockProps.put(HoodieLockConfig.HIVE_METASTORE_URI.key(), "thrift://hms-host:9083");

    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path/")
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder().fromProperties(lockProps).build())
        .build();

    HoodieLockConfig lockConfig = HoodieLockConfig.deriveLockConfigForDifferentTable(
        HoodieLockConfig.HIVE_METASTORE_BASED_LOCK_PROVIDER_CLASS, writeConfig);
    assertEquals(HoodieLockConfig.HIVE_METASTORE_BASED_LOCK_PROVIDER_CLASS,
        lockConfig.getString(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME));
    assertEquals("my_database", lockConfig.getString(HoodieLockConfig.HIVE_DATABASE_NAME));
    assertEquals("my_table", lockConfig.getString(HoodieLockConfig.HIVE_TABLE_NAME));
    assertEquals("thrift://hms-host:9083", lockConfig.getString(HoodieLockConfig.HIVE_METASTORE_URI));
  }

  @Test
  public void testGetLockConfigForDynamoDBLockProvider() {
    Properties lockProps = new Properties();
    lockProps.put(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(),
        HoodieLockConfig.DYNAMODB_BASED_LOCK_PROVIDER_CLASS);
    lockProps.put("hoodie.write.lock.dynamodb.table", "my_lock_table");
    lockProps.put("hoodie.write.lock.dynamodb.region", "us-west-2");
    lockProps.put("hoodie.write.lock.dynamodb.partition_key", "test_table");

    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path/")
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder().fromProperties(lockProps).build())
        .build();

    HoodieLockConfig lockConfig = HoodieLockConfig.deriveLockConfigForDifferentTable(
        HoodieLockConfig.DYNAMODB_BASED_LOCK_PROVIDER_CLASS, writeConfig);
    assertEquals(HoodieLockConfig.DYNAMODB_BASED_LOCK_PROVIDER_CLASS,
        lockConfig.getString(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME));
    assertEquals("my_lock_table", lockConfig.getProps().getProperty("hoodie.write.lock.dynamodb.table"));
    assertEquals("us-west-2", lockConfig.getProps().getProperty("hoodie.write.lock.dynamodb.region"));
    assertEquals("test_table", lockConfig.getProps().getProperty("hoodie.write.lock.dynamodb.partition_key"));
  }

  @Test
  public void testGetLockConfigRejectsStorageBasedLockProvider() {
    Properties lockProps = new Properties();
    lockProps.put(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(),
        StorageBasedLockProvider.class.getCanonicalName());
    lockProps.put("hoodie.write.lock.storage.validity.timeout.secs", "600");
    lockProps.put("hoodie.write.lock.storage.renew.interval.secs", "60");

    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path/")
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder().fromProperties(lockProps).build())
        .build();

    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
        HoodieLockConfig.deriveLockConfigForDifferentTable(
            StorageBasedLockProvider.class.getCanonicalName(), writeConfig));
    assertTrue(ex.getMessage().contains("derives its lock identity from the table's base path"));
    assertTrue(ex.getMessage().contains("does not provide an explicit lock path override"));
  }

  @Test
  public void testGetLockConfigCopiesCommonRetryConfigs() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path/")
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(FileSystemBasedLockProvider.class)
            .withFileSystemLockPath("/tmp/lock_dir")
            .withNumRetries(5)
            .withRetryWaitTimeInMillis(2000L)
            .withRetryMaxWaitTimeInMillis(32000L)
            .withClientNumRetries(10)
            .withClientRetryWaitTimeInMillis(3000L)
            .withLockWaitTimeInMillis(90000L)
            .withHeartbeatIntervalInMillis(45000L)
            .build())
        .build();

    HoodieLockConfig lockConfig = HoodieLockConfig.deriveLockConfigForDifferentTable(
        FileSystemBasedLockProvider.class.getCanonicalName(), writeConfig);
    assertEquals("5", lockConfig.getString(HoodieLockConfig.LOCK_ACQUIRE_NUM_RETRIES));
    assertEquals("2000", lockConfig.getString(HoodieLockConfig.LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS));
    assertEquals("32000", lockConfig.getString(HoodieLockConfig.LOCK_ACQUIRE_RETRY_MAX_WAIT_TIME_IN_MILLIS));
    assertEquals("10", lockConfig.getString(HoodieLockConfig.LOCK_ACQUIRE_CLIENT_NUM_RETRIES));
    assertEquals("3000", lockConfig.getString(HoodieLockConfig.LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS));
    assertEquals("90000", lockConfig.getString(HoodieLockConfig.LOCK_ACQUIRE_WAIT_TIMEOUT_MS));
    assertEquals("45000", lockConfig.getString(HoodieLockConfig.LOCK_HEARTBEAT_INTERVAL_MS));
  }

  @Test
  public void testGetLockConfigRejectsCustomLockProvider() {
    String customLockProviderClass = "com.example.custom.MyCustomLockProvider";
    Properties lockProps = new Properties();
    lockProps.put(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(), customLockProviderClass);

    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path/")
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder().fromProperties(lockProps).build())
        .build();

    HoodieException ex = assertThrows(HoodieException.class, () ->
        HoodieLockConfig.deriveLockConfigForDifferentTable(customLockProviderClass, writeConfig));
    assertTrue(ex.getMessage().contains("only supported for built-in lock providers"));
    assertTrue(ex.getMessage().contains(customLockProviderClass));
  }

  @Test
  public void testGetLockConfigRejectsZookeeperProviderWithoutLockKey() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path/")
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(ZookeeperBasedLockProvider.class)
            .withZkQuorum("zk-host:2181")
            .withZkBasePath("/hudi/locks")
            .withZkPort("2181")
            .build())
        .build();

    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
        HoodieLockConfig.deriveLockConfigForDifferentTable(
            ZookeeperBasedLockProvider.class.getCanonicalName(), writeConfig));
    assertTrue(ex.getMessage().contains(LockConfiguration.ZK_LOCK_KEY_PROP_KEY));
  }

  @Test
  public void testGetLockConfigRejectsFileSystemProviderWithoutLockPath() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path/")
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(FileSystemBasedLockProvider.class)
            .withFileSystemLockExpire(10)
            .build())
        .build();

    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
        HoodieLockConfig.deriveLockConfigForDifferentTable(
            FileSystemBasedLockProvider.class.getCanonicalName(), writeConfig));
    assertTrue(ex.getMessage().contains(LockConfiguration.FILESYSTEM_LOCK_PATH_PROP_KEY));
  }

  @Test
  public void testGetLockConfigRejectsDynamoDBProviderWithoutPartitionKey() {
    Properties lockProps = new Properties();
    lockProps.put(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(),
        HoodieLockConfig.DYNAMODB_BASED_LOCK_PROVIDER_CLASS);
    lockProps.put("hoodie.write.lock.dynamodb.table", "my_lock_table");
    lockProps.put("hoodie.write.lock.dynamodb.region", "us-west-2");

    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path/")
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder().fromProperties(lockProps).build())
        .build();

    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
        HoodieLockConfig.deriveLockConfigForDifferentTable(
            HoodieLockConfig.DYNAMODB_BASED_LOCK_PROVIDER_CLASS, writeConfig));
    assertTrue(ex.getMessage().contains("hoodie.write.lock.dynamodb.partition_key"));
  }

  @Test
  public void testGetLockConfigRejectsDynamoDBImplicitPartitionKeyLockProvider() {
    Properties lockProps = new Properties();
    lockProps.put(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(),
        HoodieLockConfig.DYNAMODB_BASED_IMPLICIT_PARTITION_KEY_LOCK_PROVIDER_CLASS);
    lockProps.put("hoodie.write.lock.dynamodb.table", "my_lock_table");
    lockProps.put("hoodie.write.lock.dynamodb.region", "us-west-2");

    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path/")
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder().fromProperties(lockProps).build())
        .build();

    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
        HoodieLockConfig.deriveLockConfigForDifferentTable(
            HoodieLockConfig.DYNAMODB_BASED_IMPLICIT_PARTITION_KEY_LOCK_PROVIDER_CLASS, writeConfig));
    assertTrue(ex.getMessage().contains("derives its lock identity from the table's base path"));
    assertTrue(ex.getMessage().contains(HoodieLockConfig.DYNAMODB_BASED_LOCK_PROVIDER_CLASS));
  }

  @Test
  public void testGetLockConfigRejectsHiveMetastoreProviderWithoutDatabase() {
    Properties lockProps = new Properties();
    lockProps.put(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(),
        HoodieLockConfig.HIVE_METASTORE_BASED_LOCK_PROVIDER_CLASS);
    lockProps.put(HoodieLockConfig.HIVE_TABLE_NAME.key(), "my_table");
    lockProps.put(HoodieLockConfig.HIVE_METASTORE_URI.key(), "thrift://hms-host:9083");

    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path/")
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder().fromProperties(lockProps).build())
        .build();

    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
        HoodieLockConfig.deriveLockConfigForDifferentTable(
            HoodieLockConfig.HIVE_METASTORE_BASED_LOCK_PROVIDER_CLASS, writeConfig));
    assertTrue(ex.getMessage().contains(LockConfiguration.HIVE_DATABASE_NAME_PROP_KEY));
  }

  @Test
  public void testGetLockConfigRejectsHiveMetastoreProviderWithoutTable() {
    Properties lockProps = new Properties();
    lockProps.put(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(),
        HoodieLockConfig.HIVE_METASTORE_BASED_LOCK_PROVIDER_CLASS);
    lockProps.put(HoodieLockConfig.HIVE_DATABASE_NAME.key(), "my_database");
    lockProps.put(HoodieLockConfig.HIVE_METASTORE_URI.key(), "thrift://hms-host:9083");

    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path/")
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder().fromProperties(lockProps).build())
        .build();

    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
        HoodieLockConfig.deriveLockConfigForDifferentTable(
            HoodieLockConfig.HIVE_METASTORE_BASED_LOCK_PROVIDER_CLASS, writeConfig));
    assertTrue(ex.getMessage().contains(LockConfiguration.HIVE_TABLE_NAME_PROP_KEY));
  }
}
