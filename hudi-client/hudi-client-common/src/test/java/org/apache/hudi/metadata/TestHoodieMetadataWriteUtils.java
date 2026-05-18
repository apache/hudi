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

package org.apache.hudi.metadata;

import org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider;
import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.client.transaction.lock.StorageBasedLockProvider;
import org.apache.hudi.client.transaction.lock.ZookeeperBasedImplicitBasePathLockProvider;
import org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.ActionType;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieMetadataWriteUtils {

  @Test
  public void testCreateMetadataWriteConfigForCleaner() {
    HoodieWriteConfig writeConfig1 = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/")
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .retainCommits(5).build())
        .build();

    HoodieWriteConfig metadataWriteConfig1 = HoodieMetadataWriteUtils.createMetadataWriteConfig(writeConfig1, HoodieFailedWritesCleaningPolicy.EAGER,
        HoodieTableVersion.SIX);
    assertEquals(HoodieFailedWritesCleaningPolicy.EAGER, metadataWriteConfig1.getFailedWritesCleanPolicy());
    assertEquals(HoodieCleaningPolicy.KEEP_LATEST_COMMITS, metadataWriteConfig1.getCleanerPolicy());
    assertEquals(1, metadataWriteConfig1.getCleanTriggerMaxCommits());
    // default value already greater than data cleaner commits retained * 1.2
    assertEquals(HoodieMetadataConfig.DEFAULT_METADATA_CLEANER_COMMITS_RETAINED, metadataWriteConfig1.getCleanerCommitsRetained());

    assertNotEquals(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS, metadataWriteConfig1.getCleanerPolicy());
    assertNotEquals(HoodieCleaningPolicy.KEEP_LATEST_BY_HOURS, metadataWriteConfig1.getCleanerPolicy());

    HoodieWriteConfig writeConfig2 = HoodieWriteConfig.newBuilder()
        .withPath("/tmp")
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .retainCommits(20)
            .withMaxCommitsBeforeCleaning(10)
            .build())
        .build();
    HoodieWriteConfig metadataWriteConfig2 = HoodieMetadataWriteUtils.createMetadataWriteConfig(writeConfig2, HoodieFailedWritesCleaningPolicy.EAGER,
        HoodieTableVersion.SIX);
    assertEquals(HoodieFailedWritesCleaningPolicy.EAGER, metadataWriteConfig2.getFailedWritesCleanPolicy());
    assertEquals(HoodieCleaningPolicy.KEEP_LATEST_COMMITS, metadataWriteConfig2.getCleanerPolicy());
    // data cleaner commits retained * 1.2 is greater than default
    assertEquals(24, metadataWriteConfig2.getCleanerCommitsRetained());
    assertEquals(10, metadataWriteConfig2.getCleanTriggerMaxCommits());
  }

  @Test
  public void testCreateMetadataWriteConfigForNBCC() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path/")
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .retainCommits(5).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().withStreamingWriteEnabled(true).build())
        .build();

    HoodieWriteConfig metadataWriteConfig = HoodieMetadataWriteUtils.createMetadataWriteConfig(writeConfig, HoodieFailedWritesCleaningPolicy.EAGER,
        HoodieTableVersion.EIGHT);
    validateMetadataWriteConfig(metadataWriteConfig, HoodieFailedWritesCleaningPolicy.LAZY,
        WriteConcurrencyMode.NON_BLOCKING_CONCURRENCY_CONTROL, InProcessLockProvider.class.getCanonicalName());

    // disable streaming writes to metadata table.
    Properties properties = new Properties();
    properties.put(HoodieMetadataConfig.STREAMING_WRITE_ENABLED.key(), "false");
    writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path/.hoodie/metadata/")
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .retainCommits(5).build())
        .withProperties(properties)
        .build();

    metadataWriteConfig = HoodieMetadataWriteUtils.createMetadataWriteConfig(writeConfig, HoodieFailedWritesCleaningPolicy.EAGER,
        HoodieTableVersion.EIGHT);
    validateMetadataWriteConfig(metadataWriteConfig, HoodieFailedWritesCleaningPolicy.EAGER,
        WriteConcurrencyMode.SINGLE_WRITER, null);
  }

  @Test
  public void testCreateMetadataWriteConfigForOCC() {
    String dataTableBasePath = "/tmp/base_path/";
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(dataTableBasePath)
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .retainCommits(5).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withStreamingWriteEnabled(false)
            .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(FileSystemBasedLockProvider.class)
            .withFileSystemLockPath("/tmp/lock_dir")
            .build())
        .build();

    HoodieWriteConfig metadataWriteConfig = HoodieMetadataWriteUtils.createMetadataWriteConfig(
        writeConfig, HoodieFailedWritesCleaningPolicy.EAGER, HoodieTableVersion.EIGHT);
    // HoodieWriteConfig builder auto-adjusts failed writes policy to LAZY for multi-writer modes
    validateMetadataWriteConfig(metadataWriteConfig, HoodieFailedWritesCleaningPolicy.LAZY,
        WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL, FileSystemBasedLockProvider.class.getCanonicalName());
    // MDT base path should NOT be overwritten to data table's base path
    String expectedMdtBasePath = HoodieTableMetadata.getMetadataTableBasePath(dataTableBasePath);
    assertEquals(expectedMdtBasePath, metadataWriteConfig.getBasePath());
  }

  @Test
  public void testCreateMetadataWriteConfigRejectsInProcessLockProvider() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path/")
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withStreamingWriteEnabled(false)
            .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(InProcessLockProvider.class).build())
        .build();

    IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
        HoodieMetadataWriteUtils.createMetadataWriteConfig(
            writeConfig, HoodieFailedWritesCleaningPolicy.EAGER, HoodieTableVersion.EIGHT));
    assertTrue(ex.getMessage().contains("InProcessLockProvider cannot be used"));
  }

  @Test
  public void testCreateMetadataWriteConfigForcesStreamingWritesOffWithMultiWriter() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path/")
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .retainCommits(5).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withStreamingWriteEnabled(true)
            .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(FileSystemBasedLockProvider.class)
            .withFileSystemLockPath("/tmp/lock_dir")
            .build())
        .build();

    HoodieWriteConfig metadataWriteConfig = HoodieMetadataWriteUtils.createMetadataWriteConfig(
        writeConfig, HoodieFailedWritesCleaningPolicy.EAGER, HoodieTableVersion.EIGHT);
    // Multi-writer takes precedence over streaming writes; streaming writes are forced off
    validateMetadataWriteConfig(metadataWriteConfig, HoodieFailedWritesCleaningPolicy.LAZY,
        WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL, FileSystemBasedLockProvider.class.getCanonicalName());
  }

  @Test
  public void testCreateMetadataWriteConfigWithTableServiceManager() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path/")
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withTableServiceManagerEnabled(true)
            .withTableServiceManagerActions("compaction,logcompaction")
            .build())
        .build();

    HoodieWriteConfig metadataWriteConfig = HoodieMetadataWriteUtils.createMetadataWriteConfig(
        writeConfig, HoodieFailedWritesCleaningPolicy.EAGER, HoodieTableVersion.EIGHT);
    assertTrue(metadataWriteConfig.getTableServiceManagerConfig().isTableServiceManagerEnabled());
    assertTrue(metadataWriteConfig.getTableServiceManagerConfig().isEnabledAndActionSupported(ActionType.compaction));
    assertTrue(metadataWriteConfig.getTableServiceManagerConfig().isEnabledAndActionSupported(ActionType.logcompaction));
    assertFalse(metadataWriteConfig.getTableServiceManagerConfig().isEnabledAndActionSupported(ActionType.clean));
  }

  @Test
  public void testCreateMetadataWriteConfigWithTableServiceManagerLogCompactionOnly() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path/")
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withTableServiceManagerEnabled(true)
            .withTableServiceManagerActions("logcompaction")
            .build())
        .build();

    HoodieWriteConfig metadataWriteConfig = HoodieMetadataWriteUtils.createMetadataWriteConfig(
        writeConfig, HoodieFailedWritesCleaningPolicy.EAGER, HoodieTableVersion.EIGHT);
    assertTrue(metadataWriteConfig.getTableServiceManagerConfig().isTableServiceManagerEnabled());
    assertFalse(metadataWriteConfig.getTableServiceManagerConfig().isEnabledAndActionSupported(ActionType.compaction),
        "compaction should not match when only logcompaction is configured");
    assertTrue(metadataWriteConfig.getTableServiceManagerConfig().isEnabledAndActionSupported(ActionType.logcompaction));
  }

  @Test
  public void testCreateMetadataWriteConfigWithTableServiceManagerDisabled() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path/")
        .build();

    HoodieWriteConfig metadataWriteConfig = HoodieMetadataWriteUtils.createMetadataWriteConfig(
        writeConfig, HoodieFailedWritesCleaningPolicy.EAGER, HoodieTableVersion.EIGHT);
    assertFalse(metadataWriteConfig.getTableServiceManagerConfig().isTableServiceManagerEnabled());
    assertFalse(metadataWriteConfig.getTableServiceManagerConfig().isEnabledAndActionSupported(ActionType.compaction));
  }

  @Test
  public void testCreateMetadataWriteConfigForOCCWithZookeeperLockProvider() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path/")
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .retainCommits(5).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withStreamingWriteEnabled(false)
            .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL).build())
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

    HoodieWriteConfig metadataWriteConfig = HoodieMetadataWriteUtils.createMetadataWriteConfig(
        writeConfig, HoodieFailedWritesCleaningPolicy.EAGER, HoodieTableVersion.EIGHT);
    validateMetadataWriteConfig(metadataWriteConfig, HoodieFailedWritesCleaningPolicy.LAZY,
        WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL, ZookeeperBasedLockProvider.class.getCanonicalName());
    assertEquals("zk-host:2181", metadataWriteConfig.getProps().getString(HoodieLockConfig.ZK_CONNECT_URL.key()));
    assertEquals("/hudi/locks", metadataWriteConfig.getProps().getString(HoodieLockConfig.ZK_BASE_PATH.key()));
    assertEquals("test_table", metadataWriteConfig.getProps().getString(HoodieLockConfig.ZK_LOCK_KEY.key()));
    assertEquals("2181", metadataWriteConfig.getProps().getString(HoodieLockConfig.ZK_PORT.key()));
    assertEquals(30000, metadataWriteConfig.getProps().getInteger(HoodieLockConfig.ZK_SESSION_TIMEOUT_MS.key()));
    assertEquals(15000, metadataWriteConfig.getProps().getInteger(HoodieLockConfig.ZK_CONNECTION_TIMEOUT_MS.key()));
  }

  @Test
  public void testCreateMetadataWriteConfigForOCCWithHiveMetastoreLockProvider() {
    String hmsLockProviderClass = "org.apache.hudi.hive.transaction.lock.HiveMetastoreBasedLockProvider";
    Properties lockProps = new Properties();
    lockProps.put(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(), hmsLockProviderClass);
    lockProps.put(HoodieLockConfig.HIVE_DATABASE_NAME.key(), "my_database");
    lockProps.put(HoodieLockConfig.HIVE_TABLE_NAME.key(), "my_table");
    lockProps.put(HoodieLockConfig.HIVE_METASTORE_URI.key(), "thrift://hms-host:9083");

    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path/")
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .retainCommits(5).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withStreamingWriteEnabled(false)
            .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .fromProperties(lockProps)
            .build())
        .build();

    HoodieWriteConfig metadataWriteConfig = HoodieMetadataWriteUtils.createMetadataWriteConfig(
        writeConfig, HoodieFailedWritesCleaningPolicy.EAGER, HoodieTableVersion.EIGHT);
    validateMetadataWriteConfig(metadataWriteConfig, HoodieFailedWritesCleaningPolicy.LAZY,
        WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL, hmsLockProviderClass);
    assertEquals("my_database", metadataWriteConfig.getProps().getString(HoodieLockConfig.HIVE_DATABASE_NAME.key()));
    assertEquals("my_table", metadataWriteConfig.getProps().getString(HoodieLockConfig.HIVE_TABLE_NAME.key()));
    assertEquals("thrift://hms-host:9083", metadataWriteConfig.getProps().getString(HoodieLockConfig.HIVE_METASTORE_URI.key()));
  }

  @Test
  public void testCreateMetadataWriteConfigForOCCWithFileSystemLockProvider() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path/")
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .retainCommits(5).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withStreamingWriteEnabled(false)
            .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(FileSystemBasedLockProvider.class)
            .withFileSystemLockPath("/tmp/lock_dir")
            .withFileSystemLockExpire(10)
            .build())
        .build();

    HoodieWriteConfig metadataWriteConfig = HoodieMetadataWriteUtils.createMetadataWriteConfig(
        writeConfig, HoodieFailedWritesCleaningPolicy.EAGER, HoodieTableVersion.EIGHT);
    validateMetadataWriteConfig(metadataWriteConfig, HoodieFailedWritesCleaningPolicy.LAZY,
        WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL, FileSystemBasedLockProvider.class.getCanonicalName());
    assertEquals("/tmp/lock_dir", metadataWriteConfig.getProps().getString(HoodieLockConfig.FILESYSTEM_LOCK_PATH.key()));
    assertEquals(10, metadataWriteConfig.getProps().getInteger(HoodieLockConfig.FILESYSTEM_LOCK_EXPIRE.key()));
  }

  @Test
  public void testCreateMetadataWriteConfigRejectsCustomLockProvider() {
    String customLockProviderClass = "com.example.custom.MyCustomLockProvider";
    Properties lockProps = new Properties();
    lockProps.put(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(), customLockProviderClass);

    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path/")
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withStreamingWriteEnabled(false)
            .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .fromProperties(lockProps)
            .build())
        .build();

    HoodieException ex = assertThrows(HoodieException.class, () ->
        HoodieMetadataWriteUtils.createMetadataWriteConfig(
            writeConfig, HoodieFailedWritesCleaningPolicy.EAGER, HoodieTableVersion.EIGHT));
    assertTrue(ex.getMessage().contains("only supported for built-in lock providers"));
    assertTrue(ex.getMessage().contains(customLockProviderClass));
  }

  @Test
  public void testCreateMetadataWriteConfigRejectsConcurrencyModeMismatch() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path/")
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withStreamingWriteEnabled(false)
            .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL).build())
        .build();

    IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
        HoodieMetadataWriteUtils.createMetadataWriteConfig(
            writeConfig, HoodieFailedWritesCleaningPolicy.EAGER, HoodieTableVersion.EIGHT));
    assertTrue(ex.getMessage().contains("must match the data table concurrency mode"));
  }

  @Test
  public void testCreateMetadataWriteConfigForOCCPreservesMdtSpecificValues() {
    String dataTableBasePath = "/tmp/base_path/";

    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(dataTableBasePath)
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .retainCommits(5).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withStreamingWriteEnabled(false)
            .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(FileSystemBasedLockProvider.class)
            .withFileSystemLockPath("/tmp/lock_dir")
            .withFileSystemLockExpire(10)
            .build())
        .build();

    HoodieWriteConfig metadataWriteConfig = HoodieMetadataWriteUtils.createMetadataWriteConfig(
        writeConfig, HoodieFailedWritesCleaningPolicy.EAGER, HoodieTableVersion.EIGHT);

    validateMetadataWriteConfig(metadataWriteConfig, HoodieFailedWritesCleaningPolicy.LAZY,
        WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL, FileSystemBasedLockProvider.class.getCanonicalName());

    // MDT-specific values must be preserved
    String expectedMdtBasePath = HoodieTableMetadata.getMetadataTableBasePath(dataTableBasePath);
    assertEquals(expectedMdtBasePath, metadataWriteConfig.getBasePath());
    assertFalse(metadataWriteConfig.isAutoClean(), "Auto clean should be disabled for MDT");
    assertFalse(metadataWriteConfig.inlineCompactionEnabled(), "Inline compaction should be disabled for MDT");
    assertFalse(metadataWriteConfig.isMetadataTableEnabled(), "Metadata listing should be disabled for MDT");
    assertNotEquals(dataTableBasePath, metadataWriteConfig.getBasePath(),
        "MDT base path should not be overwritten to data table base path");
  }

  @Test
  public void testCreateMetadataWriteConfigForOCCWithDynamoDBLockProvider() {
    Properties lockProps = new Properties();
    lockProps.put(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(),
        HoodieLockConfig.DYNAMODB_BASED_LOCK_PROVIDER_CLASS);
    lockProps.put("hoodie.write.lock.dynamodb.table", "my_lock_table");
    lockProps.put("hoodie.write.lock.dynamodb.region", "us-west-2");
    lockProps.put("hoodie.write.lock.dynamodb.partition_key", "test_table");

    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path/")
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withStreamingWriteEnabled(false)
            .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .fromProperties(lockProps)
            .build())
        .build();

    HoodieWriteConfig metadataWriteConfig = HoodieMetadataWriteUtils.createMetadataWriteConfig(
        writeConfig, HoodieFailedWritesCleaningPolicy.EAGER, HoodieTableVersion.EIGHT);
    validateMetadataWriteConfig(metadataWriteConfig, HoodieFailedWritesCleaningPolicy.LAZY,
        WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL,
        HoodieLockConfig.DYNAMODB_BASED_LOCK_PROVIDER_CLASS);
    assertEquals("my_lock_table", metadataWriteConfig.getProps().getString("hoodie.write.lock.dynamodb.table"));
    assertEquals("us-west-2", metadataWriteConfig.getProps().getString("hoodie.write.lock.dynamodb.region"));
    assertEquals("test_table", metadataWriteConfig.getProps().getString("hoodie.write.lock.dynamodb.partition_key"));
  }

  @Test
  public void testCreateMetadataWriteConfigRejectsStorageBasedLockProvider() {
    Properties lockProps = new Properties();
    lockProps.put(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(),
        StorageBasedLockProvider.class.getCanonicalName());
    lockProps.put("hoodie.write.lock.storage.validity.timeout.secs", "600");
    lockProps.put("hoodie.write.lock.storage.renew.interval.secs", "60");

    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path/")
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withStreamingWriteEnabled(false)
            .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .fromProperties(lockProps)
            .build())
        .build();

    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
        HoodieMetadataWriteUtils.createMetadataWriteConfig(
            writeConfig, HoodieFailedWritesCleaningPolicy.EAGER, HoodieTableVersion.EIGHT));
    assertTrue(ex.getMessage().contains("derives its lock identity from the table's base path"));
  }

  @Test
  public void testCreateMetadataWriteConfigRejectsZookeeperImplicitBasePathLockProvider() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path/")
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withStreamingWriteEnabled(false)
            .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(ZookeeperBasedImplicitBasePathLockProvider.class)
            .withZkQuorum("zk-host:2181")
            .withZkPort("2181")
            .build())
        .build();

    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
        HoodieMetadataWriteUtils.createMetadataWriteConfig(
            writeConfig, HoodieFailedWritesCleaningPolicy.EAGER, HoodieTableVersion.EIGHT));
    assertTrue(ex.getMessage().contains("derives its lock identity from the table's base path"));
  }

  @Test
  public void testCreateMetadataWriteConfigRejectsCustomLockProviders() {
    String customLockProviderClass = "com.example.custom.DistributedLockProvider";
    Properties lockProps = new Properties();
    lockProps.put(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(), customLockProviderClass);

    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path/")
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withStreamingWriteEnabled(false)
            .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .fromProperties(lockProps)
            .build())
        .build();

    HoodieException ex = assertThrows(HoodieException.class, () ->
        HoodieMetadataWriteUtils.createMetadataWriteConfig(
            writeConfig, HoodieFailedWritesCleaningPolicy.EAGER, HoodieTableVersion.EIGHT));
    assertTrue(ex.getMessage().contains("only supported for built-in lock providers"));
  }

  private void validateMetadataWriteConfig(HoodieWriteConfig metadataWriteConfig, HoodieFailedWritesCleaningPolicy expectedPolicy,
                                           WriteConcurrencyMode expectedWriteConcurrencyMode, String expectedLockProviderClass) {
    assertEquals(expectedPolicy, metadataWriteConfig.getFailedWritesCleanPolicy());
    assertEquals(expectedWriteConcurrencyMode, metadataWriteConfig.getWriteConcurrencyMode());
    if (expectedLockProviderClass != null) {
      assertEquals(expectedLockProviderClass, metadataWriteConfig.getLockProviderClass());
    } else {
      assertNull(metadataWriteConfig.getLockProviderClass());
    }
  }
}
