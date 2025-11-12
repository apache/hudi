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

package org.apache.hudi.aws.transaction.integ;

import org.apache.hudi.aws.transaction.lock.DynamoDBBasedImplicitPartitionKeyLockProvider;
import org.apache.hudi.aws.transaction.lock.DynamoDBBasedLockProvider;
import org.apache.hudi.aws.transaction.lock.DynamoDBBasedLockProviderBase;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.hash.HashID;
import org.apache.hudi.config.DynamoDbBasedLockConfig;
import org.apache.hudi.storage.StorageConfiguration;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;

import java.net.URI;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY;

/**
 * Test for {@link DynamoDBBasedLockProvider}.
 * Set it as integration test because it requires setting up docker environment.
 */
@Disabled("HUDI-7475 The tests do not work. Disabling them to unblock Azure CI")
public class ITTestDynamoDBBasedLockProvider {

  private static final LockConfiguration LOCK_CONFIGURATION;
  private static final LockConfiguration IMPLICIT_PART_KEY_LOCK_CONFIG_NO_BASE_PATH;
  private static final LockConfiguration IMPLICIT_PART_KEY_LOCK_CONFIG_WITH_PART_KEY;
  private static final LockConfiguration IMPLICIT_PART_KEY_LOCK_CONFIG_NO_TBL_NAME;
  private static final LockConfiguration IMPLICIT_PART_KEY_LOCK_CONFIG;
  private static final String TABLE_NAME_PREFIX = "testDDBTable-";
  private static final String REGION = "us-east-2";
  private static DynamoDbClient dynamoDb;
  private static String SCHEME_S3 = "s3";
  private static String SCHEME_S3A = "s3a";
  private static String URI_NO_CLOUD_PROVIDER_PREFIX = "://my-bucket-8b2a4b30/1718662238400/be715573/my_lake/my_table";
  static {
    Properties dynamoDblpProps = new Properties();
    Properties implicitPartKeyLpProps;
    // Common properties shared by both lock providers.
    dynamoDblpProps.setProperty(DynamoDbBasedLockConfig.DYNAMODB_LOCK_BILLING_MODE.key(), BillingMode.PAY_PER_REQUEST.name());
    dynamoDblpProps.setProperty(DynamoDbBasedLockConfig.DYNAMODB_LOCK_TABLE_CREATION_TIMEOUT.key(), Integer.toString(20 * 1000 * 5));
    dynamoDblpProps.setProperty(DynamoDbBasedLockConfig.DYNAMODB_LOCK_REGION.key(), REGION);
    dynamoDblpProps.setProperty(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "1000");
    dynamoDblpProps.setProperty(DynamoDbBasedLockConfig.DYNAMODB_LOCK_READ_CAPACITY.key(), "0");
    dynamoDblpProps.setProperty(DynamoDbBasedLockConfig.DYNAMODB_LOCK_WRITE_CAPACITY.key(), "0");

    implicitPartKeyLpProps = TypedProperties.copy(dynamoDblpProps);
    dynamoDblpProps = TypedProperties.copy(dynamoDblpProps);

    // For the day-1 DynamoDbBasedLockProvider, it requires an optional partition key
    dynamoDblpProps.setProperty(DynamoDbBasedLockConfig.DYNAMODB_LOCK_PARTITION_KEY.key(), "testKey");
    LOCK_CONFIGURATION = new LockConfiguration(dynamoDblpProps);

    // For the newly added implicit partition key DDB lock provider, it can derive the partition key from
    // hudi table base path and hudi table name. These properties are available in lockConfig as of today.
    implicitPartKeyLpProps.setProperty(
        HoodieCommonConfig.BASE_PATH.key(), SCHEME_S3A + URI_NO_CLOUD_PROVIDER_PREFIX);
    implicitPartKeyLpProps.setProperty(
        HoodieTableConfig.HOODIE_TABLE_NAME_KEY, "ma_po_tofu_is_awesome");
    IMPLICIT_PART_KEY_LOCK_CONFIG = new LockConfiguration(implicitPartKeyLpProps);
    // With partition key nothing should break, the field should be simply ignored.
    TypedProperties withPartKey = TypedProperties.copy(implicitPartKeyLpProps);
    withPartKey.setProperty(DynamoDbBasedLockConfig.DYNAMODB_LOCK_PARTITION_KEY.key(), "testKey");
    IMPLICIT_PART_KEY_LOCK_CONFIG_WITH_PART_KEY = new LockConfiguration(withPartKey);

    // Missing either base path or hoodie table name is a bad config for implicit partition key lock provider.
    TypedProperties missingBasePath = TypedProperties.copy(implicitPartKeyLpProps);
    missingBasePath.remove(HoodieCommonConfig.BASE_PATH.key());
    IMPLICIT_PART_KEY_LOCK_CONFIG_NO_BASE_PATH = new LockConfiguration(missingBasePath);

    TypedProperties missingTableName = TypedProperties.copy(implicitPartKeyLpProps);
    missingTableName.remove(HoodieTableConfig.HOODIE_TABLE_NAME_KEY);
    IMPLICIT_PART_KEY_LOCK_CONFIG_NO_TBL_NAME = new LockConfiguration(missingTableName);
  }

  @BeforeAll
  public static void setup() throws InterruptedException {
    dynamoDb = getDynamoClientWithLocalEndpoint();
  }

  public static Stream<Object> badTestDimensions() {
    return Stream.of(
        Arguments.of(IMPLICIT_PART_KEY_LOCK_CONFIG_NO_TBL_NAME, DynamoDBBasedLockProvider.class),
        Arguments.of(IMPLICIT_PART_KEY_LOCK_CONFIG_NO_TBL_NAME, DynamoDBBasedImplicitPartitionKeyLockProvider.class),
        Arguments.of(IMPLICIT_PART_KEY_LOCK_CONFIG_NO_BASE_PATH, DynamoDBBasedImplicitPartitionKeyLockProvider.class)
    );
  }

  private static DynamoDbClient getDynamoClientWithLocalEndpoint() {
    String endpoint = System.getProperty("dynamodb-local.endpoint");
    if (endpoint == null || endpoint.isEmpty()) {
      throw new IllegalStateException("dynamodb-local.endpoint system property not set");
    }
    return DynamoDbClient.builder()
        .region(Region.of(REGION))
        .endpointOverride(URI.create(endpoint))
        .credentialsProvider(getCredentials())
        .build();
  }

  private static AwsCredentialsProvider getCredentials() {
    return StaticCredentialsProvider.create(AwsBasicCredentials.create("random-access-key", "random-secret-key"));
  }

  @ParameterizedTest
  @MethodSource("badTestDimensions")
  void testBadConfig(LockConfiguration lockConfig, Class<?> lockProviderClass) {
    lockConfig.getConfig().setProperty(DynamoDbBasedLockConfig.DYNAMODB_LOCK_TABLE_NAME.key(), TABLE_NAME_PREFIX + UUID.randomUUID());
    Exception e = new Exception();
    try {
      ReflectionUtils.loadClass(
          lockProviderClass.getName(),
          new Class<?>[] {LockConfiguration.class, StorageConfiguration.class, DynamoDbClient.class},
          lockConfig, null, dynamoDb);
    } catch (Exception ex) {
      e = ex;
    }
    Assertions.assertEquals(IllegalArgumentException.class, e.getCause().getCause().getClass());
  }

  public static Stream<Arguments> testDimensions() {
    return Stream.of(
        // Without partition key, only table name is used.
        Arguments.of(IMPLICIT_PART_KEY_LOCK_CONFIG, DynamoDBBasedLockProvider.class),
        // Even if we have partition key set, nothing would break.
        Arguments.of(LOCK_CONFIGURATION, DynamoDBBasedLockProvider.class),
        // Even if we have partition key set, nothing would break.
        Arguments.of(IMPLICIT_PART_KEY_LOCK_CONFIG_WITH_PART_KEY, DynamoDBBasedImplicitPartitionKeyLockProvider.class),
        Arguments.of(IMPLICIT_PART_KEY_LOCK_CONFIG, DynamoDBBasedImplicitPartitionKeyLockProvider.class)
    );
  }

  @ParameterizedTest
  @MethodSource("testDimensions")
  void testAcquireLock(LockConfiguration lockConfig, Class<?> lockProviderClass) {
    lockConfig.getConfig().setProperty(DynamoDbBasedLockConfig.DYNAMODB_LOCK_TABLE_NAME.key(), TABLE_NAME_PREFIX + UUID.randomUUID());
    DynamoDBBasedLockProviderBase dynamoDbBasedLockProvider = (DynamoDBBasedLockProviderBase) ReflectionUtils.loadClass(
        lockProviderClass.getName(),
        new Class<?>[] {LockConfiguration.class, StorageConfiguration.class, DynamoDbClient.class},
        new Object[] {lockConfig, null, dynamoDb});

    // Also validate the partition key is properly constructed.
    if (lockProviderClass.equals(DynamoDBBasedLockProvider.class)) {
      String tableName = (String) lockConfig.getConfig().get(HoodieTableConfig.HOODIE_TABLE_NAME_KEY);
      String partitionKey = (String) lockConfig.getConfig().get(DynamoDbBasedLockConfig.DYNAMODB_LOCK_PARTITION_KEY.key());
      if (partitionKey != null) {
        Assertions.assertEquals(partitionKey, dynamoDbBasedLockProvider.getPartitionKey());
      } else {
        Assertions.assertEquals(tableName, dynamoDbBasedLockProvider.getPartitionKey());
      }
    } else if (lockProviderClass.equals(DynamoDBBasedImplicitPartitionKeyLockProvider.class)) {
      String tableName = (String) lockConfig.getConfig().get(HoodieTableConfig.HOODIE_TABLE_NAME_KEY);
      String basePath = (String) lockConfig.getConfig().get(HoodieCommonConfig.BASE_PATH.key());
      // Base path is constructed with prefix s3a, verify that for partition key calculation, s3a is replaced with s3
      Assertions.assertTrue(basePath.startsWith(SCHEME_S3A));
      // Verify base path only scheme partition key
      Assertions.assertEquals(
          HashID.generateXXHashAsString(SCHEME_S3 + URI_NO_CLOUD_PROVIDER_PREFIX, HashID.Size.BITS_64),
          dynamoDbBasedLockProvider.getPartitionKey());
    }

    // Test lock acquisition and release
    Assertions.assertTrue(dynamoDbBasedLockProvider.tryLock(lockConfig.getConfig().getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    dynamoDbBasedLockProvider.unlock();
  }

  @ParameterizedTest
  @MethodSource("testDimensions")
  void testUnlock(LockConfiguration lockConfig, Class<?> lockProviderClass) {
    lockConfig.getConfig().setProperty(DynamoDbBasedLockConfig.DYNAMODB_LOCK_TABLE_NAME.key(), TABLE_NAME_PREFIX + UUID.randomUUID());
    DynamoDBBasedLockProviderBase dynamoDbBasedLockProvider = (DynamoDBBasedLockProviderBase) ReflectionUtils.loadClass(
        lockProviderClass.getName(),
        new Class<?>[] {LockConfiguration.class, StorageConfiguration.class, DynamoDbClient.class},
        new Object[] {lockConfig, null, dynamoDb});
    Assertions.assertTrue(dynamoDbBasedLockProvider.tryLock(lockConfig.getConfig().getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    dynamoDbBasedLockProvider.unlock();
    Assertions.assertTrue(dynamoDbBasedLockProvider.tryLock(lockConfig.getConfig().getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
  }

  @ParameterizedTest
  @MethodSource("testDimensions")
  void testReentrantLock(LockConfiguration lockConfig, Class<?> lockProviderClass) {
    lockConfig.getConfig().setProperty(DynamoDbBasedLockConfig.DYNAMODB_LOCK_TABLE_NAME.key(), TABLE_NAME_PREFIX + UUID.randomUUID());
    DynamoDBBasedLockProviderBase dynamoDbBasedLockProvider = (DynamoDBBasedLockProviderBase) ReflectionUtils.loadClass(
        lockProviderClass.getName(),
        new Class<?>[] {LockConfiguration.class, StorageConfiguration.class, DynamoDbClient.class},
        new Object[] {lockConfig, null, dynamoDb});
    Assertions.assertTrue(dynamoDbBasedLockProvider.tryLock(lockConfig.getConfig().getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    Assertions.assertFalse(dynamoDbBasedLockProvider.tryLock(lockConfig.getConfig().getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    dynamoDbBasedLockProvider.unlock();
  }

  @ParameterizedTest
  @MethodSource("testDimensions")
  void testUnlockWithoutLock(LockConfiguration lockConfig, Class<?> lockProviderClass) {
    lockConfig.getConfig().setProperty(DynamoDbBasedLockConfig.DYNAMODB_LOCK_TABLE_NAME.key(), TABLE_NAME_PREFIX + UUID.randomUUID());
    DynamoDBBasedLockProviderBase dynamoDbBasedLockProvider = (DynamoDBBasedLockProviderBase) ReflectionUtils.loadClass(
        lockProviderClass.getName(),
        new Class<?>[] {LockConfiguration.class, StorageConfiguration.class, DynamoDbClient.class},
        new Object[] {lockConfig, null, dynamoDb});
    dynamoDbBasedLockProvider.unlock();
  }
}
