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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import org.apache.hudi.aws.transaction.lock.DynamoDBBasedLockProvider;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.config.DynamoDbBasedLockConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY;

/**
 * Test for {@link DynamoDBBasedLockProvider}.
 * Set it as integration test because it requires setting up docker environment.
 */
public class ITTestDynamoDBBasedLockProvider {

  private static LockConfiguration lockConfiguration;
  private static AmazonDynamoDB dynamoDb;

  private static final String TABLE_NAME_PREFIX = "testDDBTable-";
  private static final String REGION = "us-east-2";

  @BeforeAll
  public static void setup() throws InterruptedException {
    Properties properties = new Properties();
    properties.setProperty(DynamoDbBasedLockConfig.DYNAMODB_LOCK_BILLING_MODE.key(), BillingMode.PAY_PER_REQUEST.name());
    // properties.setProperty(AWSLockConfig.DYNAMODB_LOCK_TABLE_NAME.key(), TABLE_NAME_PREFIX);
    properties.setProperty(DynamoDbBasedLockConfig.DYNAMODB_LOCK_PARTITION_KEY.key(), "testKey");
    properties.setProperty(DynamoDbBasedLockConfig.DYNAMODB_LOCK_REGION.key(), REGION);
    properties.setProperty(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "1000");
    properties.setProperty(DynamoDbBasedLockConfig.DYNAMODB_LOCK_READ_CAPACITY.key(), "0");
    properties.setProperty(DynamoDbBasedLockConfig.DYNAMODB_LOCK_WRITE_CAPACITY.key(), "0");
    lockConfiguration = new LockConfiguration(properties);
    dynamoDb = getDynamoClientWithLocalEndpoint();
  }

  @Test
  public void testAcquireLock() {
    lockConfiguration.getConfig().setProperty(DynamoDbBasedLockConfig.DYNAMODB_LOCK_TABLE_NAME.key(), TABLE_NAME_PREFIX + UUID.randomUUID());
    DynamoDBBasedLockProvider dynamoDbBasedLockProvider = new DynamoDBBasedLockProvider(lockConfiguration, null, dynamoDb);
    Assertions.assertTrue(dynamoDbBasedLockProvider.tryLock(lockConfiguration.getConfig()
            .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    dynamoDbBasedLockProvider.unlock();
  }

  @Test
  public void testUnlock() {
    lockConfiguration.getConfig().setProperty(DynamoDbBasedLockConfig.DYNAMODB_LOCK_TABLE_NAME.key(), TABLE_NAME_PREFIX + UUID.randomUUID());
    DynamoDBBasedLockProvider dynamoDbBasedLockProvider = new DynamoDBBasedLockProvider(lockConfiguration, null, dynamoDb);
    Assertions.assertTrue(dynamoDbBasedLockProvider.tryLock(lockConfiguration.getConfig()
            .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    dynamoDbBasedLockProvider.unlock();
    Assertions.assertTrue(dynamoDbBasedLockProvider.tryLock(lockConfiguration.getConfig()
            .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
  }

  @Test
  public void testReentrantLock() {
    lockConfiguration.getConfig().setProperty(DynamoDbBasedLockConfig.DYNAMODB_LOCK_TABLE_NAME.key(), TABLE_NAME_PREFIX + UUID.randomUUID());
    DynamoDBBasedLockProvider dynamoDbBasedLockProvider = new DynamoDBBasedLockProvider(lockConfiguration, null, dynamoDb);
    Assertions.assertTrue(dynamoDbBasedLockProvider.tryLock(lockConfiguration.getConfig()
            .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    Assertions.assertFalse(dynamoDbBasedLockProvider.tryLock(lockConfiguration.getConfig()
            .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    dynamoDbBasedLockProvider.unlock();
  }

  @Test
  public void testUnlockWithoutLock() {
    lockConfiguration.getConfig().setProperty(DynamoDbBasedLockConfig.DYNAMODB_LOCK_TABLE_NAME.key(), TABLE_NAME_PREFIX + UUID.randomUUID());
    DynamoDBBasedLockProvider dynamoDbBasedLockProvider = new DynamoDBBasedLockProvider(lockConfiguration, null, dynamoDb);
    dynamoDbBasedLockProvider.unlock();
  }

  private static AmazonDynamoDB getDynamoClientWithLocalEndpoint() {
    String endpoint = System.getProperty("dynamodb-local.endpoint");
    if (endpoint == null || endpoint.isEmpty()) {
      throw new IllegalStateException("dynamodb-local.endpoint system property not set");
    }
    return AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, REGION))
            .withCredentials(getCredentials())
            .build();
  }

  private static AWSCredentialsProvider getCredentials() {
    return new AWSStaticCredentialsProvider(new BasicAWSCredentials("random-access-key", "random-secret-key"));
  }
}
