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

package org.apache.hudi.aws.transaction.lock;

import org.apache.hudi.aws.credentials.HoodieAWSCredentialsProviderFactory;
import org.apache.hudi.aws.utils.DynamoTableUtils;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.lock.LockProvider;
import org.apache.hudi.common.lock.LockState;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.DynamoDbBasedLockConfig;
import org.apache.hudi.exception.HoodieLockException;
import org.apache.hudi.storage.StorageConfiguration;

import com.amazonaws.services.dynamodbv2.AcquireLockOptions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClientOptions;
import com.amazonaws.services.dynamodbv2.LockItem;
import com.amazonaws.services.dynamodbv2.model.LockNotGrantedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import javax.annotation.concurrent.NotThreadSafe;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.config.DynamoDbBasedLockConfig.DYNAMODB_ENDPOINT_URL;
import static org.apache.hudi.config.DynamoDbBasedLockConfig.DYNAMODB_LOCK_BILLING_MODE;
import static org.apache.hudi.config.DynamoDbBasedLockConfig.DYNAMODB_LOCK_READ_CAPACITY;
import static org.apache.hudi.config.DynamoDbBasedLockConfig.DYNAMODB_LOCK_REGION;
import static org.apache.hudi.config.DynamoDbBasedLockConfig.DYNAMODB_LOCK_TABLE_CREATION_TIMEOUT;
import static org.apache.hudi.config.DynamoDbBasedLockConfig.DYNAMODB_LOCK_WRITE_CAPACITY;

/**
 * A DynamoDB based lock. This {@link LockProvider} implementation allows to lock table operations
 * using DynamoDB. Users need to have access to AWS DynamoDB to be able to use this lock.
 */
@NotThreadSafe
public abstract class DynamoDBBasedLockProviderBase implements LockProvider<LockItem> {

  protected static final Logger LOG = LoggerFactory.getLogger(DynamoDBBasedLockProviderBase.class);

  protected static final String DYNAMODB_ATTRIBUTE_NAME = "key";

  protected final DynamoDbBasedLockConfig dynamoDbBasedLockConfig;
  protected final AmazonDynamoDBLockClient client;
  protected final String tableName;
  protected final String dynamoDBPartitionKey;
  protected volatile LockItem lock;

  protected DynamoDBBasedLockProviderBase(final LockConfiguration lockConfiguration, final StorageConfiguration<?> conf, DynamoDbClient dynamoDB) {
    this.dynamoDbBasedLockConfig = new DynamoDbBasedLockConfig.Builder()
        .fromProperties(lockConfiguration.getConfig())
        .build();
    this.tableName = dynamoDbBasedLockConfig.getString(DynamoDbBasedLockConfig.DYNAMODB_LOCK_TABLE_NAME);
    long leaseDuration = dynamoDbBasedLockConfig.getInt(DynamoDbBasedLockConfig.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY);
    dynamoDBPartitionKey = getDynamoDBPartitionKey(lockConfiguration);
    if (dynamoDB == null) {
      dynamoDB = getDynamoDBClient(dynamoDbBasedLockConfig);
    }
    // build the dynamoDb lock client
    this.client = new AmazonDynamoDBLockClient(
        AmazonDynamoDBLockClientOptions.builder(dynamoDB, tableName)
            .withTimeUnit(TimeUnit.MILLISECONDS)
            .withLeaseDuration(leaseDuration)
            .withHeartbeatPeriod(leaseDuration / 3)
            .withCreateHeartbeatBackgroundThread(true)
            .build());

    if (!this.client.lockTableExists()) {
      createLockTableInDynamoDB(dynamoDB, tableName);
    }
  }

  public abstract String getDynamoDBPartitionKey(LockConfiguration lockConfiguration);

  public String getPartitionKey() {
    return dynamoDBPartitionKey;
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) {
    LOG.info(generateLogStatement(LockState.ACQUIRING, generateLogSuffixString()));
    try {
      lock = client.acquireLock(AcquireLockOptions.builder(dynamoDBPartitionKey)
          .withAdditionalTimeToWaitForLock(time)
          .withTimeUnit(TimeUnit.MILLISECONDS)
          .build());
      LOG.info(generateLogStatement(LockState.ACQUIRED, generateLogSuffixString()));
    } catch (InterruptedException e) {
      throw new HoodieLockException(generateLogStatement(LockState.FAILED_TO_ACQUIRE, generateLogSuffixString()), e);
    } catch (LockNotGrantedException e) {
      return false;
    }
    return lock != null && !lock.isExpired();
  }

  @Override
  public void unlock() {
    try {
      LOG.info(generateLogStatement(LockState.RELEASING, generateLogSuffixString()));
      if (lock == null) {
        return;
      }
      if (!client.releaseLock(lock)) {
        LOG.info("The lock has already been stolen");
      }
      lock = null;
      LOG.info(generateLogStatement(LockState.RELEASED, generateLogSuffixString()));
    } catch (Exception e) {
      throw new HoodieLockException(generateLogStatement(LockState.FAILED_TO_RELEASE, generateLogSuffixString()), e);
    }
  }

  @Override
  public void close() {
    try {
      if (lock != null) {
        if (!client.releaseLock(lock)) {
          LOG.info("The lock has already been stolen");
        }
        lock = null;
      }
      this.client.close();
    } catch (Exception e) {
      LOG.error(generateLogStatement(LockState.FAILED_TO_RELEASE, generateLogSuffixString()));
    }
  }

  @Override
  public LockItem getLock() {
    return lock;
  }

  private static DynamoDbClient getDynamoDBClient(DynamoDbBasedLockConfig dynamoDbBasedLockConfig) {
    String region = dynamoDbBasedLockConfig.getString(DYNAMODB_LOCK_REGION);
    String endpointURL = dynamoDbBasedLockConfig.contains(DYNAMODB_ENDPOINT_URL.key())
        ? dynamoDbBasedLockConfig.getString(DYNAMODB_ENDPOINT_URL)
        : DynamoDbClient.serviceMetadata().endpointFor(Region.of(region)).toString();

    if (!endpointURL.startsWith("https://") && !endpointURL.startsWith("http://")) {
      endpointURL = "https://" + endpointURL;
    }

    return DynamoDbClient.builder()
        .endpointOverride(URI.create(endpointURL))
        .credentialsProvider(HoodieAWSCredentialsProviderFactory.getAwsCredentialsProvider(dynamoDbBasedLockConfig.getProps()))
        .build();
  }

  private void createLockTableInDynamoDB(DynamoDbClient dynamoDB, String tableName) {
    String billingMode = dynamoDbBasedLockConfig.getString(DYNAMODB_LOCK_BILLING_MODE);
    KeySchemaElement partitionKeyElement = KeySchemaElement
        .builder()
        .attributeName(DYNAMODB_ATTRIBUTE_NAME)
        .keyType(KeyType.HASH)
        .build();

    List<KeySchemaElement> keySchema = new ArrayList<>();
    keySchema.add(partitionKeyElement);

    Collection<AttributeDefinition> attributeDefinitions = new ArrayList<>();
    attributeDefinitions.add(AttributeDefinition.builder().attributeName(DYNAMODB_ATTRIBUTE_NAME).attributeType(ScalarAttributeType.S).build());
    CreateTableRequest.Builder createTableRequestBuilder = CreateTableRequest.builder();
    if (billingMode.equals(BillingMode.PROVISIONED.name())) {
      createTableRequestBuilder.provisionedThroughput(ProvisionedThroughput.builder()
          .readCapacityUnits(dynamoDbBasedLockConfig.getLong(DYNAMODB_LOCK_READ_CAPACITY))
          .writeCapacityUnits(dynamoDbBasedLockConfig.getLong(DYNAMODB_LOCK_WRITE_CAPACITY))
          .build());
    }
    createTableRequestBuilder.tableName(tableName)
        .keySchema(keySchema)
        .attributeDefinitions(attributeDefinitions)
        .billingMode(billingMode);
    dynamoDB.createTable(createTableRequestBuilder.build());

    LOG.info("Creating dynamoDB table {}, waiting for table to be active", tableName);
    try {

      DynamoTableUtils.waitUntilActive(dynamoDB, tableName, dynamoDbBasedLockConfig.getInt(DYNAMODB_LOCK_TABLE_CREATION_TIMEOUT), 20 * 1000);
    } catch (DynamoTableUtils.TableNeverTransitionedToStateException e) {
      throw new HoodieLockException("Created dynamoDB table never transits to active", e);
    } catch (InterruptedException e) {
      throw new HoodieLockException("Thread interrupted while waiting for dynamoDB table to turn active", e);
    }
    LOG.info("Created dynamoDB table {}", tableName);
  }

  protected String generateLogSuffixString() {
    return StringUtils.join("DynamoDb table = ", tableName, ", partition key = ", dynamoDBPartitionKey);
  }

  protected String generateLogStatement(LockState state, String suffix) {
    return StringUtils.join(state.name(), " lock at ", suffix);
  }
}
