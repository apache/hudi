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

package org.apache.hudi.client.transaction.lock;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.dynamodbv2.AcquireLockOptions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClientOptions;
import com.amazonaws.services.dynamodbv2.LockItem;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LockNotGrantedException;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.aws.HoodieAWSCredentialsProviderFactory;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.lock.LockProvider;
import org.apache.hudi.common.lock.LockState;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieLockException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.NotThreadSafe;

import static org.apache.hudi.common.config.LockConfiguration.DYNAMODB_BILLING_MODE_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.DYNAMODB_PARTITION_KEY_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.DYNAMODB_READ_CAPACITY_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.DYNAMODB_REGION_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.DYNAMODB_TABLE_NAME_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.DYNAMODB_WRITE_CAPACITY_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY;

/**
 * A DynamoDB based lock. This {@link LockProvider} implementation allows to lock table operations
 * using DynamoDB. Users need to have access to AWS DynamoDB to be able to use this lock.
 */
@NotThreadSafe
public class DynamoDBBasedLockProvider implements LockProvider<LockItem> {

  private static final Logger LOG = LogManager.getLogger(DynamoDBBasedLockProvider.class);

  private final AmazonDynamoDBLockClient client;
  private final long leaseDuration;
  private final String tableName;
  protected LockConfiguration lockConfiguration;
  private volatile LockItem lock;

  public DynamoDBBasedLockProvider(final LockConfiguration lockConfiguration, final Configuration conf) {
    checkRequiredProps(lockConfiguration);
    this.lockConfiguration = lockConfiguration;
    this.tableName = lockConfiguration.getConfig().getString(DYNAMODB_TABLE_NAME_PROP_KEY);
    this.leaseDuration = Long.parseLong(lockConfiguration.getConfig().getString(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY));
    AmazonDynamoDB dynamoDB = getDynamoClient();
    // build the dynamoDb lock client
    this.client = new AmazonDynamoDBLockClient(
        AmazonDynamoDBLockClientOptions.builder(dynamoDB, tableName)
                .withTimeUnit(TimeUnit.MILLISECONDS)
                .withLeaseDuration(leaseDuration)
                .withHeartbeatPeriod(leaseDuration / 3)
                .withCreateHeartbeatBackgroundThread(true)
                .build());
    if (!client.lockTableExists()) {
      createLockTableInDynamoDB(
              dynamoDB,
              tableName,
              lockConfiguration.getConfig().getString(DYNAMODB_BILLING_MODE_PROP_KEY),
              Long.parseLong(lockConfiguration.getConfig().getString(DYNAMODB_READ_CAPACITY_PROP_KEY)),
              Long.parseLong(lockConfiguration.getConfig().getString(DYNAMODB_WRITE_CAPACITY_PROP_KEY)));
    }
  }

  // Only used for testing
  public DynamoDBBasedLockProvider(final LockConfiguration lockConfiguration, final AmazonDynamoDBLockClient client) {
    checkRequiredProps(lockConfiguration);
    this.lockConfiguration = lockConfiguration;
    this.tableName = lockConfiguration.getConfig().getString(DYNAMODB_TABLE_NAME_PROP_KEY);
    this.leaseDuration = Long.parseLong(lockConfiguration.getConfig().getString(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY));
    this.client = client;
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) {
    LOG.info(generateLogStatement(LockState.ACQUIRING, generateLogSuffixString()));
    long millisTime = unit.toMillis(time);
    try {
      lock = client.acquireLock(AcquireLockOptions.builder(lockConfiguration.getConfig().getString(DYNAMODB_PARTITION_KEY_PROP_KEY))
              .withAdditionalTimeToWaitForLock(millisTime - leaseDuration > 0 ? millisTime - leaseDuration : 0)
              .withTimeUnit(TimeUnit.MILLISECONDS)
              .build());
      LOG.info(generateLogStatement(LockState.ACQUIRED, generateLogSuffixString()));
    } catch (InterruptedException | LockNotGrantedException e) {
      throw new HoodieLockException(generateLogStatement(LockState.FAILED_TO_ACQUIRE, generateLogSuffixString()), e);
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
        LOG.warn("The lock has already been stolen");
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
          LOG.warn("The lock has already been stolen");
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

  private AmazonDynamoDB getDynamoClient() {
    String region = this.lockConfiguration.getConfig().getString(DYNAMODB_REGION_PROP_KEY);
    String endpointURL = RegionUtils.getRegion(region).getServiceEndpoint(AmazonDynamoDB.ENDPOINT_PREFIX);
    AwsClientBuilder.EndpointConfiguration dynamodbEndpoint =
            new AwsClientBuilder.EndpointConfiguration(endpointURL, region);
    return AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(dynamodbEndpoint)
            .withCredentials(HoodieAWSCredentialsProviderFactory.getAwsCredentialsProvider(lockConfiguration.getConfig()))
            .build();
  }

  public static void createLockTableInDynamoDB(AmazonDynamoDB dynamoDB, String tableName) {
    createLockTableInDynamoDB(dynamoDB, tableName, "PAY_PER_REQUEST", 0L, 0L);
  }

  private static void createLockTableInDynamoDB(AmazonDynamoDB dynamoDB, String tableName, String billingMode,
                                               Long readCapacityUnits, Long writeCapacityUnits) {
    KeySchemaElement partitionKeyElement = new KeySchemaElement();
    partitionKeyElement.setAttributeName("key");
    partitionKeyElement.setKeyType(KeyType.HASH);

    List<KeySchemaElement> keySchema = new ArrayList<>();
    keySchema.add(partitionKeyElement);

    Collection<AttributeDefinition> attributeDefinitions = new ArrayList<>();
    attributeDefinitions.add(new AttributeDefinition().withAttributeName("key").withAttributeType(ScalarAttributeType.S));

    CreateTableRequest createTableRequest = new CreateTableRequest(tableName, keySchema);
    createTableRequest.setAttributeDefinitions(attributeDefinitions);
    createTableRequest.setBillingMode(billingMode);
    if (billingMode.equals("PROVISIONED")) {
      createTableRequest.setProvisionedThroughput(
              new ProvisionedThroughput().withReadCapacityUnits(readCapacityUnits).withWriteCapacityUnits(writeCapacityUnits));
    }
    dynamoDB.createTable(createTableRequest);

    LOG.info("Creating dynamoDB table " + tableName + ", waiting for table to be active");
    try {
      TableUtils.waitUntilActive(dynamoDB, tableName);
    } catch (TableUtils.TableNeverTransitionedToStateException e) {
      throw new HoodieLockException("Created dynamoDB table never transits to active", e);
    } catch (InterruptedException e) {
      throw new HoodieLockException("Thread interrupted while waiting for dynamoDB table to turn active", e);
    }
    LOG.info("Created dynamoDB table " + tableName);
  }

  private void checkRequiredProps(final LockConfiguration config) {
    ValidationUtils.checkArgument(config.getConfig().getString(DYNAMODB_BILLING_MODE_PROP_KEY) != null);
    ValidationUtils.checkArgument(config.getConfig().getString(DYNAMODB_TABLE_NAME_PROP_KEY) != null);
    ValidationUtils.checkArgument(config.getConfig().getString(DYNAMODB_PARTITION_KEY_PROP_KEY) != null);
    ValidationUtils.checkArgument(config.getConfig().getString(DYNAMODB_REGION_PROP_KEY) != null);
  }

  private String generateLogSuffixString() {
    String dynamoDbPartitionKey = this.lockConfiguration.getConfig().getString(DYNAMODB_PARTITION_KEY_PROP_KEY);
    return StringUtils.join("DynamoDb table = ", tableName, ", partition key = ", dynamoDbPartitionKey);
  }

  protected String generateLogStatement(LockState state, String suffix) {
    return StringUtils.join(state.name(), " lock at ", suffix);
  }
}
