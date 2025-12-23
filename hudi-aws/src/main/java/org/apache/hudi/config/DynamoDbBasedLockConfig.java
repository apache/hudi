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

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.RegionMetadata;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;

/**
 * Hoodie Configs for Locks.
 */
@ConfigClassProperty(name = "DynamoDB based Locks Configurations",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    subGroupName = ConfigGroups.SubGroupNames.LOCK,
    description = "Configs that control DynamoDB based locking mechanisms required for concurrency control "
        + " between writers to a Hudi table. Concurrency between Hudi's own table services "
        + " are auto managed internally.")
@NoArgsConstructor(access = AccessLevel.PACKAGE)
public class DynamoDbBasedLockConfig extends HoodieConfig {

  // configs for DynamoDb based locks
  public static final String DYNAMODB_BASED_LOCK_PROPERTY_PREFIX = LockConfiguration.LOCK_PREFIX + "dynamodb.";

  public static final ConfigProperty<String> DYNAMODB_LOCK_TABLE_NAME = ConfigProperty
      .key(DYNAMODB_BASED_LOCK_PROPERTY_PREFIX + "table")
      .defaultValue("hudi_locks")
      .markAdvanced()
      .sinceVersion("0.10.0")
      .withDocumentation("For DynamoDB based lock provider, the name of the DynamoDB table acting as lock table");

  public static final ConfigProperty<String> DYNAMODB_LOCK_PARTITION_KEY = ConfigProperty
      .key(DYNAMODB_BASED_LOCK_PROPERTY_PREFIX + "partition_key")
      .noDefaultValue()
      .markAdvanced()
      .sinceVersion("0.10.0")
      .withInferFunction(cfg -> {
        if (cfg.contains(HoodieTableConfig.NAME)) {
          return Option.of(cfg.getString(HoodieTableConfig.NAME));
        }
        return Option.empty();
      })
      .withDocumentation("For DynamoDB based lock provider, the partition key for the DynamoDB lock table. "
          + "Each Hudi dataset should has it's unique key so concurrent writers could refer to the same partition key."
          + " By default we use the Hudi table name specified to be the partition key");

  public static final ConfigProperty<String> DYNAMODB_LOCK_REGION =
      ConfigProperty.key(DYNAMODB_BASED_LOCK_PROPERTY_PREFIX + "region")
          .defaultValue("us-east-1")
          .markAdvanced()
          .sinceVersion("0.10.0")
          .withInferFunction(
              cfg -> {
                String regionFromEnv = System.getenv("AWS_REGION");
                if (regionFromEnv != null) {
                  return Option.of(RegionMetadata.of(Region.of(regionFromEnv)).id());
                }
                return Option.empty();
              })
          .withDocumentation(
              "For DynamoDB based lock provider, the region used in endpoint for Amazon DynamoDB service."
                  + " Would try to first get it from AWS_REGION environment variable. If not find, by default use us-east-1");

  public static final ConfigProperty<String> DYNAMODB_LOCK_BILLING_MODE = ConfigProperty
      .key(DYNAMODB_BASED_LOCK_PROPERTY_PREFIX + "billing_mode")
      .defaultValue(BillingMode.PAY_PER_REQUEST.name())
      .markAdvanced()
      .sinceVersion("0.10.0")
      .withDocumentation("For DynamoDB based lock provider, by default it is `PAY_PER_REQUEST` mode. Alternative is `PROVISIONED`.");

  public static final ConfigProperty<String> DYNAMODB_LOCK_READ_CAPACITY = ConfigProperty
      .key(DYNAMODB_BASED_LOCK_PROPERTY_PREFIX + "read_capacity")
      .defaultValue("20")
      .markAdvanced()
      .sinceVersion("0.10.0")
      .withDocumentation("For DynamoDB based lock provider, read capacity units when using PROVISIONED billing mode");

  public static final ConfigProperty<String> DYNAMODB_LOCK_WRITE_CAPACITY = ConfigProperty
      .key(DYNAMODB_BASED_LOCK_PROPERTY_PREFIX + "write_capacity")
      .defaultValue("10")
      .markAdvanced()
      .sinceVersion("0.10.0")
      .withDocumentation("For DynamoDB based lock provider, write capacity units when using PROVISIONED billing mode");

  public static final ConfigProperty<String> DYNAMODB_LOCK_TABLE_CREATION_TIMEOUT = ConfigProperty
      .key(DYNAMODB_BASED_LOCK_PROPERTY_PREFIX + "table_creation_timeout")
      .defaultValue(String.valueOf(2 * 60 * 1000))
      .markAdvanced()
      .sinceVersion("0.10.0")
      .withDocumentation("For DynamoDB based lock provider, the maximum number of milliseconds to wait for creating DynamoDB table");

  public static final ConfigProperty<String> DYNAMODB_ENDPOINT_URL = ConfigProperty
      .key(DYNAMODB_BASED_LOCK_PROPERTY_PREFIX + "endpoint_url")
      .noDefaultValue()
      .markAdvanced()
      .sinceVersion("0.10.1")
      .withDocumentation("For DynamoDB based lock provider, the url endpoint used for Amazon DynamoDB service."
          + " Useful for development with a local dynamodb instance.");

  public static final ConfigProperty<Integer> LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY = ConfigProperty
      .key(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY)
      .defaultValue(LockConfiguration.DEFAULT_LOCK_ACQUIRE_WAIT_TIMEOUT_MS)
      .markAdvanced()
      .sinceVersion("0.10.0")
      .withDocumentation("Lock Acquire Wait Timeout in milliseconds");

  public static DynamoDbBasedLockConfig from(TypedProperties properties) {
    DynamoDbBasedLockConfig config = new DynamoDbBasedLockConfig();
    config.getProps().putAll(properties);
    config.setDefaults(DynamoDbBasedLockConfig.class.getName());
    ValidationUtils.checkArgument(config.contains(DYNAMODB_LOCK_TABLE_NAME.key()), "Config key is not found: " + DYNAMODB_LOCK_TABLE_NAME.key());
    ValidationUtils.checkArgument(config.contains(DYNAMODB_LOCK_REGION.key()), "Config key is not found: " + DYNAMODB_LOCK_REGION.key());
    return config;
  }
}
