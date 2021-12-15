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
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;

import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.dynamodbv2.model.BillingMode;

import static org.apache.hudi.common.config.LockConfiguration.LOCK_PREFIX;

/**
 * Hoodie Configs for Locks.
 */
@ConfigClassProperty(name = "DynamoDB based Locks Configurations",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "Configs that control DynamoDB based locking mechanisms required for concurrency control "
        + " between writers to a Hudi table. Concurrency between Hudi's own table services "
        + " are auto managed internally.")
public class DynamoDbBasedLockConfig extends HoodieConfig {

  // configs for DynamoDb based locks
  public static final String DYNAMODB_BASED_LOCK_PROPERTY_PREFIX = LOCK_PREFIX + "dynamodb.";

  public static final ConfigProperty<String> DYNAMODB_LOCK_TABLE_NAME = ConfigProperty
      .key(DYNAMODB_BASED_LOCK_PROPERTY_PREFIX + "table")
      .noDefaultValue()
      .sinceVersion("0.10.0")
      .withDocumentation("For DynamoDB based lock provider, the name of the DynamoDB table acting as lock table");

  public static final ConfigProperty<String> DYNAMODB_LOCK_PARTITION_KEY = ConfigProperty
      .key(DYNAMODB_BASED_LOCK_PROPERTY_PREFIX + "partition_key")
      .noDefaultValue()
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

  public static final ConfigProperty<String> DYNAMODB_LOCK_REGION = ConfigProperty
      .key(DYNAMODB_BASED_LOCK_PROPERTY_PREFIX + "region")
      .defaultValue("us-east-1")
      .sinceVersion("0.10.0")
      .withInferFunction(cfg -> {
        String regionFromEnv = System.getenv("AWS_REGION");
        if (regionFromEnv != null) {
          return Option.of(RegionUtils.getRegion(regionFromEnv).getName());
        }
        return Option.empty();
      })
      .withDocumentation("For DynamoDB based lock provider, the region used in endpoint for Amazon DynamoDB service."
          + " Would try to first get it from AWS_REGION environment variable. If not find, by default use us-east-1");

  public static final ConfigProperty<String> DYNAMODB_LOCK_BILLING_MODE = ConfigProperty
      .key(DYNAMODB_BASED_LOCK_PROPERTY_PREFIX + "billing_mode")
      .defaultValue(BillingMode.PAY_PER_REQUEST.name())
      .sinceVersion("0.10.0")
      .withDocumentation("For DynamoDB based lock provider, by default it is PAY_PER_REQUEST mode");

  public static final ConfigProperty<String> DYNAMODB_LOCK_READ_CAPACITY = ConfigProperty
      .key(DYNAMODB_BASED_LOCK_PROPERTY_PREFIX + "read_capacity")
      .defaultValue("20")
      .sinceVersion("0.10.0")
      .withDocumentation("For DynamoDB based lock provider, read capacity units when using PROVISIONED billing mode");

  public static final ConfigProperty<String> DYNAMODB_LOCK_WRITE_CAPACITY = ConfigProperty
      .key(DYNAMODB_BASED_LOCK_PROPERTY_PREFIX + "write_capacity")
      .defaultValue("10")
      .sinceVersion("0.10.0")
      .withDocumentation("For DynamoDB based lock provider, write capacity units when using PROVISIONED billing mode");

  public static final ConfigProperty<String> DYNAMODB_LOCK_TABLE_CREATION_TIMEOUT = ConfigProperty
      .key(DYNAMODB_BASED_LOCK_PROPERTY_PREFIX + "table_creation_timeout")
      .defaultValue(String.valueOf(10 * 60 * 1000))
      .sinceVersion("0.10.0")
      .withDocumentation("For DynamoDB based lock provider, the maximum number of milliseconds to wait for creating DynamoDB table");
}
