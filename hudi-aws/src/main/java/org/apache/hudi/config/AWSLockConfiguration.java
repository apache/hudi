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

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.util.Option;

import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.dynamodbv2.model.BillingMode;

import static org.apache.hudi.common.config.LockConfiguration.LOCK_PREFIX;

public class AWSLockConfiguration {

  // configs for DynamoDb based locks
  public static final String DYNAMODB_BASED_LOCK_PROPERTY_PREFIX = LOCK_PREFIX + "dynamodb.";

  public static final String DYNAMODB_LOCK_TABLE_NAME_PROP_KEY = DYNAMODB_BASED_LOCK_PROPERTY_PREFIX + "table";

  public static final String DYNAMODB_LOCK_PARTITION_KEY_PROP_KEY = DYNAMODB_BASED_LOCK_PROPERTY_PREFIX + "partition_key";

  public static final String DYNAMODB_LOCK_REGION_PROP_KEY = DYNAMODB_BASED_LOCK_PROPERTY_PREFIX + "region";

  public static final String DYNAMODB_LOCK_BILLING_MODE_PROP_KEY = DYNAMODB_BASED_LOCK_PROPERTY_PREFIX + "billing_mode";

  public static final String DYNAMODB_LOCK_READ_CAPACITY_PROP_KEY = DYNAMODB_BASED_LOCK_PROPERTY_PREFIX + "read_capacity";

  public static final String DYNAMODB_LOCK_WRITE_CAPACITY_PROP_KEY = DYNAMODB_BASED_LOCK_PROPERTY_PREFIX + "write_capacity";

  public static final String DYNAMODB_LOCK_TABLE_CREATION_TIMEOUT_PROP_KEY = DYNAMODB_BASED_LOCK_PROPERTY_PREFIX + "table_creation_timeout";

  public static final ConfigProperty<String> DYNAMODB_LOCK_TABLE_NAME = ConfigProperty
      .key(DYNAMODB_LOCK_TABLE_NAME_PROP_KEY)
      .noDefaultValue()
      .withDocumentation("For DynamoDB based lock provider, the name of the DynamoDB table acting as lock table");

  public static final ConfigProperty<String> DYNAMODB_LOCK_PARTITION_KEY = ConfigProperty
      .key(DYNAMODB_LOCK_PARTITION_KEY_PROP_KEY)
      .noDefaultValue()
      .withInferFunction(cfg -> {
        if (cfg.contains(HoodieWriteConfig.TBL_NAME)) {
          return Option.of(cfg.getString(HoodieWriteConfig.TBL_NAME));
        }
        return Option.empty();
      })
      .withDocumentation("For DynamoDB based lock provider, the partition key for the DynamoDB lock table. "
          + "Each Hudi dataset should has it's unique key so concurrent writers could refer to the same partition key."
          + " By default we use the Hudi table name specified to be the partition key");

  public static final ConfigProperty<String> DYNAMODB_LOCK_REGION = ConfigProperty
      .key(DYNAMODB_LOCK_REGION_PROP_KEY)
      .defaultValue("us-east-1")
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
      .key(DYNAMODB_LOCK_BILLING_MODE_PROP_KEY)
      .defaultValue(BillingMode.PAY_PER_REQUEST.name())
      .withDocumentation("For DynamoDB based lock provider, by default it is PAY_PER_REQUEST mode");

  public static final ConfigProperty<String> DYNAMODB_LOCK_READ_CAPACITY = ConfigProperty
      .key(DYNAMODB_LOCK_READ_CAPACITY_PROP_KEY)
      .defaultValue("20")
      .withDocumentation("For DynamoDB based lock provider, read capacity units when using PROVISIONED billing mode");

  public static final ConfigProperty<String> DYNAMODB_LOCK_WRITE_CAPACITY = ConfigProperty
      .key(DYNAMODB_LOCK_WRITE_CAPACITY_PROP_KEY)
      .defaultValue("10")
      .withDocumentation("For DynamoDB based lock provider, write capacity units when using PROVISIONED billing mode");

  public static final ConfigProperty<String> DYNAMODB_LOCK_TABLE_CREATION_TIMEOUT = ConfigProperty
      .key(DYNAMODB_LOCK_TABLE_CREATION_TIMEOUT_PROP_KEY)
      .defaultValue(String.valueOf(10 * 60 * 1000))
      .withDocumentation("For DynamoDB based lock provider, the maximum number of milliseconds to wait for creating DynamoDB table");


}
