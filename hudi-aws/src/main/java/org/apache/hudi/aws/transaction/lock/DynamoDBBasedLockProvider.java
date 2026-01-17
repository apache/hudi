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

import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.DynamoDbBasedLockConfig;
import org.apache.hudi.storage.StorageConfiguration;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import javax.annotation.concurrent.NotThreadSafe;

import static org.apache.hudi.config.DynamoDbBasedLockConfig.DYNAMODB_LOCK_PARTITION_KEY;

/**
 * A DynamoDB based lock.
 * It expects partition key is explicitly available in DynamoDbBasedLockConfig.
 */
@NotThreadSafe
public class DynamoDBBasedLockProvider extends DynamoDBBasedLockProviderBase {

  public DynamoDBBasedLockProvider(final LockConfiguration lockConfiguration, final StorageConfiguration<?> conf) {
    this(lockConfiguration, conf, null);
  }

  public DynamoDBBasedLockProvider(final LockConfiguration lockConfiguration, final StorageConfiguration<?> conf, DynamoDbClient dynamoDB) {
    super(lockConfiguration, conf, dynamoDB);
  }

  @Override
  public String getDynamoDBPartitionKey(LockConfiguration lockConfiguration) {
    DynamoDbBasedLockConfig config = DynamoDbBasedLockConfig.from(lockConfiguration.getConfig());
    ValidationUtils.checkArgument(
        config.contains(DYNAMODB_LOCK_PARTITION_KEY),
        "Config key is not found: " + DYNAMODB_LOCK_PARTITION_KEY.key());
    return config.getString(DYNAMODB_LOCK_PARTITION_KEY);
  }
}
