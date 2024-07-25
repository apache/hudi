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

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.hash.HashID;
import org.apache.hudi.storage.StorageConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import javax.annotation.concurrent.NotThreadSafe;

import static org.apache.hudi.common.util.StringUtils.concatenateWithThreshold;
import static org.apache.hudi.config.DynamoDbBasedLockConfig.MAX_PARTITION_KEY_SIZE_BYTE;

/**
 * A DynamoDB based lock.
 * It implicitly derives the partition key from the hudi table name and hudi table base path
 * available in the lock configuration.
 */
@NotThreadSafe
public class DynamoDBBasedImplicitPartitionKeyLockProvider extends DynamoDBBasedLockProviderBase {
  protected static final Logger LOG = LoggerFactory.getLogger(DynamoDBBasedImplicitPartitionKeyLockProvider.class);

  public DynamoDBBasedImplicitPartitionKeyLockProvider(final LockConfiguration lockConfiguration, final StorageConfiguration<?> conf) {
    this(lockConfiguration, conf, null);
  }

  public DynamoDBBasedImplicitPartitionKeyLockProvider(
      final LockConfiguration lockConfiguration, final StorageConfiguration<?> conf, DynamoDbClient dynamoDB) {
    super(lockConfiguration, conf, dynamoDB);
  }

  public static String generatePartitionKey(String basePath, String tableName) {
    String hashPart = '-' + HashID.generateXXHashAsString(basePath, HashID.Size.BITS_64);
    String partitionKey = concatenateWithThreshold(tableName, hashPart, MAX_PARTITION_KEY_SIZE_BYTE);
    LOG.info(String.format("Partition key for base path %s is %s", basePath, partitionKey));
    return partitionKey;
  }

  @Override
  public String getDynamoDBPartitionKey(LockConfiguration lockConfiguration) {
    String hudiTableBasePath = lockConfiguration.getConfig().getString(HoodieCommonConfig.BASE_PATH.key());
    String hudiTableName = lockConfiguration.getConfig().getString(HoodieTableConfig.HOODIE_TABLE_NAME_KEY);
    return generatePartitionKey(hudiTableBasePath, hudiTableName);
  }
}
