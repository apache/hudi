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
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.hash.HashID;
import org.apache.hudi.storage.StorageConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import javax.annotation.concurrent.NotThreadSafe;

import static org.apache.hudi.common.fs.FSUtils.normalizeBasePathForLocking;
import static org.apache.hudi.common.fs.FSUtils.s3aToS3;

/**
 * A DynamoDB based lock.
 * It implicitly derives the partition key from the hudi table name and hudi table base path
 * available in the lock configuration.
 */
@NotThreadSafe
public class DynamoDBBasedImplicitPartitionKeyLockProvider extends DynamoDBBasedLockProviderBase {
  protected static final Logger LOG = LoggerFactory.getLogger(DynamoDBBasedImplicitPartitionKeyLockProvider.class);

  private final String normalizedHudiTableBasePath;

  public DynamoDBBasedImplicitPartitionKeyLockProvider(final LockConfiguration lockConfiguration, final StorageConfiguration<?> conf) {
    this(lockConfiguration, conf, null);
  }

  public DynamoDBBasedImplicitPartitionKeyLockProvider(
      final LockConfiguration lockConfiguration, final StorageConfiguration<?> conf, DynamoDbClient dynamoDB) {
    super(lockConfiguration, conf, dynamoDB);
    normalizedHudiTableBasePath = normalizeBasePathForLocking(
        lockConfiguration.getConfig().getString(HoodieCommonConfig.BASE_PATH.key()));
  }

  /**
   * Compute the DynamoDB partition key for a given Hudi table base path. Exposed as a static
   * helper so that the formula is testable without standing up a DynamoDB client.
   *
   * <p>Accepts a raw basePath - normalization is applied here. {@code normalizeBasePathForLocking}
   * is idempotent, so passing an already-normalized path is safe. Note that the instance field
   * {@code normalizedHudiTableBasePath} cannot be used here: the parent constructor invokes this
   * through {@code getDynamoDBPartitionKey} before the subclass has a chance to assign the field.
   * That ordering is also why the path is normalized twice per construction - once here and once
   * for the field; it is a pure function over a short string, run once per provider.
   *
   * <p>ROLLOUT: for a table whose configured {@code hoodie.base.path} ends in '/' or carries
   * surrounding whitespace, this returns a different DynamoDB partition key than releases before
   * HUDI's normalization fix. Such a table must have all of its writers upgraded together - a
   * rolling upgrade would leave old and new writers on two different lock rows for the same
   * table, losing mutual exclusion. Base paths without a trailing slash are unaffected.
   */
  @VisibleForTesting
  public static String derivePartitionKey(String hudiTableBasePath) {
    String normalized = normalizeBasePathForLocking(hudiTableBasePath);
    String partitionKey = HashID.generateXXHashAsString(normalized, HashID.Size.BITS_64);
    LOG.info("The DynamoDB partition key of the lock provider for the base path {} (normalized: {}) is {}",
        hudiTableBasePath, normalized, partitionKey);
    // Releases before this change hashed s3aToS3(basePath) directly. When the canonical form
    // differs, this writer has moved to a new lock row and cannot exclude a writer still running
    // the old code, so say so loudly rather than leaving it to be inferred from the INFO line.
    String legacyForm = s3aToS3(hudiTableBasePath);
    if (!legacyForm.equals(normalized)) {
      LOG.warn("DynamoDB partition key for base path {} moved from {} to {}. Every writer of this "
              + "table must be upgraded together; a writer still on the previous release locks on "
              + "the old partition key and will NOT be excluded by this one.",
          hudiTableBasePath,
          HashID.generateXXHashAsString(legacyForm, HashID.Size.BITS_64),
          partitionKey);
    }
    return partitionKey;
  }

  @Override
  public String getDynamoDBPartitionKey(LockConfiguration lockConfiguration) {
    return derivePartitionKey(lockConfiguration.getConfig().getString(HoodieCommonConfig.BASE_PATH.key()));
  }

  @Override
  protected String generateLogSuffixString() {
    return StringUtils.join("DynamoDb table = ", tableName,
        ", partition key = ", dynamoDBPartitionKey,
        ", hudi table base path = ", normalizedHudiTableBasePath);
  }
}
