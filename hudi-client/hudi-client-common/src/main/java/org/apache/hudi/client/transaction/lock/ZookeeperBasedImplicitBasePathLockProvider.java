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

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.lock.LockProvider;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.hash.HashID;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.common.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import static org.apache.hudi.common.fs.FSUtils.s3aToS3;

/**
 * A zookeeper based lock. This {@link LockProvider} implementation allows to lock table operations
 * using zookeeper. Users need to have a Zookeeper cluster deployed to be able to use this lock.
 *
 * This class derives the zookeeper base path from the hudi table base path (hoodie.base.path) and
 * table name (hoodie.table.name), with lock key set to a hard-coded value.
 */
@NotThreadSafe
public class ZookeeperBasedImplicitBasePathLockProvider extends BaseZookeeperBasedLockProvider {
  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperBasedImplicitBasePathLockProvider.class);

  public static final String LOCK_KEY = "lock_key";
  private final String hudiTableBasePath;

  public static String getLockBasePath(String hudiTableBasePath) {
    // Ensure consistent format for S3 URI.
    String lockBasePath = "/tmp/" + HashID.generateXXHashAsString(s3aToS3(hudiTableBasePath), HashID.Size.BITS_64);
    LOG.info("The Zookeeper lock key for the base path {} is {}", hudiTableBasePath, lockBasePath);
    return lockBasePath;
  }

  public ZookeeperBasedImplicitBasePathLockProvider(final LockConfiguration lockConfiguration, final StorageConfiguration<?> conf) {
    super(lockConfiguration, conf);
    hudiTableBasePath = s3aToS3(lockConfiguration.getConfig().getString(HoodieCommonConfig.BASE_PATH.key()));
  }

  @Override
  protected String getZkBasePath(LockConfiguration lockConfiguration) {
    String hudiTableBasePath = lockConfiguration.getConfig().getString(HoodieCommonConfig.BASE_PATH.key());
    ValidationUtils.checkArgument(hudiTableBasePath != null);
    return getLockBasePath(hudiTableBasePath);
  }

  @Override
  protected String getLockKey(LockConfiguration lockConfiguration) {
    return LOCK_KEY;
  }

  @Override
  protected String generateLogSuffixString() {
    return StringUtils.join("ZkBasePath = ", zkBasePath,
        ", lock key = ", lockKey, ", hudi table base path = ", hudiTableBasePath);
  }
}
