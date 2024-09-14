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
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.hash.HashID;
import org.apache.hudi.storage.StorageConfiguration;

import javax.annotation.concurrent.NotThreadSafe;

import static org.apache.hudi.aws.utils.S3Utils.s3aToS3;
import static org.apache.hudi.common.util.StringUtils.concatenateWithThreshold;

/**
 * A zookeeper based lock. This {@link LockProvider} implementation allows to lock table operations
 * using zookeeper. Users need to have a Zookeeper cluster deployed to be able to use this lock.
 *
 * This class derives the zookeeper base path from the hudi table base path (hoodie.base.path) and
 * table name (hoodie.table.name), with lock key set to a hard-coded value.
 */
@NotThreadSafe
public class ZookeeperBasedImplicitBasePathLockProvider extends BaseZookeeperBasedLockProvider {

  public static final String LOCK_KEY = "lock_key";

  public static String getLockBasePath(String hudiTableBasePath, String hudiTableName) {
    // Ensure consistent format for S3 URI.
    String hashPart = '-' + HashID.generateXXHashAsString(s3aToS3(hudiTableBasePath), HashID.Size.BITS_64);
    String folderName = concatenateWithThreshold(hudiTableName, hashPart, MAX_ZK_BASE_PATH_NUM_BYTES);
    return "/tmp/" + folderName;
  }

  public ZookeeperBasedImplicitBasePathLockProvider(final LockConfiguration lockConfiguration, final StorageConfiguration<?> conf) {
    super(lockConfiguration, conf);
  }

  @Override
  protected String getZkBasePath(LockConfiguration lockConfiguration) {
    String hudiTableBasePath = ConfigUtils.getStringWithAltKeys(lockConfiguration.getConfig(), HoodieCommonConfig.BASE_PATH);
    String hudiTableName = ConfigUtils.getStringWithAltKeys(lockConfiguration.getConfig(), HoodieTableConfig.NAME);
    ValidationUtils.checkArgument(hudiTableBasePath != null);
    ValidationUtils.checkArgument(hudiTableName != null);
    return getLockBasePath(hudiTableBasePath, hudiTableName);
  }

  @Override
  protected String getLockKey(LockConfiguration lockConfiguration) {
    return LOCK_KEY;
  }
}
