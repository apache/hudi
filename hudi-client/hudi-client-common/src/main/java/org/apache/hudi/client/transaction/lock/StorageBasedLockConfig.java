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

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.TypedProperties;

import org.apache.hadoop.fs.Path;

import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.config.HoodieCommonConfig.BASE_PATH;
import static org.apache.hudi.client.transaction.lock.StorageBasedLockProvider.DEFAULT_TABLE_LOCK_FILE_NAME;
import static org.apache.hudi.common.table.HoodieTableMetaClient.LOCKS_FOLDER_NAME;

public class StorageBasedLockConfig extends HoodieConfig {
  private static final String SINCE_VERSION_1_0_2 = "1.0.2";
  private static final String STORAGE_BASED_LOCK_PROPERTY_PREFIX = LockConfiguration.LOCK_PREFIX
      + "storage";
  public static final ConfigProperty<String> LOCK_INTERNAL_STORAGE_LOCATION = ConfigProperty
      .key(STORAGE_BASED_LOCK_PROPERTY_PREFIX + "locks_location")
      .defaultValue("")
      .markAdvanced()
      .sinceVersion(SINCE_VERSION_1_0_2)
      .withDocumentation(
          "For storage-based lock provider, the optional URI where lock files are written. "
          + "For example, if `/lock/location` is specified, `/lock/location/<lock_file_name>` is used as the lock file,"
              + "where the lock file name `lock_file_name` is determined based on the table's base path. "
              + "Must be the same filesystem as the table path and should support conditional writes. "
              + "By default, writes to " + LOCKS_FOLDER_NAME + Path.SEPARATOR
              + DEFAULT_TABLE_LOCK_FILE_NAME + ".json under the table base path.");

  public static final ConfigProperty<Long> LOCK_VALIDITY_TIMEOUT = ConfigProperty
      .key(STORAGE_BASED_LOCK_PROPERTY_PREFIX + "validity.timeout")
      .defaultValue(TimeUnit.MINUTES.toSeconds(5))
      .markAdvanced()
      .sinceVersion(SINCE_VERSION_1_0_2)
      .withDocumentation(
          "For storage-based lock provider, the amount of time in seconds each new lock is valid for. "
              + "The lock provider will attempt to renew its lock until it successfully extends the lock lease period "
              + "or the validity timeout is reached.");

  public static final ConfigProperty<Long> HEARTBEAT_POLL = ConfigProperty
      .key(STORAGE_BASED_LOCK_PROPERTY_PREFIX + "heartbeat.poll")
      .defaultValue(TimeUnit.SECONDS.toSeconds(30))
      .markAdvanced()
      .sinceVersion(SINCE_VERSION_1_0_2)
      .withDocumentation(
          "For storage-based conditional write lock provider, the amount of time in seconds to wait before renewing the lock."
                  + "Defaults to 30 seconds.");

  public long getLockValidityTimeout() {
    return getLong(LOCK_VALIDITY_TIMEOUT);
  }

  public long getHeartbeatPoll() {
    return getLong(HEARTBEAT_POLL);
  }

  public String getHudiTableBasePath() {
    return getString(BASE_PATH);
  }

  public String getLocksLocation() {
    return getString(LOCK_INTERNAL_STORAGE_LOCATION);
  }

  public static class Builder {
    private final StorageBasedLockConfig lockConfig = new StorageBasedLockConfig();

    public StorageBasedLockConfig build() {
      lockConfig.setDefaults(StorageBasedLockConfig.class.getName());
      return lockConfig;
    }

    public StorageBasedLockConfig.Builder fromProperties(TypedProperties props) {
      lockConfig.getProps().putAll(props);
      checkRequiredProps();
      return this;
    }

    private void checkRequiredProps() {
      String notExistsMsg = " does not exist!";
      if (!lockConfig.contains(BASE_PATH)) {
        throw new IllegalArgumentException(BASE_PATH.key() + notExistsMsg);
      }
      if (lockConfig.getStringOrDefault(LOCK_INTERNAL_STORAGE_LOCATION)
          .startsWith(lockConfig.getHudiTableBasePath())) {
        throw new IllegalArgumentException(
            LOCK_INTERNAL_STORAGE_LOCATION.key() + " cannot start with the hudi table base path.");
      }
      if (lockConfig.getLongOrDefault(LOCK_VALIDITY_TIMEOUT) < lockConfig.getLongOrDefault(HEARTBEAT_POLL)
          * 3) {
        throw new IllegalArgumentException(
            LOCK_VALIDITY_TIMEOUT.key() + " should be more than triple " + HEARTBEAT_POLL.key());
      }
      if (lockConfig.getLongOrDefault(LOCK_VALIDITY_TIMEOUT) < 5) {
        throw new IllegalArgumentException(
            LOCK_VALIDITY_TIMEOUT.key() + " should be greater than or equal to 5 seconds.");
      }
      if (lockConfig.getLongOrDefault(HEARTBEAT_POLL) < 1) {
        throw new IllegalArgumentException(
            HEARTBEAT_POLL.key() + " should be greater than or equal to 1 second.");
      }
    }
  }
}
