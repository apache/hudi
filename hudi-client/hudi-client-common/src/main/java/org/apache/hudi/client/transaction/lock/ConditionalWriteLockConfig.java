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
import static org.apache.hudi.client.transaction.lock.ConditionalWriteLockProvider.DEFAULT_TABLE_LOCK_FILE_NAME;
import static org.apache.hudi.common.table.HoodieTableMetaClient.LOCKS_FOLDER_NAME;

public class ConditionalWriteLockConfig extends HoodieConfig {
  private static final String SINCE_VERSION_1_0_2 = "1.0.2";
  private static final String CONDITIONAL_WRITE_LOCK_PROPERTY_PREFIX = LockConfiguration.LOCK_PREFIX
      + "conditional_write.";
  public static final ConfigProperty<String> LOCK_INTERNAL_STORAGE_LOCATION = ConfigProperty
      .key(CONDITIONAL_WRITE_LOCK_PROPERTY_PREFIX + "locks_location")
      .defaultValue("")
      .markAdvanced()
      .sinceVersion(SINCE_VERSION_1_0_2)
      .withDocumentation(
          "For conditional write based lock provider, the optional URI where lock files are written. "
              + "Must be the same filesystem as the table path and should conditional writes. "
              + "By default, writes to " + LOCKS_FOLDER_NAME + Path.SEPARATOR
              + DEFAULT_TABLE_LOCK_FILE_NAME + ".json under the table base path.");

  public static final ConfigProperty<Long> LOCK_VALIDITY_TIMEOUT_MS = ConfigProperty
      .key(CONDITIONAL_WRITE_LOCK_PROPERTY_PREFIX + "lock_validity_timeout_ms")
      .defaultValue(TimeUnit.MINUTES.toMillis(5))
      .markAdvanced()
      .sinceVersion(SINCE_VERSION_1_0_2)
      .withDocumentation(
          "For storage based conditional write lock provider, the amount of time each new lock is valid for."
              + "The lock provider will attempt to renew its lock until it successful extends the lock lease period"
              + "or the validity timeout is reached.");

  public static final ConfigProperty<Long> HEARTBEAT_POLL_MS = ConfigProperty
      .key(CONDITIONAL_WRITE_LOCK_PROPERTY_PREFIX + "heartbeat_poll_ms")
      .defaultValue(TimeUnit.SECONDS.toMillis(30))
      .markAdvanced()
      .sinceVersion(SINCE_VERSION_1_0_2)
      .withDocumentation(
          "For storage based conditional write lock provider, the amount of time to wait before renewing the lock. Defaults to 30 seconds.");

  public long getLockValidityTimeoutMs() {
    return getLong(LOCK_VALIDITY_TIMEOUT_MS);
  }

  public long getHeartbeatPollMs() {
    return getLong(HEARTBEAT_POLL_MS);
  }

  public String getHudiTableBasePath() {
    // Required!!
    return getString(BASE_PATH.key());
  }

  public String getLocksLocation() {
    return getString(LOCK_INTERNAL_STORAGE_LOCATION);
  }

  public static class Builder {
    private final ConditionalWriteLockConfig lockConfig = new ConditionalWriteLockConfig();

    public ConditionalWriteLockConfig build() {
      lockConfig.setDefaults(ConditionalWriteLockConfig.class.getName());
      return lockConfig;
    }

    public ConditionalWriteLockConfig.Builder fromProperties(TypedProperties props) {
      lockConfig.getProps().putAll(props);
      checkRequiredProps();
      return this;
    }

    private void checkRequiredProps() {
      String notExistsMsg = " does not exist!";
      if (Boolean.FALSE.equals(lockConfig.contains(BASE_PATH.key()))) {
        throw new IllegalArgumentException(BASE_PATH.key() + notExistsMsg);
      }
      if (lockConfig.getStringOrDefault(LOCK_INTERNAL_STORAGE_LOCATION)
          .startsWith(lockConfig.getHudiTableBasePath())) {
        throw new IllegalArgumentException(
            LOCK_INTERNAL_STORAGE_LOCATION.key() + " cannot start with the hudi table base path.");
      }
      if (lockConfig.getLongOrDefault(LOCK_VALIDITY_TIMEOUT_MS) < lockConfig.getLongOrDefault(HEARTBEAT_POLL_MS)
          * 3) {
        throw new IllegalArgumentException(
            LOCK_VALIDITY_TIMEOUT_MS.key() + " should be more than triple " + HEARTBEAT_POLL_MS.key());
      }
      if (lockConfig.getLongOrDefault(LOCK_VALIDITY_TIMEOUT_MS) < 5000) {
        throw new IllegalArgumentException(
            LOCK_VALIDITY_TIMEOUT_MS.key() + " should be greater than or equal to 5 seconds.");
      }
      if (lockConfig.getLongOrDefault(HEARTBEAT_POLL_MS) < 1000) {
        throw new IllegalArgumentException(
            HEARTBEAT_POLL_MS.key() + " should be greater than or equal to 1 second.");
      }
    }
  }
}