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
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.TypedProperties;

import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.config.HoodieCommonConfig.BASE_PATH;

public class StorageBasedLockConfig extends HoodieConfig {
  private static final String SINCE_VERSION_1_0_2 = "1.0.2";
  private static final String STORAGE_BASED_LOCK_PROPERTY_PREFIX = LockConfiguration.LOCK_PREFIX
      + "storage.";

  public static final ConfigProperty<Long> VALIDITY_TIMEOUT_SECONDS = ConfigProperty
      .key(STORAGE_BASED_LOCK_PROPERTY_PREFIX + "validity.timeout.secs")
      .defaultValue(TimeUnit.MINUTES.toSeconds(5))
      .markAdvanced()
      .sinceVersion(SINCE_VERSION_1_0_2)
      .withDocumentation(
          "For storage-based lock provider, the amount of time in seconds each new lock is valid for. "
              + "The lock provider will attempt to renew its lock until it successfully extends the lock lease period "
              + "or the validity timeout is reached.");

  public static final ConfigProperty<Long> HEARTBEAT_POLL_SECONDS = ConfigProperty
      .key(STORAGE_BASED_LOCK_PROPERTY_PREFIX + "heartbeat.poll.secs")
      .defaultValue(30L)
      .markAdvanced()
      .sinceVersion(SINCE_VERSION_1_0_2)
      .withDocumentation(
          "For storage-based lock provider, the amount of time in seconds to wait before renewing the lock. "
              + "Defaults to 30 seconds.");

  public long getValiditySeconds() {
    return getLong(VALIDITY_TIMEOUT_SECONDS);
  }

  public long getHeartbeatPollSeconds() {
    return getLong(HEARTBEAT_POLL_SECONDS);
  }

  public String getHudiTableBasePath() {
    return getString(BASE_PATH);
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
      if (lockConfig.getLongOrDefault(VALIDITY_TIMEOUT_SECONDS) < lockConfig.getLongOrDefault(HEARTBEAT_POLL_SECONDS)
          * 10) {
        throw new IllegalArgumentException(
            VALIDITY_TIMEOUT_SECONDS.key() + " should be greater than or equal to 10x " + HEARTBEAT_POLL_SECONDS.key());
      }
      if (lockConfig.getLongOrDefault(VALIDITY_TIMEOUT_SECONDS) < 10) {
        throw new IllegalArgumentException(
            VALIDITY_TIMEOUT_SECONDS.key() + " should be greater than or equal to 10 seconds.");
      }
      if (lockConfig.getLongOrDefault(HEARTBEAT_POLL_SECONDS) < 1) {
        throw new IllegalArgumentException(
            HEARTBEAT_POLL_SECONDS.key() + " should be greater than or equal to 1 second.");
      }
    }
  }
}
