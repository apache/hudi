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

package org.apache.hudi.common.fs;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * The consistency guard relevant config options.
 */
public class ConsistencyGuardConfig extends HoodieConfig {

  // time between successive attempts to ensure written data's metadata is consistent on storage
  @Deprecated
  public static final ConfigProperty<String> CONSISTENCY_CHECK_ENABLED_PROP = ConfigProperty
      .key("hoodie.consistency.check.enabled")
      .defaultValue("false")
      .sinceVersion("0.5.0")
      .withDocumentation("Enabled to handle S3 eventual consistency issue. This property is no longer required "
          + "since S3 is now strongly consistent. Will be removed in the future releases.");

  public static final ConfigProperty<Long> INITIAL_CONSISTENCY_CHECK_INTERVAL_MS_PROP = ConfigProperty
      .key("hoodie.consistency.check.initial_interval_ms")
      .defaultValue(400L)
      .sinceVersion("0.5.0")
      .withDocumentation("");

  // max interval time
  public static final ConfigProperty<Long> MAX_CONSISTENCY_CHECK_INTERVAL_MS_PROP = ConfigProperty
      .key("hoodie.consistency.check.max_interval_ms")
      .defaultValue(20000L)
      .sinceVersion("0.5.0")
      .withDocumentation("");

  // maximum number of checks, for consistency of written data. Will wait upto 140 Secs
  public static final ConfigProperty<Integer> MAX_CONSISTENCY_CHECKS_PROP = ConfigProperty
      .key("hoodie.consistency.check.max_checks")
      .defaultValue(6)
      .sinceVersion("0.5.0")
      .withDocumentation("");

  // sleep time for OptimisticConsistencyGuard
  public static final ConfigProperty<Long> OPTIMISTIC_CONSISTENCY_GUARD_SLEEP_TIME_MS_PROP = ConfigProperty
      .key("hoodie.optimistic.consistency.guard.sleep_time_ms")
      .defaultValue(500L)
      .sinceVersion("0.6.0")
      .withDocumentation("");

  // config to enable OptimisticConsistencyGuard in finalizeWrite instead of FailSafeConsistencyGuard
  public static final ConfigProperty<Boolean> ENABLE_OPTIMISTIC_CONSISTENCY_GUARD_PROP = ConfigProperty
      .key("_hoodie.optimistic.consistency.guard.enable")
      .defaultValue(true)
      .sinceVersion("0.6.0")
      .withDocumentation("");

  private ConsistencyGuardConfig() {
    super();
  }

  public static ConsistencyGuardConfig.Builder newBuilder() {
    return new Builder();
  }

  public boolean isConsistencyCheckEnabled() {
    return getBoolean(CONSISTENCY_CHECK_ENABLED_PROP);
  }

  public int getMaxConsistencyChecks() {
    return getInt(MAX_CONSISTENCY_CHECKS_PROP);
  }

  public int getInitialConsistencyCheckIntervalMs() {
    return getInt(INITIAL_CONSISTENCY_CHECK_INTERVAL_MS_PROP);
  }

  public int getMaxConsistencyCheckIntervalMs() {
    return getInt(MAX_CONSISTENCY_CHECK_INTERVAL_MS_PROP);
  }

  public long getOptimisticConsistencyGuardSleepTimeMs() {
    return getLong(OPTIMISTIC_CONSISTENCY_GUARD_SLEEP_TIME_MS_PROP);
  }

  public boolean shouldEnableOptimisticConsistencyGuard() {
    return getBoolean(ENABLE_OPTIMISTIC_CONSISTENCY_GUARD_PROP);
  }

  /**
   * The builder used to build consistency configurations.
   */
  public static class Builder {

    private final ConsistencyGuardConfig consistencyGuardConfig = new ConsistencyGuardConfig();

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        consistencyGuardConfig.getProps().load(reader);
        return this;
      }
    }

    public Builder fromProperties(Properties props) {
      this.consistencyGuardConfig.getProps().putAll(props);
      return this;
    }

    public Builder withConsistencyCheckEnabled(boolean enabled) {
      consistencyGuardConfig.setValue(CONSISTENCY_CHECK_ENABLED_PROP, String.valueOf(enabled));
      return this;
    }

    public Builder withInitialConsistencyCheckIntervalMs(int initialIntevalMs) {
      consistencyGuardConfig.setValue(INITIAL_CONSISTENCY_CHECK_INTERVAL_MS_PROP, String.valueOf(initialIntevalMs));
      return this;
    }

    public Builder withMaxConsistencyCheckIntervalMs(int maxIntervalMs) {
      consistencyGuardConfig.setValue(MAX_CONSISTENCY_CHECK_INTERVAL_MS_PROP, String.valueOf(maxIntervalMs));
      return this;
    }

    public Builder withMaxConsistencyChecks(int maxConsistencyChecks) {
      consistencyGuardConfig.setValue(MAX_CONSISTENCY_CHECKS_PROP, String.valueOf(maxConsistencyChecks));
      return this;
    }

    public Builder withOptimisticConsistencyGuardSleepTimeMs(long sleepTimeMs) {
      consistencyGuardConfig.setValue(OPTIMISTIC_CONSISTENCY_GUARD_SLEEP_TIME_MS_PROP, String.valueOf(sleepTimeMs));
      return this;
    }

    public Builder withEnableOptimisticConsistencyGuard(boolean enableOptimisticConsistencyGuard) {
      consistencyGuardConfig.setValue(ENABLE_OPTIMISTIC_CONSISTENCY_GUARD_PROP, String.valueOf(enableOptimisticConsistencyGuard));
      return this;
    }

    public ConsistencyGuardConfig build() {
      consistencyGuardConfig.setDefaults(ConsistencyGuardConfig.class.getName());
      return consistencyGuardConfig;
    }
  }
}
