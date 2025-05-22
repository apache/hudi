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

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * The consistency guard relevant config options.
 */
@ConfigClassProperty(name = "Consistency Guard Configurations",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "The consistency guard related config options, to help talk to eventually consistent object storage."
        + "(Tip: S3 is NOT eventually consistent anymore!)")
public class ConsistencyGuardConfig extends HoodieConfig {

  public static final ConfigProperty<String> ENABLE = ConfigProperty
      .key("hoodie.consistency.check.enabled")
      .defaultValue("false")
      .markAdvanced()
      .sinceVersion("0.5.0")
      .deprecatedAfter("0.7.0")
      .withDocumentation("Enabled to handle S3 eventual consistency issue. This property is no longer required "
          + "since S3 is now strongly consistent. Will be removed in the future releases.");

  public static final ConfigProperty<Long> INITIAL_CHECK_INTERVAL_MS = ConfigProperty
      .key("hoodie.consistency.check.initial_interval_ms")
      .defaultValue(400L)
      .markAdvanced()
      .sinceVersion("0.5.0")
      .deprecatedAfter("0.7.0")
      .withDocumentation("Amount of time (in ms) to wait, before checking for consistency after an operation on storage.");

  public static final ConfigProperty<Long> MAX_CHECK_INTERVAL_MS = ConfigProperty
      .key("hoodie.consistency.check.max_interval_ms")
      .defaultValue(20000L)
      .markAdvanced()
      .sinceVersion("0.5.0")
      .deprecatedAfter("0.7.0")
      .withDocumentation("Maximum amount of time (in ms), to wait for consistency checking.");

  // maximum number of checks, for consistency of written data. Will wait upto 140 Secs
  public static final ConfigProperty<Integer> MAX_CHECKS = ConfigProperty
      .key("hoodie.consistency.check.max_checks")
      .defaultValue(6)
      .markAdvanced()
      .sinceVersion("0.5.0")
      .deprecatedAfter("0.7.0")
      .withDocumentation("Maximum number of consistency checks to perform, with exponential backoff.");

  // sleep time for OptimisticConsistencyGuard
  public static final ConfigProperty<Long> OPTIMISTIC_CONSISTENCY_GUARD_SLEEP_TIME_MS = ConfigProperty
      .key("hoodie.optimistic.consistency.guard.sleep_time_ms")
      .defaultValue(500L)
      .markAdvanced()
      .sinceVersion("0.6.0")
      .withDocumentation("Amount of time (in ms), to wait after which we assume storage is consistent.");

  // config to enable OptimisticConsistencyGuard in finalizeWrite instead of FailSafeConsistencyGuard
  public static final ConfigProperty<Boolean> OPTIMISTIC_CONSISTENCY_GUARD_ENABLE = ConfigProperty
      .key("_hoodie.optimistic.consistency.guard.enable")
      .defaultValue(false)
      .markAdvanced()
      .sinceVersion("0.6.0")
      .withDocumentation("Enable consistency guard, which optimistically assumes consistency is achieved after a certain time period.");

  private ConsistencyGuardConfig() {
    super();
  }

  public static ConsistencyGuardConfig.Builder newBuilder() {
    return new Builder();
  }

  public boolean isConsistencyCheckEnabled() {
    return getBoolean(ENABLE);
  }

  public int getMaxConsistencyChecks() {
    return getInt(MAX_CHECKS);
  }

  public int getInitialConsistencyCheckIntervalMs() {
    return getInt(INITIAL_CHECK_INTERVAL_MS);
  }

  public int getMaxConsistencyCheckIntervalMs() {
    return getInt(MAX_CHECK_INTERVAL_MS);
  }

  public long getOptimisticConsistencyGuardSleepTimeMs() {
    return getLong(OPTIMISTIC_CONSISTENCY_GUARD_SLEEP_TIME_MS);
  }

  public boolean shouldEnableOptimisticConsistencyGuard() {
    return getBoolean(OPTIMISTIC_CONSISTENCY_GUARD_ENABLE);
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
      consistencyGuardConfig.setValue(ENABLE, String.valueOf(enabled));
      return this;
    }

    public Builder withInitialConsistencyCheckIntervalMs(int initialIntervalMs) {
      consistencyGuardConfig.setValue(INITIAL_CHECK_INTERVAL_MS, String.valueOf(initialIntervalMs));
      return this;
    }

    public Builder withMaxConsistencyCheckIntervalMs(int maxIntervalMs) {
      consistencyGuardConfig.setValue(MAX_CHECK_INTERVAL_MS, String.valueOf(maxIntervalMs));
      return this;
    }

    public Builder withMaxConsistencyChecks(int maxConsistencyChecks) {
      consistencyGuardConfig.setValue(MAX_CHECKS, String.valueOf(maxConsistencyChecks));
      return this;
    }

    public Builder withOptimisticConsistencyGuardSleepTimeMs(long sleepTimeMs) {
      consistencyGuardConfig.setValue(OPTIMISTIC_CONSISTENCY_GUARD_SLEEP_TIME_MS, String.valueOf(sleepTimeMs));
      return this;
    }

    public Builder withEnableOptimisticConsistencyGuard(boolean enableOptimisticConsistencyGuard) {
      consistencyGuardConfig.setValue(OPTIMISTIC_CONSISTENCY_GUARD_ENABLE, String.valueOf(enableOptimisticConsistencyGuard));
      return this;
    }

    public ConsistencyGuardConfig build() {
      consistencyGuardConfig.setDefaults(ConsistencyGuardConfig.class.getName());
      return consistencyGuardConfig;
    }
  }

  /**
   * @deprecated use {@link #ENABLE} and its methods.
   */
  @Deprecated
  private static final String CONSISTENCY_CHECK_ENABLED_PROP = ENABLE.key();
  /**
   * @deprecated use {@link #ENABLE} and its methods.
   */
  @Deprecated
  private static final String DEFAULT_CONSISTENCY_CHECK_ENABLED = ENABLE.defaultValue();
  /**
   * @deprecated use {@link #INITIAL_CHECK_INTERVAL_MS} and its methods.
   */
  @Deprecated
  private static final String INITIAL_CONSISTENCY_CHECK_INTERVAL_MS_PROP = INITIAL_CHECK_INTERVAL_MS.key();
  /**
   * @deprecated use {@link #INITIAL_CHECK_INTERVAL_MS} and its methods.
   */
  @Deprecated
  private static final long DEFAULT_INITIAL_CONSISTENCY_CHECK_INTERVAL_MS = INITIAL_CHECK_INTERVAL_MS.defaultValue();
  /**
   * @deprecated use {@link #MAX_CHECK_INTERVAL_MS} and its methods.
   */
  @Deprecated
  private static final String MAX_CONSISTENCY_CHECK_INTERVAL_MS_PROP = MAX_CHECK_INTERVAL_MS.key();
  /**
   * @deprecated use {@link #MAX_CHECK_INTERVAL_MS} and its methods.
   */
  @Deprecated
  private static final long DEFAULT_MAX_CONSISTENCY_CHECK_INTERVAL_MS = MAX_CHECK_INTERVAL_MS.defaultValue();
  /**
   * @deprecated use {@link #MAX_CHECKS} and its methods.
   */
  @Deprecated
  private static final String MAX_CONSISTENCY_CHECKS_PROP = MAX_CHECKS.key();
  /**
   * @deprecated use {@link #MAX_CHECKS} and its methods.
   */
  @Deprecated
  private static final int DEFAULT_MAX_CONSISTENCY_CHECKS = MAX_CHECKS.defaultValue();
  /**
   * @deprecated use {@link #OPTIMISTIC_CONSISTENCY_GUARD_SLEEP_TIME_MS} and its methods.
   */
  @Deprecated
  private static final String OPTIMISTIC_CONSISTENCY_GUARD_SLEEP_TIME_MS_PROP = OPTIMISTIC_CONSISTENCY_GUARD_SLEEP_TIME_MS.key();
  /**
   * @deprecated use {@link #OPTIMISTIC_CONSISTENCY_GUARD_SLEEP_TIME_MS} and its methods.
   */
  @Deprecated
  private static final long DEFAULT_OPTIMISTIC_CONSISTENCY_GUARD_SLEEP_TIME_MS_PROP = OPTIMISTIC_CONSISTENCY_GUARD_SLEEP_TIME_MS.defaultValue();
  /**
   * @deprecated use {@link #OPTIMISTIC_CONSISTENCY_GUARD_ENABLE} and its methods.
   */
  @Deprecated
  private static final String ENABLE_OPTIMISTIC_CONSISTENCY_GUARD = OPTIMISTIC_CONSISTENCY_GUARD_ENABLE.key();
  /**
   * @deprecated use {@link #OPTIMISTIC_CONSISTENCY_GUARD_ENABLE} and its methods.
   */
  @Deprecated
  private static final boolean DEFAULT_ENABLE_OPTIMISTIC_CONSISTENCY_GUARD = OPTIMISTIC_CONSISTENCY_GUARD_ENABLE.defaultValue();
}
