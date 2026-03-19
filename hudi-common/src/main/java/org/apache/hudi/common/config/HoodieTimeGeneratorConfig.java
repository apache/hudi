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

package org.apache.hudi.common.config;

import org.apache.hudi.common.table.timeline.TimeGeneratorType;
import org.apache.hudi.common.util.Option;

import java.util.Properties;

import static org.apache.hudi.common.config.HoodieCommonConfig.BASE_PATH;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_PREFIX;

/**
 * Configuration for Hoodie time generation.
 */
public class HoodieTimeGeneratorConfig extends HoodieConfig {

  public static final String LOCK_PROVIDER_KEY = LOCK_PREFIX + "provider";
  private static final String DEFAULT_LOCK_PROVIDER = "org.apache.hudi.client.transaction.lock.InProcessLockProvider";

  public static final ConfigProperty<String> TIME_GENERATOR_TYPE = ConfigProperty
      .key("hoodie.time.generator.type")
      .defaultValue(TimeGeneratorType.WAIT_TO_ADJUST_SKEW.name())
      .withValidValues(TimeGeneratorType.WAIT_TO_ADJUST_SKEW.name())
      .sinceVersion("1.0.0")
      .markAdvanced()
      .withDocumentation("Time generator type, which is used to generate globally monotonically increasing timestamp");

  public static final ConfigProperty<Long> MAX_EXPECTED_CLOCK_SKEW_MS = ConfigProperty
      .key("hoodie.time.generator.max_expected_clock_skew_ms")
      .defaultValue(200L)
      .withInferFunction(cfg -> {
        if (DEFAULT_LOCK_PROVIDER.equals(cfg.getString(LOCK_PROVIDER_KEY))) {
          return Option.of(1L);
        }
        return Option.empty();
      })
      .sinceVersion("1.0.0")
      .markAdvanced()
      .withDocumentation("The max expected clock skew time in ms between two processes generating time. Used by "
          + TimeGeneratorType.WAIT_TO_ADJUST_SKEW.name() + " time generator to implement TrueTime semantics.");

  private HoodieTimeGeneratorConfig() {
    super();
  }

  public TimeGeneratorType getTimeGeneratorType() {
    return TimeGeneratorType.valueOf(getString(TIME_GENERATOR_TYPE));
  }

  public long getMaxExpectedClockSkewMs() {
    return getLong(MAX_EXPECTED_CLOCK_SKEW_MS);
  }

  public String getBasePath() {
    return getString(BASE_PATH);
  }

  public static HoodieTimeGeneratorConfig.Builder newBuilder() {
    return new HoodieTimeGeneratorConfig.Builder();
  }

  /**
   * Returns the default configuration.
   */
  public static HoodieTimeGeneratorConfig defaultConfig(String tablePath) {
    return newBuilder().withPath(tablePath).build();
  }

  public static class Builder {
    private final HoodieTimeGeneratorConfig timeGeneratorConfig = new HoodieTimeGeneratorConfig();

    public HoodieTimeGeneratorConfig.Builder fromProperties(Properties props) {
      timeGeneratorConfig.getProps().putAll(props);
      return this;
    }

    public Builder withDefaultLockProvider(boolean useDefaultLockProvider) {
      if (useDefaultLockProvider) {
        timeGeneratorConfig.getProps().setPropertyIfNonNull(LOCK_PROVIDER_KEY, DEFAULT_LOCK_PROVIDER);
      }
      return this;
    }

    public Builder withTimeGeneratorType(TimeGeneratorType type) {
      timeGeneratorConfig.setValue(TIME_GENERATOR_TYPE, type.name());
      return this;
    }

    public Builder withMaxExpectedClockSkewMs(long skewMs) {
      timeGeneratorConfig.setValue(MAX_EXPECTED_CLOCK_SKEW_MS, String.valueOf(skewMs));
      return this;
    }

    public Builder withPath(String basePath) {
      timeGeneratorConfig.setValue(BASE_PATH, basePath);
      return this;
    }

    public HoodieTimeGeneratorConfig build() {
      if (!timeGeneratorConfig.contains(LOCK_PROVIDER_KEY)) {
        timeGeneratorConfig.setValue(LOCK_PROVIDER_KEY, DEFAULT_LOCK_PROVIDER);
      }
      timeGeneratorConfig.setDefaults(HoodieTimeGeneratorConfig.class.getName());
      return timeGeneratorConfig;
    }
  }

  public LockConfiguration getLockConfiguration() {
    return new LockConfiguration(props);
  }
}
