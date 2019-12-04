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

package org.apache.hudi.common.util;

import org.apache.hudi.config.DefaultHoodieConfig;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * The consistency guard relevant config options.
 */
public class ConsistencyGuardConfig extends DefaultHoodieConfig {

  private static final String CONSISTENCY_CHECK_ENABLED_PROP = "hoodie.consistency.check.enabled";
  private static final String DEFAULT_CONSISTENCY_CHECK_ENABLED = "false";

  // time between successive attempts to ensure written data's metadata is consistent on storage
  private static final String INITIAL_CONSISTENCY_CHECK_INTERVAL_MS_PROP =
      "hoodie.consistency.check.initial_interval_ms";
  private static long DEFAULT_INITIAL_CONSISTENCY_CHECK_INTERVAL_MS = 2000L;

  // max interval time
  private static final String MAX_CONSISTENCY_CHECK_INTERVAL_MS_PROP = "hoodie.consistency.check.max_interval_ms";
  private static long DEFAULT_MAX_CONSISTENCY_CHECK_INTERVAL_MS = 300000L;

  // maximum number of checks, for consistency of written data. Will wait upto 256 Secs
  private static final String MAX_CONSISTENCY_CHECKS_PROP = "hoodie.consistency.check.max_checks";
  private static int DEFAULT_MAX_CONSISTENCY_CHECKS = 7;

  public ConsistencyGuardConfig(Properties props) {
    super(props);
  }

  public static ConsistencyGuardConfig.Builder newBuilder() {
    return new Builder();
  }

  public boolean isConsistencyCheckEnabled() {
    return Boolean.parseBoolean(props.getProperty(CONSISTENCY_CHECK_ENABLED_PROP));
  }

  public int getMaxConsistencyChecks() {
    return Integer.parseInt(props.getProperty(MAX_CONSISTENCY_CHECKS_PROP));
  }

  public int getInitialConsistencyCheckIntervalMs() {
    return Integer.parseInt(props.getProperty(INITIAL_CONSISTENCY_CHECK_INTERVAL_MS_PROP));
  }

  public int getMaxConsistencyCheckIntervalMs() {
    return Integer.parseInt(props.getProperty(MAX_CONSISTENCY_CHECK_INTERVAL_MS_PROP));
  }

  /**
   * The builder used to build consistency configurations.
   */
  public static class Builder {

    private final Properties props = new Properties();

    public Builder fromFile(File propertiesFile) throws IOException {
      FileReader reader = new FileReader(propertiesFile);
      try {
        props.load(reader);
        return this;
      } finally {
        reader.close();
      }
    }

    public Builder fromProperties(Properties props) {
      this.props.putAll(props);
      return this;
    }

    public Builder withConsistencyCheckEnabled(boolean enabled) {
      props.setProperty(CONSISTENCY_CHECK_ENABLED_PROP, String.valueOf(enabled));
      return this;
    }

    public Builder withInitialConsistencyCheckIntervalMs(int initialIntevalMs) {
      props.setProperty(INITIAL_CONSISTENCY_CHECK_INTERVAL_MS_PROP, String.valueOf(initialIntevalMs));
      return this;
    }

    public Builder withMaxConsistencyCheckIntervalMs(int maxIntervalMs) {
      props.setProperty(MAX_CONSISTENCY_CHECK_INTERVAL_MS_PROP, String.valueOf(maxIntervalMs));
      return this;
    }

    public Builder withMaxConsistencyChecks(int maxConsistencyChecks) {
      props.setProperty(MAX_CONSISTENCY_CHECKS_PROP, String.valueOf(maxConsistencyChecks));
      return this;
    }

    public ConsistencyGuardConfig build() {
      setDefaultOnCondition(props, !props.containsKey(CONSISTENCY_CHECK_ENABLED_PROP), CONSISTENCY_CHECK_ENABLED_PROP,
          DEFAULT_CONSISTENCY_CHECK_ENABLED);
      setDefaultOnCondition(props, !props.containsKey(INITIAL_CONSISTENCY_CHECK_INTERVAL_MS_PROP),
          INITIAL_CONSISTENCY_CHECK_INTERVAL_MS_PROP, String.valueOf(DEFAULT_INITIAL_CONSISTENCY_CHECK_INTERVAL_MS));
      setDefaultOnCondition(props, !props.containsKey(MAX_CONSISTENCY_CHECK_INTERVAL_MS_PROP),
          MAX_CONSISTENCY_CHECK_INTERVAL_MS_PROP, String.valueOf(DEFAULT_MAX_CONSISTENCY_CHECK_INTERVAL_MS));
      setDefaultOnCondition(props, !props.containsKey(MAX_CONSISTENCY_CHECKS_PROP), MAX_CONSISTENCY_CHECKS_PROP,
          String.valueOf(DEFAULT_MAX_CONSISTENCY_CHECKS));

      return new ConsistencyGuardConfig(props);
    }
  }
}
