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

package org.apache.hudi.config.metrics;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import static org.apache.hudi.config.metrics.HoodieMetricsConfig.METRIC_PREFIX;

/**
 * Configs for Graphite reporter type.
 * <p>
 * {@link org.apache.hudi.metrics.MetricsReporterType#GRAPHITE}
 */
@ConfigClassProperty(name = "Metrics Configurations for Graphite",
    groupName = ConfigGroups.Names.METRICS,
    description = "Enables reporting on Hudi metrics using Graphite. "
        + " Hudi publishes metrics on every commit, clean, rollback etc.")
public class HoodieMetricsGraphiteConfig extends HoodieConfig {

  public static final String GRAPHITE_PREFIX = METRIC_PREFIX + ".graphite";

  public static final ConfigProperty<String> GRAPHITE_SERVER_HOST_NAME = ConfigProperty
      .key(GRAPHITE_PREFIX + ".host")
      .defaultValue("localhost")
      .sinceVersion("0.5.0")
      .withDocumentation("Graphite host to connect to.");

  public static final ConfigProperty<Integer> GRAPHITE_SERVER_PORT_NUM = ConfigProperty
      .key(GRAPHITE_PREFIX + ".port")
      .defaultValue(4756)
      .sinceVersion("0.5.0")
      .withDocumentation("Graphite port to connect to.");

  public static final ConfigProperty<String> GRAPHITE_METRIC_PREFIX_VALUE = ConfigProperty
      .key(GRAPHITE_PREFIX + ".metric.prefix")
      .noDefaultValue()
      .sinceVersion("0.5.1")
      .withDocumentation("Standard prefix applied to all metrics. This helps to add datacenter, environment information for e.g");

  public static final ConfigProperty<Integer> GRAPHITE_REPORT_PERIOD_IN_SECONDS = ConfigProperty
      .key(GRAPHITE_PREFIX + ".report.period.seconds")
      .defaultValue(30)
      .sinceVersion("0.10.0")
      .withDocumentation("Graphite reporting period in seconds. Default to 30.");

  /**
   * @deprecated Use {@link #GRAPHITE_SERVER_HOST_NAME} and its methods instead
   */
  @Deprecated
  public static final String GRAPHITE_SERVER_HOST = GRAPHITE_SERVER_HOST_NAME.key();
  /**
   * @deprecated Use {@link #GRAPHITE_SERVER_HOST_NAME} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_GRAPHITE_SERVER_HOST = GRAPHITE_SERVER_HOST_NAME.defaultValue();
  /**
   * @deprecated Use {@link #GRAPHITE_SERVER_PORT_NUM} and its methods instead
   */
  @Deprecated
  public static final String GRAPHITE_SERVER_PORT = GRAPHITE_SERVER_PORT_NUM.key();
  /**
   * @deprecated Use {@link #GRAPHITE_SERVER_PORT_NUM} and its methods instead
   */
  @Deprecated
  public static final int DEFAULT_GRAPHITE_SERVER_PORT = GRAPHITE_SERVER_PORT_NUM.defaultValue();
  /**
   * @deprecated Use {@link #GRAPHITE_METRIC_PREFIX_VALUE} and its methods instead
   */
  @Deprecated
  public static final String GRAPHITE_METRIC_PREFIX = GRAPHITE_METRIC_PREFIX_VALUE.key();

  private HoodieMetricsGraphiteConfig() {
    super();
  }

  public static HoodieMetricsGraphiteConfig.Builder newBuilder() {
    return new HoodieMetricsGraphiteConfig.Builder();
  }

  public static class Builder {

    private final HoodieMetricsGraphiteConfig hoodieMetricsGraphiteConfig = new HoodieMetricsGraphiteConfig();

    public HoodieMetricsGraphiteConfig.Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.hoodieMetricsGraphiteConfig.getProps().load(reader);
        return this;
      }
    }

    public HoodieMetricsGraphiteConfig.Builder fromProperties(Properties props) {
      this.hoodieMetricsGraphiteConfig.getProps().putAll(props);
      return this;
    }

    public HoodieMetricsGraphiteConfig.Builder toGraphiteHost(String host) {
      hoodieMetricsGraphiteConfig.setValue(GRAPHITE_SERVER_HOST_NAME, host);
      return this;
    }

    public HoodieMetricsGraphiteConfig.Builder onGraphitePort(int port) {
      hoodieMetricsGraphiteConfig.setValue(GRAPHITE_SERVER_PORT_NUM, String.valueOf(port));
      return this;
    }

    public HoodieMetricsGraphiteConfig.Builder usePrefix(String prefix) {
      hoodieMetricsGraphiteConfig.setValue(GRAPHITE_METRIC_PREFIX_VALUE, prefix);
      return this;
    }

    public HoodieMetricsGraphiteConfig.Builder periodSeconds(String periodSeconds) {
      hoodieMetricsGraphiteConfig.setValue(GRAPHITE_REPORT_PERIOD_IN_SECONDS, periodSeconds);
      return this;
    }

    public HoodieMetricsGraphiteConfig build() {
      hoodieMetricsGraphiteConfig.setDefaults(HoodieMetricsGraphiteConfig.class.getName());
      return hoodieMetricsGraphiteConfig;
    }
  }
}
