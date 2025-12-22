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
 * Configs for Jmx reporter type.
 * <p>
 * {@link org.apache.hudi.metrics.MetricsReporterType#JMX}
 */
@ConfigClassProperty(name = "Metrics Configurations for Jmx",
    groupName = ConfigGroups.Names.METRICS,
    description = "Enables reporting on Hudi metrics using Jmx. "
                      + " Hudi publishes metrics on every commit, clean, rollback etc.")
public class HoodieMetricsJmxConfig extends HoodieConfig {

  public static final String JMX_PREFIX = METRIC_PREFIX + ".jmx";

  public static final ConfigProperty<String> JMX_HOST_NAME = ConfigProperty
      .key(JMX_PREFIX + ".host")
      .defaultValue("localhost")
      .markAdvanced()
      .sinceVersion("0.5.1")
      .withDocumentation("Jmx host to connect to");

  public static final ConfigProperty<Integer> JMX_PORT_NUM = ConfigProperty
      .key(JMX_PREFIX + ".port")
      .defaultValue(9889)
      .markAdvanced()
      .sinceVersion("0.5.1")
      .withDocumentation("Jmx port to connect to");

  /**
   * @deprecated Use {@link #JMX_HOST_NAME} and its methods instead
   */
  @Deprecated
  public static final String JMX_HOST = JMX_HOST_NAME.key();
  /**
   * @deprecated Use {@link #JMX_HOST_NAME} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_JMX_HOST = JMX_HOST_NAME.defaultValue();
  /**
   * @deprecated Use {@link #JMX_PORT_NUM} and its methods instead
   */
  @Deprecated
  public static final String JMX_PORT = JMX_PORT_NUM.key();
  /**
   * @deprecated Use {@link #JMX_PORT_NUM} and its methods instead
   */
  @Deprecated
  public static final int DEFAULT_JMX_PORT = JMX_PORT_NUM.defaultValue();

  private HoodieMetricsJmxConfig() {
    super();
  }

  public static HoodieMetricsJmxConfig.Builder newBuilder() {
    return new HoodieMetricsJmxConfig.Builder();
  }

  public static class Builder {

    private final HoodieMetricsJmxConfig hoodieMetricsJmxConfig = new HoodieMetricsJmxConfig();

    public HoodieMetricsJmxConfig.Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.hoodieMetricsJmxConfig.getProps().load(reader);
        return this;
      }
    }

    public HoodieMetricsJmxConfig.Builder fromProperties(Properties props) {
      this.hoodieMetricsJmxConfig.getProps().putAll(props);
      return this;
    }

    public HoodieMetricsJmxConfig.Builder toJmxHost(String host) {
      hoodieMetricsJmxConfig.setValue(JMX_HOST_NAME, host);
      return this;
    }

    public HoodieMetricsJmxConfig.Builder onJmxPort(String port) {
      hoodieMetricsJmxConfig.setValue(JMX_PORT_NUM, port);
      return this;
    }

    public HoodieMetricsJmxConfig build() {
      hoodieMetricsJmxConfig.setDefaults(HoodieMetricsJmxConfig.class.getName());
      return hoodieMetricsJmxConfig;
    }
  }
}
