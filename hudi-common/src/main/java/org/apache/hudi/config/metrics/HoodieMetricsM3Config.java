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
 * Configs for M3 reporter type.
 * <p>
 * {@link org.apache.hudi.metrics.MetricsReporterType#M3}
 */
@ConfigClassProperty(name = "Metrics Configurations for M3",
    groupName = ConfigGroups.Names.METRICS,
    description = "Enables reporting on Hudi metrics using M3. "
        + " Hudi publishes metrics on every commit, clean, rollback etc.")
public class HoodieMetricsM3Config extends HoodieConfig {

  public static final String M3_PREFIX = METRIC_PREFIX + ".m3";

  public static final ConfigProperty<String> M3_SERVER_HOST_NAME = ConfigProperty
      .key(M3_PREFIX + ".host")
      .defaultValue("localhost")
      .sinceVersion("0.15.0")
      .withDocumentation("M3 host to connect to.");

  public static final ConfigProperty<Integer> M3_SERVER_PORT_NUM = ConfigProperty
      .key(M3_PREFIX + ".port")
      .defaultValue(9052)
      .sinceVersion("0.15.0")
      .withDocumentation("M3 port to connect to.");

  public static final ConfigProperty<String> M3_TAGS = ConfigProperty
      .key(M3_PREFIX + ".tags")
      .defaultValue("")
      .sinceVersion("0.15.0")
      .withDocumentation("Optional M3 tags applied to all metrics.");

  public static final ConfigProperty<String> M3_ENV = ConfigProperty
      .key(M3_PREFIX + ".env")
      .defaultValue("production")
      .sinceVersion("0.15.0")
      .withDocumentation("M3 tag to label the environment (defaults to 'production'), "
          + "applied to all metrics.");

  public static final ConfigProperty<String> M3_SERVICE = ConfigProperty
      .key(M3_PREFIX + ".service")
      .defaultValue("hoodie")
      .sinceVersion("0.15.0")
      .withDocumentation("M3 tag to label the service name (defaults to 'hoodie'), "
          + "applied to all metrics.");

  private HoodieMetricsM3Config() {
    super();
  }

  public static HoodieMetricsM3Config.Builder newBuilder() {
    return new HoodieMetricsM3Config.Builder();
  }

  public static class Builder {

    private final HoodieMetricsM3Config hoodieMetricsM3Config = new HoodieMetricsM3Config();

    public HoodieMetricsM3Config.Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.hoodieMetricsM3Config.getProps().load(reader);
        return this;
      }
    }

    public HoodieMetricsM3Config.Builder fromProperties(Properties props) {
      this.hoodieMetricsM3Config.getProps().putAll(props);
      return this;
    }

    public HoodieMetricsM3Config.Builder toM3Host(String host) {
      hoodieMetricsM3Config.setValue(M3_SERVER_HOST_NAME, host);
      return this;
    }

    public HoodieMetricsM3Config.Builder onM3Port(int port) {
      hoodieMetricsM3Config.setValue(M3_SERVER_PORT_NUM, String.valueOf(port));
      return this;
    }

    public HoodieMetricsM3Config.Builder useM3Tags(String tags) {
      hoodieMetricsM3Config.setValue(M3_TAGS, tags);
      return this;
    }

    public HoodieMetricsM3Config.Builder useM3Env(String env) {
      hoodieMetricsM3Config.setValue(M3_ENV, env);
      return this;
    }

    public HoodieMetricsM3Config.Builder useM3Service(String service) {
      hoodieMetricsM3Config.setValue(M3_SERVICE, service);
      return this;
    }

    public HoodieMetricsM3Config build() {
      hoodieMetricsM3Config.setDefaults(HoodieMetricsM3Config.class.getName());
      return hoodieMetricsM3Config;
    }
  }
}
