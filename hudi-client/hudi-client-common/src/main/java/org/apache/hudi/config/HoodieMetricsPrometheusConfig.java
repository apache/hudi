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

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import java.util.Properties;

import static org.apache.hudi.config.HoodieMetricsConfig.METRIC_PREFIX;

@ConfigClassProperty(name = "Metrics Configurations for Prometheus",
    groupName = ConfigGroups.Names.METRICS,
    description = "Enables reporting on Hudi metrics using Prometheus. " +
        " Hudi publishes metrics on every commit, clean, rollback etc.")
public class HoodieMetricsPrometheusConfig extends HoodieConfig {

  // Prometheus PushGateWay
  public static final String PUSHGATEWAY_PREFIX = METRIC_PREFIX + ".pushgateway";

  public static final ConfigProperty<String> PUSHGATEWAY_HOST = ConfigProperty
      .key(PUSHGATEWAY_PREFIX + ".host")
      .defaultValue("localhost")
      .sinceVersion("0.6.0")
      .withDocumentation("Hostname of the prometheus push gateway");

  public static final ConfigProperty<Integer> PUSHGATEWAY_PORT = ConfigProperty
      .key(PUSHGATEWAY_PREFIX + ".port")
      .defaultValue(9091)
      .sinceVersion("0.6.0")
      .withDocumentation("Port for the push gateway.");

  public static final ConfigProperty<Integer> PUSHGATEWAY_REPORT_PERIOD_SECONDS = ConfigProperty
      .key(PUSHGATEWAY_PREFIX + ".report.period.seconds")
      .defaultValue(30)
      .sinceVersion("0.6.0")
      .withDocumentation("Reporting interval in seconds.");

  public static final ConfigProperty<Boolean> PUSHGATEWAY_DELETE_ON_SHUTDOWN = ConfigProperty
      .key(PUSHGATEWAY_PREFIX + ".delete.on.shutdown")
      .defaultValue(true)
      .sinceVersion("0.6.0")
      .withDocumentation("");

  public static final ConfigProperty<String> PUSHGATEWAY_JOB_NAME = ConfigProperty
      .key(PUSHGATEWAY_PREFIX + ".job.name")
      .defaultValue("")
      .sinceVersion("0.6.0")
      .withDocumentation("Name of the push gateway job.");

  public static final ConfigProperty<Boolean> PUSHGATEWAY_RANDOM_JOB_NAME_SUFFIX = ConfigProperty
      .key(PUSHGATEWAY_PREFIX + ".random.job.name.suffix")
      .defaultValue(true)
      .sinceVersion("0.6.0")
      .withDocumentation("");

  // Prometheus HttpServer
  public static final String PROMETHEUS_PREFIX = METRIC_PREFIX + ".prometheus";

  public static final ConfigProperty<Integer> PROMETHEUS_PORT = ConfigProperty
      .key(PROMETHEUS_PREFIX + ".port")
      .defaultValue(9090)
      .sinceVersion("0.6.0")
      .withDocumentation("Port for prometheus server.");

  private HoodieMetricsPrometheusConfig() {
    super();
  }

  public static HoodieMetricsPrometheusConfig.Builder newBuilder() {
    return new HoodieMetricsPrometheusConfig.Builder();
  }

  @Override
  public Properties getProps() {
    return super.getProps();
  }

  public static class Builder {

    private HoodieMetricsPrometheusConfig hoodieMetricsPrometheusConfig = new HoodieMetricsPrometheusConfig();

    public Builder fromProperties(Properties props) {
      this.hoodieMetricsPrometheusConfig.getProps().putAll(props);
      return this;
    }

    public HoodieMetricsPrometheusConfig build() {
      hoodieMetricsPrometheusConfig.setDefaults(HoodieMetricsPrometheusConfig.class.getName());
      return hoodieMetricsPrometheusConfig;
    }
  }
}
