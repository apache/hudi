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

import java.util.Properties;

import static org.apache.hudi.config.metrics.HoodieMetricsConfig.METRIC_PREFIX;

/**
 * Configs for Prometheus/Pushgaeway reporter type.
 * <p>
 * {@link org.apache.hudi.metrics.MetricsReporterType#PROMETHEUS}
 * {@link org.apache.hudi.metrics.MetricsReporterType#PROMETHEUS_PUSHGATEWAY}
 */
@ConfigClassProperty(name = "Metrics Configurations for Prometheus",
    groupName = ConfigGroups.Names.METRICS,
    description = "Enables reporting on Hudi metrics using Prometheus. "
        + " Hudi publishes metrics on every commit, clean, rollback etc.")
public class HoodieMetricsPrometheusConfig extends HoodieConfig {

  // Prometheus PushGateWay
  public static final String PUSHGATEWAY_PREFIX = METRIC_PREFIX + ".pushgateway";

  public static final ConfigProperty<String> PUSHGATEWAY_HOST_NAME = ConfigProperty
      .key(PUSHGATEWAY_PREFIX + ".host")
      .defaultValue("localhost")
      .markAdvanced()
      .sinceVersion("0.6.0")
      .withDocumentation("Hostname of the prometheus push gateway.");

  public static final ConfigProperty<Integer> PUSHGATEWAY_PORT_NUM = ConfigProperty
      .key(PUSHGATEWAY_PREFIX + ".port")
      .defaultValue(9091)
      .markAdvanced()
      .sinceVersion("0.6.0")
      .withDocumentation("Port for the push gateway.");

  public static final ConfigProperty<Integer> PUSHGATEWAY_REPORT_PERIOD_IN_SECONDS = ConfigProperty
      .key(PUSHGATEWAY_PREFIX + ".report.period.seconds")
      .defaultValue(30)
      .markAdvanced()
      .sinceVersion("0.6.0")
      .withDocumentation("Reporting interval in seconds.");

  public static final ConfigProperty<Boolean> PUSHGATEWAY_DELETE_ON_SHUTDOWN_ENABLE = ConfigProperty
      .key(PUSHGATEWAY_PREFIX + ".delete.on.shutdown")
      .defaultValue(true)
      .markAdvanced()
      .sinceVersion("0.6.0")
      .withDocumentation("Delete the pushgateway info or not when job shutdown, true by default.");

  public static final ConfigProperty<String> PUSHGATEWAY_JOBNAME = ConfigProperty
      .key(PUSHGATEWAY_PREFIX + ".job.name")
      .defaultValue("")
      .markAdvanced()
      .sinceVersion("0.6.0")
      .withDocumentation("Name of the push gateway job.");

  public static final ConfigProperty<String> PUSHGATEWAY_LABELS = ConfigProperty
      .key(PUSHGATEWAY_PREFIX + ".report.labels")
      .defaultValue("")
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Label for the metrics emitted to the Pushgateway. Labels can be specified with key:value pairs separated by commas");

  public static final ConfigProperty<Boolean> PUSHGATEWAY_RANDOM_JOBNAME_SUFFIX = ConfigProperty
      .key(PUSHGATEWAY_PREFIX + ".random.job.name.suffix")
      .defaultValue(true)
      .markAdvanced()
      .sinceVersion("0.6.0")
      .withDocumentation("Whether the pushgateway name need a random suffix , default true.");

  // Prometheus HttpServer
  public static final String PROMETHEUS_PREFIX = METRIC_PREFIX + ".prometheus";

  public static final ConfigProperty<Integer> PROMETHEUS_PORT_NUM = ConfigProperty
      .key(PROMETHEUS_PREFIX + ".port")
      .defaultValue(9090)
      .markAdvanced()
      .sinceVersion("0.6.0")
      .withDocumentation("Port for prometheus server.");

  /**
   * @deprecated Use {@link #PUSHGATEWAY_HOST_NAME} and its methods instead
   */
  @Deprecated
  public static final String PUSHGATEWAY_HOST = PUSHGATEWAY_HOST_NAME.key();
  /**
   * @deprecated Use {@link #PUSHGATEWAY_HOST_NAME} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_PUSHGATEWAY_HOST = PUSHGATEWAY_HOST_NAME.defaultValue();
  /**
   * @deprecated Use {@link #PUSHGATEWAY_PORT_NUM} and its methods instead
   */
  @Deprecated
  public static final String PUSHGATEWAY_PORT = PUSHGATEWAY_PORT_NUM.key();
  /**
   * @deprecated Use {@link #PUSHGATEWAY_PORT_NUM} and its methods instead
   */
  @Deprecated
  public static final int DEFAULT_PUSHGATEWAY_PORT = PUSHGATEWAY_PORT_NUM.defaultValue();
  /**
   * @deprecated Use {@link #PUSHGATEWAY_REPORT_PERIOD_IN_SECONDS} and its methods instead
   */
  @Deprecated
  public static final String PUSHGATEWAY_REPORT_PERIOD_SECONDS = PUSHGATEWAY_REPORT_PERIOD_IN_SECONDS.key();
  /**
   * @deprecated Use {@link #PUSHGATEWAY_REPORT_PERIOD_IN_SECONDS} and its methods instead
   */
  @Deprecated
  public static final int DEFAULT_PUSHGATEWAY_REPORT_PERIOD_SECONDS = PUSHGATEWAY_REPORT_PERIOD_IN_SECONDS.defaultValue();
  /**
   * @deprecated Use {@link #PUSHGATEWAY_DELETE_ON_SHUTDOWN_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String PUSHGATEWAY_DELETE_ON_SHUTDOWN = PUSHGATEWAY_DELETE_ON_SHUTDOWN_ENABLE.key();
  /**
   * @deprecated Use {@link #PUSHGATEWAY_DELETE_ON_SHUTDOWN_ENABLE} and its methods instead
   */
  @Deprecated
  public static final boolean DEFAULT_PUSHGATEWAY_DELETE_ON_SHUTDOWN = PUSHGATEWAY_DELETE_ON_SHUTDOWN_ENABLE.defaultValue();
  /**
   * @deprecated Use {@link #PUSHGATEWAY_JOBNAME} and its methods instead
   */
  @Deprecated
  public static final String PUSHGATEWAY_JOB_NAME = PUSHGATEWAY_JOBNAME.key();
  /**
   * @deprecated Use {@link #PUSHGATEWAY_JOBNAME} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_PUSHGATEWAY_JOB_NAME = PUSHGATEWAY_JOBNAME.defaultValue();
  /**
   * @deprecated Use {@link #PUSHGATEWAY_RANDOM_JOBNAME_SUFFIX} and its methods instead
   */
  @Deprecated
  public static final String PUSHGATEWAY_RANDOM_JOB_NAME_SUFFIX = PUSHGATEWAY_RANDOM_JOBNAME_SUFFIX.key();
  /**
   * @deprecated Use {@link #PUSHGATEWAY_RANDOM_JOBNAME_SUFFIX} and its methods instead
   */
  @Deprecated
  public static final boolean DEFAULT_PUSHGATEWAY_RANDOM_JOB_NAME_SUFFIX = PUSHGATEWAY_RANDOM_JOBNAME_SUFFIX.defaultValue();
  /**
   * @deprecated Use {@link #PROMETHEUS_PORT_NUM} and its methods instead
   */
  @Deprecated
  public static final String PROMETHEUS_PORT = PROMETHEUS_PORT_NUM.key();
  /**
   * @deprecated Use {@link #PROMETHEUS_PORT_NUM} and its methods instead
   */
  @Deprecated
  public static final int DEFAULT_PROMETHEUS_PORT = PROMETHEUS_PORT_NUM.defaultValue();

  private HoodieMetricsPrometheusConfig() {
    super();
  }

  public static HoodieMetricsPrometheusConfig.Builder newBuilder() {
    return new HoodieMetricsPrometheusConfig.Builder();
  }

  public static class Builder {

    private final HoodieMetricsPrometheusConfig hoodieMetricsPrometheusConfig = new HoodieMetricsPrometheusConfig();

    public Builder fromProperties(Properties props) {
      this.hoodieMetricsPrometheusConfig.getProps().putAll(props);
      return this;
    }

    public HoodieMetricsPrometheusConfig.Builder withPushgatewayHostName(String hostName) {
      hoodieMetricsPrometheusConfig.setValue(PUSHGATEWAY_HOST_NAME, String.valueOf(hostName));
      return this;
    }

    public HoodieMetricsPrometheusConfig.Builder withPushgatewayPortNum(Integer pushgatewayPortNum) {
      hoodieMetricsPrometheusConfig.setValue(PUSHGATEWAY_PORT_NUM, String.valueOf(pushgatewayPortNum));
      return this;
    }

    public HoodieMetricsPrometheusConfig.Builder withPushgatewayReportPeriodInSeconds(String periodTime) {
      hoodieMetricsPrometheusConfig.setValue(PUSHGATEWAY_REPORT_PERIOD_IN_SECONDS, periodTime);
      return this;
    }

    public HoodieMetricsPrometheusConfig.Builder withPushgatewayDeleteOnShutdownEnable(boolean deleteOnShutdownEnable) {
      hoodieMetricsPrometheusConfig.setValue(PUSHGATEWAY_DELETE_ON_SHUTDOWN_ENABLE, String.valueOf(deleteOnShutdownEnable));
      return this;
    }

    public HoodieMetricsPrometheusConfig.Builder withPushgatewayJobname(String jobname) {
      hoodieMetricsPrometheusConfig.setValue(PUSHGATEWAY_JOBNAME, jobname);
      return this;
    }

    public HoodieMetricsPrometheusConfig.Builder withPushgatewayRandomJobnameSuffix(boolean randomJobnameSuffix) {
      hoodieMetricsPrometheusConfig.setValue(PUSHGATEWAY_RANDOM_JOBNAME_SUFFIX, String.valueOf(randomJobnameSuffix));
      return this;
    }

    public Builder withPushgatewayLabels(String pushGatewayLabels) {
      hoodieMetricsPrometheusConfig.setValue(PUSHGATEWAY_LABELS, pushGatewayLabels);
      return this;
    }

    public HoodieMetricsPrometheusConfig.Builder withPrometheusPortNum(int prometheusPortNum) {
      hoodieMetricsPrometheusConfig.setValue(PROMETHEUS_PORT_NUM, String.valueOf(prometheusPortNum));
      return this;
    }

    public HoodieMetricsPrometheusConfig build() {
      hoodieMetricsPrometheusConfig.setDefaults(HoodieMetricsPrometheusConfig.class.getName());
      return hoodieMetricsPrometheusConfig;
    }
  }
}
