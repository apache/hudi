/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.metrics.config;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieMetricsCloudWatchConfig;
import org.apache.hudi.metrics.MetricsReporterType;
import org.apache.hudi.metrics.datadog.DatadogHttpClient;

import javax.annotation.concurrent.Immutable;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Fetch the configurations used by the Metrics system.
 */
@Immutable
@ConfigClassProperty(name = "Metrics Configurations",
    groupName = ConfigGroups.Names.METRICS,
    description = "Enables reporting on Hudi metrics. Hudi publishes metrics on "
        + "every commit, clean, rollback etc. The following sections list the supported reporters.")
public class HoodieMetricsConfig extends HoodieConfig {

  public static final String METRIC_PREFIX = "hoodie.metrics";

  public static final ConfigProperty<String> TBL_NAME = ConfigProperty
          .key("hoodie.table.name")
          .noDefaultValue()
          .withDocumentation("Table name that will be used for registering with metastores like HMS. Needs to be same across runs.");

  public static final ConfigProperty<Boolean> TURN_METRICS_ON = ConfigProperty
      .key(METRIC_PREFIX + ".on")
      .defaultValue(false)
      .sinceVersion("0.5.0")
      .withDocumentation("Turn on/off metrics reporting. off by default.");

  public static final ConfigProperty<MetricsReporterType> METRICS_REPORTER_TYPE_VALUE = ConfigProperty
      .key(METRIC_PREFIX + ".reporter.type")
      .defaultValue(MetricsReporterType.GRAPHITE)
      .sinceVersion("0.5.0")
      .withDocumentation("Type of metrics reporter.");

  // User defined
  public static final ConfigProperty<String> METRICS_REPORTER_CLASS_NAME = ConfigProperty
      .key(METRIC_PREFIX + ".reporter.class")
      .defaultValue("")
      .sinceVersion("0.6.0")
      .withDocumentation("");

  public static final ConfigProperty<String> METRICS_REPORTER_PREFIX = ConfigProperty
      .key(METRIC_PREFIX + ".reporter.metricsname.prefix")
      .defaultValue("")
      .sinceVersion("0.11.0")
      .withInferFunction(cfg -> {
        if (cfg.contains(HoodieTableConfig.NAME)) {
          return Option.of(cfg.getString(HoodieTableConfig.NAME));
        }
        return Option.empty();
      })
      .withDocumentation("The prefix given to the metrics names.");

  // Enable metrics collection from executors
  public static final ConfigProperty<String> EXECUTOR_METRICS_ENABLE = ConfigProperty
      .key(METRIC_PREFIX + ".executor.enable")
      .noDefaultValue()
      .sinceVersion("0.7.0")
      .withDocumentation("");

  /**
   * @deprecated Use {@link #TURN_METRICS_ON} and its methods instead
   */
  @Deprecated
  public static final String METRICS_ON = TURN_METRICS_ON.key();
  /**
   * @deprecated Use {@link #TURN_METRICS_ON} and its methods instead
   */
  @Deprecated
  public static final boolean DEFAULT_METRICS_ON = TURN_METRICS_ON.defaultValue();
  /**
   * @deprecated Use {@link #METRICS_REPORTER_TYPE_VALUE} and its methods instead
   */
  @Deprecated
  public static final String METRICS_REPORTER_TYPE = METRICS_REPORTER_TYPE_VALUE.key();
  /**
   * @deprecated Use {@link #METRICS_REPORTER_TYPE_VALUE} and its methods instead
   */
  @Deprecated
  public static final MetricsReporterType DEFAULT_METRICS_REPORTER_TYPE = METRICS_REPORTER_TYPE_VALUE.defaultValue();
  /**
   * @deprecated Use {@link #METRICS_REPORTER_CLASS_NAME} and its methods instead
   */
  @Deprecated
  public static final String METRICS_REPORTER_CLASS = METRICS_REPORTER_CLASS_NAME.key();
  /**
   * @deprecated Use {@link #METRICS_REPORTER_CLASS_NAME} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_METRICS_REPORTER_CLASS = METRICS_REPORTER_CLASS_NAME.defaultValue();
  /**
   * @deprecated Use {@link #EXECUTOR_METRICS_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String ENABLE_EXECUTOR_METRICS = EXECUTOR_METRICS_ENABLE.key();

  /**
   * metrics properties.
   */

  public String getTableName() {
    return getString(TBL_NAME);
  }

  public boolean isMetricsOn() {
    return getBoolean(HoodieMetricsConfig.TURN_METRICS_ON);
  }

  public boolean isExecutorMetricsEnabled() {
    return Boolean.parseBoolean(
        getStringOrDefault(HoodieMetricsConfig.EXECUTOR_METRICS_ENABLE, "false"));
  }

  public MetricsReporterType getMetricsReporterType() {
    return MetricsReporterType.valueOf(getString(HoodieMetricsConfig.METRICS_REPORTER_TYPE_VALUE));
  }

  public String getGraphiteServerHost() {
    return getString(HoodieMetricsGraphiteConfig.GRAPHITE_SERVER_HOST_NAME);
  }

  public int getGraphiteServerPort() {
    return getInt(HoodieMetricsGraphiteConfig.GRAPHITE_SERVER_PORT_NUM);
  }

  public String getGraphiteMetricPrefix() {
    return getString(HoodieMetricsGraphiteConfig.GRAPHITE_METRIC_PREFIX_VALUE);
  }

  public int getGraphiteReportPeriodSeconds() {
    return getInt(HoodieMetricsGraphiteConfig.GRAPHITE_REPORT_PERIOD_IN_SECONDS);
  }

  public String getJmxHost() {
    return getString(HoodieMetricsJmxConfig.JMX_HOST_NAME);
  }

  public String getJmxPort() {
    return getString(HoodieMetricsJmxConfig.JMX_PORT_NUM);
  }

  public int getDatadogReportPeriodSeconds() {
    return getInt(HoodieMetricsDatadogConfig.REPORT_PERIOD_IN_SECONDS);
  }

  public DatadogHttpClient.ApiSite getDatadogApiSite() {
    return DatadogHttpClient.ApiSite.valueOf(getString(HoodieMetricsDatadogConfig.API_SITE_VALUE));
  }

  public String getDatadogApiKey() {
    if (props.containsKey(HoodieMetricsDatadogConfig.API_KEY.key())) {
      return getString(HoodieMetricsDatadogConfig.API_KEY);
    } else {
      Supplier<String> apiKeySupplier = ReflectionUtils.loadClass(
          getString(HoodieMetricsDatadogConfig.API_KEY_SUPPLIER));
      return apiKeySupplier.get();
    }
  }

  public boolean getDatadogApiKeySkipValidation() {
    return getBoolean(HoodieMetricsDatadogConfig.API_KEY_SKIP_VALIDATION);
  }

  public int getDatadogApiTimeoutSeconds() {
    return getInt(HoodieMetricsDatadogConfig.API_TIMEOUT_IN_SECONDS);
  }

  public String getDatadogMetricPrefix() {
    return getString(HoodieMetricsDatadogConfig.METRIC_PREFIX_VALUE);
  }

  public String getDatadogMetricHost() {
    return getString(HoodieMetricsDatadogConfig.METRIC_HOST_NAME);
  }

  public List<String> getDatadogMetricTags() {
    return Arrays.stream(getStringOrDefault(
        HoodieMetricsDatadogConfig.METRIC_TAG_VALUES, ",").split("\\s*,\\s*")).collect(Collectors.toList());
  }

  public int getCloudWatchReportPeriodSeconds() {
    return getInt(HoodieMetricsCloudWatchConfig.REPORT_PERIOD_SECONDS);
  }

  public String getCloudWatchMetricPrefix() {
    return getString(HoodieMetricsCloudWatchConfig.METRIC_PREFIX);
  }

  public String getCloudWatchMetricNamespace() {
    return getString(HoodieMetricsCloudWatchConfig.METRIC_NAMESPACE);
  }

  public int getCloudWatchMaxDatumsPerRequest() {
    return getInt(HoodieMetricsCloudWatchConfig.MAX_DATUMS_PER_REQUEST);
  }

  public String getMetricReporterClassName() {
    return getString(HoodieMetricsConfig.METRICS_REPORTER_CLASS_NAME);
  }

  public int getPrometheusPort() {
    return getInt(HoodieMetricsPrometheusConfig.PROMETHEUS_PORT_NUM);
  }

  public String getPushGatewayHost() {
    return getString(HoodieMetricsPrometheusConfig.PUSHGATEWAY_HOST_NAME);
  }

  public int getPushGatewayPort() {
    return getInt(HoodieMetricsPrometheusConfig.PUSHGATEWAY_PORT_NUM);
  }

  public int getPushGatewayReportPeriodSeconds() {
    return getInt(HoodieMetricsPrometheusConfig.PUSHGATEWAY_REPORT_PERIOD_IN_SECONDS);
  }

  public boolean getPushGatewayDeleteOnShutdown() {
    return getBoolean(HoodieMetricsPrometheusConfig.PUSHGATEWAY_DELETE_ON_SHUTDOWN_ENABLE);
  }

  public String getPushGatewayJobName() {
    return getString(HoodieMetricsPrometheusConfig.PUSHGATEWAY_JOBNAME);
  }

  public boolean getPushGatewayRandomJobNameSuffix() {
    return getBoolean(HoodieMetricsPrometheusConfig.PUSHGATEWAY_RANDOM_JOBNAME_SUFFIX);
  }

  public String getMetricReporterMetricsNamePrefix() {
    return getStringOrDefault(HoodieMetricsConfig.METRICS_REPORTER_PREFIX);
  }

  private HoodieMetricsConfig() {
    super();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private final HoodieMetricsConfig hoodieMetricsConfig = new HoodieMetricsConfig();

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.hoodieMetricsConfig.getProps().load(reader);
        return this;
      }
    }

    public Builder fromProperties(Properties props) {
      this.hoodieMetricsConfig.getProps().putAll(props);
      return this;
    }

    public Builder on(boolean metricsOn) {
      hoodieMetricsConfig.setValue(TURN_METRICS_ON, String.valueOf(metricsOn));
      return this;
    }

    public Builder withReporterType(String reporterType) {
      hoodieMetricsConfig.setValue(METRICS_REPORTER_TYPE_VALUE, reporterType);
      return this;
    }

    public Builder withReporterClass(String className) {
      hoodieMetricsConfig.setValue(METRICS_REPORTER_CLASS_NAME, className);
      return this;
    }

    public Builder withExecutorMetrics(boolean enable) {
      hoodieMetricsConfig.setValue(EXECUTOR_METRICS_ENABLE, String.valueOf(enable));
      return this;
    }

    public HoodieMetricsConfig build() {

      hoodieMetricsConfig.setDefaults(HoodieMetricsConfig.class.getName());

      MetricsReporterType reporterType = MetricsReporterType.valueOf(hoodieMetricsConfig.getString(METRICS_REPORTER_TYPE_VALUE));

      hoodieMetricsConfig.setDefaultOnCondition(reporterType == MetricsReporterType.DATADOG,
          HoodieMetricsDatadogConfig.newBuilder().fromProperties(hoodieMetricsConfig.getProps()).build());
      hoodieMetricsConfig.setDefaultOnCondition(reporterType == MetricsReporterType.PROMETHEUS_PUSHGATEWAY,
              HoodieMetricsPrometheusConfig.newBuilder().fromProperties(hoodieMetricsConfig.getProps()).build());
      hoodieMetricsConfig.setDefaultOnCondition(reporterType == MetricsReporterType.PROMETHEUS,
              HoodieMetricsPrometheusConfig.newBuilder().fromProperties(hoodieMetricsConfig.getProps()).build());
      hoodieMetricsConfig.setDefaultOnCondition(reporterType == MetricsReporterType.JMX,
          HoodieMetricsJmxConfig.newBuilder().fromProperties(hoodieMetricsConfig.getProps()).build());
      hoodieMetricsConfig.setDefaultOnCondition(reporterType == MetricsReporterType.GRAPHITE,
          HoodieMetricsGraphiteConfig.newBuilder().fromProperties(hoodieMetricsConfig.getProps()).build());
      hoodieMetricsConfig.setDefaultOnCondition(reporterType == MetricsReporterType.CLOUDWATCH,
              HoodieMetricsCloudWatchConfig.newBuilder().fromProperties(hoodieMetricsConfig.getProps()).build());
      return hoodieMetricsConfig;
    }
  }

}
