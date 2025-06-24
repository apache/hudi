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
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.metrics.MetricsReporterType;
import org.apache.hudi.metrics.datadog.DatadogHttpClient;

import javax.annotation.concurrent.Immutable;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
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
  public static final String META_SYNC_BASE_PATH_KEY = "hoodie.datasource.meta.sync.base.path";

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
      .markAdvanced()
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
      .markAdvanced()
      .withDocumentation("The prefix given to the metrics names.");

  // Enable metrics collection from executors
  public static final ConfigProperty<String> EXECUTOR_METRICS_ENABLE = ConfigProperty
      .key(METRIC_PREFIX + ".executor.enable")
      .noDefaultValue()
      .markAdvanced()
      .sinceVersion("0.7.0")
      .withDocumentation("");

  public static final ConfigProperty<Boolean> LOCK_METRICS_ENABLE = ConfigProperty
      .key(METRIC_PREFIX + ".lock.enable")
      .defaultValue(false)
      .withInferFunction(cfg -> {
        if (cfg.contains(TURN_METRICS_ON)) {
          return Option.of(cfg.getBoolean(TURN_METRICS_ON));
        }
        return Option.empty();
      })
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation("Enable metrics for locking infra. Useful when operating in multiwriter mode");

  public static final ConfigProperty<String> METRICS_REPORTER_FILE_BASED_CONFIGS_PATH = ConfigProperty
      .key(METRIC_PREFIX + ".configs.properties")
      .defaultValue("")
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Comma separated list of config file paths for metric exporter configs");

  public static final ConfigProperty<Boolean> TURN_METRICS_COMPACTION_LOG_BLOCKS_ON = ConfigProperty
      .key(METRIC_PREFIX + "compaction.log.blocks.on")
      .defaultValue(false)
      .sinceVersion("0.14.0")
      .withDocumentation("Turn on/off metrics reporting for log blocks with compaction commit. off by default.");

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

  private HoodieMetricsConfig() {
    super();
  }

  public static HoodieMetricsConfig.Builder newBuilder() {
    return new Builder();
  }

  /**
   * base properties.
   */
  public String getBasePath() {
    // First search default base path key.
    String basePathKey = getString(HoodieCommonConfig.BASE_PATH);
    // This works when initialized in a meta-sync tool.
    if (basePathKey == null) {
      return getString(META_SYNC_BASE_PATH_KEY);
    }
    return basePathKey;
  }

  /**
   * metrics properties.
   */
  public boolean isMetricsOn() {
    return getBoolean(HoodieMetricsConfig.TURN_METRICS_ON);
  }

  /**
   * metrics properties.
   */
  public boolean isCompactionLogBlockMetricsOn() {
    return getBoolean(HoodieMetricsConfig.TURN_METRICS_COMPACTION_LOG_BLOCKS_ON);
  }

  public boolean isExecutorMetricsEnabled() {
    return Boolean.parseBoolean(
        getStringOrDefault(HoodieMetricsConfig.EXECUTOR_METRICS_ENABLE, "false"));
  }

  public boolean isLockingMetricsEnabled() {
    return getBoolean(HoodieMetricsConfig.LOCK_METRICS_ENABLE);
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

  public String getM3ServerHost() {
    return getString(HoodieMetricsM3Config.M3_SERVER_HOST_NAME);
  }

  public int getM3ServerPort() {
    return getInt(HoodieMetricsM3Config.M3_SERVER_PORT_NUM);
  }

  public String getM3Tags() {
    return getString(HoodieMetricsM3Config.M3_TAGS);
  }

  public String getM3Env() {
    return getString(HoodieMetricsM3Config.M3_ENV);
  }

  public String getM3Service() {
    return getString(HoodieMetricsM3Config.M3_SERVICE);
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

  public String getPushGatewayLabels() {
    return getString(HoodieMetricsPrometheusConfig.PUSHGATEWAY_LABELS);
  }

  public boolean getPushGatewayRandomJobNameSuffix() {
    return getBoolean(HoodieMetricsPrometheusConfig.PUSHGATEWAY_RANDOM_JOBNAME_SUFFIX);
  }

  public String getMetricReporterMetricsNamePrefix() {
    // Metrics prefixes should not have a dot as this is usually a separator
    return getStringOrDefault(HoodieMetricsConfig.METRICS_REPORTER_PREFIX).replaceAll("\\.", "_");
  }

  public String getMetricReporterFileBasedConfigs() {
    return getStringOrDefault(HoodieMetricsConfig.METRICS_REPORTER_FILE_BASED_CONFIGS_PATH);
  }

  public static class Builder {

    private final HoodieMetricsConfig hoodieMetricsConfig = new HoodieMetricsConfig();

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.hoodieMetricsConfig.getProps().load(reader);
        return this;
      }
    }

    public Builder fromInputStream(InputStream inputStream) throws IOException {
      try {
        this.hoodieMetricsConfig.getProps().load(inputStream);
        return this;
      } finally {
        inputStream.close();
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

    public Builder compactionLogBlocksEnable(boolean compactionLogBlockMetricsEnable) {
      hoodieMetricsConfig.setValue(TURN_METRICS_COMPACTION_LOG_BLOCKS_ON, String.valueOf(compactionLogBlockMetricsEnable));
      return this;
    }

    public Builder withPath(String basePath) {
      hoodieMetricsConfig.setValue(HoodieCommonConfig.BASE_PATH, basePath);
      return this;
    }

    public Builder withReporterType(String reporterType) {
      hoodieMetricsConfig.setValue(METRICS_REPORTER_TYPE_VALUE, reporterType);
      return this;
    }

    public Builder withMetricsReporterMetricNamePrefix(String metricNamePrefix) {
      hoodieMetricsConfig.setValue(METRICS_REPORTER_PREFIX, metricNamePrefix);
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

    public Builder withLockingMetrics(boolean enable) {
      hoodieMetricsConfig.setValue(LOCK_METRICS_ENABLE, String.valueOf(enable));
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
      hoodieMetricsConfig.setDefaultOnCondition(reporterType == MetricsReporterType.M3,
              HoodieMetricsM3Config.newBuilder().fromProperties(hoodieMetricsConfig.getProps()).build());
      return hoodieMetricsConfig;
    }
  }

}
