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

import org.apache.hudi.common.config.ConfigGroupProperty;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import javax.annotation.concurrent.Immutable;

import java.util.Properties;

import static org.apache.hudi.config.HoodieMetricsConfig.METRIC_PREFIX;

/**
 * Configs for Datadog reporter type.
 * <p>
 * {@link org.apache.hudi.metrics.MetricsReporterType#DATADOG}
 */
@Immutable
@ConfigGroupProperty(name = "Metrics Configurations for Datadog reporter", description = "Enables reporting on Hudi metrics using the Datadog reporter type. Hudi publishes metrics on every commit, clean, rollback etc.")
public class HoodieMetricsDatadogConfig extends HoodieConfig {

  public static final String DATADOG_PREFIX = METRIC_PREFIX + ".datadog";

  public static final ConfigProperty<Integer> DATADOG_REPORT_PERIOD_SECONDS = ConfigProperty
      .key(DATADOG_PREFIX + ".report.period.seconds")
      .defaultValue(30)
      .sinceVersion("0.6.0")
      .withDocumentation("Datadog reporting period in seconds. Default to 30.");

  public static final ConfigProperty<String> DATADOG_API_SITE = ConfigProperty
      .key(DATADOG_PREFIX + ".api.site")
      .noDefaultValue()
      .sinceVersion("0.6.0")
      .withDocumentation("Datadog API site: EU or US");

  public static final ConfigProperty<String> DATADOG_API_KEY = ConfigProperty
      .key(DATADOG_PREFIX + ".api.key")
      .noDefaultValue()
      .sinceVersion("0.6.0")
      .withDocumentation("Datadog API key");

  public static final ConfigProperty<Boolean> DATADOG_API_KEY_SKIP_VALIDATION = ConfigProperty
      .key(DATADOG_PREFIX + ".api.key.skip.validation")
      .defaultValue(false)
      .sinceVersion("0.6.0")
      .withDocumentation("Before sending metrics via Datadog API, whether to skip validating Datadog API key or not. "
          + "Default to false.");

  public static final ConfigProperty<String> DATADOG_API_KEY_SUPPLIER = ConfigProperty
      .key(DATADOG_PREFIX + ".api.key.supplier")
      .noDefaultValue()
      .sinceVersion("0.6.0")
      .withDocumentation("Datadog API key supplier to supply the API key at runtime. "
          + "This will take effect if hoodie.metrics.datadog.api.key is not set.");

  public static final ConfigProperty<Integer> DATADOG_API_TIMEOUT_SECONDS = ConfigProperty
      .key(DATADOG_PREFIX + ".api.timeout.seconds")
      .defaultValue(3)
      .sinceVersion("0.6.0")
      .withDocumentation("Datadog API timeout in seconds. Default to 3.");

  public static final ConfigProperty<String> DATADOG_METRIC_PREFIX = ConfigProperty
      .key(DATADOG_PREFIX + ".metric.prefix")
      .noDefaultValue()
      .sinceVersion("0.6.0")
      .withDocumentation("Datadog metric prefix to be prepended to each metric name with a dot as delimiter. "
          + "For example, if it is set to foo, foo. will be prepended.");

  public static final ConfigProperty<String> DATADOG_METRIC_HOST = ConfigProperty
      .key(DATADOG_PREFIX + ".metric.host")
      .noDefaultValue()
      .sinceVersion("0.6.0")
      .withDocumentation("Datadog metric host to be sent along with metrics data.");

  public static final ConfigProperty<String> DATADOG_METRIC_TAGS = ConfigProperty
      .key(DATADOG_PREFIX + ".metric.tags")
      .noDefaultValue()
      .sinceVersion("0.6.0")
      .withDocumentation("Datadog metric tags (comma-delimited) to be sent along with metrics data.");

  private HoodieMetricsDatadogConfig() {
    super();
  }

  public static HoodieMetricsDatadogConfig.Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private final HoodieMetricsDatadogConfig metricsDatadogConfig = new HoodieMetricsDatadogConfig();

    public Builder fromProperties(Properties props) {
      this.metricsDatadogConfig.getProps().putAll(props);
      return this;
    }

    public Builder withDatadogReportPeriodSeconds(int period) {
      metricsDatadogConfig.setValue(DATADOG_REPORT_PERIOD_SECONDS, String.valueOf(period));
      return this;
    }

    public Builder withDatadogApiSite(String apiSite) {
      metricsDatadogConfig.setValue(DATADOG_API_SITE, apiSite);
      return this;
    }

    public Builder withDatadogApiKey(String apiKey) {
      metricsDatadogConfig.setValue(DATADOG_API_KEY, apiKey);
      return this;
    }

    public Builder withDatadogApiKeySkipValidation(boolean skip) {
      metricsDatadogConfig.setValue(DATADOG_API_KEY_SKIP_VALIDATION, String.valueOf(skip));
      return this;
    }

    public Builder withDatadogApiKeySupplier(String apiKeySupplier) {
      metricsDatadogConfig.setValue(DATADOG_API_KEY_SUPPLIER, apiKeySupplier);
      return this;
    }

    public Builder withDatadogApiTimeoutSeconds(int timeout) {
      metricsDatadogConfig.setValue(DATADOG_API_TIMEOUT_SECONDS, String.valueOf(timeout));
      return this;
    }

    public Builder withDatadogPrefix(String prefix) {
      metricsDatadogConfig.setValue(DATADOG_METRIC_PREFIX, prefix);
      return this;
    }

    public Builder withDatadogHost(String host) {
      metricsDatadogConfig.setValue(DATADOG_METRIC_HOST, host);
      return this;
    }

    public Builder withDatadogTags(String tags) {
      metricsDatadogConfig.setValue(DATADOG_METRIC_TAGS, tags);
      return this;
    }

    public HoodieMetricsDatadogConfig build() {
      metricsDatadogConfig.setDefaults(HoodieMetricsDatadogConfig.class.getName());
      return metricsDatadogConfig;
    }
  }
}
