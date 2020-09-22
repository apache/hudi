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

import org.apache.hudi.common.config.DefaultHoodieConfig;

import javax.annotation.concurrent.Immutable;

import java.util.Properties;

import static org.apache.hudi.config.HoodieMetricsConfig.METRIC_PREFIX;

/**
 * Configs for Datadog reporter type.
 * <p>
 * {@link org.apache.hudi.metrics.MetricsReporterType#DATADOG}
 */
@Immutable
public class HoodieMetricsDatadogConfig extends DefaultHoodieConfig {

  public static final String DATADOG_PREFIX = METRIC_PREFIX + ".datadog";
  public static final String DATADOG_REPORT_PERIOD_SECONDS = DATADOG_PREFIX + ".report.period.seconds";
  public static final int DEFAULT_DATADOG_REPORT_PERIOD_SECONDS = 30;
  public static final String DATADOG_API_SITE = DATADOG_PREFIX + ".api.site";
  public static final String DATADOG_API_KEY = DATADOG_PREFIX + ".api.key";
  public static final String DATADOG_API_KEY_SKIP_VALIDATION = DATADOG_PREFIX + ".api.key.skip.validation";
  public static final boolean DEFAULT_DATADOG_API_KEY_SKIP_VALIDATION = false;
  public static final String DATADOG_API_KEY_SUPPLIER = DATADOG_PREFIX + ".api.key.supplier";
  public static final String DATADOG_API_TIMEOUT_SECONDS = DATADOG_PREFIX + ".api.timeout.seconds";
  public static final int DEFAULT_DATADOG_API_TIMEOUT_SECONDS = 3;
  public static final String DATADOG_METRIC_PREFIX = DATADOG_PREFIX + ".metric.prefix";
  public static final String DATADOG_METRIC_HOST = DATADOG_PREFIX + ".metric.host";
  public static final String DATADOG_METRIC_TAGS = DATADOG_PREFIX + ".metric.tags";

  private HoodieMetricsDatadogConfig(Properties props) {
    super(props);
  }

  public static HoodieMetricsDatadogConfig.Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private final Properties props = new Properties();

    public Builder fromProperties(Properties props) {
      this.props.putAll(props);
      return this;
    }

    public Builder withDatadogReportPeriodSeconds(int period) {
      props.setProperty(DATADOG_REPORT_PERIOD_SECONDS, String.valueOf(period));
      return this;
    }

    public Builder withDatadogApiSite(String apiSite) {
      props.setProperty(DATADOG_API_SITE, apiSite);
      return this;
    }

    public Builder withDatadogApiKey(String apiKey) {
      props.setProperty(DATADOG_API_KEY, apiKey);
      return this;
    }

    public Builder withDatadogApiKeySkipValidation(boolean skip) {
      props.setProperty(DATADOG_API_KEY_SKIP_VALIDATION, String.valueOf(skip));
      return this;
    }

    public Builder withDatadogApiKeySupplier(String apiKeySupplier) {
      props.setProperty(DATADOG_API_KEY_SUPPLIER, apiKeySupplier);
      return this;
    }

    public Builder withDatadogApiTimeoutSeconds(int timeout) {
      props.setProperty(DATADOG_API_TIMEOUT_SECONDS, String.valueOf(timeout));
      return this;
    }

    public Builder withDatadogPrefix(String prefix) {
      props.setProperty(DATADOG_METRIC_PREFIX, prefix);
      return this;
    }

    public Builder withDatadogHost(String host) {
      props.setProperty(DATADOG_METRIC_HOST, host);
      return this;
    }

    public Builder withDatadogTags(String tags) {
      props.setProperty(DATADOG_METRIC_TAGS, tags);
      return this;
    }

    public HoodieMetricsDatadogConfig build() {
      HoodieMetricsDatadogConfig config = new HoodieMetricsDatadogConfig(props);
      setDefaultOnCondition(props, !props.containsKey(DATADOG_REPORT_PERIOD_SECONDS),
          DATADOG_REPORT_PERIOD_SECONDS,
          String.valueOf(DEFAULT_DATADOG_REPORT_PERIOD_SECONDS));
      setDefaultOnCondition(props, !props.containsKey(DATADOG_API_KEY_SKIP_VALIDATION),
          DATADOG_API_KEY_SKIP_VALIDATION,
          String.valueOf(DEFAULT_DATADOG_API_KEY_SKIP_VALIDATION));
      setDefaultOnCondition(props, !props.containsKey(DATADOG_API_TIMEOUT_SECONDS),
          DATADOG_API_TIMEOUT_SECONDS,
          String.valueOf(DEFAULT_DATADOG_API_TIMEOUT_SECONDS));
      return config;
    }
  }
}
