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

package org.apache.hudi.metrics.datadog;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsDatadogConfig;
import org.apache.hudi.metrics.MetricsReporter;
import org.apache.hudi.metrics.datadog.DatadogHttpClient.ApiSite;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Hudi Datadog metrics reporter.
 * <p>
 * Responsible for reading Hoodie metrics configurations and hooking up with {@link org.apache.hudi.metrics.Metrics}.
 * <p>
 * Internally delegate reporting tasks to {@link DatadogReporter}.
 */
public class DatadogMetricsReporter extends MetricsReporter {

  private final DatadogReporter reporter;
  private final int reportPeriodSeconds;

  public DatadogMetricsReporter(HoodieWriteConfig config, MetricRegistry registry) {
    reportPeriodSeconds = config.getInt(HoodieMetricsDatadogConfig.REPORT_PERIOD_IN_SECONDS);
    ApiSite apiSite = ApiSite.valueOf(config.getString(HoodieMetricsDatadogConfig.API_SITE_VALUE));
    String apiKey = config.getDatadogApiKey();
    ValidationUtils.checkState(!StringUtils.isNullOrEmpty(apiKey),
        "Datadog cannot be initialized: API key is null or empty.");
    boolean skipValidation = config.getBoolean(HoodieMetricsDatadogConfig.API_KEY_SKIP_VALIDATION);
    int timeoutSeconds = config.getInt(HoodieMetricsDatadogConfig.API_TIMEOUT_IN_SECONDS);
    String prefix = config.getString(HoodieMetricsDatadogConfig.METRIC_PREFIX_VALUE);
    ValidationUtils.checkState(!StringUtils.isNullOrEmpty(prefix),
        "Datadog cannot be initialized: Metric prefix is null or empty.");
    Option<String> host = Option.ofNullable(config.getString(HoodieMetricsDatadogConfig.METRIC_HOST_NAME));
    List<String> tagList = config.getDatadogMetricTags();
    Option<List<String>> tags = tagList.isEmpty() ? Option.empty() : Option.of(tagList);

    reporter = new DatadogReporter(
        registry,
        new DatadogHttpClient(apiSite, apiKey, skipValidation, timeoutSeconds),
        prefix,
        host,
        tags,
        MetricFilter.ALL,
        TimeUnit.SECONDS,
        TimeUnit.SECONDS
    );
  }

  @Override
  public void start() {
    reporter.start(reportPeriodSeconds, TimeUnit.SECONDS);
  }

  @Override
  public void report() {
    reporter.report();
  }

  @Override
  public void stop() {
    reporter.stop();
  }
}
