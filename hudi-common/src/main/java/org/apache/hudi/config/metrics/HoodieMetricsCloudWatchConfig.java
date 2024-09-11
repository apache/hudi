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

package org.apache.hudi.config.metrics;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import java.util.Properties;

@ConfigClassProperty(
    name = "Metrics Configurations for Amazon CloudWatch",
    groupName = ConfigGroups.Names.METRICS,
    description =
        "Enables reporting on Hudi metrics using Amazon CloudWatch. "
            + " Hudi publishes metrics on every commit, clean, rollback etc.")
public class HoodieMetricsCloudWatchConfig extends HoodieConfig {

  public static final String CLOUDWATCH_PREFIX = "hoodie.metrics.cloudwatch";

  public static final ConfigProperty<Integer> REPORT_PERIOD_SECONDS = ConfigProperty
      .key(CLOUDWATCH_PREFIX + ".report.period.seconds")
      .defaultValue(60)
      .markAdvanced()
      .sinceVersion("0.10.0")
      .withDocumentation("Reporting interval in seconds");

  public static final ConfigProperty<String> METRIC_PREFIX = ConfigProperty
      .key(CLOUDWATCH_PREFIX + ".metric.prefix")
      .defaultValue("")
      .markAdvanced()
      .sinceVersion("0.10.0")
      .withDocumentation("Metric prefix of reporter");

  public static final ConfigProperty<String> METRIC_NAMESPACE = ConfigProperty
      .key(CLOUDWATCH_PREFIX + ".namespace")
      .defaultValue("Hudi")
      .markAdvanced()
      .sinceVersion("0.10.0")
      .withDocumentation("Namespace of reporter");
  /*
  Amazon CloudWatch allows a maximum of 20 metrics per request. Choosing this as the default maximum.
  Reference: https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_PutMetricData.html
  */
  public static final ConfigProperty<Integer> MAX_DATUMS_PER_REQUEST =
      ConfigProperty.key(CLOUDWATCH_PREFIX + ".maxDatumsPerRequest")
          .defaultValue(20)
          .markAdvanced()
          .sinceVersion("0.10.0")
          .withDocumentation("Max number of Datums per request");

  public HoodieMetricsCloudWatchConfig() {
    super();
  }

  static Builder newBuilder() {
    return new Builder();
  }

  static class Builder {

    private final HoodieMetricsCloudWatchConfig hoodieMetricsCloudWatchConfig = new HoodieMetricsCloudWatchConfig();

    public HoodieMetricsCloudWatchConfig.Builder fromProperties(Properties props) {
      this.hoodieMetricsCloudWatchConfig.getProps().putAll(props);
      return this;
    }

    public HoodieMetricsCloudWatchConfig build() {
      hoodieMetricsCloudWatchConfig.setDefaults(HoodieMetricsCloudWatchConfig.class.getName());
      return hoodieMetricsCloudWatchConfig;
    }
  }
}
