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

import java.util.Properties;

import static org.apache.hudi.config.HoodieMetricsConfig.METRIC_PREFIX;

public class HoodieMetricsPrometheusConfig extends DefaultHoodieConfig {

  // Prometheus PushGateWay
  public static final String PUSHGATEWAY_PREFIX = METRIC_PREFIX + ".pushgateway";

  public static final String PUSHGATEWAY_HOST = PUSHGATEWAY_PREFIX + ".host";
  public static final String DEFAULT_PUSHGATEWAY_HOST = "localhost";

  public static final String PUSHGATEWAY_PORT = PUSHGATEWAY_PREFIX + ".port";
  public static final int DEFAULT_PUSHGATEWAY_PORT = 9091;

  public static final String PUSHGATEWAY_REPORT_PERIOD_SECONDS = PUSHGATEWAY_PREFIX + ".report.period.seconds";
  public static final int DEFAULT_PUSHGATEWAY_REPORT_PERIOD_SECONDS = 30;

  public static final String PUSHGATEWAY_DELETE_ON_SHUTDOWN = PUSHGATEWAY_PREFIX + ".delete.on.shutdown";
  public static final boolean DEFAULT_PUSHGATEWAY_DELETE_ON_SHUTDOWN = true;

  public static final String PUSHGATEWAY_JOB_NAME = PUSHGATEWAY_PREFIX + ".job.name";
  public static final String DEFAULT_PUSHGATEWAY_JOB_NAME = "";

  public static final String PUSHGATEWAY_RANDOM_JOB_NAME_SUFFIX = PUSHGATEWAY_PREFIX + ".random.job.name.suffix";
  public static final boolean DEFAULT_PUSHGATEWAY_RANDOM_JOB_NAME_SUFFIX = true;


  // Prometheus HttpServer
  public static final String PROMETHEUS_PREFIX = METRIC_PREFIX + ".prometheus";
  public static final String PROMETHEUS_PORT = PROMETHEUS_PREFIX + ".port";
  public static final int DEFAULT_PROMETHEUS_PORT = 9090;

  public HoodieMetricsPrometheusConfig(Properties props) {
    super(props);
  }

  public static HoodieMetricsPrometheusConfig.Builder newBuilder() {
    return new HoodieMetricsPrometheusConfig.Builder();
  }

  @Override
  public Properties getProps() {
    return super.getProps();
  }

  public static class Builder {

    private Properties props = new Properties();

    public Builder fromProperties(Properties props) {
      this.props.putAll(props);
      return this;
    }

    public HoodieMetricsPrometheusConfig build() {
      HoodieMetricsPrometheusConfig config = new HoodieMetricsPrometheusConfig(props);
      setDefaultOnCondition(props, !props.containsKey(PROMETHEUS_PORT), PROMETHEUS_PORT,
              String.valueOf(DEFAULT_PROMETHEUS_PORT));
      setDefaultOnCondition(props, !props.containsKey(PUSHGATEWAY_HOST),
              PUSHGATEWAY_HOST,
              DEFAULT_PUSHGATEWAY_HOST);
      setDefaultOnCondition(props, !props.containsKey(PUSHGATEWAY_PORT),
              PUSHGATEWAY_PORT,
              String.valueOf(DEFAULT_PUSHGATEWAY_PORT));
      setDefaultOnCondition(props, !props.containsKey(PUSHGATEWAY_REPORT_PERIOD_SECONDS),
              PUSHGATEWAY_REPORT_PERIOD_SECONDS,
              String.valueOf(DEFAULT_PUSHGATEWAY_REPORT_PERIOD_SECONDS));
      setDefaultOnCondition(props, !props.containsKey(PUSHGATEWAY_DELETE_ON_SHUTDOWN),
              PUSHGATEWAY_DELETE_ON_SHUTDOWN,
              String.valueOf(DEFAULT_PUSHGATEWAY_DELETE_ON_SHUTDOWN));
      setDefaultOnCondition(props, !props.containsKey(PUSHGATEWAY_JOB_NAME),
              PUSHGATEWAY_JOB_NAME, DEFAULT_PUSHGATEWAY_JOB_NAME);
      setDefaultOnCondition(props, !props.containsKey(PUSHGATEWAY_RANDOM_JOB_NAME_SUFFIX),
              PUSHGATEWAY_RANDOM_JOB_NAME_SUFFIX,
              String.valueOf(DEFAULT_PUSHGATEWAY_RANDOM_JOB_NAME_SUFFIX));
      return config;
    }
  }
}
