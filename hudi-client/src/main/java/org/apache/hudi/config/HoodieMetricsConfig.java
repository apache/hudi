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

import org.apache.hudi.metrics.MetricsReporterType;

import javax.annotation.concurrent.Immutable;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Fetch the configurations used by the Metrics system.
 */
@Immutable
public class HoodieMetricsConfig extends DefaultHoodieConfig {

  public static final String METRIC_PREFIX = "hoodie.metrics";
  public static final String METRICS_ON = METRIC_PREFIX + ".on";
  public static final boolean DEFAULT_METRICS_ON = false;
  public static final String METRICS_REPORTER_TYPE = METRIC_PREFIX + ".reporter.type";
  public static final MetricsReporterType DEFAULT_METRICS_REPORTER_TYPE = MetricsReporterType.GRAPHITE;

  // Graphite
  public static final String GRAPHITE_PREFIX = METRIC_PREFIX + ".graphite";
  public static final String GRAPHITE_SERVER_HOST = GRAPHITE_PREFIX + ".host";
  public static final String DEFAULT_GRAPHITE_SERVER_HOST = "localhost";

  public static final String GRAPHITE_SERVER_PORT = GRAPHITE_PREFIX + ".port";
  public static final int DEFAULT_GRAPHITE_SERVER_PORT = 4756;

  // Jmx
  public static final String JMX_PREFIX = METRIC_PREFIX + ".jmx";
  public static final String JMX_HOST = JMX_PREFIX + ".host";
  public static final String DEFAULT_JMX_HOST = "localhost";

  public static final String JMX_PORT = JMX_PREFIX + ".port";
  public static final int DEFAULT_JMX_PORT = 9889;

  public static final String GRAPHITE_METRIC_PREFIX = GRAPHITE_PREFIX + ".metric.prefix";

  private HoodieMetricsConfig(Properties props) {
    super(props);
  }

  public static HoodieMetricsConfig.Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private final Properties props = new Properties();

    public Builder fromFile(File propertiesFile) throws IOException {
      FileReader reader = new FileReader(propertiesFile);
      try {
        this.props.load(reader);
        return this;
      } finally {
        reader.close();
      }
    }

    public Builder fromProperties(Properties props) {
      this.props.putAll(props);
      return this;
    }

    public Builder on(boolean metricsOn) {
      props.setProperty(METRICS_ON, String.valueOf(metricsOn));
      return this;
    }

    public Builder withReporterType(String reporterType) {
      props.setProperty(METRICS_REPORTER_TYPE, reporterType);
      return this;
    }

    public Builder toGraphiteHost(String host) {
      props.setProperty(GRAPHITE_SERVER_HOST, host);
      return this;
    }

    public Builder onGraphitePort(int port) {
      props.setProperty(GRAPHITE_SERVER_PORT, String.valueOf(port));
      return this;
    }

    public Builder usePrefix(String prefix) {
      props.setProperty(GRAPHITE_METRIC_PREFIX, prefix);
      return this;
    }

    public HoodieMetricsConfig build() {
      HoodieMetricsConfig config = new HoodieMetricsConfig(props);
      setDefaultOnCondition(props, !props.containsKey(METRICS_ON), METRICS_ON, String.valueOf(DEFAULT_METRICS_ON));
      setDefaultOnCondition(props, !props.containsKey(METRICS_REPORTER_TYPE), METRICS_REPORTER_TYPE,
          DEFAULT_METRICS_REPORTER_TYPE.name());
      setDefaultOnCondition(props, !props.containsKey(GRAPHITE_SERVER_HOST), GRAPHITE_SERVER_HOST,
          DEFAULT_GRAPHITE_SERVER_HOST);
      setDefaultOnCondition(props, !props.containsKey(GRAPHITE_SERVER_PORT), GRAPHITE_SERVER_PORT,
          String.valueOf(DEFAULT_GRAPHITE_SERVER_PORT));
      setDefaultOnCondition(props, !props.containsKey(GRAPHITE_SERVER_PORT), GRAPHITE_SERVER_PORT,
          String.valueOf(DEFAULT_GRAPHITE_SERVER_PORT));
      return config;
    }
  }

}
