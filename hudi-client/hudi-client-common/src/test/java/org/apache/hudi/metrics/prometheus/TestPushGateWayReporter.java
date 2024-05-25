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

package org.apache.hudi.metrics.prometheus;

import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.metrics.MetricUtils;
import org.apache.hudi.metrics.Metrics;
import org.apache.hudi.metrics.MetricsReporterType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestPushGateWayReporter {

  static final URL PROP_FILE_PROMETHEUS_URL = TestPushGateWayReporter.class.getClassLoader().getResource("prometheus.properties");
  static final URL PROP_FILE_DATADOG_URL = TestPushGateWayReporter.class.getClassLoader().getResource("datadog.properties");

  @Mock
  HoodieWriteConfig writeConfig;
  @Mock
  HoodieMetricsConfig metricsConfig;

  HoodieMetrics hoodieMetrics;
  Metrics metrics;

  @AfterEach
  void shutdownMetrics() {
    if (metrics != null) {
      metrics.shutdown();
    }
  }

  @Test
  public void testRegisterGauge() {
    when(writeConfig.isMetricsOn()).thenReturn(true);
    when(writeConfig.getMetricsConfig()).thenReturn(metricsConfig);
    configureDefaultReporter();

    assertDoesNotThrow(() -> {
      hoodieMetrics = new HoodieMetrics(writeConfig, HoodieTestUtils.getDefaultStorage());
      metrics = hoodieMetrics.getMetrics();
    });

    metrics.registerGauge("pushGateWayReporter_metric", 123L);
    assertEquals("123", metrics.getRegistry().getGauges()
        .get("pushGateWayReporter_metric").getValue().toString());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testMultiReporter(boolean addDefaultReporter) throws IOException, InterruptedException, URISyntaxException {
    when(writeConfig.getMetricsConfig()).thenReturn(metricsConfig);
    when(writeConfig.isMetricsOn()).thenReturn(true);

    String propPrometheusPath = Objects.requireNonNull(PROP_FILE_PROMETHEUS_URL).toURI().getPath();
    String propDatadogPath = Objects.requireNonNull(PROP_FILE_DATADOG_URL).toURI().getPath();
    if (addDefaultReporter) {
      configureDefaultReporter();
    } else {
      when(metricsConfig.getBasePath()).thenReturn("s3://test" + UUID.randomUUID());
      when(metricsConfig.getMetricReporterMetricsNamePrefix()).thenReturn(TestPushGateWayReporter.class.getSimpleName());
      when(metricsConfig.getMetricReporterFileBasedConfigs()).thenReturn(propPrometheusPath + "," + propDatadogPath);
    }

    hoodieMetrics = new HoodieMetrics(writeConfig, HoodieTestUtils.getDefaultStorage());
    metrics = hoodieMetrics.getMetrics();

    Map<String, Long> metricsMap = new HashMap<>();
    Map<String, Long> labellessMetricMap = new HashMap<>();
    Map<String, String> labels = new HashMap<>();
    labels.put("group", "a");
    labels.put("job", "0");
    metricsMap.put("with_label_metric;group:a,job:0", 1L);
    labellessMetricMap.put("without_label_metric", 1L);
    metrics.registerGauges(metricsMap, Option.empty());
    metrics.registerGauges(labellessMetricMap, Option.empty());
    List<String> metricKeys = new ArrayList<>(metrics.getRegistry().getGauges().keySet());
    assertEquals(0, MetricUtils.getLabelsAndMetricMap(metricKeys.stream()
        .filter(x -> x.contains("without_label_metric")).findFirst().get()).getValue().size());
    assertEquals(labels, MetricUtils.getLabelsAndMetricMap(metricKeys.stream()
        .filter(x -> x.contains("with_label_metric")).findFirst().get()).getValue());
  }

  @Test
  public void testMetricLabels() {
    PushGatewayMetricsReporter reporter;
    Map<String, String> labels;

    when(metricsConfig.getPushGatewayLabels()).thenReturn("hudi:prometheus");
    reporter = new PushGatewayMetricsReporter(metricsConfig, null);
    labels = reporter.getLabels();
    assertEquals(1, labels.size());
    assertTrue(labels.containsKey("hudi"));
    assertTrue(labels.containsValue("prometheus"));

    when(metricsConfig.getPushGatewayLabels()).thenReturn("hudi:prome:theus");
    reporter = new PushGatewayMetricsReporter(metricsConfig, null);
    labels = reporter.getLabels();
    assertEquals(1, labels.size());
    assertTrue(labels.containsKey("hudi"));
    assertTrue(labels.containsValue("prome:theus"));

    when(metricsConfig.getPushGatewayLabels()).thenReturn("hudiprometheus");
    reporter = new PushGatewayMetricsReporter(metricsConfig, null);
    labels = reporter.getLabels();
    assertEquals(1, labels.size());
    assertTrue(labels.containsKey("hudiprometheus"));
    assertTrue(labels.containsValue(""));

    when(metricsConfig.getPushGatewayLabels()).thenReturn("hudi1:prometheus,hudi2:prometheus");
    reporter = new PushGatewayMetricsReporter(metricsConfig, null);
    labels = reporter.getLabels();
    assertEquals(2, labels.size());
    assertTrue(labels.containsKey("hudi1"));
    assertTrue(labels.containsKey("hudi2"));
    assertTrue(labels.containsValue("prometheus"));

    try {
      when(metricsConfig.getPushGatewayLabels()).thenReturn("hudi:prometheus,hudi:prom");
      reporter = new PushGatewayMetricsReporter(metricsConfig, null);
      fail("Should fail");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("Multiple values {prometheus, prom} for same key"));
    }
  }

  private void configureDefaultReporter() {
    when(metricsConfig.getBasePath()).thenReturn("s3://test" + UUID.randomUUID());
    when(metricsConfig.getMetricsReporterType()).thenReturn(MetricsReporterType.PROMETHEUS_PUSHGATEWAY);
    when(metricsConfig.getPushGatewayReportPeriodSeconds()).thenReturn(30);
  }
}
