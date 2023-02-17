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

import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.metrics.Metrics;
import org.apache.hudi.metrics.MetricsReporterType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.apache.hudi.metrics.Metrics.registerGauge;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestPushGateWayReporter {

  @Mock
  HoodieWriteConfig config;

  @AfterEach
  void shutdownMetrics() {
    Metrics.shutdown();
  }

  @Test
  public void testRegisterGauge() {
    when(config.isMetricsOn()).thenReturn(true);
    when(config.getTableName()).thenReturn("foo");
    when(config.getMetricsReporterType()).thenReturn(MetricsReporterType.PROMETHEUS_PUSHGATEWAY);
    when(config.getPushGatewayHost()).thenReturn("localhost");
    when(config.getPushGatewayPort()).thenReturn(9091);
    when(config.getPushGatewayReportPeriodSeconds()).thenReturn(30);
    when(config.getPushGatewayDeleteOnShutdown()).thenReturn(true);
    when(config.getPushGatewayJobName()).thenReturn("foo");
    when(config.getPushGatewayRandomJobNameSuffix()).thenReturn(false);
    when(config.getPushGatewayLabels()).thenReturn("hudi:prometheus");

    assertDoesNotThrow(() -> {
      new HoodieMetrics(config);
    });

    registerGauge("pushGateWayReporter_metric", 123L);
    assertEquals("123", Metrics.getInstance().getRegistry().getGauges()
        .get("pushGateWayReporter_metric").getValue().toString());
  }

  @Test
  public void testMetricLabels() {
    PushGatewayMetricsReporter reporter;
    Map<String, String> labels;

    when(config.getPushGatewayLabels()).thenReturn("hudi:prometheus");
    reporter = new PushGatewayMetricsReporter(config, null);
    labels = reporter.getLabels();
    assertEquals(1, labels.size());
    assertTrue(labels.containsKey("hudi"));
    assertTrue(labels.containsValue("prometheus"));

    when(config.getPushGatewayLabels()).thenReturn("hudi:prome:theus");
    reporter = new PushGatewayMetricsReporter(config, null);
    labels = reporter.getLabels();
    assertEquals(1, labels.size());
    assertTrue(labels.containsKey("hudi"));
    assertTrue(labels.containsValue("prome:theus"));

    when(config.getPushGatewayLabels()).thenReturn("hudiprometheus");
    reporter = new PushGatewayMetricsReporter(config, null);
    labels = reporter.getLabels();
    assertEquals(1, labels.size());
    assertTrue(labels.containsKey("hudiprometheus"));
    assertTrue(labels.containsValue(""));

    when(config.getPushGatewayLabels()).thenReturn("hudi1:prometheus,hudi2:prometheus");
    reporter = new PushGatewayMetricsReporter(config, null);
    labels = reporter.getLabels();
    assertEquals(2, labels.size());
    assertTrue(labels.containsKey("hudi1"));
    assertTrue(labels.containsKey("hudi2"));
    assertTrue(labels.containsValue("prometheus"));

    try {
      when(config.getPushGatewayLabels()).thenReturn("hudi:prometheus,hudi:prometheus");
      reporter = new PushGatewayMetricsReporter(config, null);
      fail("Should fail");
    } catch (HoodieException e) {
      assertTrue(e.getMessage().contains("Duplicate key=hudi found in labels"));
    }
  }
}
