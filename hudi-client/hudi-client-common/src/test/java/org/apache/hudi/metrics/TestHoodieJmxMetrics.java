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

package org.apache.hudi.metrics;

import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.NetworkTestUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.exception.HoodieException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.when;

/**
 * Test for the Jmx metrics report.
 */
@ExtendWith(MockitoExtension.class)
public class TestHoodieJmxMetrics {

  @Mock
  HoodieWriteConfig writeConfig;
  @Mock
  HoodieMetricsConfig metricsConfig;
  HoodieMetrics hoodieMetrics;
  Metrics metrics;

  @BeforeEach
  void setup() {
    when(writeConfig.getMetricsConfig()).thenReturn(metricsConfig);
    when(writeConfig.isMetricsOn()).thenReturn(true);
    when(metricsConfig.getMetricsReporterType()).thenReturn(MetricsReporterType.JMX);
    when(metricsConfig.getJmxHost()).thenReturn("localhost");
    when(metricsConfig.getJmxPort()).thenReturn(String.valueOf(NetworkTestUtils.nextFreePort()));
    when(metricsConfig.getBasePath()).thenReturn("s3://test" + UUID.randomUUID());
    hoodieMetrics = new HoodieMetrics(writeConfig, HoodieTestUtils.getDefaultStorage());
    metrics = hoodieMetrics.getMetrics();
  }

  @AfterEach
  void shutdownMetrics() {
    metrics.shutdown();
  }

  @Test
  public void testRegisterGauge() {
    metrics.registerGauge("jmx_metric1", 123L);
    assertEquals("123", metrics.getRegistry().getGauges()
        .get("jmx_metric1").getValue().toString());
  }

  @Test
  public void testRegisterGaugeByRangerPort() {
    metrics.registerGauge("jmx_metric2", 123L);
    assertEquals("123", metrics.getRegistry().getGauges()
        .get("jmx_metric2").getValue().toString());
  }

  @Test
  public void testMultipleJmxReporterServer() {
    String ports = "9889-9890";
    clearInvocations(metricsConfig);
    when(metricsConfig.getMetricsReporterType()).thenReturn(MetricsReporterType.JMX);
    when(metricsConfig.getJmxHost()).thenReturn("localhost");
    when(metricsConfig.getJmxPort()).thenReturn(ports);
    when(metricsConfig.getBasePath()).thenReturn("s3://test" + UUID.randomUUID());
    hoodieMetrics = new HoodieMetrics(writeConfig, HoodieTestUtils.getDefaultStorage());
    metrics = hoodieMetrics.getMetrics();

    clearInvocations(metricsConfig);
    when(metricsConfig.getMetricsReporterType()).thenReturn(MetricsReporterType.JMX);
    when(metricsConfig.getJmxHost()).thenReturn("localhost");
    when(metricsConfig.getJmxPort()).thenReturn(ports);
    when(metricsConfig.getBasePath()).thenReturn("s3://test2" + UUID.randomUUID());

    hoodieMetrics = new HoodieMetrics(writeConfig, HoodieTestUtils.getDefaultStorage());
    Metrics metrics2 = hoodieMetrics.getMetrics();

    metrics.registerGauge("jmx_metric3", 123L);
    assertEquals("123", metrics.getRegistry().getGauges()
        .get("jmx_metric3").getValue().toString());

    metrics2.registerGauge("jmx_metric4", 123L);
    assertEquals("123", metrics2.getRegistry().getGauges()
        .get("jmx_metric4").getValue().toString());
  }

  @Test
  public void testMultipleJmxReporterServerFailedForOnePort() {
    String ports = "9891";
    clearInvocations(metricsConfig);
    when(metricsConfig.getMetricsReporterType()).thenReturn(MetricsReporterType.JMX);
    when(metricsConfig.getJmxHost()).thenReturn("localhost");
    when(metricsConfig.getJmxPort()).thenReturn(ports);
    when(metricsConfig.getBasePath()).thenReturn("s3://test" + UUID.randomUUID());
    hoodieMetrics = new HoodieMetrics(writeConfig, HoodieTestUtils.getDefaultStorage());
    metrics = hoodieMetrics.getMetrics();

    clearInvocations(metricsConfig);
    when(metricsConfig.getMetricsReporterType()).thenReturn(MetricsReporterType.JMX);
    when(metricsConfig.getJmxHost()).thenReturn("localhost");
    when(metricsConfig.getJmxPort()).thenReturn(ports);
    when(metricsConfig.getBasePath()).thenReturn("s3://test2" + UUID.randomUUID());

    assertThrows(HoodieException.class, () -> {
      hoodieMetrics = new HoodieMetrics(writeConfig, HoodieTestUtils.getDefaultStorage());
    });
  }
}
