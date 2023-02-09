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

import org.apache.hudi.common.testutils.NetworkTestUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.apache.hudi.metrics.Metrics.registerGauge;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Test for the Graphite metrics report.
 */
@ExtendWith(MockitoExtension.class)
public class TestHoodieGraphiteMetrics {

  @Mock
  HoodieWriteConfig config;

  @AfterEach
  void shutdownMetrics() {
    Metrics.shutdown();
  }

  @Test
  public void testRegisterGauge() {
    when(config.isMetricsOn()).thenReturn(true);
    when(config.getTableName()).thenReturn("table1");
    when(config.getMetricsReporterType()).thenReturn(MetricsReporterType.GRAPHITE);
    when(config.getGraphiteServerHost()).thenReturn("localhost");
    when(config.getGraphiteServerPort()).thenReturn(NetworkTestUtils.nextFreePort());
    when(config.getGraphiteReportPeriodSeconds()).thenReturn(30);
    new HoodieMetrics(config);
    registerGauge("graphite_metric", 123L);
    assertEquals("123", Metrics.getInstance().getRegistry().getGauges()
                            .get("graphite_metric").getValue().toString());
  }
}
