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

import org.apache.hudi.config.HoodieMetricsConfig;
import org.apache.hudi.config.HoodieWriteConfig;

import org.junit.Test;

import static org.apache.hudi.metrics.Metrics.registerGauge;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for the Jmx metrics report.
 */
public class TestHoodieJmxMetrics extends TestHoodieMetrics {

  @Override
  public void start() {
    HoodieWriteConfig config = mock(HoodieWriteConfig.class);
    when(config.isMetricsOn()).thenReturn(true);
    when(config.getMetricsReporterType()).thenReturn(MetricsReporterType.JMX);
    when(config.getJmxHost()).thenReturn(HoodieMetricsConfig.DEFAULT_JMX_HOST);
    when(config.getJmxPort()).thenReturn(HoodieMetricsConfig.DEFAULT_JMX_PORT);
    new HoodieMetrics(config, "raw_table");
  }

  @Test
  public void testRegisterGauge() {
    registerGauge("jmx_metric", 123L);
    assertTrue(Metrics.getInstance().getRegistry().getGauges()
        .get("jmx_metric").getValue().toString().equals("123"));
  }
}
