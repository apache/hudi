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
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.metrics.Metrics;
import org.apache.hudi.metrics.MetricsReporterType;
import org.junit.jupiter.api.Test;

import static org.apache.hudi.metrics.Metrics.registerGauge;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestPushGateWayReporter {

  HoodieWriteConfig config = mock(HoodieWriteConfig.class);

  @Test
  public void testRegisterGauge() {
    when(config.isMetricsOn()).thenReturn(true);
    when(config.getMetricsReporterType()).thenReturn(MetricsReporterType.PROMETHEUS_PUSHGATEWAY);
    when(config.getPushGatewayHost()).thenReturn("localhost");
    when(config.getPushGatewayPort()).thenReturn(9091);
    new HoodieMetrics(config, "raw_table");
    registerGauge("pushGateWayReporter_metric", 123L);
    assertEquals("123", Metrics.getInstance().getRegistry().getGauges()
        .get("pushGateWayReporter_metric").getValue().toString());
  }
}