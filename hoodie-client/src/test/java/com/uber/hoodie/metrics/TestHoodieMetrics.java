/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.metrics;

import static com.uber.hoodie.metrics.Metrics.registerGauge;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.uber.hoodie.config.HoodieClientConfig;
import org.apache.commons.configuration.ConfigurationException;
import org.junit.Before;
import org.junit.Test;

public class TestHoodieMetrics {

  private HoodieMetrics metrics = null;

  @Before
  public void start() throws ConfigurationException {
    HoodieClientConfig config = mock(HoodieClientConfig.class);
    when(config.isMetricsOn()).thenReturn(true);
    when(config.getMetricsReporterType()).thenReturn(MetricsReporterType.INMEMORY);
    metrics = new HoodieMetrics(config, "raw_table");
  }

  @Test
  public void testRegisterGauge() {
    registerGauge("metric1", 123L);
    assertTrue(Metrics.getInstance().getRegistry().getGauges().get("metric1").getValue().toString().equals("123"));
  }
}
