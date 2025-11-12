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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.storage.HoodieStorageUtils;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests {@link HoodieStreamerMetrics}.
 */
public class TestHoodieStreamerMetrics {
  @Test
  public void testHoodieStreamerMetricsForErrorTableIfEnabled() throws InterruptedException {
    HoodieMetricsConfig metricsConfig = HoodieMetricsConfig.newBuilder()
        .on(true)
        .withPath("/tmp/path1")
        .withReporterType("INMEMORY")
        .build();
    HoodieStreamerMetrics metrics = new HoodieStreamerMetrics(
        metricsConfig, HoodieStorageUtils.getStorage(getDefaultStorageConf()));
    Timer.Context timerContext = metrics.getErrorTableWriteTimerContext();
    Thread.sleep(TimeUnit.SECONDS.toMillis(1));
    long duration = timerContext.stop();
    metrics.updateErrorTableCommitDuration(duration);
    MetricRegistry registry = metrics.getMetrics().getRegistry();
    assertEquals(1, registry.getGauges().size());
    assertEquals(".deltastreamer.errorTableCommitDuration", registry.getGauges().firstKey());
  }

  @Test
  public void testHoodieStreamerMetricsForErrorTableIfDisabled() {
    HoodieMetricsConfig metricsConfig = HoodieMetricsConfig.newBuilder()
        .on(false)
        .withPath("/tmp/path2")
        .withReporterType("INMEMORY")
        .build();
    HoodieStreamerMetrics metrics = new HoodieStreamerMetrics(
        metricsConfig, HoodieStorageUtils.getStorage(getDefaultStorageConf()));
    Timer.Context timerContext = metrics.getErrorTableWriteTimerContext();
    assertNull(timerContext);
    metrics.updateErrorTableCommitDuration(0L);
    assertNull(metrics.getMetrics());
  }
}
