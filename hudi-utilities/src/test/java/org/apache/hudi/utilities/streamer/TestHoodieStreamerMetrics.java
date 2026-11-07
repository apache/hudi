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

import org.apache.hudi.common.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerMetrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests {@link HoodieStreamerMetrics}.
 */
public class TestHoodieStreamerMetrics {
  @Test
  public void testPre12543TimerMetricNamesRemainStable() {
    assertPre12543TimerMetricNames("", ".", "/tmp/path8");
    assertPre12543TimerMetricNames("my_prefix", "my_prefix.", "/tmp/path9");
  }

  private void assertPre12543TimerMetricNames(String configuredPrefix, String expectedPrefix, String path) {
    HoodieMetricsConfig metricsConfig = HoodieMetricsConfig.newBuilder()
        .on(true)
        .withPath(path)
        .withReporterType("INMEMORY")
        .withMetricsReporterMetricNamePrefix(configuredPrefix)
        .build();
    HoodieStreamerMetrics metrics = new HoodieStreamerMetrics(
        metricsConfig, HoodieStorageUtils.getStorage(getDefaultStorageConf()));

    metrics.getOverallTimerContext().stop();
    metrics.getHiveSyncTimerContext().stop();
    metrics.getMetaSyncTimerContext().stop();
    metrics.getErrorTableWriteTimerContext().stop();

    Set<String> expectedNames = new HashSet<>(Arrays.asList(
        expectedPrefix + "timer.deltastreamer",
        expectedPrefix + "timer.deltastreamerHiveSync",
        expectedPrefix + "timer.deltastreamerMetaSync",
        expectedPrefix + "timer.errorTableWrite"));
    assertEquals(expectedNames, metrics.getMetrics().getRegistry().getTimers().keySet());
    metrics.shutdown();
  }

  @Test
  public void testHoodieStreamerMetricsForErrorTableIfEnabled() throws InterruptedException {
    HoodieMetricsConfig metricsConfig = HoodieMetricsConfig.newBuilder()
        .on(true)
        .withPath("/tmp/path1")
        .withReporterType("INMEMORY")
        .build();
    HoodieStreamerMetrics metrics = new HoodieStreamerMetrics(
        metricsConfig, getDefaultStorage());
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
        metricsConfig, getDefaultStorage());
    Timer.Context timerContext = metrics.getErrorTableWriteTimerContext();
    assertNull(timerContext);
    metrics.updateErrorTableCommitDuration(0L);
    assertNull(metrics.getMetrics());
  }

  @Test
  public void testEmitStreamerJobSuccessMetrics() {
    HoodieMetricsConfig metricsConfig = HoodieMetricsConfig.newBuilder()
        .on(true)
        .withPath("/tmp/path3")
        .withReporterType("INMEMORY")
        .build();
    HoodieStreamerMetrics metrics = new HoodieStreamerMetrics(
        metricsConfig, getDefaultStorage());
    metrics.emitStreamerJobSuccessMetrics();
    MetricRegistry registry = metrics.getMetrics().getRegistry();
    assertEquals(1, registry.getGauges().size());
    assertEquals(".deltastreamer.success", registry.getGauges().firstKey());
    assertEquals(1L, registry.getGauges().get(".deltastreamer.success").getValue());
  }

  @Test
  public void testEmitStreamerJobFailedMetrics() {
    HoodieMetricsConfig metricsConfig = HoodieMetricsConfig.newBuilder()
        .on(true)
        .withPath("/tmp/path4")
        .withReporterType("INMEMORY")
        .build();
    HoodieStreamerMetrics metrics = new HoodieStreamerMetrics(
        metricsConfig, getDefaultStorage());
    metrics.emitStreamerJobFailedMetrics();
    MetricRegistry registry = metrics.getMetrics().getRegistry();
    assertEquals(1, registry.getGauges().size());
    assertEquals(".deltastreamer.failure", registry.getGauges().firstKey());
    assertEquals(1L, registry.getGauges().get(".deltastreamer.failure").getValue());
  }

  @Test
  public void testEmitStreamerJobMetricsIfDisabled() {
    HoodieMetricsConfig metricsConfig = HoodieMetricsConfig.newBuilder()
        .on(false)
        .withPath("/tmp/path5")
        .withReporterType("INMEMORY")
        .build();
    HoodieStreamerMetrics metrics = new HoodieStreamerMetrics(
        metricsConfig, getDefaultStorage());
    // Should not throw when metrics are disabled
    metrics.emitStreamerJobSuccessMetrics();
    metrics.emitStreamerJobFailedMetrics();
    assertNull(metrics.getMetrics());
  }

  @Test
  public void testDeprecatedDeltaStreamerMetricsAlias() {
    HoodieMetricsConfig metricsConfig = HoodieMetricsConfig.newBuilder()
        .on(true)
        .withPath("/tmp/path6")
        .withReporterType("INMEMORY")
        .build();
    HoodieDeltaStreamerMetrics metrics = new HoodieDeltaStreamerMetrics(
        metricsConfig, getDefaultStorage());
    metrics.emitStreamerJobSuccessMetrics();
    assertEquals(".deltastreamer.success", metrics.getMetrics().getRegistry().getGauges().firstKey());

    // the write config overload reports against the metrics config derived from the write config
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/path7")
        .withMetricsConfig(HoodieMetricsConfig.newBuilder().on(true).withReporterType("INMEMORY").build())
        .build();
    HoodieDeltaStreamerMetrics metricsFromWriteConfig = new HoodieDeltaStreamerMetrics(
        writeConfig, getDefaultStorage());
    metricsFromWriteConfig.emitStreamerJobFailedMetrics();
    assertEquals(".deltastreamer.failure",
        metricsFromWriteConfig.getMetrics().getRegistry().getGauges().firstKey());
  }
}
