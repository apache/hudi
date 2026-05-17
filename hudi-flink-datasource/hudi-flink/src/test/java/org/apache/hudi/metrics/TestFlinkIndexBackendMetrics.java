/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.metrics;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link FlinkIndexBackendMetrics}.
 */
class TestFlinkIndexBackendMetrics {

  private CapturingMetricGroup metricGroup;
  private FlinkIndexBackendMetrics metrics;

  @BeforeEach
  void setUp() {
    metricGroup = new CapturingMetricGroup();
    metrics = new FlinkIndexBackendMetrics(metricGroup);
    metrics.registerMetrics();
  }

  @Test
  void testRegisterMetricsRegistersHistograms() {
    assertNotNull(metricGroup.getHistogram("localIndexLookupLatency"));
    assertNotNull(metricGroup.getHistogram("remoteIndexLookupLatency"));
    assertNotNull(metricGroup.getHistogram("localLookupKeysNum"));
    assertNotNull(metricGroup.getHistogram("remoteLookupKeysNum"));
  }

  @Test
  void testRegisterMetricsRegistersHitRatioGauge() {
    Gauge<?> gauge = metricGroup.getGauge(FlinkIndexBackendMetrics.LOOKUP_CACHE_HIT_RATIO);
    assertNotNull(gauge);
    assertEquals(0.0D, ((Double) gauge.getValue()).doubleValue());
  }

  @Test
  void testUpdateLookupCacheHitRatioTracksLatestMiniBatch() {
    Gauge<?> gauge = metricGroup.getGauge(FlinkIndexBackendMetrics.LOOKUP_CACHE_HIT_RATIO);

    metrics.updateLookupCacheHitRatio(3L, 1L);
    assertEquals(0.75D, ((Double) gauge.getValue()).doubleValue());
    assertEquals(0.75D, metrics.getLookupCacheHitRatio());

    // gauge reflects the latest mini-batch, not a running average.
    metrics.updateLookupCacheHitRatio(1L, 3L);
    assertEquals(0.25D, ((Double) gauge.getValue()).doubleValue());

    metrics.updateLookupCacheHitRatio(5L, 0L);
    assertEquals(1.0D, ((Double) gauge.getValue()).doubleValue());

    metrics.updateLookupCacheHitRatio(0L, 5L);
    assertEquals(0.0D, ((Double) gauge.getValue()).doubleValue());
  }

  @Test
  void testUpdateLookupCacheHitRatioPreservesPreviousValueOnEmptyBatch() {
    metrics.updateLookupCacheHitRatio(3L, 1L);
    assertEquals(0.75D, metrics.getLookupCacheHitRatio());

    // empty mini-batch (no keys looked up) must not reset the ratio to NaN or 0.
    metrics.updateLookupCacheHitRatio(0L, 0L);
    assertEquals(0.75D, metrics.getLookupCacheHitRatio());

    // negative counts are treated as an empty batch (defensive guard).
    metrics.updateLookupCacheHitRatio(-1L, -2L);
    assertEquals(0.75D, metrics.getLookupCacheHitRatio());
  }

  @Test
  void testLocalIndexLookupUpdatesHistogramCount() {
    Histogram hist = metricGroup.getHistogram("localIndexLookupLatency");
    assertEquals(0, hist.getCount());

    metrics.startLocalIndexLookup();
    metrics.endLocalIndexLookup();
    assertEquals(1, hist.getCount());

    metrics.startLocalIndexLookup();
    metrics.endLocalIndexLookup();
    assertEquals(2, hist.getCount());
  }

  @Test
  void testRemoteIndexLookupUpdatesHistogramCount() {
    Histogram hist = metricGroup.getHistogram("remoteIndexLookupLatency");
    assertEquals(0, hist.getCount());

    metrics.startRemoteIndexLookup();
    metrics.endRemoteIndexLookup();
    assertEquals(1, hist.getCount());
  }

  @Test
  void testLocalLookupKeysNumUpdatesHistogram() {
    Histogram hist = metricGroup.getHistogram("localLookupKeysNum");
    assertEquals(0, hist.getCount());

    metrics.updateLocalLookupKeysCount(5);
    assertEquals(1, hist.getCount());
    assertEquals(5, hist.getStatistics().getMax());

    metrics.updateLocalLookupKeysCount(3);
    assertEquals(2, hist.getCount());
  }

  @Test
  void testRemoteLookupKeysNumUpdatesHistogram() {
    Histogram hist = metricGroup.getHistogram("remoteLookupKeysNum");
    assertEquals(0, hist.getCount());

    metrics.updateRemoteLookupKeysCount(10);
    assertEquals(1, hist.getCount());
    assertEquals(10, hist.getStatistics().getMax());
  }

  @Test
  void testLocalIndexLookupLatencyIsNonNegative() {
    metrics.startLocalIndexLookup();
    metrics.endLocalIndexLookup();

    Histogram hist = metricGroup.getHistogram("localIndexLookupLatency");
    assertTrue(hist.getStatistics().getMin() >= 0);
  }

  @Test
  void testRemoteIndexLookupLatencyIsNonNegative() {
    metrics.startRemoteIndexLookup();
    metrics.endRemoteIndexLookup();

    Histogram hist = metricGroup.getHistogram("remoteIndexLookupLatency");
    assertTrue(hist.getStatistics().getMin() >= 0);
  }

  @Test
  void testEndLocalIndexLookupWithoutStartRecordsZero() {
    metrics.endLocalIndexLookup();

    Histogram hist = metricGroup.getHistogram("localIndexLookupLatency");
    assertEquals(1, hist.getCount());
    assertEquals(0, hist.getStatistics().getMax());
  }

  @Test
  void testEndRemoteIndexLookupWithoutStartRecordsZero() {
    metrics.endRemoteIndexLookup();

    Histogram hist = metricGroup.getHistogram("remoteIndexLookupLatency");
    assertEquals(1, hist.getCount());
    assertEquals(0, hist.getStatistics().getMax());
  }

  @Test
  void testVisibleForTestingGetters() {
    assertEquals(0, metrics.getLocalIndexLookupCount());
    assertEquals(0, metrics.getRemoteIndexLookupCount());
    assertEquals(0, metrics.getLocalLookupKeysSampleCount());
    assertEquals(0, metrics.getRemoteLookupKeysSampleCount());

    metrics.startLocalIndexLookup();
    metrics.endLocalIndexLookup();
    assertEquals(1, metrics.getLocalIndexLookupCount());

    metrics.startRemoteIndexLookup();
    metrics.endRemoteIndexLookup();
    assertEquals(1, metrics.getRemoteIndexLookupCount());

    metrics.updateLocalLookupKeysCount(4);
    assertEquals(1, metrics.getLocalLookupKeysSampleCount());

    metrics.updateRemoteLookupKeysCount(2);
    assertEquals(1, metrics.getRemoteLookupKeysSampleCount());
  }

  @Test
  void testCombinedLocalAndRemoteLookupInOneRound() {
    metrics.startLocalIndexLookup();
    metrics.endLocalIndexLookup();
    metrics.updateLocalLookupKeysCount(3);
    metrics.startRemoteIndexLookup();
    metrics.endRemoteIndexLookup();
    metrics.updateRemoteLookupKeysCount(2);

    assertEquals(1, metrics.getLocalIndexLookupCount());
    assertEquals(1, metrics.getRemoteIndexLookupCount());
    assertEquals(1, metrics.getLocalLookupKeysSampleCount());
    assertEquals(1, metrics.getRemoteLookupKeysSampleCount());
  }

  @Test
  void testUpdateLookupKeyCountsWithZero() {
    metrics.updateLocalLookupKeysCount(0);
    metrics.updateRemoteLookupKeysCount(0);

    Histogram localHist = metricGroup.getHistogram("localLookupKeysNum");
    Histogram remoteHist = metricGroup.getHistogram("remoteLookupKeysNum");
    assertEquals(1, localHist.getCount());
    assertEquals(0, localHist.getStatistics().getMax());
    assertEquals(1, remoteHist.getCount());
    assertEquals(0, remoteHist.getStatistics().getMax());
  }

  private static class CapturingMetricGroup extends UnregisteredMetricsGroup {
    private final Map<String, Histogram> histograms = new HashMap<>();
    private final Map<String, Gauge<?>> gauges = new HashMap<>();

    @Override
    public <H extends Histogram> H histogram(String name, H histogram) {
      histograms.put(name, histogram);
      return histogram;
    }

    @Override
    public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
      gauges.put(name, gauge);
      return gauge;
    }

    Histogram getHistogram(String name) {
      return histograms.get(name);
    }

    Gauge<?> getGauge(String name) {
      return gauges.get(name);
    }
  }
}
