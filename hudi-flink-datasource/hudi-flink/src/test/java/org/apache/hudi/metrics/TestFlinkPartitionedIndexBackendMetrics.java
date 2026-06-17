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
 * Tests for {@link FlinkPartitionedIndexBackendMetrics}.
 */
class TestFlinkPartitionedIndexBackendMetrics {

  private CapturingMetricGroup metricGroup;
  private FlinkPartitionedIndexBackendMetrics metrics;

  @BeforeEach
  void setUp() {
    metricGroup = new CapturingMetricGroup();
    metrics = new FlinkPartitionedIndexBackendMetrics(metricGroup);
    metrics.registerMetrics();
  }

  @Test
  void testRegisterMetricsRegistersHistograms() {
    assertNotNull(metricGroup.getHistogram(FlinkPartitionedIndexBackendMetrics.PARTITION_BOOTSTRAP_LATENCY_MILLIS));
    assertNotNull(metricGroup.getHistogram(FlinkPartitionedIndexBackendMetrics.PARTITION_BOOTSTRAP_KEYS_LOADED));
  }

  @Test
  void testPartitionBootstrapUpdatesLatencyHistogramCount() {
    Histogram hist = metricGroup.getHistogram(FlinkPartitionedIndexBackendMetrics.PARTITION_BOOTSTRAP_LATENCY_MILLIS);
    assertEquals(0, hist.getCount());

    metrics.startPartitionBootstrap();
    metrics.endPartitionBootstrap();
    assertEquals(1, hist.getCount());

    metrics.startPartitionBootstrap();
    metrics.endPartitionBootstrap();
    assertEquals(2, hist.getCount());
  }

  @Test
  void testPartitionBootstrapLatencyIsNonNegative() {
    metrics.startPartitionBootstrap();
    metrics.endPartitionBootstrap();

    Histogram hist = metricGroup.getHistogram(FlinkPartitionedIndexBackendMetrics.PARTITION_BOOTSTRAP_LATENCY_MILLIS);
    assertTrue(hist.getStatistics().getMin() >= 0);
  }

  @Test
  void testEndPartitionBootstrapWithoutStartRecordsZero() {
    metrics.endPartitionBootstrap();

    Histogram hist = metricGroup.getHistogram(FlinkPartitionedIndexBackendMetrics.PARTITION_BOOTSTRAP_LATENCY_MILLIS);
    assertEquals(1, hist.getCount());
    assertEquals(0, hist.getStatistics().getMax());
  }

  @Test
  void testUpdatePartitionBootstrapKeysLoadedUpdatesHistogram() {
    Histogram hist = metricGroup.getHistogram(FlinkPartitionedIndexBackendMetrics.PARTITION_BOOTSTRAP_KEYS_LOADED);
    assertEquals(0, hist.getCount());

    metrics.updatePartitionBootstrapKeysLoaded(10);
    assertEquals(1, hist.getCount());
    assertEquals(10, hist.getStatistics().getMax());

    metrics.updatePartitionBootstrapKeysLoaded(20);
    assertEquals(2, hist.getCount());
    assertEquals(20, hist.getStatistics().getMax());
  }

  @Test
  void testUpdatePartitionBootstrapKeysLoadedAcceptsZero() {
    metrics.updatePartitionBootstrapKeysLoaded(0);

    Histogram hist = metricGroup.getHistogram(FlinkPartitionedIndexBackendMetrics.PARTITION_BOOTSTRAP_KEYS_LOADED);
    assertEquals(1, hist.getCount());
    assertEquals(0, hist.getStatistics().getMax());
  }

  @Test
  void testVisibleForTestingGetters() {
    assertEquals(0, metrics.getPartitionBootstrapCount());
    assertEquals(0, metrics.getPartitionBootstrapKeysLoadedSampleCount());

    metrics.startPartitionBootstrap();
    metrics.endPartitionBootstrap();
    assertEquals(1, metrics.getPartitionBootstrapCount());

    metrics.updatePartitionBootstrapKeysLoaded(7);
    assertEquals(1, metrics.getPartitionBootstrapKeysLoadedSampleCount());
  }

  @Test
  void testCombinedBootstrapRecordsBothHistograms() {
    metrics.startPartitionBootstrap();
    metrics.endPartitionBootstrap();
    metrics.updatePartitionBootstrapKeysLoaded(42);

    assertEquals(1, metrics.getPartitionBootstrapCount());
    assertEquals(1, metrics.getPartitionBootstrapKeysLoadedSampleCount());
    Histogram keysHist = metricGroup.getHistogram(FlinkPartitionedIndexBackendMetrics.PARTITION_BOOTSTRAP_KEYS_LOADED);
    assertEquals(42, keysHist.getStatistics().getMax());
  }

  private static class CapturingMetricGroup extends UnregisteredMetricsGroup {
    private final Map<String, Histogram> histograms = new HashMap<>();

    @Override
    public <H extends Histogram> H histogram(String name, H histogram) {
      histograms.put(name, histogram);
      return histogram;
    }

    Histogram getHistogram(String name) {
      return histograms.get(name);
    }
  }
}
