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

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieMetadataMetrics;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestFlinkMetricsUtils {

  // -------------------------------------------------------------------------
  //  Null guard
  // -------------------------------------------------------------------------

  @Test
  void testNullTableIsNoOp() {
    MetricGroup metricGroup = mock(MetricGroup.class);
    FlinkMetricsUtils.registerMetadataTableMetrics(null, metricGroup);
    verify(metricGroup, never()).gauge(any(), any());
  }

  // -------------------------------------------------------------------------
  //  Counter
  // -------------------------------------------------------------------------

  @Test
  void testCounterBridgedAsGauge() {
    MetricRegistry dropwizard = new MetricRegistry();
    com.codahale.metrics.Counter counter = dropwizard.counter("hits");
    counter.inc(7);

    Map<String, Gauge<?>> captured = new HashMap<>();
    FlinkMetricsUtils.registerMetadataTableMetrics(tableWith(dropwizard), trackingGroup(captured));

    assertNotNull(captured.get("hits"), "counter 'hits' should be registered as a Flink gauge");
    assertEquals(7L, captured.get("hits").getValue());

    counter.inc(3);
    assertEquals(10L, captured.get("hits").getValue(), "gauge should reflect live counter value");
  }

  // -------------------------------------------------------------------------
  //  Meter
  // -------------------------------------------------------------------------

  @Test
  void testMeterBridgesAllFiveRates() {
    MetricRegistry dropwizard = new MetricRegistry();
    dropwizard.meter("writes");

    Map<String, Gauge<?>> captured = new HashMap<>();
    FlinkMetricsUtils.registerMetadataTableMetrics(tableWith(dropwizard), trackingGroup(captured));

    assertNotNull(captured.get("writes.count"), "count");
    assertNotNull(captured.get("writes.meanRate"), "meanRate");
    assertNotNull(captured.get("writes.1minRate"), "1minRate");
    assertNotNull(captured.get("writes.5minRate"), "5minRate");
    assertNotNull(captured.get("writes.15minRate"), "15minRate");
    assertEquals(5, captured.size());
  }

  @Test
  void testMeterCountGaugeLive() {
    MetricRegistry dropwizard = new MetricRegistry();
    Meter meter = dropwizard.meter("events");

    Map<String, Gauge<?>> captured = new HashMap<>();
    FlinkMetricsUtils.registerMetadataTableMetrics(tableWith(dropwizard), trackingGroup(captured));

    meter.mark(5);
    assertEquals(5L, captured.get("events.count").getValue());
    meter.mark(3);
    assertEquals(8L, captured.get("events.count").getValue());
  }

  // -------------------------------------------------------------------------
  //  Histogram
  // -------------------------------------------------------------------------

  @Test
  void testHistogramBridgesAllSevenStats() {
    MetricRegistry dropwizard = new MetricRegistry();
    dropwizard.register("latency", new Histogram(new ExponentiallyDecayingReservoir()));

    Map<String, Gauge<?>> captured = new HashMap<>();
    FlinkMetricsUtils.registerMetadataTableMetrics(tableWith(dropwizard), trackingGroup(captured));

    assertNotNull(captured.get("latency.count"), "count");
    assertNotNull(captured.get("latency.mean"), "mean");
    assertNotNull(captured.get("latency.min"), "min");
    assertNotNull(captured.get("latency.max"), "max");
    assertNotNull(captured.get("latency.p75"), "p75");
    assertNotNull(captured.get("latency.p95"), "p95");
    assertNotNull(captured.get("latency.p99"), "p99");
    assertEquals(7, captured.size());
  }

  @Test
  void testHistogramStatsReflectRecordedValues() {
    MetricRegistry dropwizard = new MetricRegistry();
    Histogram histogram = dropwizard.register("size", new Histogram(new ExponentiallyDecayingReservoir()));
    for (int i = 1; i <= 100; i++) {
      histogram.update(i);
    }

    Map<String, Gauge<?>> captured = new HashMap<>();
    FlinkMetricsUtils.registerMetadataTableMetrics(tableWith(dropwizard), trackingGroup(captured));

    assertEquals(100L, captured.get("size.count").getValue());
    assertEquals(1L, captured.get("size.min").getValue());
    assertEquals(100L, captured.get("size.max").getValue());
  }

  // -------------------------------------------------------------------------
  //  Timer
  // -------------------------------------------------------------------------

  @Test
  void testTimerBridgesAllNineStats() {
    MetricRegistry dropwizard = new MetricRegistry();
    dropwizard.register("rpc", new Timer());

    Map<String, Gauge<?>> captured = new HashMap<>();
    FlinkMetricsUtils.registerMetadataTableMetrics(tableWith(dropwizard), trackingGroup(captured));

    // Rate metrics (same set as Meter)
    assertNotNull(captured.get("rpc.count"), "count");
    assertNotNull(captured.get("rpc.meanRate"), "meanRate");
    assertNotNull(captured.get("rpc.1minRate"), "1minRate");
    assertNotNull(captured.get("rpc.5minRate"), "5minRate");
    assertNotNull(captured.get("rpc.15minRate"), "15minRate");
    // Snapshot metrics
    assertNotNull(captured.get("rpc.mean"), "mean");
    assertNotNull(captured.get("rpc.p75"), "p75");
    assertNotNull(captured.get("rpc.p95"), "p95");
    assertNotNull(captured.get("rpc.p99"), "p99");
    assertEquals(9, captured.size());
  }

  @Test
  void testTimerAndMeterExposeTheSameRateGauges() {
    MetricRegistry meterRegistry = new MetricRegistry();
    meterRegistry.meter("op");

    MetricRegistry timerRegistry = new MetricRegistry();
    timerRegistry.register("op", new Timer());

    Map<String, Gauge<?>> meterGauges = new HashMap<>();
    Map<String, Gauge<?>> timerGauges = new HashMap<>();
    FlinkMetricsUtils.registerMetadataTableMetrics(tableWith(meterRegistry), trackingGroup(meterGauges));
    FlinkMetricsUtils.registerMetadataTableMetrics(tableWith(timerRegistry), trackingGroup(timerGauges));

    // Every rate key that Meter exposes must also appear in Timer
    for (String rateKey : new String[]{"op.count", "op.meanRate", "op.1minRate", "op.5minRate", "op.15minRate"}) {
      assertNotNull(meterGauges.get(rateKey), "meter missing " + rateKey);
      assertNotNull(timerGauges.get(rateKey), "timer missing " + rateKey);
    }
  }

  // -------------------------------------------------------------------------
  //  Helpers
  // -------------------------------------------------------------------------

  private static HoodieBackedTableMetadata tableWith(MetricRegistry dropwizard) {
    HoodieMetadataMetrics mockMetrics = mock(HoodieMetadataMetrics.class);
    when(mockMetrics.registry()).thenReturn(dropwizard);

    HoodieBackedTableMetadata mockTable = mock(HoodieBackedTableMetadata.class);
    when(mockTable.getMetrics()).thenReturn(Option.of(mockMetrics));
    return mockTable;
  }

  private static MetricGroup trackingGroup(Map<String, Gauge<?>> captured) {
    MetricGroup group = mock(MetricGroup.class);
    doAnswer(inv -> {
      captured.put(inv.getArgument(0), inv.getArgument(1));
      return null;
    }).when(group).gauge(any(String.class), any());
    return group;
  }
}
