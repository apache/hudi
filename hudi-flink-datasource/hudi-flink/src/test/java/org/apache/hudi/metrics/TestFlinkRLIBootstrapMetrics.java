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
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.metrics.FlinkRLIBootstrapMetrics.BOOTSTRAP_COST_MS;
import static org.apache.hudi.metrics.FlinkRLIBootstrapMetrics.BOOTSTRAP_RECORD_PER_MS;
import static org.apache.hudi.metrics.FlinkRLIBootstrapMetrics.NUM_FILE_SLICES_PROCESSED;
import static org.apache.hudi.metrics.FlinkRLIBootstrapMetrics.NUM_INDEX_RECORDS_EMITTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Test cases for {@link FlinkRLIBootstrapMetrics}.
 */
class TestFlinkRLIBootstrapMetrics {

  /** Subclass that captures registered gauges so tests can read their values. */
  private static class CapturingMetricGroup extends UnregisteredMetricsGroup {
    final Map<String, Gauge<?>> gauges = new HashMap<>();

    @Override
    public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
      gauges.put(name, gauge);
      return gauge;
    }
  }

  private CapturingMetricGroup metricGroup;
  private FlinkRLIBootstrapMetrics metrics;

  @BeforeEach
  void setUp() {
    metricGroup = new CapturingMetricGroup();
    metrics = new FlinkRLIBootstrapMetrics(metricGroup);
    metrics.registerMetrics();
  }

  // -------------------------------------------------------------------------
  //  Metric name constants
  // -------------------------------------------------------------------------

  @Test
  void testMetricNameConstants() {
    assertEquals("rliBootstrap.numFileSlicesProcessed", NUM_FILE_SLICES_PROCESSED);
    assertEquals("rliBootstrap.numIndexRecordsEmitted", NUM_INDEX_RECORDS_EMITTED);
    assertEquals("rliBootstrap.bootstrapCostMs", BOOTSTRAP_COST_MS);
    assertEquals("rliBootstrap.bootstrapRecordPerMs", BOOTSTRAP_RECORD_PER_MS);
  }

  // -------------------------------------------------------------------------
  //  Gauge registration
  // -------------------------------------------------------------------------

  @Test
  void testAllMetricsAreRegistered() {
    assertEquals(4, metricGroup.gauges.size());
    assertEquals(true, metricGroup.gauges.containsKey(NUM_FILE_SLICES_PROCESSED));
    assertEquals(true, metricGroup.gauges.containsKey(NUM_INDEX_RECORDS_EMITTED));
    assertEquals(true, metricGroup.gauges.containsKey(BOOTSTRAP_COST_MS));
    assertEquals(true, metricGroup.gauges.containsKey(BOOTSTRAP_RECORD_PER_MS));
  }

  @Test
  void testRegisterMetricsWithUnregisteredGroupDoesNotThrow() {
    assertDoesNotThrow(() ->
        new FlinkRLIBootstrapMetrics(new UnregisteredMetricsGroup()).registerMetrics());
  }

  // -------------------------------------------------------------------------
  //  Initial values (before any update)
  // -------------------------------------------------------------------------

  @Test
  void testInitialValuesAreZero() {
    assertEquals(0L, (Long) gaugeValue(NUM_FILE_SLICES_PROCESSED));
    assertEquals(0L, (Long) gaugeValue(NUM_INDEX_RECORDS_EMITTED));
    assertEquals(0L, (Long) gaugeValue(BOOTSTRAP_COST_MS));
    assertEquals(0.0, gaugeValue(BOOTSTRAP_RECORD_PER_MS));
  }

  // -------------------------------------------------------------------------
  //  After updateLoadResult
  // -------------------------------------------------------------------------

  @Test
  void testUpdateLoadResultReflectsInGauges() {
    metrics.updateLoadResult(8, 1000, 500);

    assertEquals(8L, (Long) gaugeValue(NUM_FILE_SLICES_PROCESSED));
    assertEquals(1000L, (Long) gaugeValue(NUM_INDEX_RECORDS_EMITTED));
    assertEquals(500L, (Long) gaugeValue(BOOTSTRAP_COST_MS));
  }

  @Test
  void testThroughputIsRecordsPerSecond() {
    // 2000 records in 500 ms → 4000 records/sec
    metrics.updateLoadResult(4, 2000, 500);
    assertEquals(4.0, gaugeValue(BOOTSTRAP_RECORD_PER_MS));
  }

  @Test
  void testThroughputIsZeroWhenCostIsZero() {
    metrics.updateLoadResult(3, 100, 0);
    assertEquals(0.0, gaugeValue(BOOTSTRAP_RECORD_PER_MS));
  }

  @Test
  void testGaugesReflectLatestUpdate() {
    metrics.updateLoadResult(2, 200, 100);
    metrics.updateLoadResult(5, 500, 250);

    assertEquals(5L, (Long) gaugeValue(NUM_FILE_SLICES_PROCESSED));
    assertEquals(500L, (Long) gaugeValue(NUM_INDEX_RECORDS_EMITTED));
    assertEquals(250L, (Long) gaugeValue(BOOTSTRAP_COST_MS));
    assertEquals(2.0, gaugeValue(BOOTSTRAP_RECORD_PER_MS));
  }

  // -------------------------------------------------------------------------
  //  Helper
  // -------------------------------------------------------------------------

  @SuppressWarnings("unchecked")
  private <T> T gaugeValue(String name) {
    return (T) metricGroup.gauges.get(name).getValue();
  }
}
