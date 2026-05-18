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
 * Tests for {@link FlinkBucketAssignMetrics}.
 */
class TestFlinkBucketAssignMetrics {

  private CapturingMetricGroup metricGroup;
  private FlinkBucketAssignMetrics metrics;

  @BeforeEach
  void setUp() {
    metricGroup = new CapturingMetricGroup();
    metrics = new FlinkBucketAssignMetrics(metricGroup);
    metrics.registerMetrics();
  }

  @Test
  void testRegisterMetricsRegistersHistograms() {
    assertNotNull(metricGroup.getHistogram("recordBufferingTime"));
  }

  @Test
  void testRecordBufferingUpdatesHistogramCount() {
    Histogram hist = metricGroup.getHistogram("recordBufferingTime");
    assertEquals(0, hist.getCount());

    metrics.startRecordBuffering();
    metrics.endRecordBuffering();
    assertEquals(1, hist.getCount());
  }

  @Test
  void testRecordBufferingTimeIsNonNegative() {
    metrics.startRecordBuffering();
    metrics.endRecordBuffering();

    Histogram hist = metricGroup.getHistogram("recordBufferingTime");
    assertTrue(hist.getStatistics().getMin() >= 0);
  }

  @Test
  void testEndRecordBufferingWithoutStartRecordsZero() {
    metrics.endRecordBuffering();

    Histogram hist = metricGroup.getHistogram("recordBufferingTime");
    assertEquals(1, hist.getCount());
    assertEquals(0, hist.getStatistics().getMax());
  }

  @Test
  void testVisibleForTestingGetters() {
    assertEquals(0, metrics.getRecordBufferingCount());

    metrics.startRecordBuffering();
    metrics.endRecordBuffering();
    assertEquals(1, metrics.getRecordBufferingCount());
  }

  @Test
  void testMultipleConsecutiveBufferingCycles() {
    for (int i = 0; i < 5; i++) {
      metrics.startRecordBuffering();
      metrics.endRecordBuffering();
    }
    assertEquals(5, metrics.getRecordBufferingCount());
  }

  @Test
  void testTimerRestartBeforeStop() {
    // Calling startRecordBuffering twice before stopping should override the first start.
    // The end call still records exactly one sample.
    metrics.startRecordBuffering();
    metrics.startRecordBuffering();
    metrics.endRecordBuffering();
    assertEquals(1, metrics.getRecordBufferingCount());
  }

  @Test
  void testSlidingWindowCapAtHundred() {
    // SlidingWindowReservoir(100) retains only the 100 most recent samples.
    for (int i = 0; i < 110; i++) {
      metrics.startRecordBuffering();
      metrics.endRecordBuffering();
    }
    assertEquals(110, metrics.getRecordBufferingCount());
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
