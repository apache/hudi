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

import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.sink.utils.EventBuffers;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for {@link FlinkWriteCoordinatorMetrics}.
 */
class TestFlinkWriteCoordinatorMetrics {

  @Test
  void testPendingCommitInstantCountGauge() {
    CapturingMetricGroup metricGroup = new CapturingMetricGroup();
    EventBuffers eventBuffers = EventBuffers.getInstance(new Configuration(), 2);
    FlinkWriteCoordinatorMetrics metrics = new FlinkWriteCoordinatorMetrics(
        metricGroup, () -> eventBuffers.getAllCompletedEvents().size());

    metrics.registerMetrics();

    Gauge<?> gauge = metricGroup.getGauge("pendingCommitInstantCount");
    assertNotNull(gauge);
    assertEquals(0, gauge.getValue());

    eventBuffers.initNewEventBuffer(1L, "001");
    eventBuffers.addEventToBuffer(completedEvent(0));
    eventBuffers.addEventToBuffer(completedEvent(1));
    assertEquals(1, gauge.getValue());

    eventBuffers.reset(1L);
    assertEquals(0, gauge.getValue());
  }

  private static WriteMetadataEvent completedEvent(int taskId) {
    return WriteMetadataEvent.builder()
        .taskID(taskId)
        .checkpointId(1L)
        .instantTime("001")
        .writeStatus(Collections.emptyList())
        .lastBatch(true)
        .build();
  }

  private static class CapturingMetricGroup extends UnregisteredMetricsGroup {
    private final Map<String, Gauge<?>> gauges = new HashMap<>();

    @Override
    public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
      gauges.put(name, gauge);
      return gauge;
    }

    Gauge<?> getGauge(String name) {
      return gauges.get(name);
    }
  }
}
