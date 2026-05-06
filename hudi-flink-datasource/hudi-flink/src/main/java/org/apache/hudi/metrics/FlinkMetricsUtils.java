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

import org.apache.flink.metrics.MetricGroup;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Utils for Flink metrics registration.
 */
public class FlinkMetricsUtils {

  /**
   * Registers metadata table metrics with Flink for the first time.
   * Use the three-argument overload when the metadata table may be reloaded, to avoid
   * duplicate gauge registration.
   */
  public static void registerMetadataTableMetrics(HoodieBackedTableMetadata metadataTable, MetricGroup metricGroup) {
    registerMetadataTableMetrics(metadataTable, metricGroup, null);
  }

  /**
   * Registers or updates metadata table metrics in Flink's metric group.
   *
   * <p>On the first call ({@code handles} is {@code null} or empty) each metric name is
   * registered once with Flink as a gauge whose value is read through an
   * {@link AtomicReference}.  On subsequent calls the references are swapped to point at
   * the new metric objects from the reloaded metadata table, so Flink never sees a
   * duplicate registration.
   *
   * @param metadataTable the (possibly reloaded) metadata table
   * @param metricGroup   Flink metric group to register gauges into
   * @param handles       map returned by a previous call, or {@code null} for the first call
   * @return updated handles map; pass it back on the next invocation
   */
  public static Map<String, AtomicReference<Supplier<Object>>> registerMetadataTableMetrics(
      HoodieBackedTableMetadata metadataTable,
      MetricGroup metricGroup,
      Map<String, AtomicReference<Supplier<Object>>> handles) {

    Map<String, AtomicReference<Supplier<Object>>> result = handles != null ? handles : new HashMap<>();
    if (metadataTable == null) {
      return result;
    }

    metadataTable.getMetrics().ifPresent(m -> {
      m.registry().getGauges().forEach((name, gauge) ->
          bindMetric(result, metricGroup, name, () -> gauge.getValue()));
      m.registry().getCounters().forEach((name, counter) ->
          bindMetric(result, metricGroup, name, () -> counter.getCount()));
      m.registry().getMeters().forEach((name, meter) -> {
        bindMetric(result, metricGroup, name + ".count", () -> meter.getCount());
        bindMetric(result, metricGroup, name + ".meanRate", () -> meter.getMeanRate());
        bindMetric(result, metricGroup, name + ".1minRate", () -> meter.getOneMinuteRate());
        bindMetric(result, metricGroup, name + ".5minRate", () -> meter.getFiveMinuteRate());
        bindMetric(result, metricGroup, name + ".15minRate", () -> meter.getFifteenMinuteRate());
      });
      m.registry().getHistograms().forEach((name, histogram) -> {
        bindMetric(result, metricGroup, name + ".count", () -> histogram.getCount());
        bindMetric(result, metricGroup, name + ".mean", () -> histogram.getSnapshot().getMean());
        bindMetric(result, metricGroup, name + ".min", () -> histogram.getSnapshot().getMin());
        bindMetric(result, metricGroup, name + ".max", () -> histogram.getSnapshot().getMax());
        bindMetric(result, metricGroup, name + ".p75", () -> histogram.getSnapshot().get75thPercentile());
        bindMetric(result, metricGroup, name + ".p95", () -> histogram.getSnapshot().get95thPercentile());
        bindMetric(result, metricGroup, name + ".p99", () -> histogram.getSnapshot().get99thPercentile());
      });
      m.registry().getTimers().forEach((name, timer) -> {
        bindMetric(result, metricGroup, name + ".count", () -> timer.getCount());
        bindMetric(result, metricGroup, name + ".meanRate", () -> timer.getMeanRate());
        bindMetric(result, metricGroup, name + ".1minRate", () -> timer.getOneMinuteRate());
        bindMetric(result, metricGroup, name + ".5minRate", () -> timer.getFiveMinuteRate());
        bindMetric(result, metricGroup, name + ".15minRate", () -> timer.getFifteenMinuteRate());
        bindMetric(result, metricGroup, name + ".mean", () -> timer.getSnapshot().getMean());
        bindMetric(result, metricGroup, name + ".p75", () -> timer.getSnapshot().get75thPercentile());
        bindMetric(result, metricGroup, name + ".p95", () -> timer.getSnapshot().get95thPercentile());
        bindMetric(result, metricGroup, name + ".p99", () -> timer.getSnapshot().get99thPercentile());
      });
    });

    return result;
  }

  /**
   * Registers a Flink gauge for {@code name} the first time it is seen, or updates the
   * supplier reference when the metric is already registered.
   */
  private static void bindMetric(
      Map<String, AtomicReference<Supplier<Object>>> handles,
      MetricGroup metricGroup,
      String name,
      Supplier<Object> supplier) {

    AtomicReference<Supplier<Object>> ref = handles.get(name);
    if (ref == null) {
      ref = new AtomicReference<>(supplier);
      handles.put(name, ref);
      AtomicReference<Supplier<Object>> capturedRef = ref;
      metricGroup.gauge(name, () -> capturedRef.get().get());
    } else {
      ref.set(supplier);
    }
  }
}
