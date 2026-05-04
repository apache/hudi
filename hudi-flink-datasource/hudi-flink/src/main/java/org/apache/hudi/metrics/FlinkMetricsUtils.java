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

/**
 * Utils for Flink metrics registration.
 */
public class FlinkMetricsUtils {

  public static void registerMetadataTableMetrics(HoodieBackedTableMetadata metadataTable, MetricGroup metricGroup) {
    if (metadataTable == null) {
      return;
    }

    // metrics is Option.empty() when metrics are disabled
    metadataTable.getMetrics().ifPresent(m -> {
      m.registry().getGauges().forEach((name, gauge) ->
          metricGroup.gauge(name, gauge::getValue));
      m.registry().getCounters().forEach((name, counter) ->
          metricGroup.gauge(name, counter::getCount));
      m.registry().getMeters().forEach((name, meter) -> {
        metricGroup.gauge(name + ".count", meter::getCount);
        metricGroup.gauge(name + ".meanRate", meter::getMeanRate);
        metricGroup.gauge(name + ".1minRate", meter::getOneMinuteRate);
        metricGroup.gauge(name + ".5minRate", meter::getFiveMinuteRate);
        metricGroup.gauge(name + ".15minRate", meter::getFifteenMinuteRate);
      });
      m.registry().getHistograms().forEach((name, histogram) -> {
        metricGroup.gauge(name + ".count", histogram::getCount);
        metricGroup.gauge(name + ".mean", () -> histogram.getSnapshot().getMean());
        metricGroup.gauge(name + ".min", () -> histogram.getSnapshot().getMin());
        metricGroup.gauge(name + ".max", () -> histogram.getSnapshot().getMax());
        metricGroup.gauge(name + ".p75", () -> histogram.getSnapshot().get75thPercentile());
        metricGroup.gauge(name + ".p95", () -> histogram.getSnapshot().get95thPercentile());
        metricGroup.gauge(name + ".p99", () -> histogram.getSnapshot().get99thPercentile());
      });
      m.registry().getTimers().forEach((name, timer) -> {
        metricGroup.gauge(name + ".count", timer::getCount);
        metricGroup.gauge(name + ".meanRate", timer::getMeanRate);
        metricGroup.gauge(name + ".1minRate", timer::getOneMinuteRate);
        metricGroup.gauge(name + ".5minRate", timer::getFiveMinuteRate);
        metricGroup.gauge(name + ".15minRate", timer::getFifteenMinuteRate);
        metricGroup.gauge(name + ".mean", () -> timer.getSnapshot().getMean());
        metricGroup.gauge(name + ".p75", () -> timer.getSnapshot().get75thPercentile());
        metricGroup.gauge(name + ".p95", () -> timer.getSnapshot().get95thPercentile());
        metricGroup.gauge(name + ".p99", () -> timer.getSnapshot().get99thPercentile());
      });
    });
  }
}
