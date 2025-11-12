/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.metrics.m3;

import org.apache.hudi.common.util.collection.Pair;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.uber.m3.tally.Scope;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of com.codahale.metrics.ScheduledReporter, to emit metrics from
 * com.codahale.metrics.MetricRegistry to M3
 */
public class M3ScopeReporterAdaptor extends ScheduledReporter {
  private final Scope scope;

  protected M3ScopeReporterAdaptor(MetricRegistry registry, Scope scope) {
    super(registry, "hudi-m3-reporter", MetricFilter.ALL, TimeUnit.SECONDS, TimeUnit.SECONDS);
    this.scope = scope;
  }

  @Override
  public void start(long period, TimeUnit unit) {
  }

  @Override
  public void stop() {
  }

  @Override
  public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters,
      SortedMap<String, Timer> timers) {
    /*
      When reporting, process each com.codahale.metrics metric and add counters & gauges to
      the passed-in com.uber.m3.tally.Scope with the same name and value. This is needed
      for the Scope to register these metrics
    */
    report(scope,
        gauges,
        counters,
        histograms,
        meters,
        timers);
  }

  private void report(Scope scope,
      Map<String, Gauge> gauges,
      Map<String, Counter> counters,
      Map<String, Histogram> histograms,
      Map<String, Meter> meters,
      Map<String, Timer> timers) {

    for (Entry<String, Gauge> entry : gauges.entrySet()) {
      scope.gauge(entry.getKey()).update(
          ((Number) entry.getValue().getValue()).doubleValue());
    }

    for (Entry<String, Counter> entry : counters.entrySet()) {
      scope.counter(entry.getKey()).inc(
          ((Number) entry.getValue().getCount()).longValue());
    }

    for (Entry<String, Histogram> entry : histograms.entrySet()) {
      scope.gauge(MetricRegistry.name(entry.getKey(), "count")).update(
          entry.getValue().getCount());
      reportSnapshot(entry.getKey(), entry.getValue().getSnapshot());
    }

    for (Entry<String, Meter> entry : meters.entrySet()) {
      reportMetered(entry.getKey(), entry.getValue());
    }

    for (Entry<String, Timer> entry : timers.entrySet()) {
      reportTimer(entry.getKey(), entry.getValue());
    }
  }

  private void reportMetered(String name, Metered meter) {
    scope.counter(MetricRegistry.name(name, "count")).inc(meter.getCount());
    List<Pair<String, Double>> meterGauges = Arrays.asList(
        Pair.of("m1_rate", meter.getOneMinuteRate()),
        Pair.of("m5_rate", meter.getFiveMinuteRate()),
        Pair.of("m15_rate", meter.getFifteenMinuteRate()),
        Pair.of("mean_rate", meter.getMeanRate())
    );
    for (Pair<String, Double> pair : meterGauges) {
      scope.gauge(MetricRegistry.name(name, pair.getLeft())).update(pair.getRight());
    }
  }

  private void reportSnapshot(String name, Snapshot snapshot) {
    List<Pair<String, Number>> snapshotGauges = Arrays.asList(
        Pair.of("max", snapshot.getMax()),
        Pair.of("mean", snapshot.getMean()),
        Pair.of("min", snapshot.getMin()),
        Pair.of("stddev", snapshot.getStdDev()),
        Pair.of("p50", snapshot.getMedian()),
        Pair.of("p75", snapshot.get75thPercentile()),
        Pair.of("p95", snapshot.get95thPercentile()),
        Pair.of("p98", snapshot.get98thPercentile()),
        Pair.of("p99", snapshot.get99thPercentile()),
        Pair.of("p999", snapshot.get999thPercentile())
    );
    for (Pair<String, Number> pair : snapshotGauges) {
      scope.gauge(MetricRegistry.name(name, pair.getLeft())).update(pair.getRight().doubleValue());
    }
  }

  private void reportTimer(String name, Timer timer) {
    reportMetered(name, timer);
    reportSnapshot(name, timer.getSnapshot());
  }

}
