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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.UniformReservoir;
import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Scope;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies that {@link M3ScopeReporterAdaptor} maps codahale registry metrics
 * onto the target m3 tally {@link Scope}. The scope is a mock so that the exact
 * counters and gauges reaching it can be asserted without any network I/O.
 */
public class TestM3ScopeReporterAdaptor {

  private MetricRegistry registry;
  private Scope scope;
  private com.uber.m3.tally.Counter scopeCounter;
  private Gauge scopeGauge;
  private M3ScopeReporterAdaptor reporter;

  @BeforeEach
  public void setUp() {
    registry = new MetricRegistry();
    scope = mock(Scope.class);
    scopeCounter = mock(com.uber.m3.tally.Counter.class);
    scopeGauge = mock(Gauge.class);
    when(scope.counter(org.mockito.ArgumentMatchers.anyString())).thenReturn(scopeCounter);
    when(scope.gauge(org.mockito.ArgumentMatchers.anyString())).thenReturn(scopeGauge);
    reporter = new M3ScopeReporterAdaptor(registry, scope);
  }

  @Test
  public void testCounterIsForwardedToScope() {
    Counter counter = registry.counter("requests");
    counter.inc(7);

    reporter.report();

    verify(scope).counter("requests");
    verify(scopeCounter).inc(7L);
  }

  @Test
  public void testGaugeValueIsForwardedToScope() {
    registry.register("in_flight", (com.codahale.metrics.Gauge<Integer>) () -> 42);

    reporter.report();

    verify(scope).gauge("in_flight");
    verify(scopeGauge).update(42.0d);
  }

  @Test
  public void testHistogramEmitsCountAndSnapshotGauges() {
    Histogram histogram = new Histogram(new UniformReservoir());
    registry.register("latency", histogram);
    histogram.update(10);
    histogram.update(20);

    reporter.report();

    // count is emitted as its own gauge suffix
    verify(scope).gauge("latency.count");
    // the snapshot expands into ten percentile/stat gauges
    verify(scope).gauge("latency.max");
    verify(scope).gauge("latency.mean");
    verify(scope).gauge("latency.min");
    verify(scope).gauge("latency.stddev");
    verify(scope).gauge("latency.p50");
    verify(scope).gauge("latency.p75");
    verify(scope).gauge("latency.p95");
    verify(scope).gauge("latency.p98");
    verify(scope).gauge("latency.p99");
    verify(scope).gauge("latency.p999");
    // no bare "latency" gauge, only the suffixed ones
    verify(scope, never()).gauge("latency");
  }

  @Test
  public void testMeterEmitsCountCounterAndRateGauges() {
    Meter meter = registry.meter("throughput");
    meter.mark(5);

    reporter.report();

    verify(scope).counter("throughput.count");
    verify(scopeCounter).inc(5L);
    verify(scope).gauge("throughput.m1_rate");
    verify(scope).gauge("throughput.m5_rate");
    verify(scope).gauge("throughput.m15_rate");
    verify(scope).gauge("throughput.mean_rate");
  }

  @Test
  public void testTimerEmitsBothMeteredAndSnapshotMetrics() {
    Timer timer = registry.timer("op_time");
    timer.update(java.time.Duration.ofMillis(3));
    timer.update(java.time.Duration.ofMillis(9));

    reporter.report();

    // timer reports the metered count as a counter
    verify(scope).counter("op_time.count");
    // and the four rate gauges from the metered portion
    verify(scope).gauge("op_time.m1_rate");
    verify(scope).gauge("op_time.mean_rate");
    // plus the full snapshot set from the timer's histogram
    verify(scope).gauge("op_time.max");
    verify(scope).gauge("op_time.p99");
  }

  @Test
  public void testEmptyRegistryTouchesNothing() {
    reporter.report();

    verify(scope, never()).counter(org.mockito.ArgumentMatchers.anyString());
    verify(scope, never()).gauge(org.mockito.ArgumentMatchers.anyString());
  }

  @Test
  public void testMultipleGaugesEachMapped() {
    registry.register("g1", (com.codahale.metrics.Gauge<Long>) () -> 1L);
    registry.register("g2", (com.codahale.metrics.Gauge<Long>) () -> 2L);

    reporter.report();

    verify(scope).gauge("g1");
    verify(scope).gauge("g2");
    verify(scopeGauge).update(1.0d);
    verify(scopeGauge).update(2.0d);
    verify(scope, times(2)).gauge(org.mockito.ArgumentMatchers.startsWith("g"));
  }
}
