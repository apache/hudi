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

import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.text.ParseException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link FlinkStreamReadMetrics}.
 */
class TestFlinkStreamReadMetrics {

  private static final String INSTANT = "20261109100000000";
  private static final String NEWER_INSTANT = "20261109110000000";

  private CapturingMetricGroup metricGroup;
  private MutableClock clock;
  private FlinkStreamReadMetrics metrics;

  @BeforeEach
  void setUp() throws ParseException {
    metricGroup = new CapturingMetricGroup();
    clock = new MutableClock(toInstant(INSTANT));
    metrics = new FlinkStreamReadMetrics(metricGroup, "test_table", clock);
    metrics.registerMetrics();
  }

  @Test
  void testGaugesReportZeroBeforeAnyValueIsSet() {
    clock.advance(Duration.ofHours(1));
    assertEquals(0L, gauge("issuedInstant"));
    assertEquals(0L, gauge("issuedInstantDelay"));
    assertEquals(0L, gauge("splitLatestCommit"));
    assertEquals(0L, gauge("splitLatestCommitDelay"));
  }

  @Test
  void testIssuedInstantDelayGrowsUntilANewInstantIsIssued() throws ParseException {
    metrics.setIssuedInstant(INSTANT);
    assertEquals(toInstant(INSTANT).getEpochSecond(), gauge("issuedInstant"));
    assertEquals(0L, gauge("issuedInstantDelay"));

    clock.advance(Duration.ofMinutes(90));
    assertEquals(5400L, gauge("issuedInstantDelay"));

    metrics.setIssuedInstant(NEWER_INSTANT);
    assertEquals(toInstant(NEWER_INSTANT).getEpochSecond(), gauge("issuedInstant"));
    assertEquals(1800L, gauge("issuedInstantDelay"));
  }

  @Test
  void testSplitLatestCommitDelayGrowsWhileReadingASplit() throws ParseException {
    metrics.setSplitLatestCommit(INSTANT);
    assertEquals(toInstant(INSTANT).getEpochSecond(), gauge("splitLatestCommit"));

    clock.advance(Duration.ofMinutes(10));
    assertEquals(600L, gauge("splitLatestCommitDelay"));
    clock.advance(Duration.ofMinutes(10));
    assertEquals(1200L, gauge("splitLatestCommitDelay"));
  }

  @Test
  void testSplitLatestCommitDelayIsZeroWithoutACurrentSplit() throws ParseException {
    metrics.setSplitLatestCommit(INSTANT);
    clock.advance(Duration.ofMinutes(10));
    metrics.clearCurrentSplit();

    assertEquals(0L, gauge("splitLatestCommitDelay"));
    assertEquals(toInstant(INSTANT).getEpochSecond(), gauge("splitLatestCommit"),
        "the last split's commit is still reported");

    metrics.setSplitLatestCommit(INSTANT);
    assertEquals(600L, gauge("splitLatestCommitDelay"));
  }

  @Test
  void testInvalidInstantKeepsThePreviousValue() throws ParseException {
    metrics.setIssuedInstant(INSTANT);
    metrics.setIssuedInstant("not-an-instant");
    assertEquals(toInstant(INSTANT).getEpochSecond(), gauge("issuedInstant"));

    metrics.setSplitLatestCommit("not-an-instant");
    assertEquals(0L, gauge("splitLatestCommit"));
    assertEquals(0L, gauge("splitLatestCommitDelay"));
  }

  private long gauge(String name) {
    return (Long) metricGroup.gauges.get(name).getValue();
  }

  private static Instant toInstant(String instantTime) throws ParseException {
    return HoodieInstantTimeGenerator.parseDateFromInstantTime(instantTime).toInstant();
  }

  private static class CapturingMetricGroup extends UnregisteredMetricsGroup {
    private final Map<String, Gauge<?>> gauges = new HashMap<>();

    @Override
    public MetricGroup addGroup(String key, String value) {
      return this;
    }

    @Override
    public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
      gauges.put(name, gauge);
      return gauge;
    }
  }

  private static class MutableClock extends Clock {
    private Instant now;

    private MutableClock(Instant now) {
      this.now = now;
    }

    private void advance(Duration duration) {
      now = now.plus(duration);
    }

    @Override
    public Instant instant() {
      return now;
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      throw new UnsupportedOperationException();
    }
  }
}
