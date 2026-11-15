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

import org.apache.hudi.common.model.HoodieTimelineTimeZone;
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
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

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
    createMetrics(() -> HoodieTimelineTimeZone.LOCAL, toInstant(INSTANT));
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
    metrics.startSplit("split-1", INSTANT);
    assertEquals(toInstant(INSTANT).getEpochSecond(), gauge("splitLatestCommit"));

    clock.advance(Duration.ofMinutes(10));
    assertEquals(600L, gauge("splitLatestCommitDelay"));
    clock.advance(Duration.ofMinutes(10));
    assertEquals(1200L, gauge("splitLatestCommitDelay"));
  }

  @Test
  void testSplitLatestCommitDelayIsZeroWithoutAnActiveSplit() throws ParseException {
    metrics.startSplit("split-1", INSTANT);
    clock.advance(Duration.ofMinutes(10));
    metrics.finishSplit("split-1");

    assertEquals(0L, gauge("splitLatestCommitDelay"));
    assertEquals(toInstant(INSTANT).getEpochSecond(), gauge("splitLatestCommit"),
        "the last split's commit is still reported");

    metrics.startSplit("split-2", INSTANT);
    assertEquals(600L, gauge("splitLatestCommitDelay"));
  }

  @Test
  void testSplitLatestCommitDelayReportsTheOldestActiveSplit() throws ParseException {
    metrics.startSplit("split-1", INSTANT);
    metrics.startSplit("split-2", NEWER_INSTANT);
    clock.advance(Duration.ofHours(2));

    assertEquals(7200L, gauge("splitLatestCommitDelay"));
    assertEquals(toInstant(NEWER_INSTANT).getEpochSecond(), gauge("splitLatestCommit"));
  }

  @Test
  void testFinishingAnOlderSplitKeepsTheNewerSplitActive() {
    metrics.startSplit("split-1", INSTANT);
    metrics.startSplit("split-2", NEWER_INSTANT);
    clock.advance(Duration.ofHours(2));

    metrics.finishSplit("split-1");

    assertEquals(3600L, gauge("splitLatestCommitDelay"));
  }

  @Test
  void testFinishingASplitThatWasNotStartedIsIgnored() {
    metrics.startSplit("split-1", INSTANT);
    clock.advance(Duration.ofMinutes(10));

    metrics.finishSplit("split-2");

    assertEquals(600L, gauge("splitLatestCommitDelay"));
  }

  @Test
  void testInvalidInstantKeepsThePreviousValue() throws ParseException {
    metrics.setIssuedInstant(INSTANT);
    metrics.setIssuedInstant("not-an-instant");
    assertEquals(toInstant(INSTANT).getEpochSecond(), gauge("issuedInstant"));

    metrics.startSplit("split-1", "not-an-instant");
    assertEquals(0L, gauge("splitLatestCommit"));
    assertEquals(0L, gauge("splitLatestCommitDelay"));
  }

  @Test
  void testUtcTimelineIsReadInUtcWhateverTheReaderJvmZone() {
    withDefaultZone("Asia/Shanghai", () -> {
      createMetrics(() -> HoodieTimelineTimeZone.UTC, Instant.parse("2026-11-11T10:10:00Z"));
      long commitEpochSecond = Instant.parse("2026-11-11T10:00:00Z").getEpochSecond();

      metrics.setIssuedInstant("20261111100000000");
      metrics.startSplit("split-1", "20261111100000000");

      assertEquals(600L, gauge("issuedInstantDelay"));
      assertEquals(600L, gauge("splitLatestCommitDelay"));
      assertEquals(commitEpochSecond, gauge("issuedInstant"));
      assertEquals(commitEpochSecond, gauge("splitLatestCommit"));
    });
  }

  @Test
  void testLocalTimelineIsReadInTheZoneOfTheReader() {
    withDefaultZone("Asia/Shanghai", () -> {
      createMetrics(() -> HoodieTimelineTimeZone.LOCAL, Instant.parse("2026-11-11T10:10:00Z"));
      long commitEpochSecond = Instant.parse("2026-11-11T10:00:00Z").getEpochSecond();

      metrics.setIssuedInstant("20261111180000000");
      metrics.startSplit("split-1", "20261111180000000");

      assertEquals(600L, gauge("issuedInstantDelay"));
      assertEquals(600L, gauge("splitLatestCommitDelay"));
      assertEquals(commitEpochSecond, gauge("issuedInstant"));
      assertEquals(commitEpochSecond, gauge("splitLatestCommit"));
    });
  }

  @Test
  void testTimelineTimeZoneIsLookedUpWhenAnInstantIsParsed() {
    withDefaultZone("Asia/Shanghai", () -> {
      // the table does not exist yet, then it is created with a UTC timeline
      AtomicReference<HoodieTimelineTimeZone> tableTimeZone = new AtomicReference<>(HoodieTimelineTimeZone.LOCAL);
      createMetrics(tableTimeZone::get, Instant.parse("2026-11-11T10:10:00Z"));
      tableTimeZone.set(HoodieTimelineTimeZone.UTC);

      metrics.setIssuedInstant("20261111100000000");

      assertEquals(600L, gauge("issuedInstantDelay"));
      assertEquals(Instant.parse("2026-11-11T10:00:00Z").getEpochSecond(), gauge("issuedInstant"));
    });
  }

  private void createMetrics(Supplier<HoodieTimelineTimeZone> timelineTimeZone, Instant now) {
    metricGroup = new CapturingMetricGroup();
    clock = new MutableClock(now);
    metrics = new FlinkStreamReadMetrics(metricGroup, "test_table", timelineTimeZone, clock);
    metrics.registerMetrics();
  }

  private static void withDefaultZone(String zoneId, Runnable body) {
    TimeZone original = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone(zoneId));
    try {
      body.run();
    } finally {
      TimeZone.setDefault(original);
    }
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
