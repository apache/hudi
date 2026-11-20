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

import org.apache.hudi.source.split.HoodieSourceSplitLagInfo;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link FlinkStreamReadLagMetrics}.
 */
class TestFlinkStreamReadLagMetrics {

  private static final String COMMIT_1 = "20261109095000000";
  private static final String COMMIT_2 = "20261109105000000";
  private static final String COMMIT_3 = "20261109115000000";
  private static final String COMPLETION_1 = "20261109100000000";
  private static final String COMPLETION_2 = "20261109110000000";
  private static final String COMPLETION_3 = "20261109120000000";

  private CapturingMetricGroup metricGroup;
  private MutableClock clock;
  private FlinkStreamReadLagMetrics metrics;

  @BeforeEach
  void setUp() {
    metricGroup = new CapturingMetricGroup();
    clock = new MutableClock(Instant.parse("2026-11-09T12:00:00Z"));
    metrics = new FlinkStreamReadLagMetrics(metricGroup, "test_table", ZoneOffset.UTC, clock);
    metrics.registerMetrics();
  }

  @Test
  void testCompletionTimesAreParsedInTheTimelineZone() {
    TimeZone defaultZone = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
    try {
      metrics.setUndiscoveredCommits(Collections.singletonList(COMPLETION_3));
      clock.advance(Duration.ofSeconds(90));

      assertEquals(90L, gauge("readLagSeconds"));
    } finally {
      TimeZone.setDefault(defaultZone);
    }
  }

  @Test
  void testCompletionTimesOfALocalTimelineAreParsedInItsZone() {
    CapturingMetricGroup shanghaiGroup = new CapturingMetricGroup();
    FlinkStreamReadLagMetrics shanghaiMetrics = new FlinkStreamReadLagMetrics(
        shanghaiGroup, "test_table", ZoneId.of("Asia/Shanghai"), new MutableClock(Instant.parse("2026-11-09T04:01:00Z")));
    shanghaiMetrics.registerMetrics();

    shanghaiMetrics.setUndiscoveredCommits(Collections.singletonList(COMPLETION_3));

    assertEquals(60L, shanghaiGroup.gauges.get("readLagSeconds").getValue());
  }

  @Test
  void testGaugesReportZeroWhenNothingIsPending() {
    assertEquals(0L, gauge("readLagSeconds"));
    assertEquals(0L, gauge("pendingCommits"));
    assertEquals(0L, gauge("undiscoveredCommits"));
    assertEquals(0L, gauge("inFlightCommits"));
    assertEquals(0L, gauge("pendingSplits"));
    assertEquals(0L, gauge("pendingRecords"));
    assertEquals(0L, gauge("pendingBytes"));
  }

  @Test
  void testDiscoveredSplitsArePendingUntilFinished() {
    Map<String, HoodieSourceSplitLagInfo> lagInfos = new HashMap<>();
    lagInfos.put("split-1", lagInfo(COMPLETION_1, 10L, 100L, COMMIT_1));
    lagInfos.put("split-2", lagInfo(COMPLETION_1, 20L, 200L, COMMIT_1));
    metrics.addSplits(lagInfos);
    metrics.finishSplits(Collections.singletonList("split-1"));

    assertEquals(1L, gauge("pendingSplits"));
    assertEquals(20L, gauge("pendingRecords"));
    assertEquals(200L, gauge("pendingBytes"));
    assertEquals(1L, gauge("inFlightCommits"));
  }

  @Test
  void testSplitReadingSeveralCommitsCountsAgainstEachOfThem() {
    metrics.addSplits(Collections.singletonMap("split-1", lagInfo(COMPLETION_1, 0L, 0L, COMMIT_1, COMMIT_2)));

    assertEquals(2L, gauge("inFlightCommits"));
  }

  @Test
  void testCommitIsNotPendingOnceAllOfItsSplitsAreFinished() {
    Map<String, HoodieSourceSplitLagInfo> lagInfos = new HashMap<>();
    lagInfos.put("split-1", lagInfo(COMPLETION_1, 0L, 0L, COMMIT_1));
    lagInfos.put("split-2", lagInfo(COMPLETION_2, 0L, 0L, COMMIT_2));
    metrics.addSplits(lagInfos);
    metrics.finishSplits(Collections.singletonList("split-1"));

    assertEquals(1L, gauge("inFlightCommits"));
    assertEquals(3600L, gauge("readLagSeconds"));
  }

  @Test
  void testReadLagGrowsWhileTheOldestCommitIsBeingRead() {
    metrics.addSplits(Collections.singletonMap("split-1", lagInfo(COMPLETION_1, 0L, 0L, COMMIT_1)));
    clock.advance(Duration.ofMinutes(30));

    assertEquals(9000L, gauge("readLagSeconds"));
  }

  @Test
  void testReadLagFallsBackToTheOldestUndiscoveredCommit() {
    metrics.setUndiscoveredCommits(Arrays.asList(COMPLETION_2, COMPLETION_3));

    assertEquals(3600L, gauge("readLagSeconds"));
    assertEquals(2L, gauge("undiscoveredCommits"));
    assertEquals(2L, gauge("pendingCommits"));
  }

  @Test
  void testPendingCommitsAddUndiscoveredAndInFlightCommits() {
    metrics.addSplits(Collections.singletonMap("split-1", lagInfo(COMPLETION_1, 0L, 0L, COMMIT_1)));
    metrics.setUndiscoveredCommits(Arrays.asList(COMPLETION_2, COMPLETION_3));

    assertEquals(3L, gauge("pendingCommits"));
    assertEquals(7200L, gauge("readLagSeconds"));
  }

  @Test
  void testAddingATrackedSplitAgainIsIgnored() {
    metrics.addSplits(Collections.singletonMap("split-1", lagInfo(COMPLETION_1, 10L, 100L, COMMIT_1)));
    metrics.addSplits(Collections.singletonMap("split-1", lagInfo(COMPLETION_1, 10L, 100L, COMMIT_1)));
    metrics.finishSplits(Collections.singletonList("split-1"));

    assertEquals(0L, gauge("pendingSplits"));
    assertEquals(0L, gauge("pendingRecords"));
    assertEquals(0L, gauge("inFlightCommits"));
    assertEquals(0L, gauge("readLagSeconds"));
  }

  @Test
  void testFinishingAnUnknownSplitIsIgnored() {
    metrics.addSplits(Collections.singletonMap("split-1", lagInfo(COMPLETION_1, 10L, 100L, COMMIT_1)));
    metrics.finishSplits(Collections.singletonList("split-2"));

    assertEquals(1L, gauge("pendingSplits"));
    assertEquals(10L, gauge("pendingRecords"));
  }

  @Test
  void testUpdatingASplitReplacesItsLagInfo() {
    metrics.addSplits(Collections.singletonMap("split-1", lagInfo(null, 0L, 0L, COMMIT_1, COMMIT_2)));
    metrics.updateSplits(Collections.singletonMap("split-1", lagInfo(COMPLETION_2, 20L, 200L, COMMIT_2)));

    assertEquals(1L, gauge("pendingSplits"));
    assertEquals(1L, gauge("inFlightCommits"));
    assertEquals(20L, gauge("pendingRecords"));
    assertEquals(200L, gauge("pendingBytes"));
    assertEquals(3600L, gauge("readLagSeconds"));
  }

  @Test
  void testUpdatingAFinishedSplitIsIgnored() {
    metrics.addSplits(Collections.singletonMap("split-1", lagInfo(null, 0L, 0L, COMMIT_1)));
    metrics.finishSplits(Collections.singletonList("split-1"));
    metrics.updateSplits(Collections.singletonMap("split-1", lagInfo(COMPLETION_1, 20L, 200L, COMMIT_1)));

    assertEquals(0L, gauge("pendingSplits"));
    assertEquals(0L, gauge("pendingRecords"));
    assertEquals(0L, gauge("readLagSeconds"));
  }

  @Test
  void testSplitWithoutCompletionTimeIsPendingButNotInTheReadLag() {
    metrics.addSplits(Collections.singletonMap("split-1", lagInfo(null, 0L, 0L, COMMIT_1)));

    assertEquals(1L, gauge("pendingSplits"));
    assertEquals(1L, gauge("inFlightCommits"));
    assertEquals(0L, gauge("readLagSeconds"));
  }

  @Test
  void testInvalidCompletionTimeReportsZeroReadLag() {
    metrics.setUndiscoveredCommits(Collections.singletonList("not-an-instant"));

    assertEquals(0L, gauge("readLagSeconds"));
  }

  private long gauge(String name) {
    return (Long) metricGroup.gauges.get(name).getValue();
  }

  private static HoodieSourceSplitLagInfo lagInfo(
      String oldestCommitCompletionTime, long records, long bytes, String... commits) {
    return new HoodieSourceSplitLagInfo(Arrays.asList(commits), oldestCommitCompletionTime, records, bytes);
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
