/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.metrics;

import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.TimelineLayout;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantComparatorV1;
import org.apache.hudi.common.table.timeline.versioning.v2.ActiveTimelineV2;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;

import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.groups.MetricGroupTest;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieInstant.State.COMPLETED;
import static org.apache.hudi.common.table.timeline.HoodieInstant.State.INFLIGHT;
import static org.apache.hudi.common.table.timeline.HoodieInstant.State.REQUESTED;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLEAN_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.ROLLBACK_ACTION;

public class TestFlinkTimeLineMetrics {

  @Mock
  HoodieWriteConfig config;

  FlinkTimeLineMetrics timeLineMetrics;

  @Test
  public void testCOWHoodieTimeMetric() {
    HoodieActiveTimeline activeTimeline = getHoodieTimeLine();
    this.config = HoodieWriteConfig.newBuilder()
        .withPath("s3://test" + UUID.randomUUID())
        .withMetricsConfig(HoodieMetricsConfig.newBuilder().on(true)
            .withReporterType(MetricsReporterType.INMEMORY.name()).build()).build(false);
    timeLineMetrics = new FlinkTimeLineMetrics(new MetricGroupTest.DummyAbstractMetricGroup(new NoOpMetricRegistry()));
    timeLineMetrics.registerMetrics();
    timeLineMetrics.updateTimeLineMetrics(activeTimeline);
  }

  @Test
  public void testCOWHoodieTimeMetricWithOutMetricGroup() {
    HoodieActiveTimeline activeTimeline = getHoodieTimeLine();
    this.config = HoodieWriteConfig.newBuilder()
        .withPath("s3://test" + UUID.randomUUID())
        .withMetricsConfig(HoodieMetricsConfig.newBuilder().on(true)
            .withReporterType(MetricsReporterType.INMEMORY.name()).build()).build(false);
    timeLineMetrics = new FlinkTimeLineMetrics(null);
    timeLineMetrics.registerMetrics();
    timeLineMetrics.updateTimeLineMetrics(activeTimeline);
  }

  private HoodieActiveTimeline getHoodieTimeLine() {
    HoodieActiveTimeline defaultTimeline = new ActiveTimelineV2();
    List<HoodieInstant> hoodieInstants = Arrays.asList(
        new HoodieInstant(REQUESTED, COMMIT_ACTION, "20240924184901", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR),
        new HoodieInstant(INFLIGHT, COMMIT_ACTION, "20240924184901", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR),
        new HoodieInstant(COMPLETED, COMMIT_ACTION, "20240924184901", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR),
        new HoodieInstant(REQUESTED, REPLACE_COMMIT_ACTION, "20240924184902", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR),
        new HoodieInstant(INFLIGHT, REPLACE_COMMIT_ACTION, "20240924184902", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR),
        new HoodieInstant(COMPLETED, REPLACE_COMMIT_ACTION, "20240924184902", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR),
        new HoodieInstant(REQUESTED, ROLLBACK_ACTION, "20240924184903",InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR),
        new HoodieInstant(INFLIGHT, ROLLBACK_ACTION, "20240924184903",InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR),
        new HoodieInstant(COMPLETED, ROLLBACK_ACTION, "20240924184903", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR),
        new HoodieInstant(REQUESTED, CLEAN_ACTION, "20240924184904", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR),
        new HoodieInstant(INFLIGHT, CLEAN_ACTION, "20240924184904", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR),
        new HoodieInstant(COMPLETED, CLEAN_ACTION, "20240924184904", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR),
        new HoodieInstant(REQUESTED, COMMIT_ACTION, "20240924184905", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR),
        new HoodieInstant(REQUESTED, REPLACE_COMMIT_ACTION, "20240924184906", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR),
        new HoodieInstant(REQUESTED, ROLLBACK_ACTION, "20240924184907", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR),
        new HoodieInstant(REQUESTED, CLEAN_ACTION, "20240924184908", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR));
    Stream<HoodieInstant> hoodieInstantStream = TimelineLayout.TIMELINE_LAYOUT_V2.filterHoodieInstants(hoodieInstants.stream());

    defaultTimeline.setInstants(hoodieInstantStream.sorted().collect(Collectors.toList()));

    return defaultTimeline;
  }
}
