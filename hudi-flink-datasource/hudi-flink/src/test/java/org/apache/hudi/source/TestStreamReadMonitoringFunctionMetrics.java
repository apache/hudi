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

package org.apache.hudi.source;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTimelineTimeZone;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.utils.MockStreamingRuntimeContext;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestUtils;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.CollectingSourceContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for the metrics of {@link StreamReadMonitoringFunction}.
 */
@Slf4j
public class TestStreamReadMonitoringFunctionMetrics {

  private static final DateTimeFormatter UTC_INSTANT_FORMAT =
      DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS").withZone(ZoneOffset.UTC);

  @TempDir
  File tableDir;

  /**
   * The metrics are registered while there is no table to read. Once the table is created with a UTC
   * timeline, the delay of the issued instant must be computed in that timeline, whatever the zone
   * of the JVM that reads it.
   *
   * <p>The commit is added to the timeline directly, so that the test does not move the shared
   * instant generator, which would make later writes in the same JVM wait for the clock to catch up.
   */
  @Test
  public void testIssuedInstantDelayFollowsTheTimelineTimezoneOfATableCreatedAfterStartup() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tableDir.getAbsolutePath());
    conf.set(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_COPY_ON_WRITE);
    GaugeCapturingRuntimeContext runtimeContext = new GaugeCapturingRuntimeContext();
    StreamReadMonitoringFunction function = TestUtils.getMonitorFunc(conf);
    List<MergeOnReadInputSplit> splits = new ArrayList<>();
    StreamSource<MergeOnReadInputSplit, StreamReadMonitoringFunction> source = new StreamSource<>(function);
    try (AbstractStreamOperatorTestHarness<MergeOnReadInputSplit> harness =
             new AbstractStreamOperatorTestHarness<>(source, 1, 1, 0)) {
      harness.setup();
      function.setRuntimeContext(runtimeContext);
      harness.open();
      CollectingSourceContext<MergeOnReadInputSplit> sourceContext = new CollectingSourceContext<>(new Object(), splits);

      // there is no table to monitor yet
      function.monitorDirAndForwardSplits(sourceContext);
      harness.snapshot(1, 1);
      assertTrue(splits.isEmpty(), "There should be no inputSplits without a table");
      assertEquals(0L, runtimeContext.gauge("issuedInstant"));
      assertEquals(0L, runtimeContext.gauge("issuedInstantDelay"));

      // the table is created with a UTC timeline, and gets a commit that was made ten minutes ago
      HoodieTableMetaClient metaClient = StreamerUtil.initTableIfNotExists(conf);
      Properties utcTimeline = new Properties();
      utcTimeline.setProperty(HoodieTableConfig.TIMELINE_TIMEZONE.key(), HoodieTimelineTimeZone.UTC.toString());
      HoodieTableConfig.update(metaClient.getStorage(), metaClient.getMetaPath(), utcTimeline);
      Instant committedAt = Instant.now().minus(10, ChronoUnit.MINUTES);
      HoodieTestTable.of(metaClient).addCommit(
          UTC_INSTANT_FORMAT.format(committedAt),
          Option.of(UTC_INSTANT_FORMAT.format(committedAt.plusSeconds(1))),
          Option.of(new HoodieCommitMetadata()));

      // the JVM that reads the table is not in UTC
      TimeZone originalZone = TimeZone.getDefault();
      TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
      try {
        function.monitorDirAndForwardSplits(sourceContext);
        harness.snapshot(2, 2);
      } finally {
        TimeZone.setDefault(originalZone);
      }

      long delay = runtimeContext.gauge("issuedInstantDelay");
      log.info("The issued instant delay is {} seconds for a commit made ten minutes ago", delay);
      assertTrue(delay >= 600 && delay < 660, "The commit was made ten minutes ago, but the delay is " + delay + " seconds");
      assertEquals(committedAt.getEpochSecond(), runtimeContext.gauge("issuedInstant"));
    }
  }

  /**
   * Runtime context whose metric group keeps the gauges that are registered on it.
   */
  private static class GaugeCapturingRuntimeContext extends MockStreamingRuntimeContext {
    private final CapturingOperatorMetricGroup metricGroup = new CapturingOperatorMetricGroup();

    GaugeCapturingRuntimeContext() {
      super(false, 1, 0);
    }

    @Override
    public OperatorMetricGroup getMetricGroup() {
      return metricGroup;
    }

    long gauge(String name) {
      return (Long) metricGroup.gauges.get(name).getValue();
    }
  }

  private static class CapturingOperatorMetricGroup extends UnregisteredMetricsGroup implements OperatorMetricGroup {
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

    @Override
    public OperatorIOMetricGroup getIOMetricGroup() {
      return UnregisteredMetricsGroup.createOperatorMetricGroup().getIOMetricGroup();
    }
  }
}
