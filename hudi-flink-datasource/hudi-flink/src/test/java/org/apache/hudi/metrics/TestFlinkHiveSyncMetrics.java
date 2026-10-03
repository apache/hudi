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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.hive.HiveSyncStats;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.metrics.FlinkHiveSyncMetrics.HIVE_SYNC_DURATION_MS;
import static org.apache.hudi.metrics.FlinkHiveSyncMetrics.HIVE_SYNC_FAILURE_COUNT;
import static org.apache.hudi.metrics.FlinkHiveSyncMetrics.HIVE_SYNC_INIT_DURATION_MS;
import static org.apache.hudi.metrics.FlinkHiveSyncMetrics.HIVE_SYNC_LAST_SUCCESS_TIME_MS;
import static org.apache.hudi.metrics.FlinkHiveSyncMetrics.HIVE_SYNC_METASTORE_DURATION_MS;
import static org.apache.hudi.metrics.FlinkHiveSyncMetrics.HIVE_SYNC_PARTITIONS_ADDED_COUNT;
import static org.apache.hudi.metrics.FlinkHiveSyncMetrics.HIVE_SYNC_PARTITION_SCAN_DURATION_MS;
import static org.apache.hudi.metrics.FlinkHiveSyncMetrics.HIVE_SYNC_SCHEMA_EVOLVED_COUNT;
import static org.apache.hudi.metrics.FlinkHiveSyncMetrics.HIVE_SYNC_SCHEMA_READ_DURATION_MS;
import static org.apache.hudi.metrics.FlinkHiveSyncMetrics.HIVE_SYNC_SUCCESS_COUNT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link FlinkHiveSyncMetrics}.
 */
class TestFlinkHiveSyncMetrics {

  private CapturingMetricGroup metricGroup;
  private FlinkHiveSyncMetrics metrics;

  @BeforeEach
  void setUp() {
    metricGroup = new CapturingMetricGroup();
    metrics = new FlinkHiveSyncMetrics(metricGroup);
    metrics.registerMetrics();
  }

  @Test
  void testRegisterMetrics() {
    assertNotNull(metricGroup.counters.get(HIVE_SYNC_SUCCESS_COUNT));
    assertNotNull(metricGroup.counters.get(HIVE_SYNC_FAILURE_COUNT));
    assertNotNull(metricGroup.histograms.get(HIVE_SYNC_DURATION_MS));
    assertNotNull(metricGroup.gauges.get(HIVE_SYNC_LAST_SUCCESS_TIME_MS));
    assertNotNull(metricGroup.histograms.get(HIVE_SYNC_INIT_DURATION_MS));
    assertNotNull(metricGroup.histograms.get(HIVE_SYNC_SCHEMA_READ_DURATION_MS));
    assertNotNull(metricGroup.histograms.get(HIVE_SYNC_PARTITION_SCAN_DURATION_MS));
    assertNotNull(metricGroup.histograms.get(HIVE_SYNC_METASTORE_DURATION_MS));
    assertNotNull(metricGroup.counters.get(HIVE_SYNC_PARTITIONS_ADDED_COUNT));
    assertNotNull(metricGroup.counters.get(HIVE_SYNC_SCHEMA_EVOLVED_COUNT));
  }

  @Test
  void testInitialValues() {
    assertEquals(0, successCount());
    assertEquals(0, failureCount());
    assertEquals(0, duration().getCount());
    assertEquals(0L, lastSuccessTime());
    assertEquals(0, histogram(HIVE_SYNC_INIT_DURATION_MS).getCount());
    assertEquals(0, histogram(HIVE_SYNC_SCHEMA_READ_DURATION_MS).getCount());
    assertEquals(0, histogram(HIVE_SYNC_PARTITION_SCAN_DURATION_MS).getCount());
    assertEquals(0, histogram(HIVE_SYNC_METASTORE_DURATION_MS).getCount());
    assertEquals(0, counter(HIVE_SYNC_PARTITIONS_ADDED_COUNT));
    assertEquals(0, counter(HIVE_SYNC_SCHEMA_EVOLVED_COUNT));
  }

  @Test
  void testUpdateInitDuration() {
    metrics.updateInitDuration(7L);

    assertEquals(1, histogram(HIVE_SYNC_INIT_DURATION_MS).getCount());
    assertEquals(7L, histogram(HIVE_SYNC_INIT_DURATION_MS).getStatistics().getMax());
  }

  @Test
  void testUpdateSyncStats() {
    metrics.updateSyncStats(stats(Option.of(10L), Option.of(20L), Option.of(30L), 4, true));
    metrics.updateSyncStats(stats(Option.of(11L), Option.of(21L), Option.of(31L), 2, false));

    assertEquals(2, histogram(HIVE_SYNC_SCHEMA_READ_DURATION_MS).getCount());
    assertEquals(11L, histogram(HIVE_SYNC_SCHEMA_READ_DURATION_MS).getStatistics().getMax());
    assertEquals(2, histogram(HIVE_SYNC_PARTITION_SCAN_DURATION_MS).getCount());
    assertEquals(21L, histogram(HIVE_SYNC_PARTITION_SCAN_DURATION_MS).getStatistics().getMax());
    assertEquals(2, histogram(HIVE_SYNC_METASTORE_DURATION_MS).getCount());
    assertEquals(31L, histogram(HIVE_SYNC_METASTORE_DURATION_MS).getStatistics().getMax());
    assertEquals(6, counter(HIVE_SYNC_PARTITIONS_ADDED_COUNT));
    assertEquals(1, counter(HIVE_SYNC_SCHEMA_EVOLVED_COUNT));
  }

  @Test
  void testUpdateSyncStatsSkipsStepsThatDidNotRun() {
    metrics.updateSyncStats(stats(Option.empty(), Option.empty(), Option.of(5L), 0, false));

    assertEquals(0, histogram(HIVE_SYNC_SCHEMA_READ_DURATION_MS).getCount(),
        "A sync that read no schema must not record a schema read of 0 ms");
    assertEquals(0, histogram(HIVE_SYNC_PARTITION_SCAN_DURATION_MS).getCount());
    assertEquals(1, histogram(HIVE_SYNC_METASTORE_DURATION_MS).getCount());
    assertEquals(0, counter(HIVE_SYNC_PARTITIONS_ADDED_COUNT));
    assertEquals(0, counter(HIVE_SYNC_SCHEMA_EVOLVED_COUNT));
  }

  @Test
  void testMarkSyncSucceeded() {
    long before = System.currentTimeMillis();
    metrics.markSyncSucceeded(42L);
    long after = System.currentTimeMillis();

    assertEquals(1, successCount());
    assertEquals(0, failureCount());
    assertEquals(1, duration().getCount());
    assertEquals(42L, duration().getStatistics().getMax());
    long lastSuccessTime = lastSuccessTime();
    assertTrue(lastSuccessTime >= before && lastSuccessTime <= after,
        "Last success time " + lastSuccessTime + " should be within [" + before + ", " + after + "]");
  }

  @Test
  void testMarkSyncFailed() {
    metrics.markSyncFailed(1000L);

    assertEquals(0, successCount());
    assertEquals(1, failureCount());
    assertEquals(1, duration().getCount());
    assertEquals(1000L, duration().getStatistics().getMax());
    assertEquals(0L, lastSuccessTime(), "A failed sync must not set the last success time");
  }

  @Test
  void testFailureKeepsLastSuccessTime() {
    metrics.markSyncSucceeded(10L);
    long lastSuccessTime = lastSuccessTime();

    metrics.markSyncFailed(20L);

    assertEquals(1, successCount());
    assertEquals(1, failureCount());
    assertEquals(2, duration().getCount());
    assertEquals(10L, duration().getStatistics().getMin());
    assertEquals(20L, duration().getStatistics().getMax());
    assertEquals(lastSuccessTime, lastSuccessTime());
  }

  @Test
  void testDurationWindowKeepsLatestSamples() {
    for (int i = 1; i <= 150; i++) {
      metrics.markSyncSucceeded(i);
    }

    assertEquals(150, successCount());
    assertEquals(150, duration().getCount(), "The count covers every sync, not just the window");
    assertEquals(100, duration().getStatistics().size());
    assertEquals(51L, duration().getStatistics().getMin());
    assertEquals(150L, duration().getStatistics().getMax());
  }

  @Test
  void testConcurrentUpdates() throws Exception {
    int threads = 2;
    int syncsPerThread = 10_000;
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      CountDownLatch start = new CountDownLatch(1);
      Future<?> succeeding = executor.submit(() -> {
        start.await();
        for (int i = 0; i < syncsPerThread; i++) {
          metrics.markSyncSucceeded(1L);
        }
        return null;
      });
      Future<?> failing = executor.submit(() -> {
        start.await();
        for (int i = 0; i < syncsPerThread; i++) {
          metrics.markSyncFailed(1L);
        }
        return null;
      });
      start.countDown();
      succeeding.get(30, TimeUnit.SECONDS);
      failing.get(30, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }

    assertEquals(syncsPerThread, successCount());
    assertEquals(syncsPerThread, failureCount());
    assertEquals(2L * syncsPerThread, duration().getCount());
  }

  private long successCount() {
    return metricGroup.counters.get(HIVE_SYNC_SUCCESS_COUNT).getCount();
  }

  private long failureCount() {
    return metricGroup.counters.get(HIVE_SYNC_FAILURE_COUNT).getCount();
  }

  private Histogram duration() {
    return metricGroup.histograms.get(HIVE_SYNC_DURATION_MS);
  }

  private long lastSuccessTime() {
    return (Long) metricGroup.gauges.get(HIVE_SYNC_LAST_SUCCESS_TIME_MS).getValue();
  }

  private Histogram histogram(String name) {
    return metricGroup.histograms.get(name);
  }

  private long counter(String name) {
    return metricGroup.counters.get(name).getCount();
  }

  private static HiveSyncStats stats(Option<Long> schemaReadMs, Option<Long> partitionScanMs,
                                     Option<Long> metastoreMs, int partitionsAdded, boolean schemaEvolved) {
    HiveSyncStats stats = mock(HiveSyncStats.class);
    when(stats.getSchemaReadMs()).thenReturn(schemaReadMs);
    when(stats.getPartitionScanMs()).thenReturn(partitionScanMs);
    when(stats.getMetastoreMs()).thenReturn(metastoreMs);
    when(stats.getPartitionsAdded()).thenReturn(partitionsAdded);
    when(stats.isSchemaEvolved()).thenReturn(schemaEvolved);
    return stats;
  }

  private static class CapturingMetricGroup extends UnregisteredMetricsGroup {
    private final Map<String, Counter> counters = new HashMap<>();
    private final Map<String, Histogram> histograms = new HashMap<>();
    private final Map<String, Gauge<?>> gauges = new HashMap<>();

    @Override
    public <C extends Counter> C counter(String name, C counter) {
      counters.put(name, counter);
      return counter;
    }

    @Override
    public <H extends Histogram> H histogram(String name, H histogram) {
      histograms.put(name, histogram);
      return histogram;
    }

    @Override
    public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
      gauges.put(name, gauge);
      return gauge;
    }
  }
}
