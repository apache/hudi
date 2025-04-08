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

package org.apache.hudi.sync.common.metrics;

import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.metrics.Metrics;
import org.apache.hudi.metrics.MetricsReporterType;
import org.apache.hudi.sync.common.HoodieSyncConfig;

import com.codahale.metrics.Timer;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestHoodieMetaSyncMetrics {

  @Mock
  HoodieSyncConfig syncConfig;
  @Mock
  HoodieMetricsConfig metricsConfig;
  HoodieMetaSyncMetrics hoodieSyncMetrics;
  Metrics metrics;
  private static Configuration hadoopConf;
  private static Path basepath;

  @BeforeEach
  void setUp() throws IOException {
    hadoopConf = new Configuration();
    basepath = Files.createTempDirectory("hivesyncmetricstest" + Instant.now().toEpochMilli());
    when(metricsConfig.isMetricsOn()).thenReturn(true);
    when(syncConfig.getMetricsConfig()).thenReturn(metricsConfig);
    when(syncConfig.getHadoopConf()).thenReturn(hadoopConf);
    when(syncConfig.getBasePath()).thenReturn(basepath.toUri().toString());
    when(metricsConfig.getMetricsReporterType()).thenReturn(MetricsReporterType.INMEMORY);
    when(metricsConfig.getBasePath()).thenReturn(basepath.toUri().toString());
    when(metricsConfig.getMetricReporterMetricsNamePrefix()).thenReturn("test_prefix");
    hoodieSyncMetrics = new HoodieMetaSyncMetrics(syncConfig, "TestHiveSyncTool");
    metrics = hoodieSyncMetrics.getMetrics();
  }

  @AfterEach
  void shutdownMetrics() throws IOException {
    Files.delete(basepath);
    metrics.shutdown();
  }

  @Test
  void testUpdateRecreateAndSyncDurationInMs() throws InterruptedException {
    Timer.Context timerCtx = hoodieSyncMetrics.getRecreateAndSyncTimer();
    Thread.sleep(5);
    long durationInNs = timerCtx.stop();
    hoodieSyncMetrics.updateRecreateAndSyncDurationInMs(durationInNs);
    String metricName = hoodieSyncMetrics.getMetricsName("meta_sync", "recreate_table_duration_ms");
    long timeIsMs = (Long) metrics.getRegistry().getGauges().get(metricName).getValue();
    assertTrue(timeIsMs > 0, "recreate_table duration metric value should be > 0");
  }

  @Test
  void testIncrementRecreateAndSyncFailureCounter() {
    hoodieSyncMetrics.incrementRecreateAndSyncFailureCounter();
    String metricsName = hoodieSyncMetrics.getMetricsName("meta_sync", "meta_sync.recreate_table.failure.counter");
    long count = metrics.getRegistry().getCounters().get(metricsName).getCount();
    assertEquals(1, count, "recreate_table failure counter value should be 1");
  }

  @Test
  void testIncrementRecreateAndSyncFailureCounter_WithoutMetricsNamePrefix() {
    when(metricsConfig.getMetricReporterMetricsNamePrefix()).thenReturn("");
    hoodieSyncMetrics = new HoodieMetaSyncMetrics(syncConfig, "TestHiveSyncTool");
    metrics = hoodieSyncMetrics.getMetrics();
    hoodieSyncMetrics.incrementRecreateAndSyncFailureCounter();
    String metricsName = hoodieSyncMetrics.getMetricsName("meta_sync", "meta_sync.recreate_table.failure.counter");
    long count = metrics.getRegistry().getCounters().get(metricsName).getCount();
    assertEquals(1, count, "recreate_table failure counter value should be 1");
  }
}
