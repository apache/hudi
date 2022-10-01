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

package org.apache.hudi.metrics;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;

import com.codahale.metrics.Timer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Random;
import java.util.stream.Stream;

import static org.apache.hudi.metrics.Metrics.registerGauge;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestHoodieMetrics {

  @Mock
  HoodieWriteConfig config;
  HoodieMetrics metrics;

  @BeforeEach
  void setUp() {
    when(config.getBoolean(HoodieMetricsConfig.TURN_METRICS_ON)).thenReturn(true);
    when(config.getTableName()).thenReturn("raw_table");
    when(MetricsReporterType.valueOf(config.getString(HoodieMetricsConfig.METRICS_REPORTER_TYPE_VALUE))).thenReturn(MetricsReporterType.INMEMORY);
    metrics = new HoodieMetrics(config);
  }

  @AfterEach
  void shutdownMetrics() {
    Metrics.shutdown();
  }

  @Test
  public void testRegisterGauge() {
    registerGauge("metric1", 123L);
    assertEquals("123", Metrics.getInstance().getRegistry().getGauges().get("metric1").getValue().toString());
  }

  @Test
  public void testTimerCtx() throws InterruptedException {
    Random rand = new Random();

    // Index metrics
    Timer.Context timer = metrics.getIndexCtx();
    Thread.sleep(5); // Ensure timer duration is > 0
    metrics.updateIndexMetrics("some_action", metrics.getDurationInMs(timer.stop()));
    String metricName = metrics.getMetricsName("index", "some_action.duration");
    long msec = (Long)Metrics.getInstance().getRegistry().getGauges().get(metricName).getValue();
    assertTrue(msec > 0);

    // Rollback metrics
    timer = metrics.getRollbackCtx();
    Thread.sleep(5); // Ensure timer duration is > 0
    long numFilesDeleted = 1 + rand.nextInt();
    metrics.updateRollbackMetrics(metrics.getDurationInMs(timer.stop()), numFilesDeleted);
    metricName = metrics.getMetricsName("rollback", "duration");
    msec = (Long)Metrics.getInstance().getRegistry().getGauges().get(metricName).getValue();
    assertTrue(msec > 0);
    metricName = metrics.getMetricsName("rollback", "numFilesDeleted");
    assertEquals((long)Metrics.getInstance().getRegistry().getGauges().get(metricName).getValue(), numFilesDeleted);

    // Clean metrics
    timer = metrics.getRollbackCtx();
    Thread.sleep(5); // Ensure timer duration is > 0
    numFilesDeleted = 1 + rand.nextInt();
    metrics.updateCleanMetrics(metrics.getDurationInMs(timer.stop()), (int)numFilesDeleted);
    metricName = metrics.getMetricsName("clean", "duration");
    msec = (Long)Metrics.getInstance().getRegistry().getGauges().get(metricName).getValue();
    assertTrue(msec > 0);
    metricName = metrics.getMetricsName("clean", "numFilesDeleted");
    assertEquals((long)Metrics.getInstance().getRegistry().getGauges().get(metricName).getValue(), numFilesDeleted);

    // Finalize metrics
    timer = metrics.getFinalizeCtx();
    Thread.sleep(5); // Ensure timer duration is > 0
    long numFilesFinalized = 1 + rand.nextInt();
    metrics.updateFinalizeWriteMetrics(metrics.getDurationInMs(timer.stop()), (int)numFilesFinalized);
    metricName = metrics.getMetricsName("finalize", "duration");
    msec = (Long)Metrics.getInstance().getRegistry().getGauges().get(metricName).getValue();
    assertTrue(msec > 0);
    metricName = metrics.getMetricsName("finalize", "numFilesFinalized");
    assertEquals((long)Metrics.getInstance().getRegistry().getGauges().get(metricName).getValue(), numFilesFinalized);

    // Commit / deltacommit / compaction metrics
    Stream.of("commit", "deltacommit", "compaction").forEach(action -> {
      Timer.Context commitTimer = action.equals("commit") ? metrics.getCommitCtx() :
          action.equals("deltacommit") ? metrics.getDeltaCommitCtx() : metrics.getCompactionCtx();

      try {
        // Ensure timer duration is > 0
        Thread.sleep(5);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      long randomValue = 1 + rand.nextInt();
      HoodieCommitMetadata metadata = mock(HoodieCommitMetadata.class);
      when(metadata.fetchTotalPartitionsWritten()).thenReturn(randomValue + 1);
      when(metadata.fetchTotalFilesInsert()).thenReturn(randomValue + 2);
      when(metadata.fetchTotalFilesUpdated()).thenReturn(randomValue + 3);
      when(metadata.fetchTotalRecordsWritten()).thenReturn(randomValue + 4);
      when(metadata.fetchTotalUpdateRecordsWritten()).thenReturn(randomValue + 5);
      when(metadata.fetchTotalInsertRecordsWritten()).thenReturn(randomValue + 6);
      when(metadata.fetchTotalBytesWritten()).thenReturn(randomValue + 7);
      when(metadata.getTotalScanTime()).thenReturn(randomValue + 8);
      when(metadata.getTotalCreateTime()).thenReturn(randomValue + 9);
      when(metadata.getTotalUpsertTime()).thenReturn(randomValue + 10);
      when(metadata.getTotalCompactedRecordsUpdated()).thenReturn(randomValue + 11);
      when(metadata.getTotalLogFilesCompacted()).thenReturn(randomValue + 12);
      when(metadata.getTotalLogFilesSize()).thenReturn(randomValue + 13);
      when(metadata.getMinAndMaxEventTime()).thenReturn(Pair.of(Option.empty(), Option.empty()));
      metrics.updateCommitMetrics(randomValue + 14, commitTimer.stop(), metadata, action);

      String metricname = metrics.getMetricsName(action, "duration");
      long duration = (Long)Metrics.getInstance().getRegistry().getGauges().get(metricname).getValue();
      assertTrue(duration > 0);
      metricname = metrics.getMetricsName(action, "totalPartitionsWritten");
      assertEquals((long)Metrics.getInstance().getRegistry().getGauges().get(metricname).getValue(), metadata.fetchTotalPartitionsWritten());
      metricname = metrics.getMetricsName(action, "totalFilesInsert");
      assertEquals((long)Metrics.getInstance().getRegistry().getGauges().get(metricname).getValue(), metadata.fetchTotalFilesInsert());
      metricname = metrics.getMetricsName(action, "totalFilesUpdate");
      assertEquals((long)Metrics.getInstance().getRegistry().getGauges().get(metricname).getValue(), metadata.fetchTotalFilesUpdated());
      metricname = metrics.getMetricsName(action, "totalRecordsWritten");
      assertEquals((long)Metrics.getInstance().getRegistry().getGauges().get(metricname).getValue(), metadata.fetchTotalRecordsWritten());
      metricname = metrics.getMetricsName(action, "totalUpdateRecordsWritten");
      assertEquals((long)Metrics.getInstance().getRegistry().getGauges().get(metricname).getValue(), metadata.fetchTotalUpdateRecordsWritten());
      metricname = metrics.getMetricsName(action, "totalInsertRecordsWritten");
      assertEquals((long)Metrics.getInstance().getRegistry().getGauges().get(metricname).getValue(), metadata.fetchTotalInsertRecordsWritten());
      metricname = metrics.getMetricsName(action, "totalBytesWritten");
      assertEquals((long)Metrics.getInstance().getRegistry().getGauges().get(metricname).getValue(), metadata.fetchTotalBytesWritten());
      metricname = metrics.getMetricsName(action, "commitTime");
      assertEquals((long)Metrics.getInstance().getRegistry().getGauges().get(metricname).getValue(), randomValue + 14);
      metricname = metrics.getMetricsName(action, "totalScanTime");
      assertEquals(Metrics.getInstance().getRegistry().getGauges().get(metricname).getValue(), metadata.getTotalScanTime());
      metricname = metrics.getMetricsName(action, "totalCreateTime");
      assertEquals(Metrics.getInstance().getRegistry().getGauges().get(metricname).getValue(), metadata.getTotalCreateTime());
      metricname = metrics.getMetricsName(action, "totalUpsertTime");
      assertEquals(Metrics.getInstance().getRegistry().getGauges().get(metricname).getValue(), metadata.getTotalUpsertTime());
      metricname = metrics.getMetricsName(action, "totalCompactedRecordsUpdated");
      assertEquals(Metrics.getInstance().getRegistry().getGauges().get(metricname).getValue(), metadata.getTotalCompactedRecordsUpdated());
      metricname = metrics.getMetricsName(action, "totalLogFilesCompacted");
      assertEquals(Metrics.getInstance().getRegistry().getGauges().get(metricname).getValue(), metadata.getTotalLogFilesCompacted());
      metricname = metrics.getMetricsName(action, "totalLogFilesSize");
      assertEquals(Metrics.getInstance().getRegistry().getGauges().get(metricname).getValue(), metadata.getTotalLogFilesSize());
    });
  }
}
