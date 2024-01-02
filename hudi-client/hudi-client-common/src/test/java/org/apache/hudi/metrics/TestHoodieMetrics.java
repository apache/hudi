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

import com.codahale.metrics.Timer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestHoodieMetrics {

  @Mock
  HoodieWriteConfig config;
  HoodieMetrics hoodieMetrics;
  Metrics metrics;

  @BeforeEach
  void setUp() {
    when(config.isMetricsOn()).thenReturn(true);
    when(config.getTableName()).thenReturn("raw_table");
    when(config.getMetricsReporterType()).thenReturn(MetricsReporterType.INMEMORY);
    when(config.getBasePath()).thenReturn("s3://test" + UUID.randomUUID());
    hoodieMetrics = new HoodieMetrics(config);
    metrics = hoodieMetrics.getMetrics();
  }

  @AfterEach
  void shutdownMetrics() {
    metrics.shutdown();
  }

  @Test
  public void testRegisterGauge() {
    metrics.registerGauge("metric1", 123L);
    assertEquals("123", metrics.getRegistry().getGauges().get("metric1").getValue().toString());
  }

  @Test
  public void testTimerCtx() throws InterruptedException {
    Random rand = new Random();
    // Index metrics
    Timer.Context timer = hoodieMetrics.getIndexCtx();
    Thread.sleep(5); // Ensure timer duration is > 0
    hoodieMetrics.updateIndexMetrics("some_action", hoodieMetrics.getDurationInMs(timer.stop()));
    String metricName = hoodieMetrics.getMetricsName("index", "some_action.duration");
    long msec = (Long)metrics.getRegistry().getGauges().get(metricName).getValue();
    assertTrue(msec > 0);

    // Rollback metrics
    timer = hoodieMetrics.getRollbackCtx();
    Thread.sleep(5); // Ensure timer duration is > 0
    long numFilesDeleted = 1 + rand.nextInt();
    hoodieMetrics.updateRollbackMetrics(hoodieMetrics.getDurationInMs(timer.stop()), numFilesDeleted);
    metricName = hoodieMetrics.getMetricsName("rollback", "duration");
    msec = (Long)metrics.getRegistry().getGauges().get(metricName).getValue();
    assertTrue(msec > 0);
    metricName = hoodieMetrics.getMetricsName("rollback", "numFilesDeleted");
    assertEquals((long)metrics.getRegistry().getGauges().get(metricName).getValue(), numFilesDeleted);

    // Clean metrics
    timer = hoodieMetrics.getRollbackCtx();
    Thread.sleep(5); // Ensure timer duration is > 0
    numFilesDeleted = 1 + rand.nextInt();
    hoodieMetrics.updateCleanMetrics(hoodieMetrics.getDurationInMs(timer.stop()), (int)numFilesDeleted);
    metricName = hoodieMetrics.getMetricsName("clean", "duration");
    msec = (Long)metrics.getRegistry().getGauges().get(metricName).getValue();
    assertTrue(msec > 0);
    metricName = hoodieMetrics.getMetricsName("clean", "numFilesDeleted");
    assertEquals((long)metrics.getRegistry().getGauges().get(metricName).getValue(), numFilesDeleted);

    // Finalize metrics
    timer = hoodieMetrics.getFinalizeCtx();
    Thread.sleep(5); // Ensure timer duration is > 0
    long numFilesFinalized = 1 + rand.nextInt();
    hoodieMetrics.updateFinalizeWriteMetrics(hoodieMetrics.getDurationInMs(timer.stop()), (int)numFilesFinalized);
    metricName = hoodieMetrics.getMetricsName("finalize", "duration");
    msec = (Long)metrics.getRegistry().getGauges().get(metricName).getValue();
    assertTrue(msec > 0);
    metricName = hoodieMetrics.getMetricsName("finalize", "numFilesFinalized");
    assertEquals((long)metrics.getRegistry().getGauges().get(metricName).getValue(), numFilesFinalized);

    // Commit / deltacommit / compaction metrics
    Stream.of("commit", "deltacommit", "compaction").forEach(action -> {
      Timer.Context commitTimer = action.equals("commit") ? hoodieMetrics.getCommitCtx() :
          action.equals("deltacommit") ? hoodieMetrics.getDeltaCommitCtx() : hoodieMetrics.getCompactionCtx();

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
      when(metadata.getTotalRecordsDeleted()).thenReturn(randomValue + 14);
      when(metadata.getTotalCorruptLogBlocks()).thenReturn(randomValue + 15);
      when(metadata.getTotalRollbackLogBlocks()).thenReturn(randomValue + 16);
      when(metadata.getMinAndMaxEventTime()).thenReturn(Pair.of(Option.empty(), Option.empty()));
      when(config.isCompactionLogBlockMetricsOn()).thenReturn(true);

      hoodieMetrics.updateCommitMetrics(randomValue + 17, commitTimer.stop(), metadata, action);

      String metricname = hoodieMetrics.getMetricsName(action, "duration");
      long duration = (Long)metrics.getRegistry().getGauges().get(metricname).getValue();
      assertTrue(duration > 0);
      metricname = hoodieMetrics.getMetricsName(action, HoodieMetrics.TOTAL_PARTITIONS_WRITTEN_STR);
      assertEquals((long)metrics.getRegistry().getGauges().get(metricname).getValue(), metadata.fetchTotalPartitionsWritten());
      metricname = hoodieMetrics.getMetricsName(action, HoodieMetrics.TOTAL_FILES_INSERT_STR);
      assertEquals((long)metrics.getRegistry().getGauges().get(metricname).getValue(), metadata.fetchTotalFilesInsert());
      metricname = hoodieMetrics.getMetricsName(action, HoodieMetrics.TOTAL_FILES_UPDATE_STR);
      assertEquals((long)metrics.getRegistry().getGauges().get(metricname).getValue(), metadata.fetchTotalFilesUpdated());
      metricname = hoodieMetrics.getMetricsName(action, HoodieMetrics.TOTAL_RECORDS_WRITTEN_STR);
      assertEquals((long)metrics.getRegistry().getGauges().get(metricname).getValue(), metadata.fetchTotalRecordsWritten());
      metricname = hoodieMetrics.getMetricsName(action, HoodieMetrics.TOTAL_UPDATE_RECORDS_WRITTEN_STR);
      assertEquals((long)metrics.getRegistry().getGauges().get(metricname).getValue(), metadata.fetchTotalUpdateRecordsWritten());
      metricname = hoodieMetrics.getMetricsName(action, HoodieMetrics.TOTAL_INSERT_RECORDS_WRITTEN_STR);
      assertEquals((long)metrics.getRegistry().getGauges().get(metricname).getValue(), metadata.fetchTotalInsertRecordsWritten());
      metricname = hoodieMetrics.getMetricsName(action, HoodieMetrics.TOTAL_BYTES_WRITTEN_STR);
      assertEquals((long)metrics.getRegistry().getGauges().get(metricname).getValue(), metadata.fetchTotalBytesWritten());
      metricname = hoodieMetrics.getMetricsName(action, "commitTime");
      assertEquals((long)metrics.getRegistry().getGauges().get(metricname).getValue(), randomValue + 17);
      metricname = hoodieMetrics.getMetricsName(action, HoodieMetrics.TOTAL_SCAN_TIME_STR);
      assertEquals(metrics.getRegistry().getGauges().get(metricname).getValue(), metadata.getTotalScanTime());
      metricname = hoodieMetrics.getMetricsName(action, HoodieMetrics.TOTAL_CREATE_TIME_STR);
      assertEquals(metrics.getRegistry().getGauges().get(metricname).getValue(), metadata.getTotalCreateTime());
      metricname = hoodieMetrics.getMetricsName(action, HoodieMetrics.TOTAL_UPSERT_TIME_STR);
      assertEquals(metrics.getRegistry().getGauges().get(metricname).getValue(), metadata.getTotalUpsertTime());
      metricname = hoodieMetrics.getMetricsName(action, HoodieMetrics.TOTAL_COMPACTED_RECORDS_UPDATED_STR);
      assertEquals(metrics.getRegistry().getGauges().get(metricname).getValue(), metadata.getTotalCompactedRecordsUpdated());
      metricname = hoodieMetrics.getMetricsName(action, HoodieMetrics.TOTAL_LOG_FILES_COMPACTED_STR);
      assertEquals(metrics.getRegistry().getGauges().get(metricname).getValue(), metadata.getTotalLogFilesCompacted());
      metricname = hoodieMetrics.getMetricsName(action, HoodieMetrics.TOTAL_LOG_FILES_SIZE_STR);
      assertEquals(metrics.getRegistry().getGauges().get(metricname).getValue(), metadata.getTotalLogFilesSize());
      metricname = hoodieMetrics.getMetricsName(action, HoodieMetrics.TOTAL_RECORDS_DELETED);
      assertEquals(metrics.getRegistry().getGauges().get(metricname).getValue(), metadata.getTotalRecordsDeleted());
      metricname = hoodieMetrics.getMetricsName(action, HoodieMetrics.TOTAL_CORRUPTED_LOG_BLOCKS_STR);
      assertEquals(metrics.getRegistry().getGauges().get(metricname).getValue(), metadata.getTotalCorruptLogBlocks());
      metricname = hoodieMetrics.getMetricsName(action, HoodieMetrics.TOTAL_ROLLBACK_LOG_BLOCKS_STR);
      assertEquals(metrics.getRegistry().getGauges().get(metricname).getValue(), metadata.getTotalRollbackLogBlocks());
    });
  }
}
