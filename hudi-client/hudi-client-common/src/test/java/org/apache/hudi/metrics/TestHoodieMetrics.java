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
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.v2.ActiveTimelineV2;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.index.HoodieIndex;

import com.codahale.metrics.Timer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.metrics.HoodieMetrics.SOURCE_READ_AND_INDEX_ACTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestHoodieMetrics {

  @Mock
  HoodieWriteConfig writeConfig;
  @Mock
  HoodieMetricsConfig metricsConfig;
  HoodieMetrics hoodieMetrics;
  Metrics metrics;

  @BeforeEach
  void setUp() {
    when(writeConfig.getMetricsConfig()).thenReturn(metricsConfig);
    when(writeConfig.isMetricsOn()).thenReturn(true);
    when(metricsConfig.getMetricsReporterType()).thenReturn(MetricsReporterType.INMEMORY);
    when(metricsConfig.getBasePath()).thenReturn("s3://test" + UUID.randomUUID());
    hoodieMetrics = new HoodieMetrics(writeConfig, HoodieTestUtils.getDefaultStorage());
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
  public void testTimerCtxandGauges() throws InterruptedException {
    Random rand = new Random();
    // Index metrics
    Timer.Context timer = hoodieMetrics.getIndexCtx();
    Thread.sleep(5); // Ensure timer duration is > 0
    hoodieMetrics.updateIndexMetrics("some_action", hoodieMetrics.getDurationInMs(timer.stop()));
    String metricName = hoodieMetrics.getMetricsName("index", "some_action.duration");
    long msec = (Long)metrics.getRegistry().getGauges().get(metricName).getValue();
    assertTrue(msec > 0);

    // Source read and index metrics
    timer = hoodieMetrics.getSourceReadAndIndexTimerCtx();
    Thread.sleep(5); // Ensure timer duration is > 0
    hoodieMetrics.updateSourceReadAndIndexMetrics("some_action", hoodieMetrics.getDurationInMs(timer.stop()));
    metricName = hoodieMetrics.getMetricsName("source_read_and_index", "some_action.duration");
    msec = (Long)metrics.getRegistry().getGauges().get(metricName).getValue();
    assertTrue(msec > 0);

    // test index type
    metricName = hoodieMetrics.getMetricsName("index", "type");
    for (HoodieIndex.IndexType indexType: HoodieIndex.IndexType.values()) {
      hoodieMetrics.emitIndexTypeMetrics(indexType.ordinal());
      long indexTypeOrdinal = (Long)metrics.getRegistry().getGauges().get(metricName).getValue();
      assertEquals(indexTypeOrdinal, indexType.ordinal());
    }

    // test metadata enablement metrics
    metricName = hoodieMetrics.getMetricsName("metadata", "isEnabled");
    String colStatsMetricName = hoodieMetrics.getMetricsName("metadata", "isColSatsEnabled");
    String bloomFilterMetricName = hoodieMetrics.getMetricsName("metadata", "isBloomFilterEnabled");
    String rliMetricName = hoodieMetrics.getMetricsName("metadata", "isRliEnabled");
    Boolean[] boolValues = new Boolean[]{true, false};
    for (Boolean mdt: boolValues) {
      for (Boolean colStats : boolValues) {
        for (Boolean bloomFilter : boolValues) {
          for (Boolean rli : boolValues) {
            hoodieMetrics.emitMetadataEnablementMetrics(mdt, colStats, bloomFilter, rli);
            assertEquals(mdt ? 1L : 0L, metrics.getRegistry().getGauges().get(metricName).getValue());
            assertEquals(colStats ? 1L : 0L, metrics.getRegistry().getGauges().get(colStatsMetricName).getValue());
            assertEquals(bloomFilter ? 1L : 0L, metrics.getRegistry().getGauges().get(bloomFilterMetricName).getValue());
            assertEquals(rli ? 1L : 0L, metrics.getRegistry().getGauges().get(rliMetricName).getValue());
          }
        }
      }
    }

    // PreWrite metrics
    timer = hoodieMetrics.getSourceReadAndIndexTimerCtx();
    Thread.sleep(5); // Ensure timer duration is > 0
    hoodieMetrics.updateSourceReadAndIndexMetrics("some_action", hoodieMetrics.getDurationInMs(timer.stop()));
    metricName = hoodieMetrics.getMetricsName(SOURCE_READ_AND_INDEX_ACTION, "some_action.duration");
    msec = (Long)metrics.getRegistry().getGauges().get(metricName).getValue();
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
      when(writeConfig.isCompactionLogBlockMetricsOn()).thenReturn(true);

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

    // MOCK Timeline Instant Metrics for Clean & Rollback
    HoodieInstant instant004 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLEAN_ACTION, "1004");
    HoodieInstant instant007 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.CLEAN_ACTION, "1007");
    HoodieInstant instant009 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.ROLLBACK_ACTION, "1009");
    HoodieInstant instant0010 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLEAN_ACTION, "10010");
    HoodieInstant instant0013 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.ROLLBACK_ACTION, "10013");
    HoodieInstant instant0015 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLEAN_ACTION, "10015");
    HoodieInstant instant0016 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.ROLLBACK_ACTION, "10016");
    HoodieInstant instant0017 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.ROLLBACK_ACTION, "10017");

    HoodieActiveTimeline activeTimeline1 = new MockHoodieActiveTimeline(instant004, instant007, instant009, instant0010, instant0013, instant0015, instant0016, instant0017);
    hoodieMetrics.updateTableServiceInstantMetrics(activeTimeline1);

    metricName = hoodieMetrics.getMetricsName(HoodieTimeline.CLEAN_ACTION, HoodieMetrics.EARLIEST_PENDING_CLEAN_INSTANT_STR);
    assertEquals((long)metrics.getRegistry().getGauges().get(metricName).getValue(), Long.valueOf("1004"));
    metricName = hoodieMetrics.getMetricsName(HoodieTimeline.ROLLBACK_ACTION, HoodieMetrics.EARLIEST_PENDING_ROLLBACK_INSTANT_STR);
    assertEquals((long)metrics.getRegistry().getGauges().get(metricName).getValue(), Long.valueOf("1009"));

    metricName = hoodieMetrics.getMetricsName(HoodieTimeline.CLEAN_ACTION, HoodieMetrics.LATEST_COMPLETED_CLEAN_INSTANT_STR);
    assertEquals((long)metrics.getRegistry().getGauges().get(metricName).getValue(), Long.valueOf("1007"));
    metricName = hoodieMetrics.getMetricsName(HoodieTimeline.ROLLBACK_ACTION, HoodieMetrics.LATEST_COMPLETED_ROLLBACK_INSTANT_STR);
    assertEquals((long)metrics.getRegistry().getGauges().get(metricName).getValue(), Long.valueOf("10017"));

    metricName = hoodieMetrics.getMetricsName(HoodieTimeline.CLEAN_ACTION, HoodieMetrics.PENDING_CLEAN_INSTANT_COUNT_STR);
    assertEquals((long)metrics.getRegistry().getGauges().get(metricName).getValue(), 3L);
    metricName = hoodieMetrics.getMetricsName(HoodieTimeline.ROLLBACK_ACTION, HoodieMetrics.PENDING_ROLLBACK_INSTANT_COUNT_STR);
    assertEquals((long)metrics.getRegistry().getGauges().get(metricName).getValue(), 2L);

    // MOCK Timeline Instant Metrics for Clustering
    HoodieInstant instant001 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.CLUSTERING_ACTION, "1001");
    HoodieInstant instant005 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.CLUSTERING_ACTION, "1005");
    HoodieInstant instant0011 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.REPLACE_COMMIT_ACTION, "10011");
    HoodieInstant instant0018 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLUSTERING_ACTION, "10018");

    HoodieActiveTimeline activeTimeline2 = new MockHoodieActiveTimeline(instant001, instant005, instant0011, instant0018);
    hoodieMetrics.updateTableServiceInstantMetrics(activeTimeline2);

    metricName = hoodieMetrics.getMetricsName(HoodieTimeline.CLUSTERING_ACTION, HoodieMetrics.EARLIEST_PENDING_CLUSTERING_INSTANT_STR);
    assertEquals((long)metrics.getRegistry().getGauges().get(metricName).getValue(), Long.valueOf("10018"));

    metricName = hoodieMetrics.getMetricsName(HoodieTimeline.REPLACE_COMMIT_ACTION, HoodieMetrics.LATEST_COMPLETED_CLUSTERING_INSTANT_STR);
    assertEquals((long)metrics.getRegistry().getGauges().get(metricName).getValue(), Long.valueOf("10011"));

    metricName = hoodieMetrics.getMetricsName(HoodieTimeline.CLUSTERING_ACTION, HoodieMetrics.PENDING_CLUSTERING_INSTANT_COUNT_STR);
    assertEquals((long)metrics.getRegistry().getGauges().get(metricName).getValue(), 1L);

    // MOCK Timeline Instant Metrics for Compaction
    HoodieInstant instant002 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "1002");
    HoodieInstant instant003 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "1003");
    HoodieInstant instant006 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "1006");
    HoodieInstant instant008 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "1008");
    HoodieInstant instant0012 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "10012");
    HoodieInstant instant0014 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "10014");
    HoodieInstant longInstant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "20250329154030600010001");

    HoodieActiveTimeline activeTimeline3 = new MockHoodieActiveTimeline(instant002, instant003, instant006, instant008, instant0012, instant0014, longInstant);
    // verify longer instant times can also be updated in the metrics. These are required for table version six
    // where suffix is added at the end of older instants for compaction in the metadata timeline
    hoodieMetrics.updateTableServiceInstantMetrics(activeTimeline3);

    metricName = hoodieMetrics.getMetricsName(HoodieTimeline.COMPACTION_ACTION, HoodieMetrics.EARLIEST_PENDING_COMPACTION_INSTANT_STR);
    assertEquals((long)metrics.getRegistry().getGauges().get(metricName).getValue(), Long.valueOf("1002"));

    metricName = hoodieMetrics.getMetricsName(HoodieTimeline.COMMIT_ACTION, HoodieMetrics.LATEST_COMPLETED_COMPACTION_INSTANT_STR);
    assertEquals((long)metrics.getRegistry().getGauges().get(metricName).getValue(), Long.valueOf("1006"));

    metricName = hoodieMetrics.getMetricsName(HoodieTimeline.COMPACTION_ACTION, HoodieMetrics.PENDING_COMPACTION_INSTANT_COUNT_STR);
    assertEquals((long)metrics.getRegistry().getGauges().get(metricName).getValue(), 6L);
  }

  private static class MockHoodieActiveTimeline extends ActiveTimelineV2 {
    public MockHoodieActiveTimeline(HoodieInstant... instants) {
      super();
      this.setInstants(Arrays.asList(instants));
    }
  }
}
