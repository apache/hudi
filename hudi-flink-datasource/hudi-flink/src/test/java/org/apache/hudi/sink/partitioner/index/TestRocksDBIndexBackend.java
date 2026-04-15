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

package org.apache.hudi.sink.partitioner.index;

import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.metrics.FlinkRocksDBIndexMetrics;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Test cases for {@link RocksDBIndexBackend}.
 */
public class TestRocksDBIndexBackend {

  @TempDir
  File tempFile;

  @Test
  void testGetAndUpdate() throws Exception {
    try (RocksDBIndexBackend rocksDBIndexBackend = new RocksDBIndexBackend(tempFile.getAbsolutePath())) {
      assertNull(rocksDBIndexBackend.get("id1"));

      HoodieRecordGlobalLocation location1 = new HoodieRecordGlobalLocation("par1", "001", UUID.randomUUID().toString());
      rocksDBIndexBackend.update("id1", location1);
      assertEquals(location1, rocksDBIndexBackend.get("id1"));

      HoodieRecordGlobalLocation location2 = new HoodieRecordGlobalLocation("par2", "002", UUID.randomUUID().toString());
      rocksDBIndexBackend.update("id2", location2);
      assertEquals(location2, rocksDBIndexBackend.get("id2"));
    }
  }

  @Test
  void testMetricsRegistrationAndSnapshot() throws Exception {
    try (RocksDBIndexBackend rocksDBIndexBackend = new RocksDBIndexBackend(tempFile.getAbsolutePath())) {
      MetricGroup metricGroup = mock(MetricGroup.class);
      Map<String, Gauge<?>> gauges = new HashMap<>();
      doAnswer(invocation -> {
        String name = invocation.getArgument(0);
        Gauge<?> gauge = invocation.getArgument(1);
        gauges.put(name, gauge);
        return gauge;
      }).when(metricGroup).gauge(anyString(), any(Gauge.class));

      rocksDBIndexBackend.registerMetrics(metricGroup);

      assertTrue(gauges.containsKey(FlinkRocksDBIndexMetrics.ROCKSDB_DISK_TOTAL_SST_FILES_SIZE));
      assertTrue(gauges.containsKey(FlinkRocksDBIndexMetrics.ROCKSDB_DISK_LIVE_SST_FILES_SIZE));
      assertTrue(gauges.containsKey(FlinkRocksDBIndexMetrics.ROCKSDB_BLOCK_CACHE_CAPACITY));
      assertTrue(gauges.containsKey(FlinkRocksDBIndexMetrics.ROCKSDB_BLOCK_CACHE_USAGE));
      assertTrue(gauges.containsKey(FlinkRocksDBIndexMetrics.ROCKSDB_BLOCK_CACHE_HIT_RATIO));
      assertTrue(gauges.containsKey(FlinkRocksDBIndexMetrics.ROCKSDB_BLOCK_CACHE_DATA_HIT_RATIO));
      assertTrue(gauges.containsKey(FlinkRocksDBIndexMetrics.ROCKSDB_BLOCK_CACHE_INDEX_HIT_RATIO));
      assertTrue(gauges.containsKey(FlinkRocksDBIndexMetrics.ROCKSDB_BLOCK_CACHE_FILTER_HIT_RATIO));
      assertTrue(gauges.containsKey(FlinkRocksDBIndexMetrics.ROCKSDB_MEMTABLE_ACTIVE_SIZE));
      assertTrue(gauges.containsKey(FlinkRocksDBIndexMetrics.ROCKSDB_MEMTABLE_ALL_SIZE));
      assertTrue(gauges.containsKey(FlinkRocksDBIndexMetrics.ROCKSDB_MEMTABLE_IMMUTABLE_COUNT));
      assertTrue(gauges.containsKey(FlinkRocksDBIndexMetrics.ROCKSDB_MEMTABLE_HIT_RATIO));

      rocksDBIndexBackend.registerMetrics(metricGroup);
      assertEquals(12, gauges.size());

      assertEquals(0D, ((Number) gauges.get(FlinkRocksDBIndexMetrics.ROCKSDB_BLOCK_CACHE_HIT_RATIO).getValue()).doubleValue());
      assertEquals(0D, ((Number) gauges.get(FlinkRocksDBIndexMetrics.ROCKSDB_MEMTABLE_HIT_RATIO).getValue()).doubleValue());
      assertEquals(0L, ((Number) gauges.get(FlinkRocksDBIndexMetrics.ROCKSDB_DISK_TOTAL_SST_FILES_SIZE).getValue()).longValue());
    }
  }

  @Test
  void testMetricsReflectWritesReadsAndAutoFlush() throws Exception {
    try (RocksDBIndexBackend rocksDBIndexBackend = new RocksDBIndexBackend(tempFile.getAbsolutePath())) {
      MetricGroup metricGroup = mock(MetricGroup.class);
      Map<String, Gauge<?>> gauges = new HashMap<>();
      doAnswer(invocation -> {
        String name = invocation.getArgument(0);
        Gauge<?> gauge = invocation.getArgument(1);
        gauges.put(name, gauge);
        return gauge;
      }).when(metricGroup).gauge(anyString(), any(Gauge.class));
      rocksDBIndexBackend.registerMetrics(metricGroup);

      for (int i = 0; i < 32; i++) {
        rocksDBIndexBackend.update("id" + i,
            new HoodieRecordGlobalLocation("par" + (i % 2), "00" + i, UUID.randomUUID().toString()));
      }

      assertTrue(((Number) gauges.get(FlinkRocksDBIndexMetrics.ROCKSDB_MEMTABLE_ACTIVE_SIZE).getValue()).longValue() > 0L);
      assertTrue(((Number) gauges.get(FlinkRocksDBIndexMetrics.ROCKSDB_MEMTABLE_ALL_SIZE).getValue()).longValue() > 0L);

      for (int i = 0; i < 32; i++) {
        assertNotNull(rocksDBIndexBackend.get("id" + i));
      }

      assertTrue(((Number) gauges.get(FlinkRocksDBIndexMetrics.ROCKSDB_MEMTABLE_HIT_RATIO).getValue()).doubleValue() > 0D);

      // Keep writing larger records to trigger auto-flushes memtable into SST.
      int nextKey = 32;
      for (int i = 0; i < 32768; i++) {
        String par = "par-" + nextKey + "-" + "x".repeat(4096);
        rocksDBIndexBackend.update("id" + nextKey,
            new HoodieRecordGlobalLocation(par, "00" + nextKey, UUID.randomUUID().toString()));
        nextKey++;
      }
      long totalSstSize =
          ((Number) gauges.get(FlinkRocksDBIndexMetrics.ROCKSDB_DISK_TOTAL_SST_FILES_SIZE).getValue()).longValue();
      long liveSstSize =
          ((Number) gauges.get(FlinkRocksDBIndexMetrics.ROCKSDB_DISK_LIVE_SST_FILES_SIZE).getValue()).longValue();
      assertTrue(totalSstSize > 0 || liveSstSize > 0);

      for (int i = 0; i < 32; i++) {
        assertNotNull(rocksDBIndexBackend.get("id" + i));
      }

      assertTrue(((Number) gauges.get(FlinkRocksDBIndexMetrics.ROCKSDB_BLOCK_CACHE_HIT_RATIO).getValue()).doubleValue() >= 0D);
      assertTrue(((Number) gauges.get(FlinkRocksDBIndexMetrics.ROCKSDB_BLOCK_CACHE_DATA_HIT_RATIO).getValue()).doubleValue() >= 0D);
      assertTrue(((Number) gauges.get(FlinkRocksDBIndexMetrics.ROCKSDB_BLOCK_CACHE_INDEX_HIT_RATIO).getValue()).doubleValue() >= 0D);
      assertTrue(((Number) gauges.get(FlinkRocksDBIndexMetrics.ROCKSDB_BLOCK_CACHE_FILTER_HIT_RATIO).getValue()).doubleValue() >= 0D);
      assertTrue(((Number) gauges.get(FlinkRocksDBIndexMetrics.ROCKSDB_BLOCK_CACHE_HIT_RATIO).getValue()).doubleValue() <= 1D);
      assertTrue(((Number) gauges.get(FlinkRocksDBIndexMetrics.ROCKSDB_MEMTABLE_IMMUTABLE_COUNT).getValue()).longValue() >= 0L);

      if (((Number) gauges.get(FlinkRocksDBIndexMetrics.ROCKSDB_BLOCK_CACHE_CAPACITY).getValue()).longValue() > 0L) {
        assertTrue(((Number) gauges.get(FlinkRocksDBIndexMetrics.ROCKSDB_BLOCK_CACHE_USAGE).getValue()).longValue() > 0L);
      }
    }
  }

  @Test
  void testMetricsSnapshotAfterCloseReturnsDefaultValues() throws Exception {
    MetricGroup metricGroup = mock(MetricGroup.class);
    Map<String, Gauge<?>> gauges = new HashMap<>();
    doAnswer(invocation -> {
      String name = invocation.getArgument(0);
      Gauge<?> gauge = invocation.getArgument(1);
      gauges.put(name, gauge);
      return gauge;
    }).when(metricGroup).gauge(anyString(), any(Gauge.class));

    RocksDBIndexBackend rocksDBIndexBackend = new RocksDBIndexBackend(tempFile.getAbsolutePath());
    rocksDBIndexBackend.registerMetrics(metricGroup);
    rocksDBIndexBackend.close();

    assertEquals(0L, ((Number) gauges.get(FlinkRocksDBIndexMetrics.ROCKSDB_DISK_TOTAL_SST_FILES_SIZE).getValue()).longValue());
    assertEquals(0L, ((Number) gauges.get(FlinkRocksDBIndexMetrics.ROCKSDB_DISK_LIVE_SST_FILES_SIZE).getValue()).longValue());
    assertEquals(0D, ((Number) gauges.get(FlinkRocksDBIndexMetrics.ROCKSDB_BLOCK_CACHE_HIT_RATIO).getValue()).doubleValue());
    assertEquals(0D, ((Number) gauges.get(FlinkRocksDBIndexMetrics.ROCKSDB_MEMTABLE_HIT_RATIO).getValue()).doubleValue());
  }
}
