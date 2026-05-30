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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.metrics.FlinkIndexBackendMetrics;
import org.apache.hudi.sink.event.Correspondent;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;
import org.apache.hudi.utils.TestUtils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test cases for {@link GlobalRecordLevelIndexBackend}.
 */
public class TestGlobalRecordLevelIndexBackend {

  private Configuration conf;
  private NoOpMetricRegistry registry = new NoOpMetricRegistry();

  @TempDir
  File tempFile;

  @BeforeEach
  void beforeEach() throws IOException {
    conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.TABLE_TYPE, COPY_ON_WRITE.name());
    conf.setString(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key(), "true");
    StreamerUtil.initTableIfNotExists(conf);
  }

  @Test
  void testRecordLevelIndexBackend() throws Exception {
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    String firstCommitTime = TestUtils.getLastCompleteInstant(tempFile.toURI().toString());

    try (GlobalRecordLevelIndexBackend globalRecordLevelIndexBackend = new GlobalRecordLevelIndexBackend(conf, -1)) {
      globalRecordLevelIndexBackend.registerMetrics(TaskManagerMetricGroup.createTaskManagerMetricGroup(registry, "localhost", ResourceID.generate()));
      // get record location
      HoodieRecordGlobalLocation location = globalRecordLevelIndexBackend.get(Collections.singletonList("id1")).get("id1");
      assertNotNull(location);
      assertEquals("par1", location.getPartitionPath());
      assertEquals(firstCommitTime, location.getInstantTime());

      // get record location with non existed key
      location = globalRecordLevelIndexBackend.get(Collections.singletonList("new_key")).get("new_key");
      assertNull(location);
      assertThrows(UnsupportedOperationException.class, () -> globalRecordLevelIndexBackend.get("id1"));

      // get records locations for multiple record keys
      Map<String, HoodieRecordGlobalLocation> locations = globalRecordLevelIndexBackend.get(Arrays.asList("id1", "id2", "id3"));
      assertEquals(3, locations.size());
      locations.values().forEach(Assertions::assertNotNull);

      // get records locations for multiple record keys with unexisted key
      locations = globalRecordLevelIndexBackend.get(Arrays.asList("id1", "id2", "new_key"));
      assertEquals(2, locations.size());
      assertNull(locations.get("new_key"));

      // new checkpoint
      globalRecordLevelIndexBackend.onCheckpoint(1);

      // update record location
      HoodieRecordGlobalLocation newLocation = new HoodieRecordGlobalLocation("par5", "1003", "file_id_4");
      globalRecordLevelIndexBackend.update("new_key", newLocation);
      location = globalRecordLevelIndexBackend.get(Collections.singletonList("new_key")).get("new_key");
      assertEquals(newLocation, location);

      // previous instant commit success, clean
      Correspondent correspondent = mock(Correspondent.class);
      Map<Long, String> inflightInstants = new HashMap<>();
      inflightInstants.put(1L, "0001");
      when(correspondent.requestInflightInstants()).thenReturn(inflightInstants);
      globalRecordLevelIndexBackend.onCheckpointComplete(correspondent, 1);
      assertEquals(2, globalRecordLevelIndexBackend.getRecordIndexCache().getCaches().size());
      // the cache contains 'new_key', and other old locations
      location = globalRecordLevelIndexBackend.getRecordIndexCache().get("new_key");
      assertEquals(newLocation, location);
      location = globalRecordLevelIndexBackend.getRecordIndexCache().get("id1");
      assertNotNull(location);
    }
  }

  @Test
  void testRecordLevelIndexCacheClean() throws Exception {
    // set a small value for RLI cache
    conf.set(FlinkOptions.INDEX_RLI_CACHE_SIZE, 1L);

    try (GlobalRecordLevelIndexBackend globalRecordLevelIndexBackend = new GlobalRecordLevelIndexBackend(conf, -1)) {
      globalRecordLevelIndexBackend.registerMetrics(TaskManagerMetricGroup.createTaskManagerMetricGroup(registry, "localhost", ResourceID.generate()));

      for (int i = 0; i < 1500; i++) {
        globalRecordLevelIndexBackend.update("id1_" + i,
            new HoodieRecordGlobalLocation("par1", "000000001", UUID.randomUUID().toString(), -1));
      }
      // new checkpoint
      globalRecordLevelIndexBackend.onCheckpoint(1);
      Correspondent correspondent = mock(Correspondent.class);
      Map<Long, String> inflightInstants = new HashMap<>();
      inflightInstants.put(1L, "0001");
      when(correspondent.requestInflightInstants()).thenReturn(inflightInstants);
      globalRecordLevelIndexBackend.onCheckpointComplete(correspondent, 1);

      for (int i = 0; i < 2000; i++) {
        globalRecordLevelIndexBackend.update("id2_" + i,
            new HoodieRecordGlobalLocation("par1", "000000001", UUID.randomUUID().toString(), -1));
      }

      // new checkpoint
      globalRecordLevelIndexBackend.onCheckpoint(2);
      correspondent = mock(Correspondent.class);
      inflightInstants = new HashMap<>();
      inflightInstants.put(2L, "0002");
      when(correspondent.requestInflightInstants()).thenReturn(inflightInstants);
      globalRecordLevelIndexBackend.onCheckpointComplete(correspondent, 2);

      for (int i = 0; i < 2000; i++) {
        globalRecordLevelIndexBackend.update("id3_" + i,
            new HoodieRecordGlobalLocation("par1", "000000001", UUID.randomUUID().toString(), -1));
      }

      // the cache for the first instant is evicted
      assertEquals(2, globalRecordLevelIndexBackend.getRecordIndexCache().getCaches().size());

      // new checkpoint
      globalRecordLevelIndexBackend.onCheckpoint(3);
      correspondent = mock(Correspondent.class);
      inflightInstants = new HashMap<>();
      inflightInstants.put(3L, "0003");
      when(correspondent.requestInflightInstants()).thenReturn(inflightInstants);
      globalRecordLevelIndexBackend.onCheckpointComplete(correspondent, 3);

      for (int i = 0; i < 500; i++) {
        globalRecordLevelIndexBackend.update("id4_" + i,
            new HoodieRecordGlobalLocation("par1", "000000001", UUID.randomUUID().toString(), -1));
      }

      assertEquals(3, globalRecordLevelIndexBackend.getRecordIndexCache().getCaches().size());

      // insert another batch of records, which will trigger the cleaning of the cache.
      // another cache clean is triggered
      for (int i = 500; i < 1500; i++) {
        globalRecordLevelIndexBackend.update("id4_" + i,
            new HoodieRecordGlobalLocation("par1", "000000001", UUID.randomUUID().toString(), -1));
      }
      assertEquals(3, globalRecordLevelIndexBackend.getRecordIndexCache().getCaches().size());
      // cache for the oldest ckp id will be cleaned
      assertNull(globalRecordLevelIndexBackend.getRecordIndexCache().getCaches().get(-1L));
      // caches for the latest 3 ckp id still in the cache
      assertEquals("par1", globalRecordLevelIndexBackend.get(Collections.singletonList("id2_0")).get("id2_0").getPartitionPath());
      assertEquals("par1", globalRecordLevelIndexBackend.get(Collections.singletonList("id3_0")).get("id3_0").getPartitionPath());
      assertEquals("par1", globalRecordLevelIndexBackend.get(Collections.singletonList("id4_0")).get("id4_0").getPartitionPath());
    }
  }

  @Test
  void testRegisterMetricsIsIdempotent() throws Exception {
    // The second registerMetrics call must be a no-op and must not throw.
    try (GlobalRecordLevelIndexBackend backend = new GlobalRecordLevelIndexBackend(conf, -1)) {
      TaskManagerMetricGroup group = TaskManagerMetricGroup.createTaskManagerMetricGroup(
          registry, "localhost", ResourceID.generate());
      backend.registerMetrics(group);
      backend.registerMetrics(group);

      HoodieRecordGlobalLocation location = new HoodieRecordGlobalLocation("par2", "000000002", "file-id-2");
      backend.update("idempotent_key", location);
      assertEquals(location, backend.get(Collections.singletonList("idempotent_key")).get("idempotent_key"));
    }
  }

  @Test
  void testGetUpdatesCacheHitRatioGauge() throws Exception {
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    try (GlobalRecordLevelIndexBackend backend = new GlobalRecordLevelIndexBackend(conf, -1)) {
      Map<String, Gauge<?>> gauges = new HashMap<>();
      backend.registerMetrics(captureGauges(gauges));

      Gauge<?> hitRatioGauge = gauges.get(FlinkIndexBackendMetrics.LOOKUP_CACHE_HIT_RATIO);
      assertNotNull(hitRatioGauge);
      // no lookups yet: gauge is at its initial value.
      assertEquals(0.0D, ((Double) hitRatioGauge.getValue()).doubleValue());

      // first lookup for two known + one unknown key: 0 / 3 hit ratio, then the two known keys
      // are populated into the cache by the metadata table fallback.
      Map<String, HoodieRecordGlobalLocation> firstLookup =
          backend.get(Arrays.asList("id1", "id2", "missing_key"));
      assertEquals(2, firstLookup.size());
      assertEquals(0.0D, ((Double) hitRatioGauge.getValue()).doubleValue());

      // second lookup: id1 and id2 are now cached so they should be hits; missing_key remains a miss.
      // ratio = 2 hits / (2 hits + 1 miss).
      backend.get(Arrays.asList("id1", "id2", "missing_key"));
      assertEquals(2.0D / 3.0D, ((Double) hitRatioGauge.getValue()).doubleValue());
    }
  }

  @Test
  void testRegisterMetricsRegistersHitRatioGauge() throws Exception {
    try (GlobalRecordLevelIndexBackend backend = new GlobalRecordLevelIndexBackend(conf, -1)) {
      Map<String, Gauge<?>> gauges = new HashMap<>();
      backend.registerMetrics(captureGauges(gauges));

      assertTrue(gauges.containsKey(FlinkIndexBackendMetrics.LOOKUP_CACHE_HIT_RATIO));
      assertEquals(0.0D, ((Double) gauges.get(FlinkIndexBackendMetrics.LOOKUP_CACHE_HIT_RATIO).getValue()).doubleValue());
    }
  }

  @Test
  void testGetUpdatesCacheHitRatioGaugeForCachedLookup() throws Exception {
    // Cache-only variant of testGetUpdatesCacheHitRatioGauge: pre-populates the cache
    // so the metadata-table fallback is skipped, which keeps the test from depending on
    // any writer/runtime setup while still exercising the get(List) -> gauge wire-up.
    try (GlobalRecordLevelIndexBackend backend = new GlobalRecordLevelIndexBackend(conf, -1)) {
      Map<String, Gauge<?>> gauges = new HashMap<>();
      backend.registerMetrics(captureGauges(gauges));

      Gauge<?> hitRatioGauge = gauges.get(FlinkIndexBackendMetrics.LOOKUP_CACHE_HIT_RATIO);
      assertNotNull(hitRatioGauge);
      // no lookups yet: gauge is at its initial value.
      assertEquals(0.0D, ((Double) hitRatioGauge.getValue()).doubleValue());

      HoodieRecordGlobalLocation location1 = new HoodieRecordGlobalLocation("par1", "000000001", "file-id-1");
      HoodieRecordGlobalLocation location2 = new HoodieRecordGlobalLocation("par1", "000000001", "file-id-2");
      backend.update("cached_key_1", location1);
      backend.update("cached_key_2", location2);

      // all keys served from the in-memory cache: ratio = 2 / 2 = 1.0.
      Map<String, HoodieRecordGlobalLocation> result =
          backend.get(Arrays.asList("cached_key_1", "cached_key_2"));
      assertEquals(2, result.size());
      assertEquals(1.0D, ((Double) hitRatioGauge.getValue()).doubleValue());

      // a second, single-key cached lookup keeps the ratio at 1.0.
      backend.get(Collections.singletonList("cached_key_1"));
      assertEquals(1.0D, ((Double) hitRatioGauge.getValue()).doubleValue());
    }
  }

  private static MetricGroup captureGauges(Map<String, Gauge<?>> sink) {
    MetricGroup metricGroup = mock(MetricGroup.class);
    doAnswer(invocation -> {
      String name = invocation.getArgument(0);
      Gauge<?> gauge = invocation.getArgument(1);
      sink.put(name, gauge);
      return gauge;
    }).when(metricGroup).gauge(anyString(), any(Gauge.class));
    return metricGroup;
  }
}
