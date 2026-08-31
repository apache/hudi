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

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.HoodieClientTestUtils;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.hudi.metrics.RecordIndexLookupMetrics.KEY_COUNT;
import static org.apache.hudi.metrics.RecordIndexLookupMetrics.KEY_HIT_COUNT;
import static org.apache.hudi.metrics.RecordIndexLookupMetrics.KEY_MISS_COUNT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Drain semantics for the record index lookup counters, at the level where they are decided. */
public class TestRecordIndexLookupMetrics {

  private static final String TABLE = "drain_test_table";
  private static final String BASE_PATH = "file:/tmp/drain_test_table";
  private static final String OTHER_BASE_PATH = "file:/tmp/somewhere_else";

  private static final String LOOKED_UP_KEY = KEY_COUNT;

  private HoodieMetrics hoodieMetrics;
  private static JavaSparkContext jsc;
  private static HoodieSparkEngineContext context;

  @BeforeAll
  static void startContext() {
    jsc = new JavaSparkContext(HoodieClientTestUtils.getSparkConfForTest("drain-test"));
    context = new HoodieSparkEngineContext(jsc);
  }

  @AfterAll
  static void stopContext() {
    if (jsc != null) {
      jsc.stop();
      jsc = null;
    }
  }

  @AfterEach
  void clearOwnedRegistries() {
    // Registries live on the context now, so draining what a test left is all the cleanup there is.
    context.removeOwnedRegistry(RecordIndexLookupMetrics.registryKey(BASE_PATH));
    context.removeOwnedRegistry(RecordIndexLookupMetrics.registryKey(OTHER_BASE_PATH));
    if (hoodieMetrics != null && hoodieMetrics.getMetrics() != null) {
      hoodieMetrics.getMetrics().shutdown();
      hoodieMetrics = null;
    }
  }

  private static HoodieWriteConfig config(String basePath, boolean gateOn) {
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .forTable(TABLE)
        .withMetricsConfig(HoodieMetricsConfig.newBuilder()
            .on(true)
            .withReporterType(MetricsReporterType.INMEMORY.name())
            .withRecordIndexLookupMetrics(gateOn)
            .build())
        .build();
  }

  /** The reporter this drain publishes into, for the table under test. */
  private HoodieMetrics metricsFor(HoodieWriteConfig config) {
    hoodieMetrics = new HoodieMetrics(config, HoodieTestUtils.getDefaultStorage());
    return hoodieMetrics;
  }

  /** Creates the entry the way a lookup does, then fills it as the executors would. */
  private static Registry seedRegistry(HoodieWriteConfig config, long lookedUp, long hits, long misses) {
    Registry registry = context.getOrCreateOwnedRegistry(
        RecordIndexLookupMetrics.registryKey(config.getBasePath()), RecordIndexLookupMetrics.REGISTRY_NAME);
    registry.add(LOOKED_UP_KEY, lookedUp);
    registry.add(KEY_HIT_COUNT, hits);
    registry.add(KEY_MISS_COUNT, misses);
    return registry;
  }

  /** Gauge name the reporter publishes a counter under. */
  private String gaugeName(HoodieMetrics metrics, String metric) {
    return metrics.getMetricsName(
        RecordIndexLookupMetrics.METRIC_ACTION,
        RecordIndexLookupMetrics.METRIC_QUALIFIER)
        + "." + metric;
  }

  private Long gauge(HoodieMetrics metrics, String metric) {
    Map<String, com.codahale.metrics.Gauge> gauges = metrics.getMetrics().getRegistry().getGauges();
    com.codahale.metrics.Gauge<?> g = gauges.get(gaugeName(metrics, metric));
    return g == null ? null : (Long) g.getValue();
  }

  @Test
  void publishReportsWhatTheRegistryHolds() {
    HoodieWriteConfig config = config(BASE_PATH, true);
    seedRegistry(config, 10L, 7L, 3L);
    HoodieMetrics metrics = metricsFor(config);

    RecordIndexLookupMetrics.publishAndRelease(context, config, metrics);

    assertEquals(10L, gauge(metrics, KEY_COUNT));
    assertEquals(7L, gauge(metrics, KEY_HIT_COUNT));
    assertEquals(3L, gauge(metrics, KEY_MISS_COUNT));
  }

  /**
   * A released counter must disappear rather than sit at zero, or a table that performed no lookup at all
   * would keep reporting the previous commit's values.
   */
  @Test
  void releasingLeavesNoZeroValuedResidue() {
    HoodieWriteConfig config = config(BASE_PATH, true);
    Registry registry = seedRegistry(config, 10L, 7L, 3L);
    HoodieMetrics metrics = metricsFor(config);

    RecordIndexLookupMetrics.publishAndRelease(context, config, metrics);

    assertTrue(registry.getAllCounts(false).isEmpty(),
        "released counters must not linger, even at zero; got " + registry.getAllCounts(false));
  }

  /**
   * {@code Metrics.shutdown()} scrapes every registry in the process with {@code flush=true}, so another
   * table's write finishing can clear this registry mid-publish. The release must clamp rather than
   * subtract into negative numbers that every later commit would report.
   */
  @Test
  void registryClearedBeforeReleaseDoesNotGoNegative() {
    HoodieWriteConfig config = config(BASE_PATH, true);
    Registry registry = seedRegistry(config, 10L, 7L, 3L);
    HoodieMetrics metrics = metricsFor(config);

    registry.clear();
    RecordIndexLookupMetrics.publishAndRelease(context, config, metrics);

    registry.getAllCounts(false).forEach((name, value) ->
        assertTrue(value >= 0L, "release must clamp at zero, found " + name + "=" + value));
  }

  /** Counts that arrive mid-publish belong to the next commit, and must survive the release. */
  @Test
  void countsArrivingDuringThePublishSurviveTheRelease() {
    HoodieWriteConfig config = config(BASE_PATH, true);
    Registry registry = seedRegistry(config, 10L, 7L, 3L);
    HoodieMetrics metrics = metricsFor(config);

    // A straggler task's accumulator update, folded in before the drain reads.
    registry.add(LOOKED_UP_KEY, 4L);
    RecordIndexLookupMetrics.publishAndRelease(context, config, metrics);

    assertTrue(registry.getAllCounts(false).isEmpty(),
        "a drain that reads and releases the same values leaves nothing behind");
    assertEquals(14L, gauge(metrics, KEY_COUNT),
        "the straggler's count is included in what was reported");
  }

  /** Two tables can share a name, so the registry key folds in the base path. */
  @Test
  void countersAreScopedByBasePathNotOnlyByTableName() {
    HoodieWriteConfig config = config(BASE_PATH, true);
    HoodieWriteConfig otherTable = config(OTHER_BASE_PATH, true);
    seedRegistry(config, 10L, 7L, 3L);
    HoodieMetrics metrics = metricsFor(otherTable);

    RecordIndexLookupMetrics.publishAndRelease(context, otherTable, metrics);

    assertNull(gauge(metrics, KEY_COUNT),
        "a table at a different base path must not report another table's lookups");
  }

  /** With the gate off, nothing is read and nothing is reported. */
  @Test
  void theGateSuppressesTheDrainEntirely() {
    HoodieWriteConfig gatedOff = config(BASE_PATH, false);
    Registry registry = seedRegistry(gatedOff, 10L, 7L, 3L);
    HoodieMetrics metrics = metricsFor(gatedOff);

    RecordIndexLookupMetrics.publishAndRelease(context, gatedOff, metrics);

    assertNull(gauge(metrics, KEY_COUNT), "gate off: nothing reaches the reporter");
    assertFalse(registry.getAllCounts(false).isEmpty(),
        "gate off: the registry is not consumed either");
  }
}
