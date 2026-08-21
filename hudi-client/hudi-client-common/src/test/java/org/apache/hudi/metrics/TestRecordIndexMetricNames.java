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

import org.apache.hudi.common.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.common.metrics.LocalRegistry;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.config.HoodieWriteConfig;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.metrics.RecordIndexMetricNames.CALLER_TAG_LOCATION;
import static org.apache.hudi.metrics.RecordIndexMetricNames.HITS;
import static org.apache.hudi.metrics.RecordIndexMetricNames.MISSES;
import static org.apache.hudi.metrics.RecordIndexMetricNames.RECORDS_LOOKED_UP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Drain semantics for the record index lookup counters, at the level where they are decided. */
class TestRecordIndexMetricNames {

  private static final String TABLE = "drain_test_table";
  private static final String BASE_PATH = "file:/tmp/drain_test_table";
  private static final String OTHER_BASE_PATH = "file:/tmp/somewhere_else";

  private static final String LOOKED_UP_KEY = RecordIndexMetricNames.key(CALLER_TAG_LOCATION, RECORDS_LOOKED_UP);

  /** The registry map is a process-wide static, so a leaked entry would leak into the next test. */
  @AfterEach
  void removeRegistries() {
    Registry.REGISTRY_MAP.keySet().removeIf(key -> key.contains(RecordIndexMetricNames.REGISTRY_NAME));
  }

  private static HoodieWriteConfig config(String basePath, boolean gateOn) {
    Properties props = new Properties();
    props.setProperty(HoodieMetricsConfig.RLI_LOOKUP_METRICS_ENABLE.key(), String.valueOf(gateOn));
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .forTable(TABLE)
        .withProperties(props)
        .build(false);
  }

  private static String registryKey(HoodieWriteConfig config) {
    return Registry.makeKey(config.getTableName(), RecordIndexMetricNames.registryName(config.getBasePath()));
  }

  /** Seeds the counters an executor-side lookup would have produced for this table. */
  private static Registry seedRegistry(HoodieWriteConfig config, long lookedUp, long hits, long misses) {
    Registry registry = new LocalRegistry(RecordIndexMetricNames.registryName(config.getBasePath()));
    registry.add(LOOKED_UP_KEY, lookedUp);
    registry.add(RecordIndexMetricNames.key(CALLER_TAG_LOCATION, HITS), hits);
    registry.add(RecordIndexMetricNames.key(CALLER_TAG_LOCATION, MISSES), misses);
    Registry.REGISTRY_MAP.put(registryKey(config), registry);
    return registry;
  }

  private static String metadataKey(String metric) {
    return RecordIndexMetricNames.COMMIT_METADATA_PREFIX
        + RecordIndexMetricNames.key(CALLER_TAG_LOCATION, metric);
  }

  @Test
  void snapshotStampsWhatTheRegistryHolds() {
    HoodieWriteConfig config = config(BASE_PATH, true);
    seedRegistry(config, 10L, 7L, 3L);
    Map<String, String> commitMetadata = new HashMap<>();

    ExecutorMetrics.DrainedCounters snapshot =
        ExecutorMetrics.snapshotIntoCommitMetadata(commitMetadata, config);

    assertFalse(snapshot.isEmpty());
    assertEquals("10", commitMetadata.get(metadataKey(RECORDS_LOOKED_UP)));
    assertEquals("7", commitMetadata.get(metadataKey(HITS)));
    assertEquals("3", commitMetadata.get(metadataKey(MISSES)));
  }

  /**
   * The reason snapshot and release are separate calls: a commit that never lands must leave the counters
   * where they were, so that the next commit to succeed reports them.
   */
  @Test
  void snapshotWithoutReleaseLeavesTheCountersIntact() {
    HoodieWriteConfig config = config(BASE_PATH, true);
    Registry registry = seedRegistry(config, 10L, 7L, 3L);

    // A commit that fails after the snapshot: publishAndRelease is never reached.
    ExecutorMetrics.snapshotIntoCommitMetadata(new HashMap<>(), config);

    // The retry adds its own lookups, and reports the accumulated total.
    registry.add(LOOKED_UP_KEY, 5L);
    Map<String, String> retried = new HashMap<>();
    ExecutorMetrics.snapshotIntoCommitMetadata(retried, config);

    assertEquals("15", retried.get(metadataKey(RECORDS_LOOKED_UP)),
        "a failed commit must not consume the counters it snapshotted");
  }

  /** A released counter must disappear rather than sit at zero. */
  @Test
  void releasingLeavesNoZeroValuedResidue() {
    HoodieWriteConfig config = config(BASE_PATH, true);
    seedRegistry(config, 10L, 7L, 3L);

    ExecutorMetrics.publishAndRelease(
        ExecutorMetrics.snapshotIntoCommitMetadata(new HashMap<>(), config), null);

    Map<String, String> nextCommit = new HashMap<>();
    ExecutorMetrics.DrainedCounters snapshot =
        ExecutorMetrics.snapshotIntoCommitMetadata(nextCommit, config);

    assertTrue(snapshot.isEmpty(), "a commit that performed no lookup must carry no counters");
    assertTrue(nextCommit.isEmpty(), "released counters must not be stamped again at zero; got " + nextCommit);
  }

  /**
   * {@code Metrics.shutdown()} scrapes every registry in the process with {@code flush=true}, so another table's write finishing can clear this table's re
   */
  @Test
  void registryClearedBeforeReleaseDoesNotGoNegative() {
    HoodieWriteConfig config = config(BASE_PATH, true);
    Registry registry = seedRegistry(config, 10L, 7L, 3L);

    ExecutorMetrics.DrainedCounters snapshot =
        ExecutorMetrics.snapshotIntoCommitMetadata(new HashMap<>(), config);
    registry.clear();
    ExecutorMetrics.publishAndRelease(snapshot, null);

    registry.getAllCounts(false).forEach((name, value) ->
        assertTrue(value >= 0L, "release must clamp at zero, found " + name + "=" + value));

    Map<String, String> nextCommit = new HashMap<>();
    assertTrue(ExecutorMetrics.snapshotIntoCommitMetadata(nextCommit, config).isEmpty());
    assertTrue(nextCommit.isEmpty(),
        "a clamped release must leave nothing for the next commit to report; got " + nextCommit);
  }

  /** Counts that arrive after the snapshot belong to the next commit, and must survive the release. */
  @Test
  void countsArrivingAfterTheSnapshotSurviveTheRelease() {
    HoodieWriteConfig config = config(BASE_PATH, true);
    Registry registry = seedRegistry(config, 10L, 7L, 3L);

    ExecutorMetrics.DrainedCounters snapshot =
        ExecutorMetrics.snapshotIntoCommitMetadata(new HashMap<>(), config);
    // A straggler task's accumulator update, folded in after the snapshot was taken.
    registry.add(LOOKED_UP_KEY, 4L);
    ExecutorMetrics.publishAndRelease(snapshot, null);

    Map<String, String> nextCommit = new HashMap<>();
    ExecutorMetrics.snapshotIntoCommitMetadata(nextCommit, config);
    assertEquals("4", nextCommit.get(metadataKey(RECORDS_LOOKED_UP)),
        "a release subtracts what it published; it does not clear the registry");
  }

  /** Two tables sharing a name are still two tables: the registry is scoped by base path as well. */
  @Test
  void countersAreScopedByBasePathNotOnlyByTableName() {
    seedRegistry(config(BASE_PATH, true), 10L, 7L, 3L);
    HoodieWriteConfig otherTable = config(OTHER_BASE_PATH, true);

    Map<String, String> commitMetadata = new HashMap<>();
    ExecutorMetrics.DrainedCounters snapshot =
        ExecutorMetrics.snapshotIntoCommitMetadata(commitMetadata, otherTable);

    assertTrue(snapshot.isEmpty(),
        "a table at a different base path must not claim these counters even under the same table name");
    assertTrue(commitMetadata.isEmpty());
    assertNull(Registry.REGISTRY_MAP.get(registryKey(otherTable)),
        "the snapshot must not create a registry for a table that never looked anything up");
  }

  @Test
  void theGateSuppressesTheDrainEntirely() {
    HoodieWriteConfig gatedOff = config(BASE_PATH, false);
    Registry registry = seedRegistry(gatedOff, 10L, 7L, 3L);
    Map<String, String> commitMetadata = new HashMap<>();

    ExecutorMetrics.DrainedCounters snapshot =
        ExecutorMetrics.snapshotIntoCommitMetadata(commitMetadata, gatedOff);

    assertTrue(snapshot.isEmpty());
    assertTrue(commitMetadata.isEmpty());
    assertEquals(10L, registry.getAllCounts(false).get(LOOKED_UP_KEY),
        "with the gate off the drain must not touch the registry either");
  }
}
