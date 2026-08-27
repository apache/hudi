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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.metrics.ExecutorMetricRegistry;
import org.apache.hudi.metrics.RecordIndexMetricNames;
import org.apache.hudi.utilities.testutils.CapturingMetricsReporter;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The RLI lookup counters must reach commit metadata on the DeltaStreamer path, not only on the Spark DataSource path.
 */
@Tag("functional")
public class TestRliMetricsOnStreamerPath extends HoodieDeltaStreamerTestBase {

  /** Selects the global or partitioned record index. */
  private static void enableRecordIndex(HoodieDeltaStreamer.Config cfg, boolean partitioned) {
    cfg.configs.add(HoodieMetadataConfig.ENABLE.key() + "=true");
    cfg.configs.add(HoodieMetricsConfig.TURN_METRICS_ON.key() + "=true");
    cfg.configs.add(HoodieMetricsConfig.RLI_LOOKUP_METRICS_ENABLE.key() + "=true");
    cfg.configs.add(HoodieMetricsConfig.METRICS_REPORTER_TYPE_VALUE.key() + "=INMEMORY");
    cfg.configs.add(HoodieMetricsConfig.METRICS_REPORTER_CLASS_NAME.key() + "="
        + CapturingMetricsReporter.class.getName());
    cfg.configs.add(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key() + "=" + !partitioned);
    cfg.configs.add(HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key() + "=" + partitioned);
    cfg.configs.add(HoodieIndexConfig.INDEX_TYPE.key() + "="
        + (partitioned ? "RECORD_LEVEL_INDEX" : "GLOBAL_RECORD_LEVEL_INDEX"));
  }

  /**
   * Reads the counters the way an operator would, off the configured reporter. Goes through the reporter
   * rather than {@code Metrics}, which is keyed by base path and cannot be addressed reliably from here.
   */
  private static Map<String, String> rliCountersOnLatestCommit(String tableBasePath) {
    String marker = ExecutorMetricRegistry.RECORD_INDEX_LOOKUP.metricAction()
        + "." + ExecutorMetricRegistry.RECORD_INDEX_LOOKUP.metricQualifier() + ".";
    Map<String, String> rli = new HashMap<>();
    CapturingMetricsReporter.captured().forEach((name, value) -> {
      int at = name.indexOf(marker);
      if (at == 0 || (at > 0 && name.charAt(at - 1) == '.')) {
        rli.put(name.substring(at + marker.length()), String.valueOf(value));
      }
    });
    return rli;
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testRliCountersReachTheReporterOnStreamerPath(boolean partitioned) throws Exception {
    String label = partitioned ? "partitioned" : "global";
    String tableBasePath = basePath + "/test_rli_metrics_streamer_" + label;

    // Sync 1 -- build the table and the record index.
    HoodieDeltaStreamer.Config insertCfg =
        TestHoodieDeltaStreamer.TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT);
    enableRecordIndex(insertCfg, partitioned);
    new HoodieDeltaStreamer(insertCfg, jsc).sync();

    // Sync 2 -- upsert, which tags incoming keys against the record index.
    HoodieDeltaStreamer.Config upsertCfg =
        TestHoodieDeltaStreamer.TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT);
    enableRecordIndex(upsertCfg, partitioned);
    new HoodieDeltaStreamer(upsertCfg, jsc).sync();

    Map<String, String> counters = rliCountersOnLatestCommit(tableBasePath);

    System.out.println("\n===== DeltaStreamer (" + label + " RLI) -- RLI counters on the commit =====");
    if (counters.isEmpty()) {
      System.out.println("  (none found)");
    } else {
      counters.entrySet().stream()
          .sorted(Map.Entry.comparingByKey())
          .forEach(e -> System.out.println(String.format("  %-52s %s", e.getKey(), e.getValue())));
    }
    System.out.println("==========================================================\n");

    assertFalse(counters.isEmpty(),
        "the commit-boundary drain must fire on the DeltaStreamer path; hudi-utilities never calls "
            + "Metrics.shutdownAllMetrics, so nothing else would publish these");

    String lookedUp = RecordIndexMetricNames.KEY_COUNT;
    assertTrue(counters.containsKey(lookedUp),
        "tag-location traffic must be attributed on the streamer path too; got " + counters.keySet());

    long records = Long.parseLong(counters.get(lookedUp));
    long hits = Long.parseLong(counters.get(RecordIndexMetricNames.KEY_HIT_COUNT));
    long misses = Long.parseLong(counters.get(RecordIndexMetricNames.KEY_MISS_COUNT));
    // Exact, not an invariant: misses is derived as records - hits at the emission site, so
    // records == hits + misses holds by construction and would survive a doubled count. The workload is
    // deterministic -- the first sync writes 1000 records, the second updates 500 and inserts 500.
    assertEquals(1000L, records, "the upsert sync looked up every key from the first sync");
    assertEquals(500L, hits, "the 500 updates hit the index");
    assertEquals(500L, misses, "the 500 fresh inserts missed");
    // Shard count is not pinned: it follows the index file-group layout, which differs between the
    // global and partitioned variants (10 and 3 on this workload).
    assertTrue(Long.parseLong(counters.get(RecordIndexMetricNames.SHARDS_READ)) > 0,
        "at least one shard was read");
  }
}
