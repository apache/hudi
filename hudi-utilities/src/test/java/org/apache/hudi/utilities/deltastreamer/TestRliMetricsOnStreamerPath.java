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
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.metrics.RecordIndexMetricNames;

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
    cfg.configs.add(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key() + "=" + !partitioned);
    cfg.configs.add(HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key() + "=" + partitioned);
    cfg.configs.add(HoodieIndexConfig.INDEX_TYPE.key() + "="
        + (partitioned ? "RECORD_LEVEL_INDEX" : "GLOBAL_RECORD_LEVEL_INDEX"));
  }

  private static Map<String, String> rliCountersOnLatestCommit(String tableBasePath) throws Exception {
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(HoodieTestUtils.getDefaultStorageConf())
        .setBasePath(tableBasePath)
        .build();
    metaClient.reloadActiveTimeline();
    HoodieInstant lastInstant = metaClient.getActiveTimeline()
        .getCommitsTimeline().filterCompletedInstants().lastInstant().get();
    HoodieCommitMetadata commitMetadata = metaClient.getActiveTimeline().readCommitMetadata(lastInstant);
    Map<String, String> rli = new HashMap<>();
    commitMetadata.getExtraMetadata().forEach((k, v) -> {
      if (k.startsWith(RecordIndexMetricNames.COMMIT_METADATA_PREFIX)) {
        rli.put(k, v);
      }
    });
    return rli;
  }

  private static String tagKey(String metric) {
    return RecordIndexMetricNames.COMMIT_METADATA_PREFIX
        + RecordIndexMetricNames.key(RecordIndexMetricNames.CALLER_TAG_LOCATION, metric);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testRliCountersReachCommitMetadataOnStreamerPath(boolean partitioned) throws Exception {
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

    String lookedUp = tagKey(RecordIndexMetricNames.KEY_COUNT);
    assertTrue(counters.containsKey(lookedUp),
        "tag-location traffic must be attributed on the streamer path too; got " + counters.keySet());

    long records = Long.parseLong(counters.get(lookedUp));
    long hits = Long.parseLong(counters.get(tagKey(RecordIndexMetricNames.KEY_HIT_COUNT)));
    long misses = Long.parseLong(counters.get(tagKey(RecordIndexMetricNames.KEY_MISS_COUNT)));
    assertTrue(records > 0, "the upsert sync looked up at least one key");
    assertEquals(records, hits + misses, "hits + misses must account for every key looked up");
    assertTrue(Long.parseLong(counters.get(tagKey(RecordIndexMetricNames.SHARDS_READ))) > 0,
        "at least one shard was read");
  }
}
