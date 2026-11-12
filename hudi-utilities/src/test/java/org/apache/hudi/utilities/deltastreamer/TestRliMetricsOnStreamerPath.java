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
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieDataUtils;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metrics.RecordIndexLookupMetrics;
import org.apache.hudi.utilities.config.SourceTestConfig;
import org.apache.hudi.utilities.testutils.CapturingMetricsReporter;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    String marker = RecordIndexLookupMetrics.METRIC_ACTION
        + "." + RecordIndexLookupMetrics.METRIC_QUALIFIER + ".";
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

    String lookedUp = RecordIndexLookupMetrics.KEY_COUNT;
    assertTrue(counters.containsKey(lookedUp),
        "tag-location traffic must be attributed on the streamer path too; got " + counters.keySet());

    long records = Long.parseLong(counters.get(lookedUp));
    long hits = Long.parseLong(counters.get(RecordIndexLookupMetrics.KEY_HIT_COUNT));
    long misses = Long.parseLong(counters.get(RecordIndexLookupMetrics.KEY_MISS_COUNT));
    // Exact, not an invariant: misses is derived as records - hits at the emission site, so
    // records == hits + misses holds by construction and would survive a doubled count. The workload is
    // deterministic -- the first sync writes 1000 records, the second updates 500 and inserts 500.
    assertEquals(1000L, records, "the upsert sync looked up every key from the first sync");
    assertEquals(500L, hits, "the 500 updates hit the index");
    assertEquals(500L, misses, "the 500 fresh inserts missed");
    // Shard count is not pinned: it follows the index file-group layout, which differs between the
    // global and partitioned variants (10 and 3 on this workload).
    assertTrue(Long.parseLong(counters.get(RecordIndexLookupMetrics.SHARDS_READ)) > 0,
        "at least one shard was read");
  }

  /**
   * A streamer running in continuous mode re-evaluates record index deferral on every commit, so the index is built
   * once the table holds the configured number of records and is sized from them, while the same streamer keeps writing.
   */
  @Test
  public void testRecordIndexDeferredUntilMinRecordsInContinuousMode() throws Exception {
    String tableBasePath = basePath + "/test_rli_deferred_continuous";
    int totalRecords = 1500;
    HoodieDeltaStreamer.Config cfg = TestHoodieDeltaStreamer.TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT);
    cfg.continuousMode = true;
    cfg.sourceLimit = 300;
    cfg.configs.add(SourceTestConfig.MAX_UNIQUE_RECORDS_PROP.key() + "=" + totalRecords);
    cfg.configs.add(HoodieMetadataConfig.ENABLE.key() + "=true");
    cfg.configs.add(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key() + "=true");
    cfg.configs.add(HoodieIndexConfig.INDEX_TYPE.key() + "=GLOBAL_RECORD_LEVEL_INDEX");
    cfg.configs.add(HoodieMetadataConfig.DEFER_RLI_INIT_FOR_FRESH_TABLE.key() + "=true");
    cfg.configs.add(HoodieMetadataConfig.DEFER_RLI_INIT_MIN_RECORDS.key() + "=1000");
    cfg.configs.add(HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_SIZE_BYTES_PROP.key() + "=9600");
    cfg.configs.add(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key() + "=1");
    cfg.configs.add(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MAX_FILE_GROUP_COUNT_PROP.key() + "=1000");

    HoodieDeltaStreamer streamer = new HoodieDeltaStreamer(cfg, jsc);
    TestHoodieDeltaStreamer.deltaStreamerTestRunner(streamer, cfg, ignored -> {
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setBasePath(tableBasePath).setConf(storage.getConf()).build();
      assertTrue(metaClient.getTableConfig().getMetadataPartitions().contains(MetadataPartitionType.RECORD_INDEX.getPartitionPath()));
      assertRecordCount(totalRecords, tableBasePath, sqlContext);
      return true;
    });

    assertDistanceCount(totalRecords, tableBasePath, sqlContext);
    List<String> recordKeys = sqlContext.read().format("hudi").load(tableBasePath).select("_hoodie_record_key").collectAsList().stream()
        .map(row -> row.getString(0)).collect(Collectors.toList());
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setBasePath(tableBasePath).setConf(storage.getConf()).build();
    try (HoodieTableMetadata metadata = metaClient.getTableFormat().getMetadataFactory()
        .create(context, storage, HoodieMetadataConfig.newBuilder().enable(true).build(), tableBasePath)) {
      // Built from at least 1000 records (>= 10 file groups) and at most the 1500 the source produces (<= 15). Building
      // it on the 2nd commit from 300 records would give 3.
      long fileGroupCount = ((HoodieBackedTableMetadata) metadata).getMetadataFileSystemView()
          .getAllFileGroups(MetadataPartitionType.RECORD_INDEX.getPartitionPath()).count();
      assertTrue(fileGroupCount >= 10 && fileGroupCount <= 15, "Unexpected record index file group count " + fileGroupCount);
      HoodiePairData<String, HoodieRecordGlobalLocation> locations = metadata.readRecordIndexLocationsWithKeys(HoodieListData.eager(recordKeys));
      try {
        assertEquals(new HashSet<>(recordKeys), HoodieDataUtils.dedupeAndCollectAsMap(locations).keySet());
      } finally {
        locations.unpersistWithDependencies();
      }
    }
  }
}
