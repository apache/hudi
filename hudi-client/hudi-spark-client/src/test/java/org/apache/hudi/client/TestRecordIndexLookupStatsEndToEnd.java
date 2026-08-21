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

package org.apache.hudi.client;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metrics.Metrics;
import org.apache.hudi.metrics.MetricsReporterType;
import org.apache.hudi.metrics.RecordIndexLookupStatsReporter;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End to end coverage for record index lookup observability: an upsert with a known
 * update/insert mix must land exact counts in commit metadata.
 */
public class TestRecordIndexLookupStatsEndToEnd extends HoodieClientTestBase {

  private static final int NUM_SHARDS = 4;

  private HoodieWriteConfig writeConfig(boolean statsEnabled) {
    return writeConfigBuilder(statsEnabled).build();
  }

  private HoodieWriteConfig.Builder writeConfigBuilder(boolean statsEnabled) {
    return getConfigBuilder()
        .withIndexConfig(org.apache.hudi.config.HoodieIndexConfig.newBuilder()
            .withIndexType(HoodieIndex.IndexType.RECORD_INDEX).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withMetadataIndexColumnStats(false)
            .withEnableGlobalRecordLevelIndex(true)
            .withRecordIndexFileGroupCount(NUM_SHARDS, NUM_SHARDS)
            .withRecordIndexLookupStats(statsEnabled)
            .build());
  }

  /**
   * Reads the stats payload written into the latest commit, or null if absent.
   *
   * <p>Scoped to the commits timeline rather than every completed instant: abandoning a commit makes
   * the next write roll it back, and a rollback instant is not commit metadata.
   */
  private String latestLookupStatsPayload() throws java.io.IOException {
    HoodieTableMetaClient reloaded = HoodieTableMetaClient.reload(metaClient);
    Option<HoodieInstant> latest = reloaded.getActiveTimeline()
        .getCommitsTimeline().filterCompletedInstants().lastInstant();
    assertTrue(latest.isPresent(), "expected a completed commit");
    HoodieCommitMetadata commitMetadata = reloaded.getActiveTimeline()
        .readCommitMetadata(latest.get());
    return commitMetadata.getExtraMetadata().get(RecordIndexLookupStatsReporter.COMMIT_METADATA_KEY);
  }

  private static long valueOf(String payload, String metric) {
    String needle = "\"" + metric + "\":";
    int start = payload.indexOf(needle);
    assertTrue(start >= 0, "metric " + metric + " missing from " + payload);
    start += needle.length();
    int end = start;
    while (end < payload.length() && (Character.isDigit(payload.charAt(end)))) {
      end++;
    }
    return Long.parseLong(payload.substring(start, end));
  }

  @Test
  public void testUpsertRecordsExactHitAndMissCounts() throws Exception {
    HoodieWriteConfig config = writeConfig(true);
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      // Seed 100 records so the record index has something to hit against.
      String firstCommit = WriteClientTestUtils.createNewInstantTime();
      List<HoodieRecord> inserts = dataGen.generateInserts(firstCommit, 100);
      WriteClientTestUtils.startCommitWithTime(client, firstCommit);
      client.commit(firstCommit, client.upsert(jsc.parallelize(inserts, 2), firstCommit));

      // 60 of the existing keys plus 40 brand new ones: 60 hits, 40 misses.
      String secondCommit = WriteClientTestUtils.createNewInstantTime();
      List<HoodieRecord> upserts = new ArrayList<>(dataGen.generateUpdates(secondCommit, inserts.subList(0, 60)));
      upserts.addAll(dataGen.generateInserts(secondCommit, 40));
      WriteClientTestUtils.startCommitWithTime(client, secondCommit);
      JavaRDD<WriteStatus> statuses = client.upsert(jsc.parallelize(upserts, 2), secondCommit);
      client.commit(secondCommit, statuses);

      String payload = latestLookupStatsPayload();
      assertNotNull(payload, "commit metadata must carry the lookup stats payload");

      assertEquals(100L, valueOf(payload, "lookup_record_index_key_count"),
          "every incoming key is probed: " + payload);
      assertEquals(60L, valueOf(payload, "lookup_record_index_key_hit_count"),
          "exactly the 60 pre-existing keys hit: " + payload);

      long shardsRead = valueOf(payload, "lookup_record_index_shards_read");
      assertTrue(shardsRead > 0 && shardsRead <= NUM_SHARDS,
          "shards read must be within the configured shard count, was " + shardsRead);
      assertTrue(valueOf(payload, "lookup_record_index_bytes_in_shards_read") > 0,
          "a shard that was read has a non-zero footprint: " + payload);
    }
  }

  @Test
  public void testDisabledByDefaultWritesNothing() throws Exception {
    HoodieWriteConfig config = writeConfig(false);
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      String firstCommit = WriteClientTestUtils.createNewInstantTime();
      List<HoodieRecord> inserts = dataGen.generateInserts(firstCommit, 50);
      WriteClientTestUtils.startCommitWithTime(client, firstCommit);
      client.commit(firstCommit, client.upsert(jsc.parallelize(inserts, 2), firstCommit));

      String secondCommit = WriteClientTestUtils.createNewInstantTime();
      List<HoodieRecord> upserts = new ArrayList<>(dataGen.generateUpdates(secondCommit, inserts.subList(0, 25)));
      WriteClientTestUtils.startCommitWithTime(client, secondCommit);
      client.commit(secondCommit, client.upsert(jsc.parallelize(upserts, 2), secondCommit));

      assertFalse(latestLookupStatsPayload() != null,
          "no payload when the feature is off");
    }
  }

  @Test
  public void testMetricsReporterReceivesGaugesWhenMetricsAreOn() throws Exception {
    // Regression guard: the commit-metadata half of this feature can pass while the reporter half
    // throws, because HoodieMetrics only builds its Metrics instance when hoodie.metrics.on is
    // true — which is off by default, and off in this harness.
    HoodieWriteConfig config = writeConfigBuilder(true)
        .withMetricsConfig(HoodieMetricsConfig.newBuilder()
            .on(true)
            .withReporterType(MetricsReporterType.INMEMORY.name())
            .build())
        .build();

    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      String firstCommit = WriteClientTestUtils.createNewInstantTime();
      List<HoodieRecord> inserts = dataGen.generateInserts(firstCommit, 60);
      WriteClientTestUtils.startCommitWithTime(client, firstCommit);
      client.commit(firstCommit, client.upsert(jsc.parallelize(inserts, 2), firstCommit));

      String secondCommit = WriteClientTestUtils.createNewInstantTime();
      List<HoodieRecord> upserts = new ArrayList<>(dataGen.generateUpdates(secondCommit, inserts.subList(0, 30)));
      WriteClientTestUtils.startCommitWithTime(client, secondCommit);
      client.commit(secondCommit, client.upsert(jsc.parallelize(upserts, 2), secondCommit));

      assertNotNull(latestLookupStatsPayload(), "commit metadata is still written");

      Set<String> gaugeNames = Metrics.getInstance(config.getMetricsConfig(), storage)
          .getRegistry().getGauges().keySet();
      assertTrue(gaugeNames.stream().anyMatch(n -> n.endsWith("lookup_record_index_key_count")),
          "key count gauge must reach the registry, saw: " + gaugeNames);
      assertTrue(gaugeNames.stream().anyMatch(n -> n.endsWith("lookup_record_index_key_hit_count")),
          "hit count gauge must reach the registry, saw: " + gaugeNames);
      assertTrue(gaugeNames.stream().anyMatch(n -> n.endsWith("lookup_record_index_shards_read")),
          "shards read gauge must reach the registry, saw: " + gaugeNames);

      // Names must be qualified the way every other Hudi metric is. An unqualified name would
      // collide across tables reporting into one backend, which is the whole point of the prefix.
      assertFalse(gaugeNames.contains("lookup_record_index_key_count"),
          "gauge must not be emitted unqualified, saw: " + gaugeNames);
      assertTrue(gaugeNames.stream()
              .filter(n -> n.endsWith("lookup_record_index_key_count"))
              .allMatch(n -> n.contains(".")),
          "gauge name must carry the configured reporter prefix, saw: " + gaugeNames);
    }
  }

  @Test
  public void testCommitMetadataStillWrittenWhenMetricsAreOff() throws Exception {
    // hoodie.metrics.on defaults to false. Collecting stats must still populate commit metadata,
    // and must not blow up on the reporter path.
    HoodieWriteConfig config = writeConfig(true);
    assertFalse(config.isMetricsOn(), "this test is only meaningful with metrics off");

    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      String firstCommit = WriteClientTestUtils.createNewInstantTime();
      List<HoodieRecord> inserts = dataGen.generateInserts(firstCommit, 40);
      WriteClientTestUtils.startCommitWithTime(client, firstCommit);
      client.commit(firstCommit, client.upsert(jsc.parallelize(inserts, 2), firstCommit));

      String secondCommit = WriteClientTestUtils.createNewInstantTime();
      List<HoodieRecord> upserts = new ArrayList<>(dataGen.generateUpdates(secondCommit, inserts.subList(0, 20)));
      WriteClientTestUtils.startCommitWithTime(client, secondCommit);
      client.commit(secondCommit, client.upsert(jsc.parallelize(upserts, 2), secondCommit));

      String payload = latestLookupStatsPayload();
      assertNotNull(payload, "history must not depend on having a metrics backend");
      assertEquals(20L, valueOf(payload, "lookup_record_index_key_hit_count"), payload);
    }
  }

  @Test
  public void testAbandonedCommitDoesNotLeakIntoTheNext() throws Exception {
    // Stats drain at preCommit, which an aborted commit never reaches. The counts it collected must
    // not then be attributed to whatever commits next.
    HoodieWriteConfig config = writeConfig(true);
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      String firstCommit = WriteClientTestUtils.createNewInstantTime();
      List<HoodieRecord> inserts = dataGen.generateInserts(firstCommit, 100);
      WriteClientTestUtils.startCommitWithTime(client, firstCommit);
      client.commit(firstCommit, client.upsert(jsc.parallelize(inserts, 2), firstCommit));

      // Tag 80 keys and then walk away without committing.
      String abandoned = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, abandoned);
      client.upsert(jsc.parallelize(dataGen.generateUpdates(abandoned, inserts.subList(0, 80)), 2), abandoned)
          .collect();

      // The next commit touches only 10 keys and must report only those.
      String next = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, next);
      client.commit(next, client.upsert(
          jsc.parallelize(dataGen.generateUpdates(next, inserts.subList(0, 10)), 2), next));

      String payload = latestLookupStatsPayload();
      assertEquals(10L, valueOf(payload, "lookup_record_index_key_count"),
          "the abandoned attempt's 80 keys must not appear here: " + payload);
      assertEquals(10L, valueOf(payload, "lookup_record_index_key_hit_count"), payload);
    }
  }

  @Test
  public void testCountersDoNotCarryAcrossCommits() throws Exception {
    HoodieWriteConfig config = writeConfig(true);
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      String firstCommit = WriteClientTestUtils.createNewInstantTime();
      List<HoodieRecord> inserts = dataGen.generateInserts(firstCommit, 100);
      WriteClientTestUtils.startCommitWithTime(client, firstCommit);
      client.commit(firstCommit, client.upsert(jsc.parallelize(inserts, 2), firstCommit));

      String secondCommit = WriteClientTestUtils.createNewInstantTime();
      List<HoodieRecord> firstUpserts = new ArrayList<>(dataGen.generateUpdates(secondCommit, inserts.subList(0, 60)));
      WriteClientTestUtils.startCommitWithTime(client, secondCommit);
      client.commit(secondCommit, client.upsert(jsc.parallelize(firstUpserts, 2), secondCommit));
      assertEquals(60L, valueOf(latestLookupStatsPayload(), "lookup_record_index_key_hit_count"));

      // A smaller third commit on the SAME write client must report only its own counts.
      String thirdCommit = WriteClientTestUtils.createNewInstantTime();
      List<HoodieRecord> secondUpserts = new ArrayList<>(dataGen.generateUpdates(thirdCommit, inserts.subList(0, 10)));
      WriteClientTestUtils.startCommitWithTime(client, thirdCommit);
      client.commit(thirdCommit, client.upsert(jsc.parallelize(secondUpserts, 2), thirdCommit));

      String payload = latestLookupStatsPayload();
      assertEquals(10L, valueOf(payload, "lookup_record_index_key_count"),
          "the third commit must not inherit the second commit's counts: " + payload);
      assertEquals(10L, valueOf(payload, "lookup_record_index_key_hit_count"), payload);
    }
  }
}
