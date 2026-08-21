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

import org.apache.hudi.metadata.RecordIndexLookupStats;
import org.apache.hudi.metadata.RecordIndexShardLookupStats;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the fold from drained lookup stats to reportable metrics and the commit metadata payload.
 */
class TestRecordIndexLookupStatsReporter {

  private static RecordIndexLookupStats twoShards() {
    return RecordIndexLookupStats
        .of(new RecordIndexShardLookupStats(0, "fg-0", 100L, 70L, 3L, 1300L, 5L))
        .merge(RecordIndexLookupStats.of(new RecordIndexShardLookupStats(2, "fg-2", 50L, 10L, 1L, 600L, 3L)));
  }

  @Test
  void testFoldsEveryMetricFromShardStats() {
    Map<String, Long> metrics = RecordIndexLookupStatsReporter.toMetrics(twoShards());

    assertEquals(5, metrics.size(), "every declared metric must be present");
    assertEquals(2L, metrics.get("lookup_record_index_shards_read"));
    assertEquals(150L, metrics.get("lookup_record_index_key_count"));
    assertEquals(80L, metrics.get("lookup_record_index_key_hit_count"));
    assertEquals(4L, metrics.get("lookup_record_index_log_files_read"));
    assertEquals(1900L, metrics.get("lookup_record_index_bytes_in_shards_read"));
  }

  @Test
  void testMissesAreDerivableFromTheEmittedCounts() {
    // No ratio is emitted; the consumer subtracts. 150 submitted - 80 hit = 70 inserts.
    Map<String, Long> metrics = RecordIndexLookupStatsReporter.toMetrics(twoShards());

    long misses = metrics.get("lookup_record_index_key_count") - metrics.get("lookup_record_index_key_hit_count");
    assertEquals(70L, misses);
  }

  @Test
  void testAllValuesAreLongForReporterCompatibility() {
    // PushGatewayReporter casts (Long) and DatadogReporter casts (long); a non-Long throws at
    // report time, which would pass a console demo and break Datadog and Prometheus.
    RecordIndexLookupStatsReporter.toMetrics(twoShards()).values()
        .forEach(value -> assertTrue(value instanceof Long, "gauge values must be Long"));
  }

  @Test
  void testEmptyStatsProduceNoPayload() {
    assertTrue(RecordIndexLookupStatsReporter.toMetrics(RecordIndexLookupStats.empty()).isEmpty());
  }

  @Test
  void testRetriedShardDoesNotInflateTotals() {
    RecordIndexLookupStats retried = twoShards()
        .merge(RecordIndexLookupStats.of(new RecordIndexShardLookupStats(0, "fg-0", 100L, 70L, 3L, 1300L, 9L)));

    Map<String, Long> metrics = RecordIndexLookupStatsReporter.toMetrics(retried);

    assertEquals(2L, metrics.get("lookup_record_index_shards_read"));
    assertEquals(150L, metrics.get("lookup_record_index_key_count"));
    assertEquals(1900L, metrics.get("lookup_record_index_bytes_in_shards_read"));
  }

  @Test
  void testJsonPayloadIsCompactVersionedAndComplete() {
    Map<String, Long> metrics = RecordIndexLookupStatsReporter.toMetrics(twoShards());

    String json = RecordIndexLookupStatsReporter.toJson(metrics);

    assertFalse(json.contains("\n"), "payload must be compact for the timeline");
    assertTrue(json.startsWith("{\"version\":1"), "payload must be versioned: " + json);
    assertTrue(json.endsWith("}"), "payload must be well formed: " + json);
    metrics.forEach((name, value) ->
        assertTrue(json.contains("\"" + name + "\":" + value), "missing " + name + " in " + json));
  }

  @Test
  void testCommitMetadataKeyIsStable() {
    // Consumers read this key off the timeline; renaming it silently breaks them.
    assertEquals("hoodie.rli.lookup.stats", RecordIndexLookupStatsReporter.COMMIT_METADATA_KEY);
  }
}
