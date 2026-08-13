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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Turns drained record index lookup stats into reportable metrics.
 *
 * <p>Purely a fold over the per-shard map. Every count, including log files and bytes, was captured
 * on the executor where the file slice was in scope, so nothing here touches storage and the commit
 * path pays no I/O for instrumentation.
 *
 * <p>Every value is a {@code Long}. This is not incidental: the Prometheus push gateway reporter
 * casts gauge values to {@code Long} and the Datadog reporter casts to {@code long}, so a
 * non-numeric gauge would pass a console-based demo and throw in production.
 */
public class RecordIndexLookupStatsReporter {

  /** Commit metadata key holding the compact JSON payload. */
  public static final String COMMIT_METADATA_KEY = "hoodie.rli.lookup.stats";

  /** Payload schema version, so fields can be added later without ambiguity. */
  static final int PAYLOAD_VERSION = 1;

  static final String SHARDS_READ = "lookup_record_index_shards_read";
  // Reuses the metric names declared in HoodieMetadataMetrics, which were never wired to anything.
  static final String KEYS_COUNT = "lookup_record_index_key_count";
  static final String KEYS_HIT_COUNT = "lookup_record_index_key_hit_count";
  static final String LOG_FILES_READ = "lookup_record_index_log_files_read";
  static final String BYTES_IN_SHARDS_READ = "lookup_record_index_bytes_in_shards_read";

  private RecordIndexLookupStatsReporter() {
  }

  /**
   * @param stats drained per-shard stats.
   * @return metric name to value, empty when no shard was read.
   */
  public static Map<String, Long> toMetrics(RecordIndexLookupStats stats) {
    if (stats.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, Long> metrics = new LinkedHashMap<>();
    metrics.put(SHARDS_READ, stats.getShardsRead());
    metrics.put(KEYS_COUNT, stats.getKeysSubmitted());
    metrics.put(KEYS_HIT_COUNT, stats.getKeysHit());
    metrics.put(LOG_FILES_READ, stats.getLogFilesRead());
    metrics.put(BYTES_IN_SHARDS_READ, stats.getBytesInShardsRead());
    return metrics;
  }

  /**
   * Compact JSON for the commit metadata payload. One versioned key rather than five flat ones
   * keeps the timeline tidy and lets the payload gain fields without new keys.
   */
  public static String toJson(Map<String, Long> metrics) {
    StringBuilder json = new StringBuilder("{\"version\":").append(PAYLOAD_VERSION);
    metrics.forEach((name, value) -> json.append(",\"").append(name).append("\":").append(value));
    return json.append('}').toString();
  }
}
