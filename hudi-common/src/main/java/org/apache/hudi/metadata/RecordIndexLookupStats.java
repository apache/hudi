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

package org.apache.hudi.metadata;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Aggregate record index lookup stats for one write, keyed by shard index.
 *
 * <p>Keying by shard rather than summing scalars is what gives the aggregate its idempotence under
 * task retry, speculation and RDD recomputation: re-reporting a shard replaces its entry instead of
 * adding to a running total. Totals are folds over the map, computed on demand.
 */
public class RecordIndexLookupStats implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final RecordIndexLookupStats EMPTY =
      new RecordIndexLookupStats(Collections.emptyMap());

  private final Map<Integer, RecordIndexShardLookupStats> shardStats;

  private RecordIndexLookupStats(Map<Integer, RecordIndexShardLookupStats> shardStats) {
    this.shardStats = shardStats;
  }

  public static RecordIndexLookupStats empty() {
    return EMPTY;
  }

  public static RecordIndexLookupStats of(RecordIndexShardLookupStats stats) {
    return new RecordIndexLookupStats(Collections.singletonMap(stats.getShardIndex(), stats));
  }

  public RecordIndexLookupStats merge(RecordIndexLookupStats other) {
    if (other.shardStats.isEmpty()) {
      return this;
    }
    if (shardStats.isEmpty()) {
      return other;
    }
    Map<Integer, RecordIndexShardLookupStats> merged = new HashMap<>(shardStats);
    other.shardStats.forEach(
        (shard, stats) -> merged.merge(shard, stats, RecordIndexShardLookupStats::merge));
    return new RecordIndexLookupStats(merged);
  }

  public Map<Integer, RecordIndexShardLookupStats> getShardStats() {
    return Collections.unmodifiableMap(shardStats);
  }

  public boolean isEmpty() {
    return shardStats.isEmpty();
  }

  /**
   * Shards genuinely read. An engine may launch one task per file group, but only a task that
   * actually reads a shard contributes an entry, so empty tasks do not inflate this count.
   */
  public long getShardsRead() {
    return shardStats.size();
  }

  public long getKeysSubmitted() {
    return shardStats.values().stream().mapToLong(RecordIndexShardLookupStats::getKeysSubmitted).sum();
  }

  public long getKeysHit() {
    return shardStats.values().stream().mapToLong(RecordIndexShardLookupStats::getKeysHit).sum();
  }

  public long getLogFilesRead() {
    return shardStats.values().stream().mapToLong(RecordIndexShardLookupStats::getLogFilesRead).sum();
  }

  public long getBytesInShardsRead() {
    return shardStats.values().stream().mapToLong(RecordIndexShardLookupStats::getBytesInShard).sum();
  }
}
