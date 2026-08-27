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

package org.apache.hudi.index;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metrics.DistributedRegistry;
import org.apache.hudi.metrics.ExecutorMetricRegistry;
import org.apache.hudi.metrics.RecordIndexMetricNames;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/** Executor-side emission for the record index lookup counters. */
public class RecordIndexLookupMetrics {

  private RecordIndexLookupMetrics() {
  }

  /**
   * The registry a lookup task collects into, or null when nothing should be collected. Captured in the
   * lookup closure and passed to {@link #recordShardLookup}, so delivery is by closure capture rather
   * than by name.
   *
   * <p>Requires the reporter to be on as well: with {@code hoodie.metrics.on} off there is nowhere to
   * publish, and collecting would register an accumulator and scan every shard for nothing.
   */
  public static Registry resolveRegistry(HoodieEngineContext context, HoodieWriteConfig config) {
    if (!config.isMetricsOn() || !ExecutorMetricRegistry.RECORD_INDEX_LOOKUP.isEnabled(config)) {
      return null;
    }
    // TBL_NAME has no default and Builder.validate() only requires BASE_PATH, so a config built without
    // forTable() reaches here with a null name. getMetricRegistry dereferences it immediately.
    String tableName = config.getTableName();
    if (tableName == null || tableName.isEmpty()) {
      return null;
    }
    Registry registry = context.getMetricRegistry(tableName,
        ExecutorMetricRegistry.RECORD_INDEX_LOOKUP.scopedName(config.getBasePath()));
    // Only the accumulator-backed registry aggregates back to the driver. A LocalRegistry here would
    // collect on the executor and be dropped on the floor, so report nothing instead.
    if (!(registry instanceof DistributedRegistry)) {
      return null;
    }
    // Runs on the driver before the closure ships, so anything held now predates this write. Publishing
    // releases what it reports, which leaves a non-empty registry only after an attempt that never
    // committed. Paths that tear metrics down between writes lose it anyway; Spark SQL DML and StreamSync
    // do not, and this commit must not report work it did not do.
    registry.clear();
    return registry;
  }

  /**
   * Records one shard's lookup outcome. Counts records rather than distinct keys, so
   * {@code hits + misses == records_looked_up} holds when a batch repeats a key. Membership is tested
   * against the found set, bounded by the hit count, not the asked-about set, bounded by shard size.
   *
   * @param registry     where to collect, or null when collection is off
   * @param keysLookedUp every record key routed to this shard
   * @param foundKeys    the subset present in the index
   * @param elapsedMs    wall-clock spent reading this shard
   */
  public static void recordShardLookup(Registry registry, Collection<String> keysLookedUp,
                                       Collection<String> foundKeys, long elapsedMs) {
    // Return before the hit scan below, which is O(keys looked up), rather than computing counts
    // nothing will read. The query read path passes null for the same reason.
    if (registry == null || keysLookedUp.isEmpty()) {
      return;
    }
    Set<String> found = foundKeys instanceof Set ? (Set<String>) foundKeys : new HashSet<>(foundKeys);
    long records = keysLookedUp.size();
    long hits = found.isEmpty() ? 0L : keysLookedUp.stream().filter(found::contains).count();
    registry.add(RecordIndexMetricNames.KEY_COUNT, records);
    registry.add(RecordIndexMetricNames.KEY_HIT_COUNT, hits);
    registry.add(RecordIndexMetricNames.KEY_MISS_COUNT, records - hits);
    registry.increment(RecordIndexMetricNames.SHARDS_READ);
    // Summed, not averaged: shards are read in parallel, so the total is per-commit read effort rather
    // than latency. Divide by shards_read for a mean.
    registry.add(RecordIndexMetricNames.LOOKUP_TIME, elapsedMs);
  }
}
