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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Executor-side emission for the record index lookup counters. */
public class RecordIndexLookupMetrics {

  private RecordIndexLookupMetrics() {
  }

  /**
   * The registries a lookup task collects into, keyed by bare name. Includes every entry on
   * {@link ExecutorMetricRegistry}. Delivery is by closure capture, which is deterministic; resolution is
   * by name, which lets code below the write API take part without a signature change.
   */
  public static Map<String, Registry> resolveBundle(HoodieEngineContext context, HoodieWriteConfig config) {
    Map<String, Registry> bundle = new HashMap<>();
    for (ExecutorMetricRegistry metricRegistry : ExecutorMetricRegistry.values()) {
      if (!metricRegistry.isEnabled(config)) {
        continue;
      }
      Registry registry = context.getMetricRegistry(config.getTableName(),
          metricRegistry.scopedName(config.getBasePath()));
      // Only the accumulator-backed registry aggregates back to the driver, so anything else is left out
      // rather than bound: a bound LocalRegistry would collect on the executor and be dropped on the floor,
      // whereas leaving it out makes the lookup resolve to a no-op that reports nothing.
      if (registry instanceof DistributedRegistry) {
        bundle.put(metricRegistry.registryName(), registry);
      }
    }
    return bundle.isEmpty() ? Collections.emptyMap() : bundle;
  }

  /**
   * Records one shard's lookup outcome. Counts records rather than distinct keys, so
   * {@code hits + misses == records_looked_up} holds when a batch repeats a key. Membership is tested
   * against the found set, bounded by the hit count, not the asked-about set, bounded by shard size.
   *
   * @param keysLookedUp every record key routed to this shard
   * @param foundKeys    the subset present in the index
   * @param elapsedMs    wall-clock spent reading this shard
   */
  public static void recordShardLookup(Collection<String> keysLookedUp,
                                       Collection<String> foundKeys, long elapsedMs) {
    if (keysLookedUp.isEmpty()) {
      return;
    }
    Registry registry = Registry.getRegistry(RecordIndexMetricNames.REGISTRY_NAME);
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
