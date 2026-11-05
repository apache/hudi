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

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.StoragePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The record index lookup counters, end to end: what they are called, where they are collected, and how
 * they reach the reporter once a commit lands.
 *
 * <p>The numbers are only knowable on executors, because a lookup returns its hits and a miss produces
 * no output row at all. So the driver resolves an accumulator-backed {@link DistributedRegistry}, the
 * lookup closure captures it, tasks add into their own copy, and Spark merges those copies home.
 */
public class RecordIndexLookupMetrics {

  private static final Logger LOG = LoggerFactory.getLogger(RecordIndexLookupMetrics.class);

  /** The registry name, as reported. Keying is done by {@link #registryKey}. */
  public static final String REGISTRY_NAME = "HoodieRecordIndexLookup";

  /** Reporter naming, as passed to {@code HoodieMetrics.getMetricsName}. */
  public static final String METRIC_ACTION = "rli";
  public static final String METRIC_QUALIFIER = "lookup";

  /** Counts records, not distinct keys: a batch repeating a key contributes once per record, which is
   * what keeps {@code hits + misses == key_count} exact. */
  public static final String KEY_COUNT = "lookup_record_index_key_count";
  public static final String KEY_HIT_COUNT = "lookup_record_index_key_hit_count";
  public static final String KEY_MISS_COUNT = "lookup_record_index_key_miss_count";
  public static final String SHARDS_READ = "lookup_record_index_shards_read";
  /**
   * Wall-clock spent in the shard read, summed across shards rather than averaged because shards are read
   * in parallel: the value is per-commit read effort, and dividing by {@link #SHARDS_READ} gives a mean.
   *
   * <p>Distinct from {@code index.lookup.duration} published by {@code HoodieMetrics.updateIndexMetrics},
   * which is driver wall-clock for the whole {@code tagLocation} including scheduling. Comparing the two
   * shows how much of a lookup was actually spent reading the index.
   */
  public static final String LOOKUP_TIME = "lookup_record_index_time";

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
  public static Option<Registry> resolveRegistry(HoodieEngineContext context, HoodieWriteConfig config) {
    if (!config.isMetricsOn() || !config.isRecordIndexLookupMetricsEnabled()
        || !(context instanceof HoodieSparkEngineContext)) {
      return Option.empty();
    }
    DistributedRegistry registry = ((HoodieSparkEngineContext) context)
        .getOrCreateOwnedRegistry(registryKey(config.getBasePath()), REGISTRY_NAME);
    // Anything held predates this write: the drain removes the entry as it publishes, so a surviving
    // one belongs to an attempt that never committed.
    registry.clear();
    return Option.of(registry);
  }

  /**
   * Records one shard's lookup outcome. Counts records rather than distinct keys, so
   * {@code hits + misses == records_looked_up} holds when a batch repeats a key. Membership is tested
   * against the found set, bounded by the hit count, not the asked-about set, bounded by shard size.
   *
   * <p>Callers must skip the call entirely when the registry is null. Building the found set is
   * O(hits), and an argument is evaluated before this method can take a null fast path, so guarding
   * here would not keep that cost off the disabled path.
   *
   * @param registry     where to collect, never null
   * @param keysLookedUp every record key routed to this shard
   * @param foundKeys    the subset present in the index
   * @param elapsedMs    wall-clock spent reading this shard
   */
  public static void recordShardLookup(Registry registry, Collection<String> keysLookedUp,
                                       Collection<String> foundKeys, long elapsedMs) {
    if (keysLookedUp.isEmpty()) {
      return;
    }
    Set<String> found = foundKeys instanceof Set ? (Set<String>) foundKeys : new HashSet<>(foundKeys);
    long records = keysLookedUp.size();
    long hits = found.isEmpty() ? 0L : keysLookedUp.stream().filter(found::contains).count();
    registry.add(KEY_COUNT, records);
    registry.add(KEY_HIT_COUNT, hits);
    registry.add(KEY_MISS_COUNT, records - hits);
    registry.increment(SHARDS_READ);
    registry.add(LOOKUP_TIME, elapsedMs);
  }

  /**
   * Reports the counters and releases them, so the next commit reports only its own work. Called once
   * the commit has landed, so a commit that never lands publishes nothing.
   *
   * <p>Release subtracts what was reported rather than clearing, so a straggler task whose update lands
   * mid-publish carries into the next commit instead of being dropped. An all-zero registry is skipped.
   */
  public static void publishAndRelease(HoodieEngineContext context, HoodieWriteConfig config,
                                       HoodieMetrics hoodieMetrics) {
    try {
      publish(context, config, hoodieMetrics);
    } catch (Exception e) {
      // This runs after the commit has landed. Reporting is not worth failing a completed write over.
      LOG.warn("Failed to publish record index lookup metrics; the commit is unaffected.", e);
    }
  }

  private static void publish(HoodieEngineContext context, HoodieWriteConfig config,
                              HoodieMetrics hoodieMetrics) {
    if (!config.isRecordIndexLookupMetricsEnabled()) {
      // Callers gate before reaching here, so this is a wiring mistake rather than normal flow.
      LOG.warn("Record index lookup metrics drain reached with the feature disabled; nothing published.");
      return;
    }
    if (!(context instanceof HoodieSparkEngineContext)) {
      return;
    }
    // Removing is what makes attribution work. An entry exists only because a lookup on this write
    // created it, so its absence means this write looked nothing up and has nothing of its own to
    // report -- which is the case an operation type cannot tell apart, since an insert that drops
    // duplicates tags through SparkRDDReadClient without requiring tagging.
    Option<DistributedRegistry> taken =
        ((HoodieSparkEngineContext) context).removeOwnedRegistry(registryKey(config.getBasePath()));
    if (!taken.isPresent()) {
      zeroPreviouslyReported(hoodieMetrics);
      return;
    }
    DistributedRegistry registry = taken.get();
    Map<String, Long> counts = new HashMap<>(registry.getAllCounts(false));
    if (counts.values().stream().allMatch(value -> value == 0L)) {
      // Gauges hold their last value until overwritten, so leaving them alone would have a reporter
      // re-emit the previous commit's numbers for this one. Zero them instead.
      zeroPreviouslyReported(hoodieMetrics);
      return;
    }
    publishToReporter(counts, hoodieMetrics);
    registry.release(counts);
  }

  /**
   * Resets the gauges to zero, for a commit that collected nothing. Only names already published are
   * touched, so a table that has never emitted stays absent rather than reporting a row of zeros.
   */
  private static void zeroPreviouslyReported(HoodieMetrics hoodieMetrics) {
    if (hoodieMetrics == null || hoodieMetrics.getMetrics() == null) {
      return;
    }
    String prefix = hoodieMetrics.getMetricsName(METRIC_ACTION, METRIC_QUALIFIER);
    if (prefix == null) {
      return;
    }
    Map<String, Long> zeroed = new HashMap<>();
    hoodieMetrics.getMetrics().getRegistry().getGauges().keySet().stream()
        .filter(name -> name.startsWith(prefix + "."))
        .forEach(name -> zeroed.put(name.substring(prefix.length() + 1), 0L));
    if (!zeroed.isEmpty()) {
      publishToReporter(zeroed, hoodieMetrics);
    }
  }

  /** Gauges, so each commit overwrites the previous value rather than accumulating. */
  private static void publishToReporter(Map<String, Long> counts, HoodieMetrics hoodieMetrics) {
    if (hoodieMetrics == null || hoodieMetrics.getMetrics() == null) {
      return;
    }
    String prefix = hoodieMetrics.getMetricsName(METRIC_ACTION, METRIC_QUALIFIER);
    hoodieMetrics.getMetrics().registerGauges(counts, Option.ofNullable(prefix));
  }

  /**
   * Key for this table's registry inside the owning context. The base path is normalized rather than used
   * raw because one table is spelled more than one way: Spark SQL builds its config from the catalog
   * location ({@code file:///data/t}) while the DataSource passes the bare path ({@code /data/t}), and
   * the two must resolve to one entry. The authority is kept so {@code s3://a/t} and {@code s3://b/t}
   * stay distinct.
   *
   * <p>No digest: this key never leaves the context and never becomes part of a metric name.
   */
  static String registryKey(String basePath) {
    StoragePath path = new StoragePath(basePath);
    String authority = path.toUri().getAuthority();
    return (authority == null ? "" : authority) + path.getPathWithoutSchemeAndAuthority();
  }
}
