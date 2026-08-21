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
import org.apache.hudi.metadata.RecordIndexLookupStatsCollector;
import org.apache.hudi.metadata.RecordIndexShardLookupStats;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.AccumulatorV2;

/**
 * Ships per-shard record index lookup stats from executors back to the driver.
 *
 * <p>The accumulated value is keyed by shard and merged per key by field-wise max, so task retries,
 * speculation and RDD recomputation are idempotent rather than additive. Only max and map union are
 * used, both commutative and associative — which the accumulator contract requires, because Spark
 * merges executor-local copies on the driver in an unspecified order.
 *
 * <p>One instance belongs to one write client and is never held in a static field, so two tables in
 * the same JVM cannot see each other's counts.
 */
public class RecordIndexLookupStatsAccumulator
    extends AccumulatorV2<RecordIndexShardLookupStats, RecordIndexLookupStats>
    implements RecordIndexLookupStatsCollector {

  private static final long serialVersionUID = 1L;

  /** Name shown in the Spark UI's accumulator list. */
  static final String ACCUMULATOR_NAME = "hoodie.record.index.lookup.stats";

  /**
   * Volatile because {@link #value()} and {@link #copy()} read it without holding the lock that
   * {@link #add}, {@link #merge} and {@link #drain} take. Safe to publish this way precisely because
   * the value is immutable — every mutation swaps the reference rather than editing in place.
   */
  private volatile RecordIndexLookupStats stats = RecordIndexLookupStats.empty();

  /**
   * Registers with the given context if not already registered. Driver-side only, and idempotent so
   * that a write client which performs many commits registers exactly once.
   */
  public void register(JavaSparkContext jsc) {
    if (!isRegistered()) {
      jsc.sc().register(this, ACCUMULATOR_NAME);
    }
  }

  @Override
  public void collect(RecordIndexShardLookupStats shardStats) {
    add(shardStats);
  }

  @Override
  public boolean isZero() {
    return stats.isEmpty();
  }

  @Override
  public AccumulatorV2<RecordIndexShardLookupStats, RecordIndexLookupStats> copy() {
    RecordIndexLookupStatsAccumulator copy = new RecordIndexLookupStatsAccumulator();
    // Sharing the reference is safe because the value and its entries are immutable: every
    // mutation below replaces the reference rather than modifying it in place.
    copy.stats = stats;
    return copy;
  }

  @Override
  public void reset() {
    stats = RecordIndexLookupStats.empty();
  }

  @Override
  public synchronized void add(RecordIndexShardLookupStats shardStats) {
    stats = stats.merge(RecordIndexLookupStats.of(shardStats));
  }

  @Override
  public synchronized void merge(AccumulatorV2<RecordIndexShardLookupStats, RecordIndexLookupStats> other) {
    stats = stats.merge(other.value());
  }

  @Override
  public RecordIndexLookupStats value() {
    return stats;
  }

  /**
   * Returns the accumulated value and resets, so the next commit starts from a clean slate rather
   * than inheriting the previous commit's counts.
   */
  public synchronized RecordIndexLookupStats drain() {
    RecordIndexLookupStats drained = stats;
    reset();
    return drained;
  }
}
