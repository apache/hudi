/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.metrics;

import com.codahale.metrics.SlidingWindowReservoir;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.hudi.common.util.VisibleForTesting;

/**
 * Metrics for the {@link org.apache.hudi.sink.partitioner.index.GlobalRecordLevelIndexBackend}.
 * Tracks the latency of local (cache) vs. remote (metadata table) lookups, the per-lookup key
 * counts, and the per-mini-batch in-memory cache hit ratio.
 */
public class FlinkIndexBackendMetrics extends HoodieFlinkMetrics {
  private static final int HISTOGRAM_WINDOW_SIZE = 100;
  private static final String LOCAL_INDEX_LOOKUP_KEY = "local_index_lookup";
  private static final String REMOTE_INDEX_LOOKUP_KEY = "remote_index_lookup";

  public static final String LOOKUP_CACHE_HIT_RATIO = "lookupCacheHitRatio";

  /** Latency of the local (cache) phase of each index lookup, in milliseconds. */
  private final Histogram localIndexLookupLatency;

  /** Latency of the remote (metadata table) phase of each index lookup, in milliseconds. */
  private final Histogram remoteIndexLookupLatency;

  /** Number of keys resolved from the local cache per lookup. */
  private final Histogram localLookupKeysNum;

  /** Number of keys that missed the local cache and were fetched remotely per lookup. */
  private final Histogram remoteLookupKeysNum;

  /**
   * Latest per-mini-batch in-memory cache hit ratio (hits / (hits + misses)).
   * Set on each {@link #updateLookupCacheHitRatio(long, long)} call; defaults to 0.0
   * and is left unchanged when a lookup observes zero total keys.
   * Marked {@code volatile} because Flink's reporter thread reads it while the
   * bucket-assign task thread writes to it.
   */
  private volatile double lookupCacheHitRatio = 0.0D;

  public FlinkIndexBackendMetrics(MetricGroup metricGroup) {
    super(metricGroup);
    this.localIndexLookupLatency = new DropwizardHistogramWrapper(
        new com.codahale.metrics.Histogram(new SlidingWindowReservoir(HISTOGRAM_WINDOW_SIZE)));
    this.remoteIndexLookupLatency = new DropwizardHistogramWrapper(
        new com.codahale.metrics.Histogram(new SlidingWindowReservoir(HISTOGRAM_WINDOW_SIZE)));
    this.localLookupKeysNum = new DropwizardHistogramWrapper(
        new com.codahale.metrics.Histogram(new SlidingWindowReservoir(HISTOGRAM_WINDOW_SIZE)));
    this.remoteLookupKeysNum = new DropwizardHistogramWrapper(
        new com.codahale.metrics.Histogram(new SlidingWindowReservoir(HISTOGRAM_WINDOW_SIZE)));
  }

  @Override
  public void registerMetrics() {
    metricGroup.histogram("localIndexLookupLatency", localIndexLookupLatency);
    metricGroup.histogram("remoteIndexLookupLatency", remoteIndexLookupLatency);
    metricGroup.histogram("localLookupKeysNum", localLookupKeysNum);
    metricGroup.histogram("remoteLookupKeysNum", remoteLookupKeysNum);
    metricGroup.gauge(LOOKUP_CACHE_HIT_RATIO, (Gauge<Double>) () -> lookupCacheHitRatio);
  }

  /**
   * Updates the per-mini-batch cache hit-ratio gauge from the hit/miss counts already
   * fed into {@link #updateLocalLookupKeysCount(long)} and {@link #updateRemoteLookupKeysCount(long)}.
   * When the lookup observed no keys, the previous value is preserved so dashboards
   * don't oscillate back to zero on idle mini-batches.
   */
  public void updateLookupCacheHitRatio(long hitCount, long missCount) {
    long total = hitCount + missCount;
    if (total <= 0L) {
      return;
    }
    lookupCacheHitRatio = (double) hitCount / total;
  }

  public void startLocalIndexLookup() {
    startTimer(LOCAL_INDEX_LOOKUP_KEY);
  }

  public void endLocalIndexLookup() {
    localIndexLookupLatency.update(stopTimer(LOCAL_INDEX_LOOKUP_KEY));
  }

  public void updateLocalLookupKeysCount(long n) {
    localLookupKeysNum.update(n);
  }

  public void startRemoteIndexLookup() {
    startTimer(REMOTE_INDEX_LOOKUP_KEY);
  }

  public void endRemoteIndexLookup() {
    remoteIndexLookupLatency.update(stopTimer(REMOTE_INDEX_LOOKUP_KEY));
  }

  public void updateRemoteLookupKeysCount(long n) {
    remoteLookupKeysNum.update(n);
  }

  @VisibleForTesting
  public long getLocalIndexLookupCount() {
    return localIndexLookupLatency.getCount();
  }

  @VisibleForTesting
  public long getRemoteIndexLookupCount() {
    return remoteIndexLookupLatency.getCount();
  }

  @VisibleForTesting
  public long getLocalLookupKeysSampleCount() {
    return localLookupKeysNum.getCount();
  }

  @VisibleForTesting
  public long getRemoteLookupKeysSampleCount() {
    return remoteLookupKeysNum.getCount();
  }

  @VisibleForTesting
  public double getLookupCacheHitRatio() {
    return lookupCacheHitRatio;
  }
}
