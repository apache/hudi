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

import org.apache.hudi.common.util.VisibleForTesting;

import com.codahale.metrics.SlidingWindowReservoir;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;

/**
 * Metrics for the {@link org.apache.hudi.sink.partitioner.index.RecordLevelIndexBackend}.
 *
 * <p>The partitioned RLI backend's only remote operation is the one-shot per-partition bootstrap
 * into the local cache; regular records are served entirely from the local cache. The lookup-
 * centric metrics in {@link FlinkIndexBackendMetrics} therefore do not apply here. This class
 * instead tracks the distribution of per-bootstrap latency and per-bootstrap key counts on each
 * subtask. The histogram {@code count} field doubles as the cumulative bootstrap count for the
 * subtask.
 */
public class FlinkPartitionedIndexBackendMetrics extends HoodieFlinkMetrics {
  private static final int HISTOGRAM_WINDOW_SIZE = 100;
  private static final String PARTITION_BOOTSTRAP_KEY = "partition_bootstrap";

  static final String PARTITION_BOOTSTRAP_LATENCY_MILLIS = "partition_bootstrap_latency_millis";
  static final String PARTITION_BOOTSTRAP_KEYS_LOADED = "partition_bootstrap_keys_loaded";

  /** Latency of each partition bootstrap on this subtask, in milliseconds. */
  private final Histogram partitionBootstrapLatencyMillis;

  /** Number of RLI rows loaded into the local cache per partition bootstrap (post ownership filter). */
  private final Histogram partitionBootstrapKeysLoaded;

  public FlinkPartitionedIndexBackendMetrics(MetricGroup metricGroup) {
    super(metricGroup);
    this.partitionBootstrapLatencyMillis = new DropwizardHistogramWrapper(
        new com.codahale.metrics.Histogram(new SlidingWindowReservoir(HISTOGRAM_WINDOW_SIZE)));
    this.partitionBootstrapKeysLoaded = new DropwizardHistogramWrapper(
        new com.codahale.metrics.Histogram(new SlidingWindowReservoir(HISTOGRAM_WINDOW_SIZE)));
  }

  @Override
  public void registerMetrics() {
    metricGroup.histogram(PARTITION_BOOTSTRAP_LATENCY_MILLIS, partitionBootstrapLatencyMillis);
    metricGroup.histogram(PARTITION_BOOTSTRAP_KEYS_LOADED, partitionBootstrapKeysLoaded);
  }

  public void startPartitionBootstrap() {
    startTimer(PARTITION_BOOTSTRAP_KEY);
  }

  public void endPartitionBootstrap() {
    partitionBootstrapLatencyMillis.update(stopTimer(PARTITION_BOOTSTRAP_KEY));
  }

  public void updatePartitionBootstrapKeysLoaded(long keysLoaded) {
    partitionBootstrapKeysLoaded.update(keysLoaded);
  }

  @VisibleForTesting
  public long getPartitionBootstrapCount() {
    return partitionBootstrapLatencyMillis.getCount();
  }

  @VisibleForTesting
  public long getPartitionBootstrapKeysLoadedSampleCount() {
    return partitionBootstrapKeysLoaded.getCount();
  }
}
