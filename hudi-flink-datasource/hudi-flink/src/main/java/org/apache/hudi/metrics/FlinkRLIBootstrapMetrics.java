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

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

/**
 * Metrics for {@link org.apache.hudi.sink.bootstrap.RLIBootstrapOperator}.
 *
 * <p>Exposes gauges that are updated once after the bootstrap loading completes.
 */
public class FlinkRLIBootstrapMetrics extends HoodieFlinkMetrics {

  public static final String NUM_FILE_SLICES_PROCESSED = "rliBootstrap.numFileSlicesProcessed";
  public static final String NUM_INDEX_RECORDS_EMITTED = "rliBootstrap.numIndexRecordsEmitted";
  public static final String BOOTSTRAP_COST_MS = "rliBootstrap.bootstrapCostMs";
  public static final String BOOTSTRAP_RECORD_PER_MS = "rliBootstrap.bootstrapRecordPerMs";

  private long numFileSlicesProcessed;
  private long numIndexRecordsEmitted;
  private long bootstrapCostMs;

  public FlinkRLIBootstrapMetrics(MetricGroup metricGroup) {
    super(metricGroup);
  }

  @Override
  public void registerMetrics() {
    metricGroup.gauge(NUM_FILE_SLICES_PROCESSED, (Gauge<Long>) () -> numFileSlicesProcessed);
    metricGroup.gauge(NUM_INDEX_RECORDS_EMITTED, (Gauge<Long>) () -> numIndexRecordsEmitted);
    metricGroup.gauge(BOOTSTRAP_COST_MS, (Gauge<Long>) () -> bootstrapCostMs);
    metricGroup.gauge(BOOTSTRAP_RECORD_PER_MS, (Gauge<Double>) this::getThroughput);
  }

  public void updateLoadResult(long numFileSlicesProcessed, long numIndexRecordsEmitted, long bootstrapCostMs) {
    this.numFileSlicesProcessed = numFileSlicesProcessed;
    this.numIndexRecordsEmitted = numIndexRecordsEmitted;
    this.bootstrapCostMs = bootstrapCostMs;
  }

  private double getThroughput() {
    return bootstrapCostMs > 0 ? (double) numIndexRecordsEmitted / bootstrapCostMs * 1000 : 0;
  }
}
