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

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.sink.bootstrap.RLIBootstrapOperator;

import lombok.Getter;
import org.apache.flink.metrics.MetricGroup;

import java.util.List;

/**
 * Metrics for flink RLI bootstrap.
 *
 * @see RLIBootstrapOperator
 */
@Getter
public class FlinkRLIBootstrapMetrics extends HoodieFlinkMetrics {

  public static final String RLI_SHARDS = "rli_shards";
  public static final String SCAN_LATENCY = "scan_latency";
  public static final String SCAN_BYTES = "scan_bytes";
  public static final String SCAN_RECORDS = "scan_records";

  private static final String SCAN_KEY = "rli_bootstrap_scan";

  private long rliShards;
  private long scanLatency;
  private long scanBytes;
  private long scanRecords;

  public FlinkRLIBootstrapMetrics(MetricGroup metricGroup) {
    super(metricGroup);
  }

  @Override
  public void registerMetrics() {
    metricGroup.gauge(RLI_SHARDS, () -> rliShards);
    metricGroup.gauge(SCAN_LATENCY, () -> scanLatency);
    metricGroup.gauge(SCAN_BYTES, () -> scanBytes);
    metricGroup.gauge(SCAN_RECORDS, () -> scanRecords);
  }

  public void updateFileSliceMetrics(List<FileSlice> fileSlices) {
    this.rliShards = fileSlices.size();
    this.scanBytes = fileSlices.stream().mapToLong(FileSlice::getTotalFileSize).sum();
  }

  public void startScan() {
    startTimer(SCAN_KEY);
  }

  public void endScan() {
    this.scanLatency = stopTimer(SCAN_KEY);
  }

  public void markRecordScanned() {
    this.scanRecords += 1;
  }

}
