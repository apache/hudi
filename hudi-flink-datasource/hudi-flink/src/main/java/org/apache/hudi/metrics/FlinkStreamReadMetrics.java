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

import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.source.StreamReadMonitoringFunction;
import org.apache.hudi.source.StreamReadOperator;
import org.apache.hudi.source.reader.HoodieSourceSplitReader;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.metrics.MetricGroup;

import java.text.ParseException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Metrics for flink stream read.
 *
 * <p>The delays are computed when the gauges are read, so they keep growing while the reader stalls
 * or discovery finds no new commits. The setters run on the reading threads and the gauges are read
 * by the metric reporter thread.
 */
@Slf4j
public class FlinkStreamReadMetrics extends HoodieFlinkMetrics {

  private final Clock clock;

  /**
   * The last issued instant in streaming read, null until the first one is issued.
   *
   * @see StreamReadMonitoringFunction
   */
  private volatile Instant issuedInstant;

  /**
   * Latest commit of the last split the reader started, null until the first one is started.
   *
   * @see StreamReadOperator
   * @see HoodieSourceSplitReader
   */
  private volatile Instant splitLatestCommit;

  /**
   * Latest commits of the splits that were started and not yet finished, keyed by split id. On
   * Source V2 a split starts on the fetcher thread and finishes on the task thread once all of its
   * records are emitted, so the next split can start before the previous one finishes.
   */
  private final Map<String, Instant> activeSplitCommits = new ConcurrentHashMap<>();

  public FlinkStreamReadMetrics(MetricGroup metricGroup, String tableName) {
    this(metricGroup, tableName, Clock.systemUTC());
  }

  @VisibleForTesting
  FlinkStreamReadMetrics(MetricGroup metricGroup, String tableName, Clock clock) {
    super(metricGroup.addGroup("table", tableName));
    this.clock = clock;
  }

  @Override
  public void registerMetrics() {
    metricGroup.gauge("issuedInstantDelay", this::getIssuedInstantDelay);
    metricGroup.gauge("issuedInstant", () -> toEpochSecond(issuedInstant));
    metricGroup.gauge("splitLatestCommit", () -> toEpochSecond(splitLatestCommit));
    metricGroup.gauge("splitLatestCommitDelay", this::getSplitLatestCommitDelay);
  }

  public void setIssuedInstant(String issuedInstant) {
    try {
      this.issuedInstant = HoodieInstantTimeGenerator.parseDateFromInstantTime(issuedInstant).toInstant();
    } catch (ParseException e) {
      log.warn("Invalid input issued instant: {}", issuedInstant);
    }
  }

  /**
   * Records that the reader started a split whose latest commit is {@code splitLatestCommit}. Call it
   * before opening the split, so that a slow open or recovery replay counts as reading.
   */
  public void startSplit(String splitId, String splitLatestCommit) {
    try {
      Instant commit = HoodieInstantTimeGenerator.parseDateFromInstantTime(splitLatestCommit).toInstant();
      this.splitLatestCommit = commit;
      activeSplitCommits.put(splitId, commit);
    } catch (ParseException e) {
      log.warn("Invalid input latest commit: {}", splitLatestCommit);
    }
  }

  /**
   * Records that all records of the split were emitted. Splits that were never started are ignored.
   * Once no split is active, {@code splitLatestCommitDelay} reports 0 and {@code splitLatestCommit}
   * keeps the last value.
   */
  public void finishSplit(String splitId) {
    activeSplitCommits.remove(splitId);
  }

  private long getIssuedInstantDelay() {
    return delaySeconds(issuedInstant);
  }

  /**
   * Returns the delay of the oldest active split, whose records are the stalest still to be emitted.
   */
  private long getSplitLatestCommitDelay() {
    return activeSplitCommits.values().stream()
        .min(Comparator.naturalOrder())
        .map(this::delaySeconds)
        .orElse(0L);
  }

  private long delaySeconds(Instant instant) {
    return instant == null ? 0L : Duration.between(instant, clock.instant()).getSeconds();
  }

  private static long toEpochSecond(Instant instant) {
    return instant == null ? 0L : instant.getEpochSecond();
  }
}
