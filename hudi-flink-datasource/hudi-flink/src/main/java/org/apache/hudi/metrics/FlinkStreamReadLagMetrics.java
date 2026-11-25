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
import org.apache.hudi.source.split.HoodieSourceSplitLagInfo;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.metrics.MetricGroup;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Metrics of how far the streaming read lags behind the commits of the table, tracked by the split
 * enumerator.
 *
 * <p>A commit is pending until every split that reads it is finished: either it is not discovered yet,
 * or some of its splits are queued or being read. The lag is computed when the gauge is read, so it keeps growing while the readers stall. The methods are
 * called from the coordinator thread and the gauges are read by the metric reporter thread.
 *
 * <p>Splits that the readers restore from their own state are not known to the enumerator, so they are
 * not counted until they are added back.
 */
@Slf4j
public class FlinkStreamReadLagMetrics extends HoodieFlinkMetrics {

  private final Clock clock;

  /**
   * Time zone of the completion times of the table.
   */
  private final ZoneId timelineZone;

  /**
   * Lag info of the discovered splits that are not finished yet, keyed by split id.
   */
  private final Map<String, HoodieSourceSplitLagInfo> pendingSplits = new HashMap<>();

  /**
   * Number of pending splits that read each commit, keyed by instant time.
   */
  private final Map<String, Integer> inFlightCommits = new HashMap<>();

  /**
   * Number of pending splits by the completion time of the oldest commit they read.
   */
  private final TreeMap<String, Integer> oldestCompletionTimes = new TreeMap<>();

  private int undiscoveredCommits;

  /**
   * Completion time of the oldest undiscovered commit, null if there is none.
   */
  private String oldestUndiscoveredCompletionTime;

  private long pendingRecords;

  private long pendingBytes;

  public FlinkStreamReadLagMetrics(MetricGroup metricGroup, String tableName, ZoneId timelineZone) {
    this(metricGroup, tableName, timelineZone, Clock.systemUTC());
  }

  @VisibleForTesting
  FlinkStreamReadLagMetrics(MetricGroup metricGroup, String tableName, ZoneId timelineZone, Clock clock) {
    super(metricGroup.addGroup("table", tableName));
    this.timelineZone = timelineZone;
    this.clock = clock;
  }

  @Override
  public void registerMetrics() {
    metricGroup.gauge("readLagSeconds", this::getReadLagSeconds);
    metricGroup.gauge("pendingCommits", this::getPendingCommits);
    metricGroup.gauge("undiscoveredCommits", this::getUndiscoveredCommits);
    metricGroup.gauge("inFlightCommits", this::getInFlightCommits);
    metricGroup.gauge("pendingSplits", this::getPendingSplits);
    metricGroup.gauge("pendingRecords", this::getPendingRecords);
    metricGroup.gauge("pendingBytes", this::getPendingBytes);
  }

  /**
   * Starts tracking the splits that were discovered, restored or added back, keyed by split id. Splits
   * that are already tracked are ignored.
   */
  public synchronized void addSplits(Map<String, HoodieSourceSplitLagInfo> lagInfos) {
    Map<Collection<String>, Integer> commitRangeCounts = new IdentityHashMap<>();
    Map<String, Integer> completionTimeCounts = new HashMap<>();
    lagInfos.forEach((splitId, lagInfo) -> {
      if (pendingSplits.putIfAbsent(splitId, lagInfo) == null) {
        commitRangeCounts.merge(lagInfo.getReadCommits(), 1, Integer::sum);
        if (lagInfo.getOldestCommitCompletionTime() != null) {
          completionTimeCounts.merge(lagInfo.getOldestCommitCompletionTime(), 1, Integer::sum);
        }
        pendingRecords += lagInfo.getEstimatedRecords();
        pendingBytes += lagInfo.getEstimatedBytes();
      }
    });
    addCounts(commitRangeCounts, completionTimeCounts);
  }

  /**
   * Replaces the lag info of the splits that are still tracked, keyed by split id.
   */
  public synchronized void updateSplits(Map<String, HoodieSourceSplitLagInfo> lagInfos) {
    Map<Collection<String>, Integer> removedCommitRanges = new IdentityHashMap<>();
    Map<String, Integer> removedCompletionTimes = new HashMap<>();
    Map<Collection<String>, Integer> addedCommitRanges = new IdentityHashMap<>();
    Map<String, Integer> addedCompletionTimes = new HashMap<>();
    lagInfos.forEach((splitId, lagInfo) -> {
      HoodieSourceSplitLagInfo previous = pendingSplits.get(splitId);
      if (previous != null) {
        removedCommitRanges.merge(previous.getReadCommits(), 1, Integer::sum);
        if (previous.getOldestCommitCompletionTime() != null) {
          removedCompletionTimes.merge(previous.getOldestCommitCompletionTime(), 1, Integer::sum);
        }
        pendingRecords -= previous.getEstimatedRecords();
        pendingBytes -= previous.getEstimatedBytes();

        pendingSplits.put(splitId, lagInfo);
        addedCommitRanges.merge(lagInfo.getReadCommits(), 1, Integer::sum);
        if (lagInfo.getOldestCommitCompletionTime() != null) {
          addedCompletionTimes.merge(lagInfo.getOldestCommitCompletionTime(), 1, Integer::sum);
        }
        pendingRecords += lagInfo.getEstimatedRecords();
        pendingBytes += lagInfo.getEstimatedBytes();
      }
    });
    removeCounts(removedCommitRanges, removedCompletionTimes);
    addCounts(addedCommitRanges, addedCompletionTimes);
  }

  /**
   * Stops tracking the splits whose records were all emitted. Splits that are not tracked are ignored.
   */
  public synchronized void finishSplits(Collection<String> splitIds) {
    Map<Collection<String>, Integer> removedCommitRanges = new IdentityHashMap<>();
    Map<String, Integer> removedCompletionTimes = new HashMap<>();
    for (String splitId : splitIds) {
      HoodieSourceSplitLagInfo lagInfo = pendingSplits.remove(splitId);
      if (lagInfo != null) {
        removedCommitRanges.merge(lagInfo.getReadCommits(), 1, Integer::sum);
        if (lagInfo.getOldestCommitCompletionTime() != null) {
          removedCompletionTimes.merge(lagInfo.getOldestCommitCompletionTime(), 1, Integer::sum);
        }
        pendingRecords -= lagInfo.getEstimatedRecords();
        pendingBytes -= lagInfo.getEstimatedBytes();
      }
    }
    removeCounts(removedCommitRanges, removedCompletionTimes);
  }

  /**
   * Sets the completion times of the completed commits that are not discovered yet, in ascending order.
   */
  public synchronized void setUndiscoveredCommits(List<String> completionTimes) {
    this.undiscoveredCommits = completionTimes.size();
    this.oldestUndiscoveredCompletionTime = completionTimes.isEmpty() ? null : completionTimes.get(0);
  }

  /**
   * Returns the seconds since the oldest pending commit completed, 0 if no commit is pending.
   */
  private synchronized long getReadLagSeconds() {
    String oldest = oldestUndiscoveredCompletionTime;
    if (!oldestCompletionTimes.isEmpty() && (oldest == null || oldestCompletionTimes.firstKey().compareTo(oldest) < 0)) {
      oldest = oldestCompletionTimes.firstKey();
    }
    if (oldest == null) {
      return 0L;
    }
    try {
      LocalDateTime completionTime = LocalDateTime.parse(
          HoodieInstantTimeGenerator.fixInstantTimeCompatibility(oldest), HoodieInstantTimeGenerator.MILLIS_INSTANT_TIME_FORMATTER);
      return Duration.between(completionTime.atZone(timelineZone).toInstant(), clock.instant()).getSeconds();
    } catch (DateTimeParseException e) {
      log.warn("Invalid commit completion time: {}", oldest);
      return 0L;
    }
  }

  private synchronized long getPendingCommits() {
    return (long) undiscoveredCommits + inFlightCommits.size();
  }

  private synchronized long getUndiscoveredCommits() {
    return undiscoveredCommits;
  }

  private synchronized long getInFlightCommits() {
    return inFlightCommits.size();
  }

  private synchronized long getPendingSplits() {
    return pendingSplits.size();
  }

  private synchronized long getPendingRecords() {
    return pendingRecords;
  }

  private synchronized long getPendingBytes() {
    return pendingBytes;
  }

  private void addCounts(Map<Collection<String>, Integer> commitRangeCounts, Map<String, Integer> completionTimeCounts) {
    commitRangeCounts.forEach((commits, count) ->
        commits.forEach(commit -> inFlightCommits.merge(commit, count, Integer::sum)));
    completionTimeCounts.forEach((completionTime, count) ->
        oldestCompletionTimes.merge(completionTime, count, Integer::sum));
  }

  private void removeCounts(Map<Collection<String>, Integer> commitRangeCounts, Map<String, Integer> completionTimeCounts) {
    commitRangeCounts.forEach((commits, count) ->
        commits.forEach(commit -> decrement(inFlightCommits, commit, count)));
    completionTimeCounts.forEach((completionTime, count) ->
        decrement(oldestCompletionTimes, completionTime, count));
  }

  private static void decrement(Map<String, Integer> counts, String key, int delta) {
    counts.computeIfPresent(key, (k, count) -> count > delta ? count - delta : null);
  }
}
