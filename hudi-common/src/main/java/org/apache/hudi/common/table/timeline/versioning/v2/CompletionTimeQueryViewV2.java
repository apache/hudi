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

package org.apache.hudi.common.table.timeline.versioning.v2;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.timeline.CompletionTimeQueryView;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantComparison;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;

import lombok.Getter;
import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.read.IncrementalQueryAnalyzer.START_COMMIT_EARLIEST;
import static org.apache.hudi.common.table.timeline.HoodieArchivedTimeline.COMPLETION_TIME_ARCHIVED_META_FIELD;
import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN_OR_EQUALS;

/**
 * Query view for instant completion time.
 */
public class CompletionTimeQueryViewV2 implements CompletionTimeQueryView, Serializable {
  private static final long serialVersionUID = 1L;

  private static final long MILLI_SECONDS_IN_ONE_DAY = 24 * 3600 * 1000;

  private static final Function<String, String> GET_INSTANT_ONE_DAY_BEFORE = instant ->
      HoodieInstantTimeGenerator.instantTimeMinusMillis(instant, MILLI_SECONDS_IN_ONE_DAY);

  private final HoodieTableMetaClient metaClient;

  /**
   * Mapping from instant time -> completion time.
   * Should be thread-safe data structure.
   */
  private final ConcurrentMap<String, String> instantTimeToCompletionTimeMap;

  /**
   * The cursor instant time to eagerly load from, by default load last N days of completed instants.
   * It can grow dynamically with lazy loading. e.g. assuming an initial cursor instant as t10,
   * a completion query for t5 would trigger lazy loading with this cursor instant updated to t5.
   * This sliding window model amortizes redundant loading from different queries.
   */
  @Getter
  private volatile String cursorInstant;

  /**
   * The first write instant on the active timeline, used for query optimization.
   */
  private final String firstNonSavepointCommit;

  /**
   * The constructor.
   *
   * @param metaClient The table meta client.
   */
  public CompletionTimeQueryViewV2(HoodieTableMetaClient metaClient) {
    this.metaClient = metaClient;
    this.instantTimeToCompletionTimeMap = new ConcurrentHashMap<>();
    this.cursorInstant = metaClient.getActiveTimeline().firstInstant().map(HoodieInstant::requestedTime).orElse("");
    // Note: use getWriteTimeline() to keep sync with the fs view visibleCommitsAndCompactionTimeline, see AbstractTableFileSystemView.refreshTimeline.
    this.firstNonSavepointCommit = metaClient.getActiveTimeline().getWriteTimeline().getFirstNonSavepointCommit().map(HoodieInstant::requestedTime).orElse("");
    load();
  }

  /**
   * Returns whether the instant is completed.
   */
  public boolean isCompleted(String instantTime) {
    // archival does not proceed beyond the first savepoint, so any instant before that is completed.
    return this.instantTimeToCompletionTimeMap.containsKey(instantTime) || isArchived(instantTime);
  }

  /**
   * Returns whether the instant is archived.
   */
  public boolean isArchived(String instantTime) {
    return InstantComparison.compareTimestamps(instantTime, LESSER_THAN, this.firstNonSavepointCommit);
  }

  /**
   * Returns whether the give instant time {@code instantTime} completed before the base instant {@code baseInstant}.
   */
  public boolean isCompletedBefore(String baseInstant, String instantTime) {
    Option<String> completionTimeOpt = getCompletionTime(baseInstant, instantTime);
    if (completionTimeOpt.isPresent()) {
      return InstantComparison.compareTimestamps(completionTimeOpt.get(), LESSER_THAN, baseInstant);
    }
    return false;
  }

  /**
   * Returns whether the given instant time {@code instantTime} is sliced after or on the base instant {@code baseInstant}.
   */
  public boolean isSlicedAfterOrOn(String baseInstant, String instantTime) {
    Option<String> completionTimeOpt = getCompletionTime(baseInstant, instantTime);
    if (completionTimeOpt.isPresent()) {
      return InstantComparison.compareTimestamps(completionTimeOpt.get(), GREATER_THAN_OR_EQUALS, baseInstant);
    }
    return true;
  }

  /**
   * Get completion time with a base instant time as a reference to fix the compatibility.
   *
   * @param baseInstant The base instant
   * @param instantTime The instant time to query the completion time with
   * @return Probability fixed completion time.
   */
  public Option<String> getCompletionTime(String baseInstant, String instantTime) {
    Option<String> completionTimeOpt = getCompletionTime(instantTime);
    if (completionTimeOpt.isPresent()) {
      String completionTime = completionTimeOpt.get();
      if (completionTime.length() != baseInstant.length()) {
        // ==============================================================
        // LEGACY CODE
        // ==============================================================
        // Fixes the completion time to reflect the completion sequence correctly
        // if the file slice base instant time is not in datetime format.
        // For example, many test cases just use integer string as the instant time.
        // CAUTION: this fix only works for OCC(Optimistic Concurrency Control).
        // for NB-CC(Non-blocking Concurrency Control), the file slicing may be incorrect.
        return Option.of(instantTime);
      }
    }
    return completionTimeOpt;
  }

  /**
   * Queries the completion time with given instant time.
   *
   * @param instantTime The instant time.
   * @return The completion time if the instant finished or empty if it is still pending.
   */
  public Option<String> getCompletionTime(String instantTime) {
    String completionTime = this.instantTimeToCompletionTimeMap.get(instantTime);
    if (completionTime != null) {
      return Option.of(completionTime);
    }
    if (InstantComparison.compareTimestamps(instantTime, GREATER_THAN_OR_EQUALS, this.cursorInstant)) {
      // the instant is still pending
      return Option.empty();
    }
    loadCompletionTimeIncrementally(instantTime);
    return Option.ofNullable(this.instantTimeToCompletionTimeMap.get(instantTime));
  }

  @Override
  public List<String> getInstantTimes(
      HoodieTimeline timeline,
      Option<String> startCompletionTime,
      Option<String> endCompletionTime,
      InstantRange.RangeType rangeType) {
    // assumes any instant/transaction lasts at most 1 day to optimize the query efficiency.
    return getInstantTimes(
        timeline, startCompletionTime, endCompletionTime, rangeType, GET_INSTANT_ONE_DAY_BEFORE);
  }

  @VisibleForTesting
  public List<String> getInstantTimes(
      String startCompletionTime,
      String endCompletionTime,
      Function<String, String> earliestInstantTimeFunc) {
    return getInstantTimes(
        metaClient.getCommitsTimeline().filterCompletedInstants(),
        Option.ofNullable(startCompletionTime),
        Option.ofNullable(endCompletionTime),
        InstantRange.RangeType.CLOSED_CLOSED,
        earliestInstantTimeFunc);
  }

  /**
   * Queries the instant times with given completion time range.
   *
   * @param timeline                The timeline.
   * @param startCompletionTime     The start completion time of the query range.
   * @param endCompletionTime       The end completion time of the query range.
   * @param rangeType               The range type.
   * @param earliestInstantTimeFunc The function to generate the earliest instant time boundary
   *                                with the minimum completion time.
   * @return The sorted instant time list.
   */
  private List<String> getInstantTimes(
      HoodieTimeline timeline,
      Option<String> startCompletionTime,
      Option<String> endCompletionTime,
      InstantRange.RangeType rangeType,
      Function<String, String> earliestInstantTimeFunc) {
    final boolean startFromEarliest = START_COMMIT_EARLIEST.equalsIgnoreCase(startCompletionTime.orElse(null));
    String earliestInstantToLoad = null;
    if (startCompletionTime.isPresent() && !startFromEarliest) {
      earliestInstantToLoad = earliestInstantTimeFunc.apply(startCompletionTime.get());
    } else if (endCompletionTime.isPresent()) {
      earliestInstantToLoad = earliestInstantTimeFunc.apply(endCompletionTime.get());
    }

    // ensure the earliest instant boundary be loaded.
    if (earliestInstantToLoad != null && InstantComparison.compareTimestamps(this.cursorInstant, GREATER_THAN, earliestInstantToLoad)) {
      loadCompletionTimeIncrementally(earliestInstantToLoad);
    }

    if (startCompletionTime.isEmpty() && endCompletionTime.isPresent()) {
      // returns the last instant that finished at or before the given completion time 'endTime'.
      String maxInstantTime = timeline.getInstantsAsStream()
          .filter(instant -> instant.isCompleted() && InstantComparison.compareTimestamps(instant.getCompletionTime(), LESSER_THAN_OR_EQUALS, endCompletionTime.get()))
          .max(Comparator.comparing(HoodieInstant::getCompletionTime)).map(HoodieInstant::requestedTime).orElse(null);
      if (maxInstantTime != null) {
        return Collections.singletonList(maxInstantTime);
      }
      // fallback to archived timeline
      return this.instantTimeToCompletionTimeMap.entrySet().stream()
          .filter(entry -> InstantComparison.compareTimestamps(entry.getValue(), LESSER_THAN_OR_EQUALS, endCompletionTime.get()))
          .map(Map.Entry::getKey).sorted().collect(Collectors.toList());
    }

    if (startFromEarliest) {
      // expedience for snapshot read: ['earliest', _) to avoid loading unnecessary instants.
      startCompletionTime = Option.empty();
    }

    if (startCompletionTime.isEmpty() && endCompletionTime.isEmpty()) {
      // (_, _): read the latest snapshot.
      return timeline.filterCompletedInstants().lastInstant().map(instant -> Collections.singletonList(instant.requestedTime())).orElse(Collections.emptyList());
    }

    final InstantRange instantRange = InstantRange.builder()
        .rangeType(rangeType)
        .startInstant(startCompletionTime.orElse(null))
        .endInstant(endCompletionTime.orElse(null))
        .nullableBoundary(true)
        .build();
    return this.instantTimeToCompletionTimeMap.entrySet().stream()
        .filter(entry -> instantRange.isInRange(entry.getValue()))
        .map(Map.Entry::getKey).sorted().collect(Collectors.toList());
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  /**
   * Loads the completion times incrementally from archived timeline, if required.
   * If the provided startTime is greater than the cursorInstant,
   * then the completion is already present in memory and no additional loading is required.
   * @param startTime The start time of the instant.
   */
  private void loadCompletionTimeIncrementally(String startTime) {
    // the 'startTime' should be out of the eager loading range, switch to a lazy loading.
    // This operation is resource costly.
    synchronized (this) {
      if (InstantComparison.compareTimestamps(startTime, LESSER_THAN, this.cursorInstant)) {
        metaClient.getTableFormat().getTimelineFactory().createArchivedTimelineLoader().loadInstants(metaClient,
            new HoodieArchivedTimeline.ClosedOpenTimeRangeFilter(startTime, this.cursorInstant),
            HoodieArchivedTimeline.LoadMode.TIME,
            r -> true,
            this::readCompletionTime);
      }
      // refresh the start instant
      this.cursorInstant = startTime;
    }
  }

  /**
   * Reads the completion times from the active timeline.
   */
  private void load() {
    // load active instants first.
    this.metaClient.getActiveTimeline()
        .filterCompletedInstants().getInstantsAsStream()
        .forEach(instant -> setCompletionTime(instant.requestedTime(), instant.getCompletionTime()));
  }

  private void readCompletionTime(String instantTime, GenericRecord record) {
    final String completionTime = record.get(COMPLETION_TIME_ARCHIVED_META_FIELD).toString();
    setCompletionTime(instantTime, completionTime);
  }

  private void setCompletionTime(String beginInstantTime, String completionTime) {
    if (completionTime == null) {
      // the meta-server instant does not have completion time
      completionTime = beginInstantTime;
    }
    this.instantTimeToCompletionTimeMap.putIfAbsent(beginInstantTime, completionTime);
  }

  @Override
  public boolean isEmptyTable() {
    return this.instantTimeToCompletionTimeMap.isEmpty();
  }

  @Override
  public void close() {
    this.instantTimeToCompletionTimeMap.clear();
  }
}
