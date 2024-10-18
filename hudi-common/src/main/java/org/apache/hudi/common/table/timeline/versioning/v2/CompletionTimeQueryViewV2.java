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
import org.apache.hudi.common.table.timeline.InstantComparatorUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;

import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;
import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.read.IncrementalQueryAnalyzer.START_COMMIT_EARLIEST;
import static org.apache.hudi.common.table.timeline.HoodieArchivedTimeline.COMPLETION_TIME_ARCHIVED_META_FIELD;
import static org.apache.hudi.common.table.timeline.InstantComparatorUtils.GREATER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparatorUtils.GREATER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparatorUtils.LESSER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparatorUtils.LESSER_THAN_OR_EQUALS;

/**
 * Query view for instant completion time.
 */
public class CompletionTimeQueryViewV2 implements CompletionTimeQueryView, Serializable {
  private static final long serialVersionUID = 1L;

  private static final long MILLI_SECONDS_IN_THREE_DAYS = 3 * 24 * 3600 * 1000;

  private static final long MILLI_SECONDS_IN_ONE_DAY = 24 * 3600 * 1000;

  private final HoodieTableMetaClient metaClient;

  /**
   * Mapping from instant start time -> completion time.
   * Should be thread-safe data structure.
   */
  private final ConcurrentMap<String, String> beginToCompletionInstantTimeMap;

  /**
   * The cursor instant time to eagerly load from, by default load last N days of completed instants.
   * It can grow dynamically with lazy loading. e.g. assuming an initial cursor instant as t10,
   * a completion query for t5 would trigger lazy loading with this cursor instant updated to t5.
   * This sliding window model amortizes redundant loading from different queries.
   */
  private volatile String cursorInstant;

  /**
   * The first write instant on the active timeline, used for query optimization.
   */
  private final String firstNonSavepointCommit;

  /**
   * The constructor.
   *
   * @param metaClient   The table meta client.
   */
  public CompletionTimeQueryViewV2(HoodieTableMetaClient metaClient) {
    this(metaClient, HoodieInstantTimeGenerator.formatDate(new Date(Instant.now().minusMillis(MILLI_SECONDS_IN_THREE_DAYS).toEpochMilli())));
  }

  /**
   * The constructor.
   *
   * @param metaClient   The table meta client.
   * @param eagerLoadInstant The earliest instant time to eagerly load from, by default load last N days of completed instants.
   */
  public CompletionTimeQueryViewV2(HoodieTableMetaClient metaClient, String eagerLoadInstant) {
    this.metaClient = metaClient;
    this.beginToCompletionInstantTimeMap = new ConcurrentHashMap<>();
    this.cursorInstant = InstantComparatorUtils.minInstant(eagerLoadInstant, metaClient.getActiveTimeline().firstInstant().map(HoodieInstant::getRequestTime).orElse(""));
    // Note: use getWriteTimeline() to keep sync with the fs view visibleCommitsAndCompactionTimeline, see AbstractTableFileSystemView.refreshTimeline.
    this.firstNonSavepointCommit = metaClient.getActiveTimeline().getWriteTimeline().getFirstNonSavepointCommit().map(HoodieInstant::getRequestTime).orElse("");
    load();
  }

  @Override
  public boolean isCompleted(String beginInstantTime) {
    // archival does not proceed beyond the first savepoint, so any instant before that is completed.
    return this.beginToCompletionInstantTimeMap.containsKey(beginInstantTime) || isArchived(beginInstantTime);
  }

  @Override
  public boolean isArchived(String instantTime) {
    return InstantComparatorUtils.compareTimestamps(instantTime, LESSER_THAN, this.firstNonSavepointCommit);
  }

  @Override
  public boolean isCompletedBefore(String baseInstant, String instantTime) {
    Option<String> completionTimeOpt = getCompletionTime(baseInstant, instantTime);
    if (completionTimeOpt.isPresent()) {
      return InstantComparatorUtils.compareTimestamps(completionTimeOpt.get(), LESSER_THAN, baseInstant);
    }
    return false;
  }

  @Override
  public boolean isSlicedAfterOrOn(String baseInstant, String instantTime) {
    Option<String> completionTimeOpt = getCompletionTime(baseInstant, instantTime);
    if (completionTimeOpt.isPresent()) {
      return InstantComparatorUtils.compareTimestamps(completionTimeOpt.get(), GREATER_THAN_OR_EQUALS, baseInstant);
    }
    return true;
  }

  @Override
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

  @Override
  public Option<String> getCompletionTime(String beginTime) {
    String completionTime = this.beginToCompletionInstantTimeMap.get(beginTime);
    if (completionTime != null) {
      return Option.of(completionTime);
    }
    if (InstantComparatorUtils.compareTimestamps(beginTime, GREATER_THAN_OR_EQUALS, this.cursorInstant)) {
      // the instant is still pending
      return Option.empty();
    }
    loadCompletionTimeIncrementally(beginTime);
    return Option.ofNullable(this.beginToCompletionInstantTimeMap.get(beginTime));
  }

  @Override
  public List<String> getStartTimes(
      HoodieTimeline timeline,
      Option<String> rangeStart,
      Option<String> rangeEnd,
      InstantRange.RangeType rangeType) {
    // assumes any instant/transaction lasts at most 1 day to optimize the query efficiency.
    return getStartTimes(timeline, rangeStart, rangeEnd, rangeType, s -> HoodieInstantTimeGenerator.instantTimeMinusMillis(s, MILLI_SECONDS_IN_ONE_DAY));
  }

  @Override
  @VisibleForTesting
  public List<String> getStartTimes(
      String rangeStart,
      String rangeEnd,
      Function<String, String> earliestInstantTimeFunc) {
    return getStartTimes(metaClient.getCommitsTimeline().filterCompletedInstants(), Option.ofNullable(rangeStart), Option.ofNullable(rangeEnd),
        InstantRange.RangeType.CLOSED_CLOSED, earliestInstantTimeFunc);
  }

  /**
   * Queries the instant start time with given completion time range.
   *
   * @param timeline            The timeline.
   * @param rangeStart              The query range start completion time.
   * @param rangeEnd                The query range end completion time.
   * @param rangeType               The range type.
   * @param earliestInstantTimeFunc The function to generate the earliest start time boundary
   *                                with the minimum completion time.
   *
   * @return The sorted instant time list.
   */
  private List<String> getStartTimes(
      HoodieTimeline timeline,
      Option<String> rangeStart,
      Option<String> rangeEnd,
      InstantRange.RangeType rangeType,
      Function<String, String> earliestInstantTimeFunc) {
    final boolean startFromEarliest = START_COMMIT_EARLIEST.equalsIgnoreCase(rangeStart.orElse(null));
    String earliestInstantToLoad = null;
    if (rangeStart.isPresent() && !startFromEarliest) {
      earliestInstantToLoad = earliestInstantTimeFunc.apply(rangeStart.get());
    } else if (rangeEnd.isPresent()) {
      earliestInstantToLoad = earliestInstantTimeFunc.apply(rangeEnd.get());
    }

    // ensure the earliest instant boundary be loaded.
    if (earliestInstantToLoad != null && InstantComparatorUtils.compareTimestamps(this.cursorInstant, GREATER_THAN, earliestInstantToLoad)) {
      loadCompletionTimeIncrementally(earliestInstantToLoad);
    }

    if (rangeStart.isEmpty() && rangeEnd.isPresent()) {
      // returns the last instant that finished at or before the given completion time 'endTime'.
      String maxInstantTime = timeline.getInstantsAsStream()
          .filter(instant -> instant.isCompleted() && InstantComparatorUtils.compareTimestamps(instant.getCompletionTime(), LESSER_THAN_OR_EQUALS, rangeEnd.get()))
          .max(Comparator.comparing(HoodieInstant::getCompletionTime)).map(HoodieInstant::getRequestTime).orElse(null);
      if (maxInstantTime != null) {
        return Collections.singletonList(maxInstantTime);
      }
      // fallback to archived timeline
      return this.beginToCompletionInstantTimeMap.entrySet().stream()
          .filter(entry -> InstantComparatorUtils.compareTimestamps(entry.getValue(), LESSER_THAN_OR_EQUALS, rangeEnd.get()))
          .map(Map.Entry::getKey).collect(Collectors.toList());
    }

    if (startFromEarliest) {
      // expedience for snapshot read: ['earliest', _) to avoid loading unnecessary instants.
      rangeStart = Option.empty();
    }

    if (rangeStart.isEmpty() && rangeEnd.isEmpty()) {
      // (_, _): read the latest snapshot.
      return timeline.filterCompletedInstants().lastInstant().map(instant -> Collections.singletonList(instant.getRequestTime())).orElse(Collections.emptyList());
    }

    final InstantRange instantRange = InstantRange.builder()
        .rangeType(rangeType)
        .startInstant(rangeStart.orElse(null))
        .endInstant(rangeEnd.orElse(null))
        .nullableBoundary(true)
        .build();
    return this.beginToCompletionInstantTimeMap.entrySet().stream()
        .filter(entry -> instantRange.isInRange(entry.getValue()))
        .map(Map.Entry::getKey).sorted().collect(Collectors.toList());
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private void loadCompletionTimeIncrementally(String startTime) {
    // the 'startTime' should be out of the eager loading range, switch to a lazy loading.
    // This operation is resource costly.
    synchronized (this) {
      if (InstantComparatorUtils.compareTimestamps(startTime, LESSER_THAN, this.cursorInstant)) {
        metaClient.getTimelineLayout().getTimelineFactory().createArchivedTimelineLoader().loadInstants(metaClient,
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
   * This is method to read instant completion time.
   * This would also update 'startToCompletionInstantTimeMap' map with start time/completion time pairs.
   * Only instants starts from 'startInstant' (inclusive) are considered.
   */
  private void load() {
    // load active instants first.
    this.metaClient.getActiveTimeline()
        .filterCompletedInstants().getInstantsAsStream()
        .forEach(instant -> setCompletionTime(instant.getRequestTime(), instant.getCompletionTime()));
    // then load the archived instants.
    metaClient.getTimelineLayout().getTimelineFactory().createArchivedTimelineLoader().loadInstants(metaClient,
        new HoodieArchivedTimeline.StartTsFilter(this.cursorInstant),
        HoodieArchivedTimeline.LoadMode.TIME,
        r -> true,
        this::readCompletionTime);
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
    this.beginToCompletionInstantTimeMap.putIfAbsent(beginInstantTime, completionTime);
  }

  @Override
  public String getCursorInstant() {
    return cursorInstant;
  }

  @Override
  public boolean isEmptyTable() {
    return this.beginToCompletionInstantTimeMap.isEmpty();
  }

  @Override
  public void close() {
    this.beginToCompletionInstantTimeMap.clear();
  }
}
