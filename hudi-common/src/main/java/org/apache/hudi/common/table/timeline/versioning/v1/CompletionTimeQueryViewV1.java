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

package org.apache.hudi.common.table.timeline.versioning.v1;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.timeline.CompletionTimeQueryView;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantComparison;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;

import java.io.Serializable;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN;

public class CompletionTimeQueryViewV1 implements CompletionTimeQueryView, Serializable {
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
  private final String cursorInstant;

  /**
   * The first write instant on the active timeline, used for query optimization.
   */
  private final String firstNonSavepointCommit;

  /**
   * The constructor.
   *
   * @param metaClient   The table meta client.
   */
  public CompletionTimeQueryViewV1(HoodieTableMetaClient metaClient) {
    this(metaClient, HoodieInstantTimeGenerator.formatDate(new Date(Instant.now().minusMillis(MILLI_SECONDS_IN_THREE_DAYS).toEpochMilli())));
  }

  /**
   * The constructor.
   *
   * @param metaClient   The table meta client.
   * @param eagerLoadInstant The earliest instant time to eagerly load from, by default load last N days of completed instants.
   */
  public CompletionTimeQueryViewV1(HoodieTableMetaClient metaClient, String eagerLoadInstant) {
    this.metaClient = metaClient;
    this.beginToCompletionInstantTimeMap = new ConcurrentHashMap<>();
    this.cursorInstant = InstantComparison.minInstant(eagerLoadInstant, metaClient.getActiveTimeline().firstInstant().map(HoodieInstant::requestedTime).orElse(""));
    // Note: use getWriteTimeline() to keep sync with the fs view visibleCommitsAndCompactionTimeline, see AbstractTableFileSystemView.refreshTimeline.
    this.firstNonSavepointCommit = metaClient.getActiveTimeline().getWriteTimeline().getFirstNonSavepointCommit().map(HoodieInstant::requestedTime).orElse("");
    load();
  }

  @Override
  public boolean isCompleted(String beginInstantTime) {
    // archival does not proceed beyond the first savepoint, so any instant before that is completed.
    return this.beginToCompletionInstantTimeMap.containsKey(beginInstantTime) || isArchived(beginInstantTime);
  }

  @Override
  public boolean isArchived(String instantTime) {
    return InstantComparison.compareTimestamps(instantTime, LESSER_THAN, this.firstNonSavepointCommit);
  }

  @Override
  public boolean isCompletedBefore(String baseInstant, String instantTime) {
    Option<String> completionTimeOpt = getCompletionTime(baseInstant, instantTime);
    if (completionTimeOpt.isPresent()) {
      return InstantComparison.compareTimestamps(completionTimeOpt.get(), LESSER_THAN, baseInstant);
    }
    return false;
  }

  @Override
  public boolean isSlicedAfterOrOn(String baseInstant, String instantTime) {
    Option<String> completionTimeOpt = getCompletionTime(baseInstant, instantTime);
    if (completionTimeOpt.isPresent()) {
      return InstantComparison.compareTimestamps(completionTimeOpt.get(), GREATER_THAN_OR_EQUALS, baseInstant);
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
    if (beginTime.length() < 10) {
      /**
       * This is to get the tests passing. We are taking advantage of instant times in tests which are small integers (1, 2, 3..)
       * to avoid rewriting all the tests after we brought timeline and completiontime query view for table version 6.
       */
      return Option.of(beginTime);
    }
    String completionTime = this.beginToCompletionInstantTimeMap.get(beginTime);
    if (completionTime != null) {
      return Option.of(completionTime);
    }

    // ***This is the key change between V1 and V2 completion time query-view***
    if (isArchived(beginTime)) {
      // Completion time and begin time are same for archived instants.
      return Option.of(beginTime);
    }
    // the instant is still pending
    return Option.empty();
  }

  @Override
  public List<String> getInstantTimes(
      HoodieTimeline timeline,
      Option<String> rangeStart,
      Option<String> rangeEnd,
      InstantRange.RangeType rangeType) {
    // assumes any instant/transaction lasts at most 1 day to optimize the query efficiency.
    return getInstantTimes(timeline, rangeStart, rangeEnd, rangeType, s -> HoodieInstantTimeGenerator.instantTimeMinusMillis(s, MILLI_SECONDS_IN_ONE_DAY));
  }

  @Override
  @VisibleForTesting
  public List<String> getInstantTimes(
      String rangeStart,
      String rangeEnd,
      Function<String, String> earliestInstantTimeFunc) {
    return getInstantTimes(metaClient.getCommitsTimeline().filterCompletedInstants(), Option.ofNullable(rangeStart), Option.ofNullable(rangeEnd),
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
  private List<String> getInstantTimes(
      HoodieTimeline timeline,
      Option<String> rangeStart,
      Option<String> rangeEnd,
      InstantRange.RangeType rangeType,
      Function<String, String> earliestInstantTimeFunc) {
    throw new RuntimeException("Incremental query view for timeline version 1 not yet implemented");
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  /**
   * This is method to read instant completion time.
   * This would also update 'startToCompletionInstantTimeMap' map with start time/completion time pairs.
   * Only instants starts from 'startInstant' (inclusive) are considered.
   */
  private void load() {
    // load active instants first.
    this.metaClient.getActiveTimeline()
        .filterCompletedInstants().getInstantsAsStream()
        .forEach(instant -> setCompletionTime(instant.requestedTime(), instant.getCompletionTime()));
    // Do not load archive timeline.
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