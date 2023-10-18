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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;

import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hudi.common.table.timeline.HoodieArchivedTimeline.COMPLETION_TIME_ARCHIVED_META_FIELD;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.EQUALS;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.GREATER_THAN;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.GREATER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.LESSER_THAN;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.compareTimestamps;

/**
 * Query view for instant completion time.
 */
public class CompletionTimeQueryView implements AutoCloseable, Serializable {
  private static final long serialVersionUID = 1L;

  private static final long MILLI_SECONDS_IN_THREE_DAYS = 3 * 24 * 3600 * 1000;

  private final HoodieTableMetaClient metaClient;

  /**
   * Mapping from instant start time -> completion time.
   * Should be thread-safe data structure.
   */
  private final Map<String, String> startToCompletionInstantTimeMap;

  /**
   * The start instant time to eagerly load from, by default load last N days of completed instants.
   */
  private final String startInstant;

  /**
   * The first write instant on the active timeline, used for query optimization.
   */
  private final String firstNonSavepointCommit;

  /**
   * The constructor.
   *
   * @param metaClient   The table meta client.
   */
  public CompletionTimeQueryView(HoodieTableMetaClient metaClient) {
    this(metaClient, HoodieInstantTimeGenerator.formatDate(new Date(Instant.now().minusMillis(MILLI_SECONDS_IN_THREE_DAYS).toEpochMilli())));
  }

  /**
   * The constructor.
   *
   * @param metaClient   The table meta client.
   * @param startInstant The earliest instant time to eagerly load from, by default load last N days of completed instants.
   */
  public CompletionTimeQueryView(HoodieTableMetaClient metaClient, String startInstant) {
    this.metaClient = metaClient;
    this.startToCompletionInstantTimeMap = new ConcurrentHashMap<>();
    this.startInstant = minInstant(startInstant, metaClient.getActiveTimeline().firstInstant().map(HoodieInstant::getTimestamp).orElse(""));
    // Note: use getWriteTimeline() to keep sync with the fs view visibleCommitsAndCompactionTimeline, see AbstractTableFileSystemView.refreshTimeline.
    this.firstNonSavepointCommit = metaClient.getActiveTimeline().getWriteTimeline().getFirstNonSavepointCommit().map(HoodieInstant::getTimestamp).orElse("");
    load();
  }

  /**
   * Returns whether the instant is completed.
   */
  public boolean isCompleted(String instantTime) {
    return this.startToCompletionInstantTimeMap.containsKey(instantTime)
        || HoodieTimeline.compareTimestamps(instantTime, LESSER_THAN, this.firstNonSavepointCommit);
  }

  /**
   * Returns whether the give instant time {@code instantTime} completed before the base instant {@code baseInstant}.
   */
  public boolean isCompletedBefore(String baseInstant, String instantTime) {
    Option<String> completionTimeOpt = getCompletionTime(baseInstant, instantTime);
    if (completionTimeOpt.isPresent()) {
      return HoodieTimeline.compareTimestamps(completionTimeOpt.get(), LESSER_THAN, baseInstant);
    }
    return false;
  }

  /**
   * Returns whether the give instant time {@code instantTime} is sliced after or on the base instant {@code baseInstant}.
   */
  public boolean isSlicedAfterOrOn(String baseInstant, String instantTime) {
    Option<String> completionTimeOpt = getCompletionTime(baseInstant, instantTime);
    if (completionTimeOpt.isPresent()) {
      return HoodieTimeline.compareTimestamps(completionTimeOpt.get(), GREATER_THAN_OR_EQUALS, baseInstant);
    }
    return true;
  }

  /**
   * Get completion time with a base instant time as a reference to fix the compatibility.
   *
   * @param baseInstant The base instant
   * @param instantTime The instant time to query the completion time with
   *
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
        // if the file slice base instant time is not in datetime format. For example,
        // 1. many test cases just use integer string as the instant time.
        // 2. MDT uses compaction instant time as [delta_instant] + "001".

        // CAUTION: this fix only works for OCC(Optimistic Concurrency Control).
        // for NB-CC(Non-blocking Concurrency Control), the file slicing may be incorrect.
        return Option.of(instantTime);
      }
    }
    return completionTimeOpt;
  }

  /**
   * Queries the instant completion time with given start time.
   *
   * @param startTime The start time.
   *
   * @return The completion time if the instant finished or empty if it is still pending.
   */
  public Option<String> getCompletionTime(String startTime) {
    String completionTime = this.startToCompletionInstantTimeMap.get(startTime);
    if (completionTime != null) {
      return Option.of(completionTime);
    }
    if (HoodieTimeline.compareTimestamps(startTime, GREATER_THAN, this.startInstant)) {
      // the instant is still pending
      return Option.empty();
    }
    // the 'startTime' should be out of the eager loading range, switch to a lazy loading.
    // This operation is resource costly.
    HoodieArchivedTimeline.loadInstants(metaClient,
        new EqualsTimestampFilter(startTime),
        HoodieArchivedTimeline.LoadMode.SLIM,
        r -> true,
        this::readCompletionTime);
    return Option.ofNullable(this.startToCompletionInstantTimeMap.get(startTime));
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
        .forEach(instant -> setCompletionTime(instant.getTimestamp(), instant.getCompletionTime()));
    // then load the archived instants.
    HoodieArchivedTimeline.loadInstants(metaClient,
        new HoodieArchivedTimeline.StartTsFilter(this.startInstant),
        HoodieArchivedTimeline.LoadMode.SLIM,
        r -> true,
        this::readCompletionTime);
  }

  private void readCompletionTime(String instantTime, GenericRecord record) {
    final String completionTime = record.get(COMPLETION_TIME_ARCHIVED_META_FIELD).toString();
    setCompletionTime(instantTime, completionTime);
  }

  private void setCompletionTime(String instantTime, String completionTime) {
    if (completionTime == null) {
      // the meta-server instant does not have completion time
      completionTime = instantTime;
    }
    this.startToCompletionInstantTimeMap.putIfAbsent(instantTime, completionTime);
  }

  private static String minInstant(String instant1, String instant2) {
    return compareTimestamps(instant1, LESSER_THAN, instant2) ? instant1 : instant2;
  }

  @Override
  public void close() throws Exception {
    this.startToCompletionInstantTimeMap.clear();
  }

  // -------------------------------------------------------------------------
  //  Inner class
  // -------------------------------------------------------------------------

  /**
   * A time based filter with equality of specified timestamp.
   */
  public static class EqualsTimestampFilter extends HoodieArchivedTimeline.TimeRangeFilter {
    private final String ts;

    public EqualsTimestampFilter(String ts) {
      super(ts, ts); // endTs is never used
      this.ts = ts;
    }

    public boolean isInRange(String instantTime) {
      return HoodieTimeline.compareTimestamps(instantTime, EQUALS, ts);
    }
  }
}
