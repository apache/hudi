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

import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hadoop.fs.FileStatus;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;

import static org.apache.hudi.common.table.timeline.HoodieActiveTimeline.COMMIT_TIME_PATTERN;

/**
 * A Hoodie Instant represents a action done on a hoodie table. All actions start with a inflight instant and then
 * create a completed instant after done.
 *
 * @see HoodieTimeline
 */
public class HoodieInstant implements Serializable, Comparable<HoodieInstant> {

  /**
   * A COMPACTION action eventually becomes COMMIT when completed. So, when grouping instants
   * for state transitions, this needs to be taken into account
   */
  private static final Map<String, String> COMPARABLE_ACTIONS =
      CollectionUtils.createImmutableMap(HoodieTimeline.COMPACTION_ACTION, HoodieTimeline.COMMIT_ACTION);

  public static final Comparator<HoodieInstant> ACTION_COMPARATOR =
      Comparator.comparing(instant -> getComparableAction(instant.getAction()));

  public static final Comparator<HoodieInstant> START_INSTANT_TIME_COMPARATOR = Comparator.comparing(HoodieInstant::getTimestamp)
      .thenComparing(ACTION_COMPARATOR).thenComparing(HoodieInstant::getState);

  public static final Comparator<HoodieInstant> END_INSTANT_TIME_COMPARATOR = Comparator.comparing(HoodieInstant::getActionEndTimestamp)
      .thenComparing(ACTION_COMPARATOR).thenComparing(HoodieInstant::getState);

  public static String getComparableAction(String action) {
    return COMPARABLE_ACTIONS.getOrDefault(action, action);
  }

  public static String getTimelineFileExtension(String fileName) {
    Objects.requireNonNull(fileName);
    int dotIndex = fileName.indexOf('.');
    return dotIndex == -1 ? "" : fileName.substring(dotIndex);
  }

  public static String getActionStartTime(String fileExtensionPrefix) {
    Objects.requireNonNull(fileExtensionPrefix);
    int dotIndex = fileExtensionPrefix.indexOf('_');
    return dotIndex == -1 ? fileExtensionPrefix : fileExtensionPrefix.substring(0, dotIndex);
  }

  public static String getActionEndTime(FileStatus fileStatus, String startTimestamp) {
    Objects.requireNonNull(fileStatus);
    String endTimestamp = String.valueOf(fileStatus.getModificationTime());
    return HoodieTimeline.LESSER_THAN_OR_EQUALS.test(endTimestamp, startTimestamp)
            || !COMMIT_TIME_PATTERN.matcher(endTimestamp).matches() ? startTimestamp : endTimestamp;
  }

  public static String getActionEndTime(String fileName, String startTimestamp) {
    Objects.requireNonNull(fileName);
    Objects.requireNonNull(startTimestamp);
    int lastIndex = fileName.lastIndexOf(".");
    String fileExtensionSuffix = fileName.substring(lastIndex);
    return COMMIT_TIME_PATTERN.matcher(fileExtensionSuffix).matches() ? fileExtensionSuffix : startTimestamp;
  }

  /**
   * Instant State.
   */
  public enum State {
    // Requested State (valid state for Compaction)
    REQUESTED,
    // Inflight instant
    INFLIGHT,
    // Committed instant
    COMPLETED,
    // Invalid instant
    INVALID
  }

  private State state = State.COMPLETED;
  private String action;
  private String actionStartTimestamp;
  private String actionEndTimestamp;

  /**
   * Load the instant from the meta FileStatus.
   */
  public HoodieInstant(FileStatus fileStatus) {
    // First read the instant startTimestamp. [==>20170101193025<==].commit
    String fileName = fileStatus.getPath().getName();
    String fileExtension = getTimelineFileExtension(fileName);
    actionStartTimestamp = fileName.replace(fileExtension, "");
    // Next read the action for this marker
    action = fileExtension.replaceFirst(".", "");
    actionEndTimestamp = getActionEndTime(fileStatus, actionStartTimestamp);
    if (action.equals("inflight")) {
      // This is to support backwards compatibility on how in-flight commit files were written
      // General rule is inflight extension is .<action>.inflight, but for commit it is .inflight
      action = "commit";
      state = State.INFLIGHT;
    } else if (action.contains(HoodieTimeline.INFLIGHT_EXTENSION)) {
      state = State.INFLIGHT;
      action = action.replace(HoodieTimeline.INFLIGHT_EXTENSION, "");
    } else if (action.contains(HoodieTimeline.REQUESTED_EXTENSION)) {
      state = State.REQUESTED;
      action = action.replace(HoodieTimeline.REQUESTED_EXTENSION, "");
    }
  }

  public HoodieInstant(boolean isInflight, String action, String actionStartTimestamp) {
    // TODO: vb - Preserving for avoiding cascading changes. This constructor will be updated in subsequent PR
    this(isInflight ? State.INFLIGHT : State.COMPLETED, action, actionStartTimestamp, actionStartTimestamp);
  }

  public HoodieInstant(State state, String action, String actionStartTimestamp) {
    this(state, action, actionStartTimestamp, actionStartTimestamp);
  }

  public HoodieInstant(State state, String action, String actionStartTimestamp, String actionEndTimestamp) {
    this.state = state;
    this.action = action;
    this.actionStartTimestamp = actionStartTimestamp;
    this.actionEndTimestamp = actionEndTimestamp;
  }

  public boolean isCompleted() {
    return state == State.COMPLETED;
  }

  public boolean isInflight() {
    return state == State.INFLIGHT;
  }

  public boolean isRequested() {
    return state == State.REQUESTED;
  }

  public String getAction() {
    return action;
  }

  public String getTimestamp() {
    return actionStartTimestamp;
  }

  public String getActionEndTimestamp() {
    return actionEndTimestamp;
  }

  /**
   * Get the filename for this instant.
   */
  public String getFileName() {
    if (HoodieTimeline.COMMIT_ACTION.equals(action)) {
      return isInflight() ? HoodieTimeline.makeInflightCommitFileName(actionStartTimestamp)
          : isRequested() ? HoodieTimeline.makeRequestedCommitFileName(actionStartTimestamp)
          : HoodieTimeline.makeCommitFileName(actionStartTimestamp);
    } else if (HoodieTimeline.CLEAN_ACTION.equals(action)) {
      return isInflight() ? HoodieTimeline.makeInflightCleanerFileName(actionStartTimestamp)
          : isRequested() ? HoodieTimeline.makeRequestedCleanerFileName(actionStartTimestamp)
          : HoodieTimeline.makeCleanerFileName(actionStartTimestamp);
    } else if (HoodieTimeline.ROLLBACK_ACTION.equals(action)) {
      return isInflight() ? HoodieTimeline.makeInflightRollbackFileName(actionStartTimestamp)
          : HoodieTimeline.makeRollbackFileName(actionStartTimestamp);
    } else if (HoodieTimeline.SAVEPOINT_ACTION.equals(action)) {
      return isInflight() ? HoodieTimeline.makeInflightSavePointFileName(actionStartTimestamp)
          : HoodieTimeline.makeSavePointFileName(actionStartTimestamp);
    } else if (HoodieTimeline.DELTA_COMMIT_ACTION.equals(action)) {
      return isInflight() ? HoodieTimeline.makeInflightDeltaFileName(actionStartTimestamp)
          : isRequested() ? HoodieTimeline.makeRequestedDeltaFileName(actionStartTimestamp)
          : HoodieTimeline.makeDeltaFileName(actionStartTimestamp);
    } else if (HoodieTimeline.COMPACTION_ACTION.equals(action)) {
      if (isInflight()) {
        return HoodieTimeline.makeInflightCompactionFileName(actionStartTimestamp);
      } else if (isRequested()) {
        return HoodieTimeline.makeRequestedCompactionFileName(actionStartTimestamp);
      } else {
        return HoodieTimeline.makeCommitFileName(actionStartTimestamp);
      }
    } else if (HoodieTimeline.RESTORE_ACTION.equals(action)) {
      return isInflight() ? HoodieTimeline.makeInflightRestoreFileName(actionStartTimestamp)
          : HoodieTimeline.makeRestoreFileName(actionStartTimestamp);
    } else if (HoodieTimeline.REPLACE_COMMIT_ACTION.equals(action)) {
      return isInflight() ? HoodieTimeline.makeInflightReplaceFileName(actionStartTimestamp)
          : isRequested() ? HoodieTimeline.makeRequestedReplaceFileName(actionStartTimestamp)
          : HoodieTimeline.makeReplaceFileName(actionStartTimestamp);
    }
    throw new IllegalArgumentException("Cannot get file name for unknown action " + action);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HoodieInstant that = (HoodieInstant) o;
    return state == that.state && Objects.equals(action, that.action) && Objects.equals(actionStartTimestamp, that.actionStartTimestamp);
  }

  public State getState() {
    return state;
  }

  @Override
  public int hashCode() {
    return Objects.hash(state, action, actionStartTimestamp);
  }

  @Override
  public int compareTo(HoodieInstant o) {
    return START_INSTANT_TIME_COMPARATOR.compare(this, o);
  }

  @Override
  public String toString() {
    return "[" + ((isInflight() || isRequested()) ? "==>" : "") + actionStartTimestamp + "__" + action + "__" + state + "]";
  }
}
