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

import org.apache.hadoop.fs.FileStatus;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

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
  private static final Map<String, String> COMPARABLE_ACTIONS = createComparableActionsMap();

  public static final Comparator<HoodieInstant> ACTION_COMPARATOR =
      Comparator.comparing(instant -> getComparableAction(instant.getAction()));

  public static final Comparator<HoodieInstant> COMPARATOR = Comparator.comparing(HoodieInstant::getTimestamp)
      .thenComparing(ACTION_COMPARATOR).thenComparing(HoodieInstant::getState);

  public static String getComparableAction(String action) {
    return COMPARABLE_ACTIONS.getOrDefault(action, action);
  }

  public static String getTimelineFileExtension(String fileName) {
    Objects.requireNonNull(fileName);
    int dotIndex = fileName.indexOf('.');
    return dotIndex == -1 ? "" : fileName.substring(dotIndex);
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
  private String timestamp;

  /**
   * Load the instant from the meta FileStatus.
   */
  public HoodieInstant(FileStatus fileStatus) {
    // First read the instant timestamp. [==>20170101193025<==].commit
    String fileName = fileStatus.getPath().getName();
    String fileExtension = getTimelineFileExtension(fileName);
    timestamp = fileName.replace(fileExtension, "");

    // Next read the action for this marker
    action = fileExtension.replaceFirst(".", "");
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

  public HoodieInstant(boolean isInflight, String action, String timestamp) {
    // TODO: vb - Preserving for avoiding cascading changes. This constructor will be updated in subsequent PR
    this.state = isInflight ? State.INFLIGHT : State.COMPLETED;
    this.action = action;
    this.timestamp = timestamp;
  }

  public HoodieInstant(State state, String action, String timestamp) {
    this.state = state;
    this.action = action;
    this.timestamp = timestamp;
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
    return timestamp;
  }

  /**
   * Get the filename for this instant.
   */
  public String getFileName() {
    if (HoodieTimeline.COMMIT_ACTION.equals(action)) {
      return isInflight() ? HoodieTimeline.makeInflightCommitFileName(timestamp)
          : isRequested() ? HoodieTimeline.makeRequestedCommitFileName(timestamp)
              : HoodieTimeline.makeCommitFileName(timestamp);
    } else if (HoodieTimeline.CLEAN_ACTION.equals(action)) {
      return isInflight() ? HoodieTimeline.makeInflightCleanerFileName(timestamp)
          : isRequested() ? HoodieTimeline.makeRequestedCleanerFileName(timestamp)
              : HoodieTimeline.makeCleanerFileName(timestamp);
    } else if (HoodieTimeline.ROLLBACK_ACTION.equals(action)) {
      return isInflight() ? HoodieTimeline.makeInflightRollbackFileName(timestamp)
          : isRequested() ? HoodieTimeline.makeRequestedRollbackFileName(timestamp)
              : HoodieTimeline.makeRollbackFileName(timestamp);
    } else if (HoodieTimeline.SAVEPOINT_ACTION.equals(action)) {
      return isInflight() ? HoodieTimeline.makeInflightSavePointFileName(timestamp)
          : HoodieTimeline.makeSavePointFileName(timestamp);
    } else if (HoodieTimeline.DELTA_COMMIT_ACTION.equals(action)) {
      return isInflight() ? HoodieTimeline.makeInflightDeltaFileName(timestamp)
          : isRequested() ? HoodieTimeline.makeRequestedDeltaFileName(timestamp)
              : HoodieTimeline.makeDeltaFileName(timestamp);
    } else if (HoodieTimeline.COMPACTION_ACTION.equals(action)) {
      if (isInflight()) {
        return HoodieTimeline.makeInflightCompactionFileName(timestamp);
      } else if (isRequested()) {
        return HoodieTimeline.makeRequestedCompactionFileName(timestamp);
      } else {
        return HoodieTimeline.makeCommitFileName(timestamp);
      }
    } else if (HoodieTimeline.LOG_COMPACTION_ACTION.equals(action)) {
      if (isInflight()) {
        return HoodieTimeline.makeInflightLogCompactionFileName(timestamp);
      } else if (isRequested()) {
        return HoodieTimeline.makeRequestedLogCompactionFileName(timestamp);
      } else {
        return HoodieTimeline.makeDeltaFileName(timestamp);
      }
    } else if (HoodieTimeline.RESTORE_ACTION.equals(action)) {
      return isInflight() ? HoodieTimeline.makeInflightRestoreFileName(timestamp)
          : isRequested() ? HoodieTimeline.makeRequestedRestoreFileName(timestamp)
          : HoodieTimeline.makeRestoreFileName(timestamp);
    } else if (HoodieTimeline.REPLACE_COMMIT_ACTION.equals(action)) {
      return isInflight() ? HoodieTimeline.makeInflightReplaceFileName(timestamp)
          : isRequested() ? HoodieTimeline.makeRequestedReplaceFileName(timestamp)
          : HoodieTimeline.makeReplaceFileName(timestamp);
    } else if (HoodieTimeline.INDEXING_ACTION.equals(action)) {
      return isInflight() ? HoodieTimeline.makeInflightIndexFileName(timestamp)
          : isRequested() ? HoodieTimeline.makeRequestedIndexFileName(timestamp)
          : HoodieTimeline.makeIndexCommitFileName(timestamp);
    } else if (HoodieTimeline.SCHEMA_COMMIT_ACTION.equals(action)) {
      return isInflight() ? HoodieTimeline.makeInflightSchemaFileName(timestamp)
          : isRequested() ? HoodieTimeline.makeRequestSchemaFileName(timestamp)
          : HoodieTimeline.makeSchemaFileName(timestamp);
    }
    throw new IllegalArgumentException("Cannot get file name for unknown action " + action);
  }

  private static final Map<String, String> createComparableActionsMap() {
    Map<String, String> comparableMap = new HashMap<>();
    comparableMap.put(HoodieTimeline.COMPACTION_ACTION, HoodieTimeline.COMMIT_ACTION);
    comparableMap.put(HoodieTimeline.LOG_COMPACTION_ACTION, HoodieTimeline.DELTA_COMMIT_ACTION);
    return comparableMap;
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
    return state == that.state && Objects.equals(action, that.action) && Objects.equals(timestamp, that.timestamp);
  }

  public State getState() {
    return state;
  }

  @Override
  public int hashCode() {
    return Objects.hash(state, action, timestamp);
  }

  @Override
  public int compareTo(HoodieInstant o) {
    return COMPARATOR.compare(this, o);
  }

  @Override
  public String toString() {
    return "[" + ((isInflight() || isRequested()) ? "==>" : "") + timestamp + "__" + action + "__" + state + "]";
  }
}
