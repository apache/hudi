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

import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
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
  public static final Map<String, String> COMPARABLE_ACTIONS = createComparableActionsMap();

  public static final Comparator<HoodieInstant> ACTION_COMPARATOR =
      Comparator.comparing(instant -> getComparableAction(instant.getAction()));

  public static final Comparator<HoodieInstant> START_INSTANT_TIME_COMPARATOR = Comparator.comparing(HoodieInstant::getTimestamp)
      .thenComparing(ACTION_COMPARATOR).thenComparing(HoodieInstant::getState);

  public static final Comparator<HoodieInstant> END_INSTANT_TIME_COMPARATOR = Comparator.comparing(HoodieInstant::getStateTransitionTime)
      .thenComparing(ACTION_COMPARATOR).thenComparing(HoodieInstant::getState);

  public static String getComparableAction(String action) {
    return COMPARABLE_ACTIONS.getOrDefault(action, action);
  }

  public static State DEFAULT_INIT_STATE = State.COMPLETED;

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
    NIL
  }

  public static class InstantTime {

    private String actionStartTimestamp;
    private String stateTransitionTime;

    private InstantTime(String stateTransitionTime) {
      this.actionStartTimestamp = stateTransitionTime;
    }

    private InstantTime(String actionStartTimestamp, String stateTransitionTime) {
      this.actionStartTimestamp = actionStartTimestamp;
      this.stateTransitionTime = stateTransitionTime;
    }

    public static InstantTime from(String actionStartTimestamp) {
      return new InstantTime(actionStartTimestamp);
    }

    public static InstantTime from(String actionStartTimestamp, String stateTransitionTime) {
      return new InstantTime(actionStartTimestamp, stateTransitionTime);
    }

    public String getActionStartTimestamp() {
      return actionStartTimestamp;
    }

    public String getStateTransitionTime() {
      return stateTransitionTime;
    }
  }

  private State state;
  private String action;
  private InstantTime instantTime;

  /**
   * Load the instant from the meta FileStatus.
   */
  public HoodieInstant(FileStatus fileStatus) {
    this.action = HoodieInstantFormat.getInstantFormat(TimelineLayoutVersion.CURR_LAYOUT_VERSION).getAction(fileStatus);
    this.state = HoodieInstantFormat.getInstantFormat(TimelineLayoutVersion.CURR_LAYOUT_VERSION).getState(fileStatus);
    this.instantTime = InstantTime.from(HoodieInstantFormat.getInstantFormat(TimelineLayoutVersion.CURR_LAYOUT_VERSION).getActionStartTime(fileStatus),
        HoodieInstantFormat.getInstantFormat(TimelineLayoutVersion.CURR_LAYOUT_VERSION).getStateTransitionTime(fileStatus));
  }

  /**
   * Load the instant from the meta FileStatus.
   */
  public HoodieInstant(FileStatus fileStatus, TimelineLayoutVersion timelineLayoutVersion) {
    this.action = HoodieInstantFormat.getInstantFormat(timelineLayoutVersion).getAction(fileStatus);
    this.state = HoodieInstantFormat.getInstantFormat(timelineLayoutVersion).getState(fileStatus);
    this.instantTime = InstantTime.from(HoodieInstantFormat.getInstantFormat(timelineLayoutVersion).getActionStartTime(fileStatus),
        HoodieInstantFormat.getInstantFormat(timelineLayoutVersion).getStateTransitionTime(fileStatus));
  }

  public HoodieInstant(boolean isInflight, String action, String actionStartTimestamp) {
    // TODO: vb - Preserving for avoiding cascading changes. This constructor will be updated in subsequent PR
    this(isInflight ? State.INFLIGHT : State.COMPLETED, action, actionStartTimestamp, actionStartTimestamp);
  }

  @Deprecated
  public HoodieInstant(State state, String action, String actionStartTimestamp) {
    this(state, action, actionStartTimestamp, actionStartTimestamp);
  }

  public HoodieInstant(State state, String action, String actionStartTimestamp, String stateTransitionTime) {
    this.state = state;
    this.action = action;
    this.instantTime = InstantTime.from(actionStartTimestamp, stateTransitionTime);
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
    return this.instantTime.getActionStartTimestamp();
  }

  public String getStateTransitionTime() {
    return this.instantTime.getStateTransitionTime();
  }

  /**
   * Get the filename for this instant.
   */
  public String getFileName() {
    return HoodieInstantFormat.getInstantFormat(TimelineLayoutVersion.CURR_LAYOUT_VERSION).getFileName(this);
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
    return state == that.state && Objects.equals(action, that.action) && Objects.equals(this.instantTime.getActionStartTimestamp(), that.instantTime.getActionStartTimestamp());
  }

  public State getState() {
    return state;
  }

  public InstantTime getInstantTime() {
    return instantTime;
  }

  @Override
  public int hashCode() {
    return Objects.hash(state, action, this.instantTime.getActionStartTimestamp());
  }

  @Override
  public int compareTo(HoodieInstant o) {
    return START_INSTANT_TIME_COMPARATOR.compare(this, o);
  }

  @Override
  public String toString() {
    return "[" + ((isInflight() || isRequested()) ? "==>" : "") + this.instantTime.getActionStartTimestamp() + "__" + action + "__" + state + "]";
  }
}
