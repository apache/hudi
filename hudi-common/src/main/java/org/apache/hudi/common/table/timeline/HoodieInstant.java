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

import org.apache.hudi.common.util.StringUtils;

import lombok.Getter;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;

/**
 * A Hoodie Instant represents a action done on a hoodie table. All actions start with a inflight instant and then
 * create a completed instant after done.
 */
public class HoodieInstant implements Serializable, Comparable<HoodieInstant> {

  public static final String FILE_NAME_FORMAT_ERROR = "The provided file name %s does not conform to the required format";
  public static final String UNDERSCORE = "_";
  public static final String EMPTY_FILE_EXTENSION = "";

  @Getter
  private final State state;
  @Getter
  private final String action;
  private final String requestedTime;
  @Getter
  private final String completionTime;
  // Marker for older formats, we need the state transition time (pre table version 7)
  @Getter
  private boolean isLegacy = false;
  private final Comparator<HoodieInstant> comparator;

  public HoodieInstant(State state, String action, String requestTime, Comparator<HoodieInstant> comparator) {
    this(state, action, requestTime, null, comparator);
  }

  public HoodieInstant(State state, String action, String requestTime, String completionTime, Comparator<HoodieInstant> comparator) {
    this(state, action, requestTime, completionTime, false, comparator);
  }

  public HoodieInstant(State state, String action, String requestedTime, String completionTime, boolean isLegacy, Comparator<HoodieInstant> comparator) {
    this.state = state;
    this.action = action;
    this.requestedTime = requestedTime;
    this.completionTime = completionTime;
    this.isLegacy = isLegacy;
    this.comparator = comparator;
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

  public String requestedTime() {
    return requestedTime;
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
    return state == that.state && Objects.equals(action, that.action) && Objects.equals(requestedTime, that.requestedTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(state, action, requestedTime);
  }

  @Override
  public int compareTo(HoodieInstant o) {
    return comparator.compare(this, o);
  }

  @Override
  public String toString() {
    return "[" + ((isInflight() || isRequested()) ? "==>" : "")
        + requestedTime
        + (StringUtils.isNullOrEmpty(completionTime) ? "" : ("__" + completionTime))
        + "__" + action + "__" + state + "]";
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
}
