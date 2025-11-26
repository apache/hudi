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

import org.apache.hudi.common.util.Option;

import java.util.Objects;

public interface HoodieArchivedTimeline extends HoodieTimeline {

  String COMPLETION_TIME_ARCHIVED_META_FIELD = "completionTime";

  void loadInstantDetailsInMemory(String startTs, String endTs);

  void loadCompletedInstantDetailsInMemory();

  void loadCompactionDetailsInMemory(String compactionInstantTime);

  void loadCompactionDetailsInMemory(String startTs, String endTs);

  void clearInstantDetailsFromMemory(String instantTime);

  void clearInstantDetailsFromMemory(String startTs, String endTs);

  HoodieArchivedTimeline reload();

  HoodieArchivedTimeline reload(String startTs);

  /**
   * Different mode for loading the archived instant metadata.
   */
  enum LoadMode {
    /**
     * Loads the instantTime, completionTime.
     */
    TIME,
    /**
     * Loads the instantTime, completionTime, action.
     */
    ACTION,
    /**
     * Loads the instantTime, completionTime, action, metadata.
     */
    METADATA,
    /**
     * Loads the instantTime, completionTime, action, plan.
     */
    PLAN,
    /**
     * Loads the instantTime, completionTime, action, plan, metadata.
     */
    FULL
  }

  /**
   * A time based filter with range, there may be different forms of interval types.
   * <li>Double open <code>(startTs, endTs)</code></li>
   * <li>Left closed right open <code>[startTs, endTs)</code></li>
   * <li>Left open right closed <code>(startTs, endTs]</code></li>
   * <li>Double closed <code>[startTs, endTs]</code></li>
   * <li>StartTs only <code>[startTs, +&#8734)</code></li>
   * <li>EndTs only <code>(-&#8734, endTs]</code></li>
   */
  class TimeRangeFilter {
    protected final Option<String> startTs;
    protected final IntervalType startType;
    protected final Option<String> endTs;
    protected final IntervalType endType;

    /**
     * default is [startTs, endTs]
     */
    public TimeRangeFilter(String startTs, String endTs) {
      this(Option.of(startTs), IntervalType.CLOSED, Option.of(endTs), IntervalType.CLOSED);
    }

    public TimeRangeFilter(Option<String> startTs, IntervalType startType, Option<String> endTs, IntervalType endType) {
      this.startTs = startTs;
      this.startType = startType;
      this.endTs = endTs;
      this.endType = endType;
    }

    public boolean isInRange(String instantTime) {
      return InstantComparison.isInRangeWithIntervalType(instantTime, this.startTs, this.startType, this.endTs, this.endType);
    }

    public boolean isInRange(HoodieInstant instant) {
      return this.isInRange(instant.requestedTime());
    }

    public boolean intersects(TimeRangeFilter other) {
      if (other == null) {
        throw new IllegalArgumentException("Other filter cannot be null");
      }

      // check left boundary overlap
      if (!isLeftOverlap(other)) {
        return false;
      }

      // check right boundary overlap
      if (!isRightOverlap(other)) {
        return false;
      }

      // check boundary equals
      if (boundaryEquals(this.endTs, other.startTs)) {
        return this.endType == IntervalType.CLOSED && other.startType == IntervalType.CLOSED;
      }
      if (boundaryEquals(this.startTs, other.endTs)) {
        return this.startType == IntervalType.CLOSED && other.endType == IntervalType.CLOSED;
      }

      return true;
    }

    private boolean boundaryEquals(Option<String> a, Option<String> b) {
      // one is infinite, the other is finite
      if (a.isPresent() != b.isPresent()) {
        return false;
      }
      // both are infinite, regard as equal
      if (a.isEmpty() && b.isEmpty()) {
        return true;
      }
      // both are finite, compare the string value
      return InstantComparison.compareTimestamps(a.get(), InstantComparison.EQUALS, b.get());
    }

    /**
     * Compute whether this range's left side overlaps with the other range's right side.
     */
    private boolean isLeftOverlap(TimeRangeFilter other) {
      if (this.endTs.isEmpty() || other.startTs.isEmpty()) {
        return true;
      }
      String aEnd = this.endTs.get();
      String bStart = other.startTs.get();
      if (InstantComparison.compareTimestamps(aEnd, InstantComparison.LESSER_THAN, bStart)) {
        return false;
      }
      // if aEnd == bStart, depends on the interval types
      if (InstantComparison.compareTimestamps(aEnd, InstantComparison.EQUALS, bStart)) {
        // need both closed to have overlap
        return this.endType == IntervalType.CLOSED && other.startType == IntervalType.CLOSED;
      }
      return true;
    }

    private boolean isRightOverlap(TimeRangeFilter other) {
      if (this.startTs.isEmpty() || other.endTs.isEmpty()) {
        return true;
      }
      String aStart = this.startTs.get();
      String bEnd = other.endTs.get();
      if (InstantComparison.compareTimestamps(bEnd, InstantComparison.LESSER_THAN, aStart)) {
        return false;
      }
      // if bEnd == aStart, depends on the interval types
      if (InstantComparison.compareTimestamps(bEnd, InstantComparison.EQUALS, aStart)) {
        // need both closed to have overlap
        return other.endType == IntervalType.CLOSED && this.startType == IntervalType.CLOSED;
      }
      return true;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TimeRangeFilter that = (TimeRangeFilter) o;
      return Objects.equals(startTs, that.startTs) && Objects.equals(endTs, that.endTs)
          && startType == that.startType && endType == that.endType;
    }

    @Override
    public int hashCode() {
      return Objects.hash(startTs, endTs, startType, endType);
    }

    @Override
    public String toString() {
      String start = startTs.map(Object::toString).orElse("-∞");
      String end = endTs.map(Object::toString).orElse("+∞");

      // when print infinite boundary, always use open bracket for math convention
      char startBracket = (startType == IntervalType.OPEN || startTs.isEmpty()) ? '(' : '[';
      char endBracket = (endType == IntervalType.OPEN || endTs.isEmpty()) ? ')' : ']';

      return startBracket + start + ", " + end + endBracket;
    }
  }

  /**
   * A time based filter with range (startTs, endTs].
   */
  class OpenClosedTimeRangeFilter extends TimeRangeFilter {

    public OpenClosedTimeRangeFilter(String startTs, String endTs) {
      super(Option.of(startTs), IntervalType.OPEN, Option.of(endTs), IntervalType.CLOSED);
    }
  }

  /**
   * A time based filter with range [startTs, endTs).
   */
  class ClosedOpenTimeRangeFilter extends TimeRangeFilter {

    public ClosedOpenTimeRangeFilter(String startTs, String endTs) {
      super(Option.of(startTs), IntervalType.CLOSED, Option.of(endTs), IntervalType.OPEN);
    }
  }

  /**
   * A time based filter with range (startTs, endTs).
   */
  class OpenOpenTimeRangeFilter extends TimeRangeFilter {

    public OpenOpenTimeRangeFilter(String startTs, String endTs) {
      super(Option.of(startTs), IntervalType.OPEN, Option.of(endTs), IntervalType.OPEN);
    }
  }

  /**
   * A time-based filter with range [startTs, endTs].
   */
  class ClosedClosedTimeRangeFilter extends TimeRangeFilter {

    public ClosedClosedTimeRangeFilter(String startTs, String endTs) {
      super(Option.of(startTs), IntervalType.CLOSED, Option.of(endTs), IntervalType.CLOSED);
    }
  }

  /**
   * A time based filter with range [startTs, +&#8734).
   */
  class StartTsFilter extends TimeRangeFilter {

    public StartTsFilter(String startTs) {
      // regard end type as closed, because in the comparison between infinity and infinity, we need to consider them as equal
      super(Option.of(startTs), IntervalType.CLOSED, Option.empty(), IntervalType.CLOSED);
    }
  }

  /**
   * A time based filter with range (-&#8734, endTs].
   */
  class EndTsFilter extends TimeRangeFilter {

    public EndTsFilter(String endTs) {
      // regard start type as closed, because in the comparison between infinity and infinity, we need to consider them as equal
      super(Option.empty(), IntervalType.CLOSED, Option.of(endTs), IntervalType.CLOSED);
    }
  }

  /**
   * A time based filter with range (-&#8734, +&#8734).
   */
  class FullFilter extends TimeRangeFilter {

    public FullFilter() {
      // regard start/end type as closed, because in the comparison between infinity and infinity, we need to consider them as equal
      super(Option.empty(), IntervalType.CLOSED, Option.empty(), IntervalType.CLOSED);
    }
  }

}
