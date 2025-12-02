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

import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;

public interface HoodieArchivedTimeline extends HoodieTimeline {

  String COMPLETION_TIME_ARCHIVED_META_FIELD = "completionTime";

  void loadInstantDetailsInMemory(String startTs, String endTs);

  void loadCompletedInstantDetailsInMemory();

  void loadCompactionDetailsInMemory(String compactionInstantTime);

  void loadCompactionDetailsInMemory(String startTs, String endTs);

  void loadCompactionDetailsInMemory(int limit);

  void loadCompletedInstantDetailsInMemory(String startTs, String endTs);

  void loadCompletedInstantDetailsInMemory(int limit);

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
   * A time based filter with range (startTs, endTs].
   */
  class TimeRangeFilter {
    protected final String startTs;
    protected final String endTs;

    public TimeRangeFilter(String startTs, String endTs) {
      this.startTs = startTs;
      this.endTs = endTs;
    }

    public boolean isInRange(String instantTime) {
      return InstantComparison.isInRange(instantTime, this.startTs, this.endTs);
    }

    /**
     * Returns whether the given instant time range has overlapping with the current range.
     */
    public boolean hasOverlappingInRange(String startInstant, String endInstant) {
      return isInRange(startInstant) || isInRange(endInstant) || isContained(startInstant, endInstant);
    }

    private boolean isContained(String startInstant, String endInstant) {
      // the given range is finite and expected to be covered by the current range which naturally cannot be infinite.
      return startTs != null && InstantComparison.compareTimestamps(startTs, InstantComparison.GREATER_THAN_OR_EQUALS, startInstant)
          && endTs != null && InstantComparison.compareTimestamps(endTs, InstantComparison.LESSER_THAN_OR_EQUALS, endInstant);
    }
  }

  /**
   * A time based filter with range [startTs, endTs).
   */
  class ClosedOpenTimeRangeFilter extends TimeRangeFilter {

    public ClosedOpenTimeRangeFilter(String startTs, String endTs) {
      super(startTs, endTs);
    }

    public boolean isInRange(String instantTime) {
      return InstantComparison.isInClosedOpenRange(instantTime, this.startTs, this.endTs);
    }
  }

  /**
   * A time-based filter with range [startTs, endTs].
   */
  class ClosedClosedTimeRangeFilter extends TimeRangeFilter {
    private final String startTs;
    private final String endTs;

    public ClosedClosedTimeRangeFilter(String startTs, String endTs) {
      super(startTs, endTs);
      this.startTs = startTs;
      this.endTs = endTs;
    }

    @Override
    public boolean isInRange(String instantTime) {
      return InstantComparison.isInClosedRange(instantTime, this.startTs, this.endTs);
    }

    public boolean isInRange(HoodieInstant instant) {
      return InstantComparison.isInClosedRange(instant.requestedTime(), this.startTs, this.endTs);
    }
  }

  /**
   * A time based filter with range [startTs, +&#8734).
   */
  class StartTsFilter extends TimeRangeFilter {

    public StartTsFilter(String startTs) {
      super(startTs, null); // endTs is never used
    }

    public boolean isInRange(String instantTime) {
      return compareTimestamps(instantTime, GREATER_THAN_OR_EQUALS, startTs);
    }
  }

}
