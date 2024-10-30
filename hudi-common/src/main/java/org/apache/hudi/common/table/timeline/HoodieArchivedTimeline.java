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

import static org.apache.hudi.common.table.timeline.InstantComparatorUtils.GREATER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparatorUtils.compareTimestamps;

public interface HoodieArchivedTimeline extends HoodieTimeline {

  public static final String COMPLETION_TIME_ARCHIVED_META_FIELD = "completionTime";

  public void loadInstantDetailsInMemory(String startTs, String endTs);

  public void loadCompletedInstantDetailsInMemory();

  public void loadCompactionDetailsInMemory(String compactionInstantTime);

  public void loadCompactionDetailsInMemory(String startTs, String endTs);

  public void clearInstantDetailsFromMemory(String instantTime);

  public void clearInstantDetailsFromMemory(String startTs, String endTs);

  public HoodieArchivedTimeline reload();

  public HoodieArchivedTimeline reload(String startTs);

  /**
   * Different mode for loading the archived instant metadata.
   */
  public enum LoadMode {
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
    PLAN
  }

  /**
   * A time based filter with range (startTs, endTs].
   */
  public static class TimeRangeFilter {
    protected final String startTs;
    protected final String endTs;

    public TimeRangeFilter(String startTs, String endTs) {
      this.startTs = startTs;
      this.endTs = endTs;
    }

    public boolean isInRange(String instantTime) {
      return InstantComparatorUtils.isInRange(instantTime, this.startTs, this.endTs);
    }
  }

  /**
   * A time based filter with range [startTs, endTs).
   */
  public static class ClosedOpenTimeRangeFilter extends TimeRangeFilter {

    public ClosedOpenTimeRangeFilter(String startTs, String endTs) {
      super(startTs, endTs);
    }

    public boolean isInRange(String instantTime) {
      return InstantComparatorUtils.isInClosedOpenRange(instantTime, this.startTs, this.endTs);
    }
  }

  /**
   * A time based filter with range [startTs, +&#8734).
   */
  public static class StartTsFilter extends TimeRangeFilter {

    public StartTsFilter(String startTs) {
      super(startTs, null); // endTs is never used
    }

    public boolean isInRange(String instantTime) {
      return compareTimestamps(instantTime, GREATER_THAN_OR_EQUALS, startTs);
    }
  }

}
