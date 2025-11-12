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

import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.InstantComparator;
import org.apache.hudi.common.table.timeline.versioning.common.InstantComparators;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.LOG_COMPACTION_ACTION;

public class InstantComparatorV1 implements Serializable, InstantComparator {

  /**
   * A COMPACTION action eventually becomes COMMIT when completed. So, when grouping instants
   * for state transitions, this needs to be taken into account
   */
  private static final Map<String, String> COMPARABLE_ACTIONS = createComparableActionsMap();

  public static final Comparator<HoodieInstant> ACTION_COMPARATOR =
      new InstantComparators.ActionComparator(COMPARABLE_ACTIONS);

  public static final Comparator<HoodieInstant> REQUESTED_TIME_BASED_COMPARATOR =
      new InstantComparators.RequestedTimeBasedComparator(COMPARABLE_ACTIONS);

  public static final Comparator<HoodieInstant> COMPLETION_TIME_BASED_COMPARATOR =
      new InstantComparators.CompletionTimeBasedComparator(COMPARABLE_ACTIONS);

  public static String getComparableAction(String action) {
    return COMPARABLE_ACTIONS.getOrDefault(action, action);
  }

  private static final Map<String, String> createComparableActionsMap() {
    Map<String, String> comparableMap = new HashMap<>();
    comparableMap.put(COMPACTION_ACTION, COMMIT_ACTION);
    comparableMap.put(LOG_COMPACTION_ACTION, DELTA_COMMIT_ACTION);
    return comparableMap;
  }

  @Override
  public Comparator<HoodieInstant> actionOnlyComparator() {
    return ACTION_COMPARATOR;
  }

  @Override
  public Comparator<HoodieInstant> requestedTimeOrderedComparator() {
    return REQUESTED_TIME_BASED_COMPARATOR;
  }

  @Override
  public Comparator<HoodieInstant> completionTimeOrderedComparator() {
    return COMPLETION_TIME_BASED_COMPARATOR;
  }
}
