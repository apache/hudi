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

package org.apache.hudi.common.table.timeline.versioning.common;

import org.apache.hudi.common.table.timeline.HoodieInstant;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Map;

/**
 * Comparators for HoodieInstant that are also serializable.
 * java.util.Comparators are not serializable.
 */
public class InstantComparators {

  public static class ActionComparator implements Serializable, Comparator<HoodieInstant> {
    private final Map<String, String> comparableActions;

    public ActionComparator(Map<String, String> comparableActions) {
      this.comparableActions = comparableActions;
    }

    @Override
    public int compare(HoodieInstant instant1, HoodieInstant instant2) {
      String action1 = getComparableAction(instant1.getAction());
      String action2 = getComparableAction(instant2.getAction());
      return action1.compareTo(action2);
    }

    private String getComparableAction(String action) {
      return comparableActions.getOrDefault(action, action);
    }

  }

  public static class RequestedTimeBasedComparator implements Serializable, Comparator<HoodieInstant> {
    private final ActionComparator actionComparator;

    public RequestedTimeBasedComparator(Map<String, String> comparableActions) {
      this.actionComparator = new ActionComparator(comparableActions);
    }

    @Override
    public int compare(HoodieInstant instant1, HoodieInstant instant2) {
      int res = instant1.requestedTime().compareTo(instant2.requestedTime());
      if (res == 0) {
        res = actionComparator.compare(instant1, instant2);
        if (res == 0) {
          res = instant1.getState().compareTo(instant2.getState());
        }
      }
      return res;
    }
  }

  public static class CompletionTimeBasedComparator implements Serializable, Comparator<HoodieInstant> {
    private final RequestedTimeBasedComparator timestampBasedComparator;

    public CompletionTimeBasedComparator(Map<String, String> comparableActions) {
      this.timestampBasedComparator = new RequestedTimeBasedComparator(comparableActions);
    }

    @Override
    public int compare(HoodieInstant instant1, HoodieInstant instant2) {
      if (instant1.getCompletionTime() == null && instant2.getCompletionTime() != null) {
        return 1; // instant1 is not completed, so it is greater than instant2
      }
      if (instant2.getCompletionTime() == null && instant1.getCompletionTime() != null) {
        return -1; // instant2 is not completed, so it is greater than instant1
      }
      if (instant1.getCompletionTime() == null && instant2.getCompletionTime() == null) {
        return timestampBasedComparator.compare(instant1, instant2); // both are not completed, compare by requested time
      }
      int res = instant1.getCompletionTime().compareTo(instant2.getCompletionTime());
      if (res == 0) {
        res = timestampBasedComparator.compare(instant1, instant2);
      }
      return res;
    }
  }
}
