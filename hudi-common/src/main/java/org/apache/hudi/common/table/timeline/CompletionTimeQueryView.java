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

import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.util.Option;

import java.util.List;
import java.util.function.Function;

public interface CompletionTimeQueryView extends AutoCloseable {

  boolean isCompleted(String beginInstantTime);

  /**
   * Returns whether the instant is archived.
   */
  boolean isArchived(String instantTime);

  /**
   * Returns whether the give instant time {@code instantTime} completed before the base instant {@code baseInstant}.
   */
  boolean isCompletedBefore(String baseInstant, String instantTime);

  /**
   * Returns whether the given instant time {@code instantTime} is sliced after or on the base instant {@code baseInstant}.
   */
  boolean isSlicedAfterOrOn(String baseInstant, String instantTime);

  /**
   * Get completion time with a base instant time as a reference to fix the compatibility.
   *
   * @param baseInstant The base instant
   * @param instantTime The instant time to query the completion time with
   *
   * @return Probability fixed completion time.
   */
  Option<String> getCompletionTime(String baseInstant, String instantTime);

  /**
   * Queries the completion time with given instant time.
   *
   * @param requestedTime The time the commit was requested.
   *
   * @return The completion time if the instant finished or empty if it is still pending.
   */
  Option<String> getCompletionTime(String requestedTime);

  /**
   * Queries the instant times with given completion time range.
   *
   * <p>By default, assumes there is at most 1 day time of duration for an instant to accelerate the queries.
   *
   * @param timeline             The timeline.
   * @param startCompletionTime  The start completion time of the query range.
   * @param endCompletionTime    The end completion time of the query range.
   * @param rangeType            The range type.
   *
   * @return The sorted instant time list.
   */
  List<String> getInstantTimes(
      HoodieTimeline timeline,
      Option<String> startCompletionTime,
      Option<String> endCompletionTime,
      InstantRange.RangeType rangeType);

  /**
   * Queries the instant times with given completion time range.
   *
   * @param startCompletionTime      The start completion time of the query range.
   * @param endCompletionTime        The end completion time of the query range.
   * @param earliestInstantTimeFunc  The function to generate the earliest instant time boundary
   *                                 with the minimum completion time.
   *
   * @return The sorted instant time list.
   */
  List<String> getInstantTimes(
      String startCompletionTime,
      String endCompletionTime,
      Function<String, String> earliestInstantTimeFunc);

  /**
   *  Get Cursor Instant
   * @return {@link String} instant time of the cursor.
   */
  String getCursorInstant();

  /**
   * Return true if the table is empty.
   * @return {@code true} if the table is empty, {@code false} otherwise.
   */
  boolean isEmptyTable();
}
