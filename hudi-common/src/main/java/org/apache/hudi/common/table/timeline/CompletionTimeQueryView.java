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

  /**
   * Returns whether the instant is completed.
   */
  public boolean isCompleted(String beginInstantTime);

  /**
   * Returns whether the instant is archived.
   */
  public boolean isArchived(String instantTime);

  /**
   * Returns whether the give instant time {@code instantTime} completed before the base instant {@code baseInstant}.
   */
  public boolean isCompletedBefore(String baseInstant, String instantTime);

  /**
   * Returns whether the given instant time {@code instantTime} is sliced after or on the base instant {@code baseInstant}.
   */
  public boolean isSlicedAfterOrOn(String baseInstant, String instantTime);

  /**
   * Get completion time with a base instant time as a reference to fix the compatibility.
   *
   * @param baseInstant The base instant
   * @param instantTime The instant time to query the completion time with
   *
   * @return Probability fixed completion time.
   */
  public Option<String> getCompletionTime(String baseInstant, String instantTime);

  /**
   * Queries the instant completion time with given start time.
   *
   * @param beginTime The start time.
   *
   * @return The completion time if the instant finished or empty if it is still pending.
   */
  public Option<String> getCompletionTime(String beginTime);

  /**
   * Queries the instant start time with given completion time range.
   *
   * <p>By default, assumes there is at most 1 day time of duration for an instant to accelerate the queries.
   *
   * @param timeline The timeline.
   * @param rangeStart   The query range start completion time.
   * @param rangeEnd     The query range end completion time.
   * @param rangeType    The range type.
   *
   * @return The sorted instant time list.
   */
  public List<String> getStartTimes(
      HoodieTimeline timeline,
      Option<String> rangeStart,
      Option<String> rangeEnd,
      InstantRange.RangeType rangeType);

  /**
   * Queries the instant start time with given completion time range.
   *
   * @param rangeStart              The query range start completion time.
   * @param rangeEnd                The query range end completion time.
   * @param earliestInstantTimeFunc The function to generate the earliest start time boundary
   *                                with the minimum completion time.
   *
   * @return The sorted instant time list.
   */
  public List<String> getStartTimes(
      String rangeStart,
      String rangeEnd,
      Function<String, String> earliestInstantTimeFunc);

  /**
   *  Get Cursor Instant
   * @return
   */
  public String getCursorInstant();

  /**
   * Return true if the table is empty.
   * @return
   */
  public boolean isEmptyTable();
}
