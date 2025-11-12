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

import java.util.Objects;
import java.util.function.BiPredicate;

/**
 * Helper methods to compare instants.
 **/
public class InstantComparison {

  public static final BiPredicate<String, String> EQUALS = (commit1, commit2) -> commit1.compareTo(commit2) == 0;
  public static final BiPredicate<String, String> GREATER_THAN_OR_EQUALS = (commit1, commit2) -> commit1.compareTo(commit2) >= 0;
  public static final BiPredicate<String, String> GREATER_THAN = (commit1, commit2) -> commit1.compareTo(commit2) > 0;
  public static final BiPredicate<String, String> LESSER_THAN_OR_EQUALS = (commit1, commit2) -> commit1.compareTo(commit2) <= 0;
  public static final BiPredicate<String, String> LESSER_THAN = (commit1, commit2) -> commit1.compareTo(commit2) < 0;

  public static boolean compareTimestamps(String commit1, BiPredicate<String, String> predicateToApply, String commit2) {
    return predicateToApply.test(commit1, commit2);
  }

  /**
   * Returns smaller of the two given timestamps. Returns the non null argument if one of the argument is null.
   */
  public static String minTimestamp(String commit1, String commit2) {
    if (StringUtils.isNullOrEmpty(commit1)) {
      return commit2;
    } else if (StringUtils.isNullOrEmpty(commit2)) {
      return commit1;
    }
    return minInstant(commit1, commit2);
  }

  /**
   * Returns smaller of the two given instants compared by their respective timestamps.
   * Returns the non null argument if one of the argument is null.
   */
  public static HoodieInstant minTimestampInstant(HoodieInstant instant1, HoodieInstant instant2) {
    String commit1 = instant1 != null ? instant1.requestedTime() : null;
    String commit2 = instant2 != null ? instant2.requestedTime() : null;
    String minTimestamp = minTimestamp(commit1, commit2);
    return Objects.equals(minTimestamp, commit1) ? instant1 : instant2;
  }

  /**
   * Returns the smaller of the given two instants.
   */
  public static String minInstant(String instant1, String instant2) {
    return compareTimestamps(instant1, LESSER_THAN, instant2) ? instant1 : instant2;
  }

  /**
   * Returns the greater of the given two instants.
   */
  public static String maxInstant(String instant1, String instant2) {
    return compareTimestamps(instant1, GREATER_THAN, instant2) ? instant1 : instant2;
  }

  /**
   * Return true if specified timestamp is in range (startTs, endTs].
   */
  public static boolean isInRange(String timestamp, String startTs, String endTs) {
    return compareTimestamps(timestamp, GREATER_THAN, startTs)
        && compareTimestamps(timestamp, LESSER_THAN_OR_EQUALS, endTs);
  }

  /**
   * Return true if specified timestamp is in range [startTs, endTs).
   */
  public static boolean isInClosedOpenRange(String timestamp, String startTs, String endTs) {
    return compareTimestamps(timestamp, GREATER_THAN_OR_EQUALS, startTs)
        && compareTimestamps(timestamp, LESSER_THAN, endTs);
  }

  /**
   * Return true if specified timestamp is in range [startTs, endTs].
   */
  public static boolean isInClosedRange(String timestamp, String startTs, String endTs) {
    return compareTimestamps(timestamp, GREATER_THAN_OR_EQUALS, startTs)
        && compareTimestamps(timestamp, LESSER_THAN_OR_EQUALS, endTs);
  }
}
