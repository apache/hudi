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

import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.storage.StoragePathInfo;

import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InstantGeneratorV1 implements InstantGenerator {

  private static final Pattern NAME_FORMAT =
      Pattern.compile("^(\\d+)(\\.\\w+)(\\.\\D+)?$");

  private static final String DELIMITER = ".";

  @Override
  public HoodieInstant createNewInstant(HoodieInstant.State state, String action, String timestamp) {
    return new HoodieInstant(state, action, timestamp, InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
  }

  @Override
  public HoodieInstant createNewInstant(HoodieInstant.State state, String action, String timestamp, String completionTime) {
    return new HoodieInstant(state, action, timestamp, completionTime, InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
  }

  @Override
  public HoodieInstant createNewInstant(HoodieInstant.State state, String action, String timestamp, String completionTime, boolean isLegacy) {
    return new HoodieInstant(state, action, timestamp, completionTime, isLegacy, InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
  }

  @Override
  public HoodieInstant createNewInstant(StoragePathInfo pathInfo) {
    // First read the instant timestamp. [==>20170101193025<==].commit
    String fileName = pathInfo.getPath().getName();
    String timestamp = null;
    String stateTransitionTime = null;
    String action = null;
    HoodieInstant.State state = HoodieInstant.State.NIL;
    Matcher matcher = NAME_FORMAT.matcher(fileName);
    if (matcher.find()) {
      timestamp = matcher.group(1);
      if (matcher.group(2).equals(org.apache.hudi.common.table.timeline.HoodieTimeline.INFLIGHT_EXTENSION)) {
        // This is to support backwards compatibility on how in-flight commit files were written
        // General rule is inflight extension is .<action>.inflight, but for commit it is .inflight
        action = HoodieTimeline.COMMIT_ACTION;
        state = HoodieInstant.State.INFLIGHT;
      } else {
        action = matcher.group(2).replaceFirst(DELIMITER, StringUtils.EMPTY_STRING);
        if (matcher.groupCount() == 3 && matcher.group(3) != null) {
          state = HoodieInstant.State.valueOf(matcher.group(3).replaceFirst(DELIMITER, StringUtils.EMPTY_STRING).toUpperCase());
        } else {
          // Like 20230104152218702.commit
          state = HoodieInstant.State.COMPLETED;
        }
      }
      stateTransitionTime =
          HoodieInstantTimeGenerator.formatDate(new Date(pathInfo.getModificationTime()));
    } else {
      throw new IllegalArgumentException("Failed to construct HoodieInstant: " + String.format(HoodieInstant.FILE_NAME_FORMAT_ERROR, fileName));
    }
    return new HoodieInstant(state, action, timestamp, stateTransitionTime, true, InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
  }

  @Override
  public HoodieInstant getRequestedInstant(final HoodieInstant instant) {
    return new HoodieInstant(HoodieInstant.State.REQUESTED, instant.getAction(), instant.requestedTime(), InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
  }

  @Override
  public HoodieInstant getCleanRequestedInstant(final String timestamp) {
    return new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLEAN_ACTION, timestamp, InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
  }

  @Override
  public HoodieInstant getCleanInflightInstant(final String timestamp) {
    return new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.CLEAN_ACTION, timestamp, InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
  }

  @Override
  public HoodieInstant getCompactionRequestedInstant(final String timestamp) {
    return new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, timestamp, InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
  }

  @Override
  public HoodieInstant getCompactionInflightInstant(final String timestamp) {
    return new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, timestamp, InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
  }

  @Override
  public HoodieInstant getLogCompactionRequestedInstant(final String timestamp) {
    return new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.LOG_COMPACTION_ACTION, timestamp, InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
  }

  @Override
  public HoodieInstant getLogCompactionInflightInstant(final String timestamp) {
    return new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.LOG_COMPACTION_ACTION, timestamp, InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
  }

  @Override
  public HoodieInstant getReplaceCommitRequestedInstant(final String timestamp) {
    return new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, timestamp, InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
  }

  @Override
  public HoodieInstant getReplaceCommitInflightInstant(final String timestamp) {
    return new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.REPLACE_COMMIT_ACTION, timestamp, InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
  }

  @Override
  public HoodieInstant getRollbackRequestedInstant(HoodieInstant instant) {
    return instant.isRequested() ? instant : getRequestedInstant(instant);
  }

  @Override
  public HoodieInstant getRestoreRequestedInstant(HoodieInstant instant) {
    return instant.isRequested() ? instant : getRequestedInstant(instant);
  }

  @Override
  public HoodieInstant getIndexRequestedInstant(final String timestamp) {
    return new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.INDEXING_ACTION, timestamp, InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
  }

  @Override
  public HoodieInstant getIndexInflightInstant(final String timestamp) {
    return new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.INDEXING_ACTION, timestamp, InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
  }

  /**
   * Special Handling of 0.x mode in 1.x goes below
   */

  @Override
  public HoodieInstant getClusteringCommitRequestedInstant(String timestamp) {
    // 1n 0.x, clustering and replace commit had the same filename
    return getReplaceCommitRequestedInstant(timestamp);
  }

  @Override
  public HoodieInstant getClusteringCommitInflightInstant(String timestamp) {
    // 1n 0.x, clustering and replace commit had the same filename
    return getReplaceCommitInflightInstant(timestamp);
  }
}
