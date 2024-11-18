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

package org.apache.hudi.common.table.timeline.versioning.v2;

import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.storage.StoragePathInfo;

import java.util.Date;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InstantGeneratorV2 implements InstantGenerator {
  // Instant like 20230104152218702.commit.request, 20230104152218702.inflight and 20230104152218702_20230104152630238.commit
  private static final Pattern NAME_FORMAT =
      Pattern.compile("^(\\d+(_\\d+)?)(\\.\\w+)(\\.\\D+)?$");

  private static final String DELIMITER = ".";

  @Override
  public HoodieInstant createNewInstant(HoodieInstant.State state, String action, String timestamp) {
    return new HoodieInstant(state, action, timestamp, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
  }

  @Override
  public HoodieInstant createNewInstant(HoodieInstant.State state, String action, String timestamp, String completionTime) {
    return new HoodieInstant(state, action, timestamp, completionTime, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
  }

  @Override
  public HoodieInstant createNewInstant(HoodieInstant.State state, String action, String timestamp, String completionTime, boolean isLegacy) {
    return new HoodieInstant(state, action, timestamp, completionTime, isLegacy, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
  }

  @Override
  public HoodieInstant createNewInstant(StoragePathInfo pathInfo) {
    // First read the instant timestamp. [==>20170101193025<==].commit
    String fileName = pathInfo.getPath().getName();
    String timestamp = null;
    String completionTime = null;
    String action = null;
    HoodieInstant.State state = HoodieInstant.State.NIL;
    boolean isLegacy = false;
    Matcher matcher = NAME_FORMAT.matcher(fileName);
    if (matcher.find()) {
      String[] timestamps = matcher.group(1).split(HoodieInstant.UNDERSCORE);
      timestamp = timestamps[0];
      if (matcher.group(3).equals(HoodieTimeline.INFLIGHT_EXTENSION)) {
        // This is to support backwards compatibility on how in-flight commit files were written
        // General rule is inflight extension is .<action>.inflight, but for commit it is .inflight
        action = HoodieTimeline.COMMIT_ACTION;
        state = HoodieInstant.State.INFLIGHT;
      } else {
        action = matcher.group(3).replaceFirst(DELIMITER, StringUtils.EMPTY_STRING);
        if (matcher.groupCount() == 4 && matcher.group(4) != null) {
          state = HoodieInstant.State.valueOf(matcher.group(4).replaceFirst(DELIMITER, StringUtils.EMPTY_STRING).toUpperCase());
        } else {
          // Like 20230104152218702.commit
          state = HoodieInstant.State.COMPLETED;
        }
      }
      if (state == HoodieInstant.State.COMPLETED) {
        if (timestamps.length > 1) {
          completionTime = timestamps[1];
        } else {
          // for backward compatibility with 0.x release.
          completionTime = HoodieInstantTimeGenerator.formatDate(new Date(pathInfo.getModificationTime()));
          isLegacy = true;
        }
      } else {
        completionTime = null;
      }
    } else {
      throw new IllegalArgumentException("Failed to construct HoodieInstant: "
          + String.format(HoodieInstant.FILE_NAME_FORMAT_ERROR, fileName));
    }
    return new HoodieInstant(state, action, timestamp, completionTime, isLegacy, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
  }

  public String extractTimestamp(String fileName) throws IllegalArgumentException {
    Matcher matcher = NAME_FORMAT.matcher(fileName);
    if (matcher.find()) {
      String timestamp = matcher.group(1);
      return timestamp.contains(HoodieInstant.UNDERSCORE) ? timestamp.split(HoodieInstant.UNDERSCORE)[0] : timestamp;
    }

    throw new IllegalArgumentException("Failed to retrieve timestamp from name: "
        + String.format(HoodieInstant.FILE_NAME_FORMAT_ERROR, fileName));
  }

  public String getTimelineFileExtension(String fileName) {
    Objects.requireNonNull(fileName);

    Matcher matcher = NAME_FORMAT.matcher(fileName);
    if (matcher.find()) {
      return fileName.substring(matcher.group(1).length());
    }
    return HoodieInstant.EMPTY_FILE_EXTENSION;
  }

  @Override
  public HoodieInstant getRequestedInstant(final HoodieInstant instant) {
    return new HoodieInstant(HoodieInstant.State.REQUESTED, instant.getAction(), instant.requestedTime(), InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
  }

  @Override
  public HoodieInstant getCleanRequestedInstant(final String timestamp) {
    return new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLEAN_ACTION, timestamp, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
  }

  @Override
  public HoodieInstant getCleanInflightInstant(final String timestamp) {
    return new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.CLEAN_ACTION, timestamp, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
  }

  @Override
  public HoodieInstant getCompactionRequestedInstant(final String timestamp) {
    return new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, timestamp, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
  }

  @Override
  public HoodieInstant getCompactionInflightInstant(final String timestamp) {
    return new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, timestamp, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
  }

  // Returns Log compaction requested instant
  @Override
  public HoodieInstant getLogCompactionRequestedInstant(final String timestamp) {
    return new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.LOG_COMPACTION_ACTION, timestamp, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
  }

  // Returns Log compaction inflight instant
  @Override
  public HoodieInstant getLogCompactionInflightInstant(final String timestamp) {
    return new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.LOG_COMPACTION_ACTION, timestamp, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
  }

  @Override
  public HoodieInstant getReplaceCommitRequestedInstant(final String timestamp) {
    return new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, timestamp, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
  }

  @Override
  public HoodieInstant getReplaceCommitInflightInstant(final String timestamp) {
    return new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.REPLACE_COMMIT_ACTION, timestamp, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
  }

  @Override
  public HoodieInstant getClusteringCommitRequestedInstant(final String timestamp) {
    return new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLUSTERING_ACTION, timestamp, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
  }

  @Override
  public HoodieInstant getClusteringCommitInflightInstant(final String timestamp) {
    return new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.CLUSTERING_ACTION, timestamp, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
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
    return new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.INDEXING_ACTION, timestamp, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
  }

  @Override
  public HoodieInstant getIndexInflightInstant(final String timestamp) {
    return new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.INDEXING_ACTION, timestamp, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
  }
}
