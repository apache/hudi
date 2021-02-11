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

import static org.apache.hudi.common.table.timeline.HoodieActiveTimeline.COMMIT_TIME_PATTERN;
import static org.apache.hudi.common.table.timeline.HoodieInstant.DEFAULT_INIT_STATE;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hudi.common.table.timeline.HoodieInstant.InstantTime;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.versioning.HoodieInstantVersion;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;

/**
 * TODO : Reduce code duplication in this class.
 */
public abstract class HoodieInstantFormat {

  private static final Map<TimelineLayoutVersion, HoodieInstantFormat> FORMAT_MAP = new HashMap<>();

  static {
    FORMAT_MAP.put(new TimelineLayoutVersion(TimelineLayoutVersion.VERSION_1), new HoodieInstantFormatV0());
    FORMAT_MAP.put(new TimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION), new HoodieInstantFormatV1());
  }

  public static HoodieInstantFormat getInstantFormat(TimelineLayoutVersion version) {
    return FORMAT_MAP.get(version);
  }

  public abstract String getAction(FileStatus fileStatus);

  public abstract State getState(FileStatus fileStatus);

  public abstract String getActionStartTime(FileStatus fileStatus);

  public abstract String getStateTransitionTime(FileStatus fileStatus);

  public abstract String getFileName(HoodieInstant instant);

  public abstract String getTimelineFileExtension(String fileName);

  public static class HoodieInstantFormatV0 extends HoodieInstantFormat {

    private HoodieInstantVersion version = new HoodieInstantVersion(HoodieInstantVersion.VERSION_0);

    @Override
    public String getAction(FileStatus fileStatus) {
      // First read the instant startTimestamp. [==>20170101193025<==].commit
      String fileName = fileStatus.getPath().getName();
      String fileExtension = getTimelineFileExtension(fileName);
      String actionStartTimestamp = fileName.replace(fileExtension, "");
      // Next read the action for this marker
      String action = fileExtension.replaceFirst(".", "");
      String stateTransitionTime = getTransitionTime(fileStatus, actionStartTimestamp);
      State state = DEFAULT_INIT_STATE;
      if (action.equals("inflight")) {
        // This is to support backwards compatibility on how in-flight commit files were written
        // General rule is inflight extension is .<action>.inflight, but for commit it is .inflight
        action = "commit";
        state = State.INFLIGHT;
      } else if (action.contains(HoodieTimeline.INFLIGHT_EXTENSION)) {
        state = State.INFLIGHT;
        action = action.replace(HoodieTimeline.INFLIGHT_EXTENSION, "");
      } else if (action.contains(HoodieTimeline.REQUESTED_EXTENSION)) {
        state = State.REQUESTED;
        action = action.replace(HoodieTimeline.REQUESTED_EXTENSION, "");
      }
      return action;
    }

    @Override
    public State getState(FileStatus fileStatus) {
      // First read the instant startTimestamp. [==>20170101193025<==].commit
      String fileName = fileStatus.getPath().getName();
      String fileExtension = getTimelineFileExtension(fileName);
      String actionStartTimestamp = fileName.replace(fileExtension, "");
      // Next read the action for this marker
      String action = fileExtension.replaceFirst(".", "");
      String stateTransitionTime = getTransitionTime(fileStatus, actionStartTimestamp);
      State state = DEFAULT_INIT_STATE;
      if (action.equals("inflight")) {
        // This is to support backwards compatibility on how in-flight commit files were written
        // General rule is inflight extension is .<action>.inflight, but for commit it is .inflight
        action = "commit";
        state = State.INFLIGHT;
      } else if (action.contains(HoodieTimeline.INFLIGHT_EXTENSION)) {
        state = State.INFLIGHT;
        action = action.replace(HoodieTimeline.INFLIGHT_EXTENSION, "");
      } else if (action.contains(HoodieTimeline.REQUESTED_EXTENSION)) {
        state = State.REQUESTED;
        action = action.replace(HoodieTimeline.REQUESTED_EXTENSION, "");
      }
      return state;
    }

    @Override
    public String getActionStartTime(FileStatus fileStatus) {
      // First read the instant startTimestamp. [==>20170101193025<==].commit
      String fileName = fileStatus.getPath().getName();
      String fileExtension = getTimelineFileExtension(fileName);
      String actionStartTimestamp = fileName.replace(fileExtension, "");
      // Next read the action for this marker
      String action = fileExtension.replaceFirst(".", "");
      String stateTransitionTime = getTransitionTime(fileStatus, actionStartTimestamp);
      State state = DEFAULT_INIT_STATE;
      if (action.equals("inflight")) {
        // This is to support backwards compatibility on how in-flight commit files were written
        // General rule is inflight extension is .<action>.inflight, but for commit it is .inflight
        action = "commit";
        state = State.INFLIGHT;
      } else if (action.contains(HoodieTimeline.INFLIGHT_EXTENSION)) {
        state = State.INFLIGHT;
        action = action.replace(HoodieTimeline.INFLIGHT_EXTENSION, "");
      } else if (action.contains(HoodieTimeline.REQUESTED_EXTENSION)) {
        state = State.REQUESTED;
        action = action.replace(HoodieTimeline.REQUESTED_EXTENSION, "");
      }
      return actionStartTimestamp;
    }

    @Override
    public String getStateTransitionTime(FileStatus fileStatus) {
      // First read the instant startTimestamp. [==>20170101193025<==].commit
      String fileName = fileStatus.getPath().getName();
      String fileExtension = getTimelineFileExtension(fileName);
      String actionStartTimestamp = fileName.replace(fileExtension, "");
      // Next read the action for this marker
      String action = fileExtension.replaceFirst(".", "");
      String stateTransitionTime = getTransitionTime(fileStatus, actionStartTimestamp);
      State state = DEFAULT_INIT_STATE;
      if (action.equals("inflight")) {
        // This is to support backwards compatibility on how in-flight commit files were written
        // General rule is inflight extension is .<action>.inflight, but for commit it is .inflight
        action = "commit";
        state = State.INFLIGHT;
      } else if (action.contains(HoodieTimeline.INFLIGHT_EXTENSION)) {
        state = State.INFLIGHT;
        action = action.replace(HoodieTimeline.INFLIGHT_EXTENSION, "");
      } else if (action.contains(HoodieTimeline.REQUESTED_EXTENSION)) {
        state = State.REQUESTED;
        action = action.replace(HoodieTimeline.REQUESTED_EXTENSION, "");
      }
      return stateTransitionTime;
    }

    @Override
    public String getFileName(HoodieInstant instant) {
      String action = instant.getAction();
      InstantTime instantTime = instant.getInstantTime();

      if (HoodieTimeline.COMMIT_ACTION.equals(action)) {
        return isInflight(instant) ? HoodieTimeline.makeInflightCommitFileName(instantTime)
          : isRequested(instant) ? HoodieTimeline.makeRequestedCommitFileName(instantTime)
          : HoodieTimeline.makeCommitFileName(instantTime);
      } else if (HoodieTimeline.CLEAN_ACTION.equals(action)) {
        return isInflight(instant) ? HoodieTimeline.makeInflightCleanerFileName(instantTime)
          : isRequested(instant) ? HoodieTimeline.makeRequestedCleanerFileName(instantTime)
          : HoodieTimeline.makeCleanerFileName(instantTime);
      } else if (HoodieTimeline.ROLLBACK_ACTION.equals(action)) {
        return isInflight(instant) ? HoodieTimeline.makeInflightRollbackFileName(instantTime)
          : isRequested(instant) ? HoodieTimeline.makeRequestedRollbackFileName(instantTime)
          : HoodieTimeline.makeRollbackFileName(instantTime);
      } else if (HoodieTimeline.SAVEPOINT_ACTION.equals(action)) {
        return isInflight(instant) ? HoodieTimeline.makeInflightSavePointFileName(instantTime)
          : HoodieTimeline.makeSavePointFileName(instantTime);
      } else if (HoodieTimeline.DELTA_COMMIT_ACTION.equals(action)) {
        return isInflight(instant) ? HoodieTimeline.makeInflightDeltaFileName(instantTime)
          : isRequested(instant) ? HoodieTimeline.makeRequestedDeltaFileName(instantTime)
          : HoodieTimeline.makeDeltaFileName(instantTime);
      } else if (HoodieTimeline.COMPACTION_ACTION.equals(action)) {
        if (isInflight(instant)) {
          return HoodieTimeline.makeInflightCompactionFileName(instantTime);
        } else if (isRequested(instant)) {
          return HoodieTimeline.makeRequestedCompactionFileName(instantTime);
        } else {
          return HoodieTimeline.makeCommitFileName(instantTime);
        }
      } else if (HoodieTimeline.LOG_COMPACTION_ACTION.equals(action)) {
        if (isInflight(instant)) {
          return HoodieTimeline.makeInflightLogCompactionFileName(instantTime);
        } else if (isRequested(instant)) {
          return HoodieTimeline.makeRequestedLogCompactionFileName(instantTime);
        } else {
          return HoodieTimeline.makeDeltaFileName(instantTime);
        }
      } else if (HoodieTimeline.RESTORE_ACTION.equals(action)) {
        return isInflight(instant) ? HoodieTimeline.makeInflightRestoreFileName(instantTime)
          : isRequested(instant) ? HoodieTimeline.makeRequestedRestoreFileName(instantTime)
          : HoodieTimeline.makeRestoreFileName(instantTime);
      } else if (HoodieTimeline.REPLACE_COMMIT_ACTION.equals(action)) {
        return isInflight(instant) ? HoodieTimeline.makeInflightReplaceFileName(instantTime)
          : isRequested(instant) ? HoodieTimeline.makeRequestedReplaceFileName(instantTime)
          : HoodieTimeline.makeReplaceFileName(instantTime);
      } else if (HoodieTimeline.INDEXING_ACTION.equals(action)) {
        return isInflight(instant) ? HoodieTimeline.makeInflightIndexFileName(instantTime)
          : isRequested(instant) ? HoodieTimeline.makeRequestedIndexFileName(instantTime)
          : HoodieTimeline.makeIndexCommitFileName(instantTime);
      } else if (HoodieTimeline.SCHEMA_COMMIT_ACTION.equals(action)) {
        return isInflight(instant) ? HoodieTimeline.makeInflightSchemaFileName(instantTime)
          : isRequested(instant) ? HoodieTimeline.makeRequestSchemaFileName(instantTime)
          : HoodieTimeline.makeSchemaFileName(instantTime);
      }
      throw new IllegalArgumentException("Cannot get file name for unknown action " + action);
    }

    @Override
    public String getTimelineFileExtension(String fileName) {
      Objects.requireNonNull(fileName);
      int dotIndex = fileName.indexOf('.');
      return dotIndex == -1 ? "" : fileName.substring(dotIndex);
    }

    public String getActionStartTime(String fileExtensionPrefix) {
      Objects.requireNonNull(fileExtensionPrefix);
      int dotIndex = fileExtensionPrefix.indexOf('_');
      return dotIndex == -1 ? fileExtensionPrefix : fileExtensionPrefix.substring(0, dotIndex);
    }

    public String getTransitionTime(FileStatus fileStatus, String startTimestamp) {
      Objects.requireNonNull(fileStatus);
      String endTimestamp = String.valueOf(fileStatus.getModificationTime());
      return HoodieTimeline.LESSER_THAN_OR_EQUALS.test(endTimestamp, startTimestamp)
          || !COMMIT_TIME_PATTERN.matcher(endTimestamp).matches() ? startTimestamp : endTimestamp;
    }

    public String getTransitionTime(String fileName, String startTimestamp) {
      Objects.requireNonNull(fileName);
      Objects.requireNonNull(startTimestamp);
      int lastDotIndex = fileName.lastIndexOf(".");
      String transitionTime = lastDotIndex == -1 ? "" : fileName.substring(lastDotIndex);
      return COMMIT_TIME_PATTERN.matcher(transitionTime).matches() ? transitionTime : startTimestamp;
    }

    private Boolean isRequested(HoodieInstant instant) {
      return instant.getState() == State.REQUESTED;
    }

    private Boolean isInflight(HoodieInstant instant) {
      return instant.getState() == State.INFLIGHT;
    }
  }

  public static class HoodieInstantFormatV1 extends HoodieInstantFormat {

    private HoodieInstantVersion version = new HoodieInstantVersion(HoodieInstantVersion.VERSION_1);

    @Override
    public String getAction(FileStatus fileStatus) {
      // First read the instant startTimestamp. [==>20170101193025<==].commit
      String fileName = fileStatus.getPath().getName();
      String fileExtension = getTimelineFileExtension(fileName);
      String actionStartTimestamp = fileName.replace(fileExtension, "");
      // Next read the action for this marker
      String action = fileExtension.replaceFirst(".", "");
      String stateTransitionTime = getTransitionTime(fileStatus, actionStartTimestamp);
      State state = DEFAULT_INIT_STATE;
      if (action.equals("inflight")) {
        // This is to support backwards compatibility on how in-flight commit files were written
        // General rule is inflight extension is .<action>.inflight, but for commit it is .inflight
        action = "commit";
        state = State.INFLIGHT;
      } else if (action.contains(HoodieTimeline.INFLIGHT_EXTENSION)) {
        state = State.INFLIGHT;
        action = action.replace(HoodieTimeline.INFLIGHT_EXTENSION, "");
      } else if (action.contains(HoodieTimeline.REQUESTED_EXTENSION)) {
        state = State.REQUESTED;
        action = action.replace(HoodieTimeline.REQUESTED_EXTENSION, "");
      }
      return action;
    }

    @Override
    public State getState(FileStatus fileStatus) {
      // First read the instant startTimestamp. [==>20170101193025<==].commit
      String fileName = fileStatus.getPath().getName();
      String fileExtension = getTimelineFileExtension(fileName);
      String actionStartTimestamp = fileName.replace(fileExtension, "");
      // Next read the action for this marker
      String action = fileExtension.replaceFirst(".", "");
      String stateTransitionTime = getTransitionTime(fileStatus, actionStartTimestamp);
      State state = DEFAULT_INIT_STATE;
      if (action.equals("inflight")) {
        // This is to support backwards compatibility on how in-flight commit files were written
        // General rule is inflight extension is .<action>.inflight, but for commit it is .inflight
        action = "commit";
        state = State.INFLIGHT;
      } else if (action.contains(HoodieTimeline.INFLIGHT_EXTENSION)) {
        state = State.INFLIGHT;
        action = action.replace(HoodieTimeline.INFLIGHT_EXTENSION, "");
      } else if (action.contains(HoodieTimeline.REQUESTED_EXTENSION)) {
        state = State.REQUESTED;
        action = action.replace(HoodieTimeline.REQUESTED_EXTENSION, "");
      }
      return state;
    }

    @Override
    public String getActionStartTime(FileStatus fileStatus) {
      // First read the instant startTimestamp. [==>20170101193025<==].commit
      String fileName = fileStatus.getPath().getName();
      String fileExtension = getTimelineFileExtension(fileName);
      String actionStartTimestamp = fileName.replace(fileExtension, "");
      // Next read the action for this marker
      String action = fileExtension.replaceFirst(".", "");
      String stateTransitionTime = getTransitionTime(fileStatus, actionStartTimestamp);
      State state = DEFAULT_INIT_STATE;
      if (action.equals("inflight")) {
        // This is to support backwards compatibility on how in-flight commit files were written
        // General rule is inflight extension is .<action>.inflight, but for commit it is .inflight
        action = "commit";
        state = State.INFLIGHT;
      } else if (action.contains(HoodieTimeline.INFLIGHT_EXTENSION)) {
        state = State.INFLIGHT;
        action = action.replace(HoodieTimeline.INFLIGHT_EXTENSION, "");
      } else if (action.contains(HoodieTimeline.REQUESTED_EXTENSION)) {
        state = State.REQUESTED;
        action = action.replace(HoodieTimeline.REQUESTED_EXTENSION, "");
      }
      return actionStartTimestamp;
    }

    @Override
    public String getStateTransitionTime(FileStatus fileStatus) {
      // First read the instant startTimestamp. [==>20170101193025<==].commit
      String fileName = fileStatus.getPath().getName();
      String fileExtension = getTimelineFileExtension(fileName);
      String actionStartTimestamp = fileName.replace(fileExtension, "");
      // Next read the action for this marker
      String action = fileExtension.replaceFirst(".", "");
      String stateTransitionTime = getTransitionTime(fileStatus, actionStartTimestamp);
      State state = DEFAULT_INIT_STATE;
      if (action.equals("inflight")) {
        // This is to support backwards compatibility on how in-flight commit files were written
        // General rule is inflight extension is .<action>.inflight, but for commit it is .inflight
        action = "commit";
        state = State.INFLIGHT;
      } else if (action.contains(HoodieTimeline.INFLIGHT_EXTENSION)) {
        state = State.INFLIGHT;
        action = action.replace(HoodieTimeline.INFLIGHT_EXTENSION, "");
      } else if (action.contains(HoodieTimeline.REQUESTED_EXTENSION)) {
        state = State.REQUESTED;
        action = action.replace(HoodieTimeline.REQUESTED_EXTENSION, "");
      }
      return stateTransitionTime;
    }

    @Override
    public String getFileName(HoodieInstant instant) {

      String action = instant.getAction();
      InstantTime instantTime = instant.getInstantTime();

      if (HoodieTimeline.COMMIT_ACTION.equals(action)) {
        return isInflight(instant) ? HoodieTimeline.makeInflightCommitFileName(instantTime)
            : isRequested(instant) ? HoodieTimeline.makeRequestedCommitFileName(instantTime)
                : HoodieTimeline.makeCommitFileName(instantTime);
      } else if (HoodieTimeline.CLEAN_ACTION.equals(action)) {
        return isInflight(instant) ? HoodieTimeline.makeInflightCleanerFileName(instantTime)
            : isRequested(instant) ? HoodieTimeline.makeRequestedCleanerFileName(instantTime)
                : HoodieTimeline.makeCleanerFileName(instantTime);
      } else if (HoodieTimeline.ROLLBACK_ACTION.equals(action)) {
        return isInflight(instant) ? HoodieTimeline.makeInflightRollbackFileName(instantTime)
          : isRequested(instant) ? HoodieTimeline.makeRequestedRollbackFileName(instantTime)
          : HoodieTimeline.makeRollbackFileName(instantTime);
      } else if (HoodieTimeline.SAVEPOINT_ACTION.equals(action)) {
        return isInflight(instant) ? HoodieTimeline.makeInflightSavePointFileName(instantTime)
            : HoodieTimeline.makeSavePointFileName(instantTime);
      } else if (HoodieTimeline.DELTA_COMMIT_ACTION.equals(action)) {
        return isInflight(instant) ? HoodieTimeline.makeInflightDeltaFileName(instantTime)
            : isRequested(instant) ? HoodieTimeline.makeRequestedDeltaFileName(instantTime)
                : HoodieTimeline.makeDeltaFileName(instantTime);
      } else if (HoodieTimeline.COMPACTION_ACTION.equals(action)) {
        if (isInflight(instant)) {
          return HoodieTimeline.makeInflightCompactionFileName(instantTime);
        } else if (isRequested(instant)) {
          return HoodieTimeline.makeRequestedCompactionFileName(instantTime);
        } else {
          return HoodieTimeline.makeCommitFileName(instantTime);
        }
      } else if (HoodieTimeline.LOG_COMPACTION_ACTION.equals(action)) {
        if (isInflight(instant)) {
          return HoodieTimeline.makeInflightLogCompactionFileName(instantTime);
        } else if (isRequested(instant)) {
          return HoodieTimeline.makeRequestedLogCompactionFileName(instantTime);
        } else {
          return HoodieTimeline.makeDeltaFileName(instantTime);
        }
      } else if (HoodieTimeline.RESTORE_ACTION.equals(action)) {
        return isInflight(instant) ? HoodieTimeline.makeInflightRestoreFileName(instantTime)
          : isRequested(instant) ? HoodieTimeline.makeRequestedRestoreFileName(instantTime)
          : HoodieTimeline.makeRestoreFileName(instantTime);
      } else if (HoodieTimeline.REPLACE_COMMIT_ACTION.equals(action)) {
        return isInflight(instant) ? HoodieTimeline.makeInflightReplaceFileName(instantTime)
          : isRequested(instant) ? HoodieTimeline.makeRequestedReplaceFileName(instantTime)
          : HoodieTimeline.makeReplaceFileName(instantTime);
      } else if (HoodieTimeline.INDEXING_ACTION.equals(action)) {
        return isInflight(instant) ? HoodieTimeline.makeInflightIndexFileName(instantTime)
          : isRequested(instant) ? HoodieTimeline.makeRequestedIndexFileName(instantTime)
          : HoodieTimeline.makeIndexCommitFileName(instantTime);
      } else if (HoodieTimeline.SCHEMA_COMMIT_ACTION.equals(action)) {
        return isInflight(instant) ? HoodieTimeline.makeInflightSchemaFileName(instantTime)
          : isRequested(instant) ? HoodieTimeline.makeRequestSchemaFileName(instantTime)
          : HoodieTimeline.makeSchemaFileName(instantTime);
      }
      throw new IllegalArgumentException("Cannot get file name for unknown action " + action);
    }

    @Override
    public String getTimelineFileExtension(String fileName) {
      Objects.requireNonNull(fileName);
      int dotIndex = fileName.indexOf('.');
      return dotIndex == -1 ? "" : fileName.substring(dotIndex);
    }

    public String getActionStartTime(String fileExtensionPrefix) {
      Objects.requireNonNull(fileExtensionPrefix);
      int dotIndex = fileExtensionPrefix.indexOf('_');
      return dotIndex == -1 ? fileExtensionPrefix : fileExtensionPrefix.substring(0, dotIndex);
    }

    public String getTransitionTime(FileStatus fileStatus, String startTimestamp) {
      Objects.requireNonNull(fileStatus);
      String endTimestamp = String.valueOf(fileStatus.getModificationTime());
      return HoodieTimeline.LESSER_THAN_OR_EQUALS.test(endTimestamp, startTimestamp)
          || !COMMIT_TIME_PATTERN.matcher(endTimestamp).matches() ? startTimestamp : endTimestamp;
    }

    public String getTransitionTime(String fileName, String startTimestamp) {
      Objects.requireNonNull(fileName);
      Objects.requireNonNull(startTimestamp);
      int lastDotIndex = fileName.lastIndexOf(".");
      String transitionTime = lastDotIndex == -1 ? "" : fileName.substring(lastDotIndex);
      return COMMIT_TIME_PATTERN.matcher(transitionTime).matches() ? transitionTime : startTimestamp;
    }

    private Boolean isRequested(HoodieInstant instant) {
      return instant.getState() == State.REQUESTED;
    }

    private Boolean isInflight(HoodieInstant instant) {
      return instant.getState() == State.INFLIGHT;
    }
  }

}
