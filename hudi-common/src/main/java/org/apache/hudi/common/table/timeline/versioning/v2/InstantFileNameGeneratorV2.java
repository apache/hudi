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

import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.InstantFileNameGenerator;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;

public class InstantFileNameGeneratorV2 implements InstantFileNameGenerator {

  @Override
  public TimelineLayoutVersion getLayoutVersion() {
    return TimelineLayoutVersion.LAYOUT_VERSION_2;
  }

  @Override
  public String makeCommitFileName(String instantTime) {
    return StringUtils.join(instantTime, HoodieTimeline.COMMIT_EXTENSION);
  }

  @Override
  public String makeInflightCommitFileName(String instantTime) {
    return StringUtils.join(instantTime, HoodieTimeline.INFLIGHT_COMMIT_EXTENSION);
  }

  @Override
  public String makeRequestedCommitFileName(String instantTime) {
    return StringUtils.join(instantTime, HoodieTimeline.REQUESTED_COMMIT_EXTENSION);
  }

  @Override
  public String makeCleanerFileName(String instant) {
    return StringUtils.join(instant, HoodieTimeline.CLEAN_EXTENSION);
  }

  @Override
  public String makeRequestedCleanerFileName(String instant) {
    return StringUtils.join(instant, HoodieTimeline.REQUESTED_CLEAN_EXTENSION);
  }

  @Override
  public String makeInflightCleanerFileName(String instant) {
    return StringUtils.join(instant, HoodieTimeline.INFLIGHT_CLEAN_EXTENSION);
  }

  @Override
  public String makeRollbackFileName(String instant) {
    return StringUtils.join(instant, HoodieTimeline.ROLLBACK_EXTENSION);
  }

  @Override
  public String makeRequestedRollbackFileName(String instant) {
    return StringUtils.join(instant, HoodieTimeline.REQUESTED_ROLLBACK_EXTENSION);
  }

  @Override
  public String makeRequestedRestoreFileName(String instant) {
    return StringUtils.join(instant, HoodieTimeline.REQUESTED_RESTORE_EXTENSION);
  }

  @Override
  public String makeInflightRollbackFileName(String instant) {
    return StringUtils.join(instant, HoodieTimeline.INFLIGHT_ROLLBACK_EXTENSION);
  }

  @Override
  public String makeInflightSavePointFileName(String instantTime) {
    return StringUtils.join(instantTime, HoodieTimeline.INFLIGHT_SAVEPOINT_EXTENSION);
  }

  @Override
  public String makeSavePointFileName(String instantTime) {
    return StringUtils.join(instantTime, HoodieTimeline.SAVEPOINT_EXTENSION);
  }

  @Override
  public String makeInflightDeltaFileName(String instantTime) {
    return StringUtils.join(instantTime, HoodieTimeline.INFLIGHT_DELTA_COMMIT_EXTENSION);
  }

  @Override
  public String makeRequestedDeltaFileName(String instantTime) {
    return StringUtils.join(instantTime, HoodieTimeline.REQUESTED_DELTA_COMMIT_EXTENSION);
  }

  @Override
  public String makeInflightCompactionFileName(String instantTime) {
    return StringUtils.join(instantTime, HoodieTimeline.INFLIGHT_COMPACTION_EXTENSION);
  }

  @Override
  public String makeRequestedCompactionFileName(String instantTime) {
    return StringUtils.join(instantTime, HoodieTimeline.REQUESTED_COMPACTION_EXTENSION);
  }

  @Override
  public String makeInflightLogCompactionFileName(String instantTime) {
    return StringUtils.join(instantTime, HoodieTimeline.INFLIGHT_LOG_COMPACTION_EXTENSION);
  }

  @Override
  public String makeRequestedLogCompactionFileName(String instantTime) {
    return StringUtils.join(instantTime, HoodieTimeline.REQUESTED_LOG_COMPACTION_EXTENSION);
  }

  @Override
  public String makeRestoreFileName(String instant) {
    return StringUtils.join(instant, HoodieTimeline.RESTORE_EXTENSION);
  }

  @Override
  public String makeInflightRestoreFileName(String instant) {
    return StringUtils.join(instant, HoodieTimeline.INFLIGHT_RESTORE_EXTENSION);
  }

  @Override
  public String makeReplaceFileName(String instant) {
    return StringUtils.join(instant, HoodieTimeline.REPLACE_COMMIT_EXTENSION);
  }

  @Override
  public String makeInflightReplaceFileName(String instant) {
    return StringUtils.join(instant, HoodieTimeline.INFLIGHT_REPLACE_COMMIT_EXTENSION);
  }

  @Override
  public String makeRequestedReplaceFileName(String instant) {
    return StringUtils.join(instant, HoodieTimeline.REQUESTED_REPLACE_COMMIT_EXTENSION);
  }

  @Override
  public String makeRequestedClusteringFileName(String instant) {
    return StringUtils.join(instant, HoodieTimeline.REQUESTED_CLUSTERING_COMMIT_EXTENSION);
  }

  @Override
  public String makeInflightClusteringFileName(String instant) {
    return StringUtils.join(instant, HoodieTimeline.INFLIGHT_CLUSTERING_COMMIT_EXTENSION);
  }

  @Override
  public String makeDeltaFileName(String instantTime) {
    return instantTime + HoodieTimeline.DELTA_COMMIT_EXTENSION;
  }

  @Override
  public String getCommitFromCommitFile(String commitFileName) {
    return commitFileName.split("\\.")[0];
  }

  @Override
  public String makeFileNameAsComplete(String fileName) {
    return fileName.replace(HoodieTimeline.INFLIGHT_EXTENSION, "");
  }

  @Override
  public String makeFileNameAsInflight(String fileName) {
    return StringUtils.join(fileName, HoodieTimeline.INFLIGHT_EXTENSION);
  }

  @Override
  public String makeIndexCommitFileName(String instant) {
    return StringUtils.join(instant, HoodieTimeline.INDEX_COMMIT_EXTENSION);
  }

  @Override
  public String makeInflightIndexFileName(String instant) {
    return StringUtils.join(instant, HoodieTimeline.INFLIGHT_INDEX_COMMIT_EXTENSION);
  }

  @Override
  public String makeRequestedIndexFileName(String instant) {
    return StringUtils.join(instant, HoodieTimeline.REQUESTED_INDEX_COMMIT_EXTENSION);
  }

  @Override
  public String makeSchemaFileName(String instantTime) {
    return StringUtils.join(instantTime, HoodieTimeline.SAVE_SCHEMA_ACTION_EXTENSION);
  }

  @Override
  public String makeInflightSchemaFileName(String instantTime) {
    return StringUtils.join(instantTime, HoodieTimeline.INFLIGHT_SAVE_SCHEMA_ACTION_EXTENSION);
  }

  @Override
  public String makeRequestSchemaFileName(String instantTime) {
    return StringUtils.join(instantTime, HoodieTimeline.REQUESTED_SAVE_SCHEMA_ACTION_EXTENSION);
  }

  private String getPendingFileName(HoodieInstant instant) {
    String action = instant.getAction();
    String timestamp = instant.requestedTime();
    if (HoodieTimeline.COMMIT_ACTION.equals(action)) {
      if (instant.isInflight()) {
        return makeInflightCommitFileName(timestamp);
      } else if (instant.isRequested()) {
        return makeRequestedCommitFileName(timestamp);
      }
    } else if (HoodieTimeline.CLEAN_ACTION.equals(action)) {
      if (instant.isInflight()) {
        return makeInflightCleanerFileName(timestamp);
      } else if (instant.isRequested()) {
        return makeRequestedCleanerFileName(timestamp);
      }
    } else if (HoodieTimeline.ROLLBACK_ACTION.equals(action)) {
      if (instant.isInflight()) {
        return makeInflightRollbackFileName(timestamp);
      } else if (instant.isRequested()) {
        return makeRequestedRollbackFileName(timestamp);
      }
    } else if (HoodieTimeline.SAVEPOINT_ACTION.equals(action)) {
      return makeInflightSavePointFileName(timestamp);
    } else if (HoodieTimeline.DELTA_COMMIT_ACTION.equals(action)) {
      if (instant.isInflight()) {
        return makeInflightDeltaFileName(timestamp);
      } else if (instant.isRequested()) {
        return makeRequestedDeltaFileName(timestamp);
      }
    } else if (HoodieTimeline.COMPACTION_ACTION.equals(action)) {
      if (instant.isInflight()) {
        return makeInflightCompactionFileName(timestamp);
      } else if (instant.isRequested()) {
        return makeRequestedCompactionFileName(timestamp);
      }
    } else if (HoodieTimeline.LOG_COMPACTION_ACTION.equals(action)) {
      if (instant.isInflight()) {
        return makeInflightLogCompactionFileName(timestamp);
      } else if (instant.isRequested()) {
        return makeRequestedLogCompactionFileName(timestamp);
      }
    } else if (HoodieTimeline.RESTORE_ACTION.equals(action)) {
      if (instant.isInflight()) {
        return makeInflightRestoreFileName(timestamp);
      } else if (instant.isRequested()) {
        return makeRequestedRestoreFileName(timestamp);
      }
    } else if (HoodieTimeline.REPLACE_COMMIT_ACTION.equals(action)) {
      if (instant.isInflight()) {
        return makeInflightReplaceFileName(timestamp);
      } else if (instant.isRequested()) {
        return makeRequestedReplaceFileName(timestamp);
      }
    } else if (HoodieTimeline.CLUSTERING_ACTION.equals(action)) {
      if (instant.isInflight()) {
        return makeInflightClusteringFileName(timestamp);
      } else if (instant.isRequested()) {
        return makeRequestedClusteringFileName(timestamp);
      }
    } else if (HoodieTimeline.INDEXING_ACTION.equals(action)) {
      if (instant.isInflight()) {
        return makeInflightIndexFileName(timestamp);
      } else if (instant.isRequested()) {
        return makeRequestedIndexFileName(timestamp);
      }
    } else if (HoodieTimeline.SCHEMA_COMMIT_ACTION.equals(action)) {
      if (instant.isInflight()) {
        return makeInflightSchemaFileName(timestamp);
      } else if (instant.isRequested()) {
        return makeRequestSchemaFileName(timestamp);
      }
    }
    throw new IllegalArgumentException("Cannot get file name for unknown action " + action);
  }

  private String getCompleteFileName(HoodieInstant instant, String completionTime) {
    ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(completionTime), "Completion time should not be empty");
    String timestampWithCompletionTime = instant.isLegacy() ? instant.requestedTime() : instant.requestedTime() + "_" + completionTime;
    switch (instant.getAction()) {
      case HoodieTimeline.COMMIT_ACTION:
      case HoodieTimeline.COMPACTION_ACTION:
        return makeCommitFileName(timestampWithCompletionTime);
      case HoodieTimeline.CLEAN_ACTION:
        return makeCleanerFileName(timestampWithCompletionTime);
      case HoodieTimeline.ROLLBACK_ACTION:
        return makeRollbackFileName(timestampWithCompletionTime);
      case HoodieTimeline.SAVEPOINT_ACTION:
        return makeSavePointFileName(timestampWithCompletionTime);
      case HoodieTimeline.DELTA_COMMIT_ACTION:
      case HoodieTimeline.LOG_COMPACTION_ACTION:
        return makeDeltaFileName(timestampWithCompletionTime);
      case HoodieTimeline.RESTORE_ACTION:
        return makeRestoreFileName(timestampWithCompletionTime);
      case HoodieTimeline.REPLACE_COMMIT_ACTION:
        return makeReplaceFileName(timestampWithCompletionTime);
      case HoodieTimeline.INDEXING_ACTION:
        return makeIndexCommitFileName(timestampWithCompletionTime);
      case HoodieTimeline.SCHEMA_COMMIT_ACTION:
        return makeSchemaFileName(timestampWithCompletionTime);
      default:
        throw new IllegalArgumentException("Cannot get complete instant's file name for unknown action "
            + instant.getAction());
    }
  }

  @Override
  public String getFileName(HoodieInstant instant) {
    if (instant.isCompleted()) {
      return getCompleteFileName(instant, instant.getCompletionTime());
    }

    return getPendingFileName(instant);
  }

  @Override
  public String getFileName(String completionTime, HoodieInstant instant) {
    ValidationUtils.checkState(instant.isCompleted());
    return getCompleteFileName(instant, completionTime);
  }
}
