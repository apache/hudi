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

/**
 * Holds the layout-independent instant file naming shared by all timeline layout versions.
 * Layout-specific members are left abstract for the concrete per-version generators.
 */
public abstract class BaseInstantFileNameGenerator implements InstantFileNameGenerator {

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
}
