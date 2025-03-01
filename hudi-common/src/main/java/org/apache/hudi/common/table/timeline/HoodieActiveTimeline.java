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

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.StoragePath;

import java.util.Set;

/**
 * Represents the Active Timeline for the Hoodie table.
 * The timeline is not automatically reloaded on any mutation operation, clients have to manually call reload() so that
 * they can chain multiple mutations to the timeline and then call reload() once.
 * <p>
 * </p>
 * This class can be serialized and de-serialized and on de-serialization the FileSystem is re-initialized.
 */
public interface HoodieActiveTimeline extends HoodieTimeline {

  /**
   * Return Valid extensions expected in active timeline.
   * @return
   */
  Set<String> getValidExtensionsInActiveTimeline();

  /**
   * Create a complete instant and save to storage with a completion time.
   * @param instant the complete instant.
   */
  void createCompleteInstant(HoodieInstant instant);

  /**
   * Create a pending instant and save to storage.
   * @param instant the pending instant.
   */
  void createNewInstant(HoodieInstant instant);

  void createRequestedCommitWithReplaceMetadata(String instantTime, String actionType);

  /**
   * Save Completed instant in active timeline.
   * @param instant Instant to be saved.
   * @param metadata metadata to write into the instant file
   */
  <T> void saveAsComplete(HoodieInstant instant, Option<T> metadata);

  /**
   * Save Completed instant in active timeline.
   * @param shouldLock Lock before writing to timeline.
   * @param instant Instant to be saved.
   * @param metadata metadata to write into the instant file
   */
  <T> void saveAsComplete(boolean shouldLock, HoodieInstant instant, Option<T> metadata);

  /**
   * Delete Compaction requested instant file from timeline.
   * @param instant Instant to be deleted.
   */
  HoodieInstant revertToInflight(HoodieInstant instant);

  /**
   * Delete inflight instant file from timeline.
   * @param instant Instant to be deleted.
   */
  void deleteInflight(HoodieInstant instant);

  /**
   * Delete pending instant file from timeline.
   * @param instant Instant to be deleted.
   */
  void deletePending(HoodieInstant instant);

  /**
   * Delete completed rollback instant file from timeline.
   * @param instant Instant to be deleted.
   */
  void deleteCompletedRollback(HoodieInstant instant);

  /**
   * Delete empty instant file from timeline.
   * @param instant Instant to be deleted.
   */
  void deleteEmptyInstantIfExists(HoodieInstant instant);

  /**
   * Delete Compaction requested instant file from timeline.
   * @param instant Instant to be deleted.
   */
  void deleteCompactionRequested(HoodieInstant instant);

  /**
   * Note: This method should only be used in the case that delete requested/inflight instant or empty clean instant,
   * and completed commit instant in an archive operation.
   */
  void deleteInstantFileIfExists(HoodieInstant instant);

  /**
   * Returns most recent instant having valid schema in its {@link HoodieCommitMetadata}
   */
  Option<Pair<HoodieInstant, HoodieCommitMetadata>> getLastCommitMetadataWithValidSchema();

  /**
   * Get the last instant with valid data, and convert this to HoodieCommitMetadata
   */
  Option<Pair<HoodieInstant, HoodieCommitMetadata>> getLastCommitMetadataWithValidData();

  /**
   * Read cleaner Info from instant file.
   * @param instant Instant to read from.
   * @return
   */
  Option<byte[]> readCleanerInfoAsBytes(HoodieInstant instant);

  //-----------------------------------------------------------------
  //      BEGIN - COMPACTION RELATED META-DATA MANAGEMENT.
  //-----------------------------------------------------------------

  /**
   * Read compaction Plan from instant file.
   * @param instant Instant to read from.
   * @return
   */
  Option<byte[]> readCompactionPlanAsBytes(HoodieInstant instant);

  /**
   * Revert instant state from inflight to requested.
   *
   * @param inflightInstant Inflight Instant
   * @return requested instant
   */
  HoodieInstant revertInstantFromInflightToRequested(HoodieInstant inflightInstant);

  /**
   * TODO: This method is not needed, since log compaction plan is not a immutable plan.
   * Revert logcompaction State from inflight to requested.
   *
   * @param inflightInstant Inflight Instant
   * @return requested instant
   */
  HoodieInstant revertLogCompactionInflightToRequested(HoodieInstant inflightInstant);

  /**
   * Transition Compaction State from requested to inflight.
   *
   * @param requestedInstant Requested instant
   * @return inflight instant
   */
  HoodieInstant transitionCompactionRequestedToInflight(HoodieInstant requestedInstant);

  /**
   * Transition LogCompaction State from requested to inflight.
   *
   * @param requestedInstant Requested instant
   * @return inflight instant
   */
  HoodieInstant transitionLogCompactionRequestedToInflight(HoodieInstant requestedInstant);

  /**
   * Transition Compaction State from inflight to Committed.
   *
   * @param shouldLock Whether to hold the lock when performing transition
   * @param inflightInstant Inflight instant
   * @param metadata metadata to write into the instant file
   * @return commit instant
   */
  <T> HoodieInstant transitionCompactionInflightToComplete(boolean shouldLock, HoodieInstant inflightInstant, Option<T> metadata);

  /**
   * Transition Log Compaction State from inflight to Committed.
   *
   * @param shouldLock Whether to hold the lock when performing transition
   * @param inflightInstant Inflight instant
   * @param metadata metadata to write into the instant file
   * @return commit instant
   */
  <T> HoodieInstant transitionLogCompactionInflightToComplete(boolean shouldLock, HoodieInstant inflightInstant, Option<T> metadata);

  //-----------------------------------------------------------------
  //      END - COMPACTION RELATED META-DATA MANAGEMENT
  //-----------------------------------------------------------------

  /**
   * Transition Clean State from inflight to Committed.
   *
   * @param shouldLock Whether to hold the lock when performing transition
   * @param inflightInstant Inflight instant
   * @param metadata metadata to write into the instant file
   * @return commit instant
   */
  <T> HoodieInstant transitionCleanInflightToComplete(boolean shouldLock, HoodieInstant inflightInstant, Option<T> metadata);

  /**
   * Transition Clean State from requested to inflight.
   *
   * @param requestedInstant requested instant
   * @param metadata metadata to write into the instant file
   * @return commit instant
   */
  <T> HoodieInstant transitionCleanRequestedToInflight(HoodieInstant requestedInstant, Option<T> metadata);

  /**
   * Transition Rollback State from inflight to Committed.
   *
   * @param shouldLock Whether to hold the lock when performing transition
   * @param inflightInstant Inflight instant
   * @param metadata metadata to write into the instant file
   * @return commit instant
   */
  <T> HoodieInstant transitionRollbackInflightToComplete(boolean shouldLock, HoodieInstant inflightInstant, Option<T> metadata);

  /**
   * Transition Rollback State from requested to inflight.
   *
   * @param requestedInstant requested instant
   * @return commit instant
   */
  <T> HoodieInstant transitionRollbackRequestedToInflight(HoodieInstant requestedInstant);

  /**
   * Transition Restore State from requested to inflight.
   *
   * @param requestedInstant requested instant
   * @return commit instant
   */
  HoodieInstant transitionRestoreRequestedToInflight(HoodieInstant requestedInstant);

  /**
   * Transition replace requested file to replace inflight.
   *
   * @param requestedInstant Requested instant
   * @param metadata metadata to write into the instant file
   * @return inflight instant
   */
  <T> HoodieInstant transitionReplaceRequestedToInflight(HoodieInstant requestedInstant, Option<T> metadata);

  /**
   * Transition cluster requested file to cluster inflight.
   *
   * @param requestedInstant Requested instant
   * @param metadata metadata to write into the instant file
   * @return inflight instant
   */
  <T> HoodieInstant transitionClusterRequestedToInflight(HoodieInstant requestedInstant, Option<T> metadata);

  /**
   * Transition replace inflight to Committed.
   *
   * @param shouldLock Whether to hold the lock when performing transition
   * @param inflightInstant Inflight instant
   * @param metadata metadata to write into the instant file
   * @return commit instant
   */
  <T> HoodieInstant transitionReplaceInflightToComplete(boolean shouldLock, HoodieInstant inflightInstant, Option<T> metadata);

  /**
   * Transition cluster inflight to replace committed.
   *
   * @param shouldLock Whether to hold the lock when performing transition
   * @param inflightInstant Inflight instant
   * @param metadata metadata to write into the instant file
   * @return commit instant
   */
  <T> HoodieInstant transitionClusterInflightToComplete(boolean shouldLock, HoodieInstant inflightInstant, Option<T> metadata);

  /**
   * Save Restore requested instant with metadata.
   * @param commitType Instant type.
   * @param inFlightInstant Instant timestamp.
   */
  void transitionRequestedToInflight(String commitType, String inFlightInstant);

  /**
   * Save Restore requested instant with metadata.
   * @param requested Instant to save.
   * @param metadata metadata to write into the instant file
   */
  <T> void transitionRequestedToInflight(HoodieInstant requested, Option<T> metadata);

  /**
   * Save Restore requested instant with metadata.
   * @param requested Instant to save.
   * @param metadata metadata to write into the instant file
   */
  <T> void transitionRequestedToInflight(HoodieInstant requested, Option<T> metadata, boolean allowRedundantTransitions);

  /**
   * Save Compaction requested instant with metadata.
   * @param instant Instant to save.
   * @param metadata metadata to write into the instant file
   */
  <T> void saveToCompactionRequested(HoodieInstant instant, Option<T> metadata);

  /**
   * Save Compaction requested instant with metadata.
   * @param instant Instant to save.
   * @param metadata metadata to write into the instant file
   * @param overwrite Overwrite existing instant file.
   */
  <T> void saveToCompactionRequested(HoodieInstant instant, Option<T> metadata, boolean overwrite);

  /**
   * Save Log Compaction requested instant with metadata.
   * @param instant Instant to save.
   * @param metadata metadata to write into the instant file
   */
  <T> void saveToLogCompactionRequested(HoodieInstant instant, Option<T> metadata);

  /**
   * Save Log Compaction requested instant with metadata.
   * @param instant Instant to save.
   * @param metadata metadata to write into the instant file
   * @param overwrite Overwrite existing instant file.
   */
  <T> void saveToLogCompactionRequested(HoodieInstant instant, Option<T> metadata, boolean overwrite);

  /**
   * Save pending replace instant with metadata.
   * @param instant Instant to save.
   * @param metadata metadata to write into the instant file
   */
  <T> void saveToPendingReplaceCommit(HoodieInstant instant, Option<T> metadata);

  /**
   * Save pending cluster instant with metadata.
   * @param instant Instant to save.
   * @param metadata metadata to write into the instant file
   */
  <T> void saveToPendingClusterCommit(HoodieInstant instant, Option<T> metadata);

  /**
   * Save clean requested instant with metadata.
   * @param instant Instant to save.
   * @param metadata metadata to write into the instant file
   */
  <T> void saveToCleanRequested(HoodieInstant instant, Option<T> metadata);

  /**
   * Save rollback requested instant with metadata.
   * @param instant Instant to save.
   * @param metadata metadata to write into the instant file
   */
  <T> void saveToRollbackRequested(HoodieInstant instant, Option<T> metadata);

  /**
   * Save Restore requested instant with metadata.
   * @param instant Instant to save.
   * @param metadata metadata to write into the instant file
   */
  <T> void saveToRestoreRequested(HoodieInstant instant, Option<T> metadata);

  /**
   * Transition index instant state from requested to inflight.
   *
   * @param requestedInstant Inflight Instant
   * @return inflight instant
   */
  <T> HoodieInstant transitionIndexRequestedToInflight(HoodieInstant requestedInstant, Option<T> metadata);

  /**
   * Transition index instant state from inflight to completed.
   *
   * @param shouldLock Whether to hold the lock when performing transition
   * @param inflightInstant Inflight Instant
   * @return completed instant
   */
  <T> HoodieInstant transitionIndexInflightToComplete(boolean shouldLock,
                                                  HoodieInstant inflightInstant, Option<T> metadata);

  /**
   * Revert index instant state from inflight to requested.
   * @param inflightInstant Inflight Instant
   * @return requested instant
   */
  <T> HoodieInstant revertIndexInflightToRequested(HoodieInstant inflightInstant);

  /**
   * Save content for inflight/requested index instant.
   */
  <T> void saveToPendingIndexAction(HoodieInstant instant, Option<T> metadata);

  /**
   * Reloads timeline from storage
   * @return
   */
  HoodieActiveTimeline reload();

  /**
   * Copies instant file from active timeline to destination directory.
   * @param instant Instant to copy.
   * @param dstDir Destination location.
   */
  void copyInstant(HoodieInstant instant, StoragePath dstDir);

  /**
   * Valid Extensions in active timeline.
   * @return
   */
  Set<String> getValidExtensions();
}
