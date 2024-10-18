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
  public Set<String> getValidExtensionsInActiveTimeline();

  /**
   * Create a complete instant and save to storage with a completion time.
   * @param instant the complete instant.
   */
  public void createCompleteInstant(HoodieInstant instant);

  /**
   * Create a pending instant and save to storage.
   * @param instant the pending instant.
   */
  public void createNewInstant(HoodieInstant instant);

  public void createRequestedCommitWithReplaceMetadata(String instantTime, String actionType);

  /**
   * Save Completed instant in active timeline.
   * @param instant Instant to be saved.
   * @param data Metadata to be written in the instant file.
   */
  public void saveAsComplete(HoodieInstant instant, Option<byte[]> data);

  /**
   * Save Completed instant in active timeline.
   * @param shouldLock Lock before writing to timeline.
   * @param instant Instant to be saved.
   * @param data Metadata to be written in the instant file.
   */
  public void saveAsComplete(boolean shouldLock, HoodieInstant instant, Option<byte[]> data);

  /**
   * Delete Compaction requested instant file from timeline.
   * @param instant Instant to be deleted.
   */
  public HoodieInstant revertToInflight(HoodieInstant instant);

  /**
   * Delete inflight instant file from timeline.
   * @param instant Instant to be deleted.
   */
  public void deleteInflight(HoodieInstant instant);

  /**
   * Delete pending instant file from timeline.
   * @param instant Instant to be deleted.
   */
  public void deletePending(HoodieInstant instant);

  /**
   * Delete completed rollback instant file from timeline.
   * @param instant Instant to be deleted.
   */
  public void deleteCompletedRollback(HoodieInstant instant);

  /**
   * Delete empty instant file from timeline.
   * @param instant Instant to be deleted.
   */
  public void deleteEmptyInstantIfExists(HoodieInstant instant);

  /**
   * Delete Compaction requested instant file from timeline.
   * @param instant Instant to be deleted.
   */
  public void deleteCompactionRequested(HoodieInstant instant);

  /**
   * Note: This method should only be used in the case that delete requested/inflight instant or empty clean instant,
   * and completed commit instant in an archive operation.
   */
  public void deleteInstantFileIfExists(HoodieInstant instant);

  /**
   * Returns most recent instant having valid schema in its {@link HoodieCommitMetadata}
   */
  public Option<Pair<HoodieInstant, HoodieCommitMetadata>> getLastCommitMetadataWithValidSchema();

  /**
   * Get the last instant with valid data, and convert this to HoodieCommitMetadata
   */
  public Option<Pair<HoodieInstant, HoodieCommitMetadata>> getLastCommitMetadataWithValidData();

  /**
   * Read cleaner Info from instant file.
   * @param instant Instant to read from.
   * @return
   */
  public Option<byte[]> readCleanerInfoAsBytes(HoodieInstant instant);

  /**
   * Read rollback info from instant file.
   * @param instant Instant to read from.
   * @return
   */
  public Option<byte[]> readRollbackInfoAsBytes(HoodieInstant instant);

  /**
   * Read Restore info from instant file.
   * @param instant Instant to read from.
   * @return
   */
  public Option<byte[]> readRestoreInfoAsBytes(HoodieInstant instant);

  //-----------------------------------------------------------------
  //      BEGIN - COMPACTION RELATED META-DATA MANAGEMENT.
  //-----------------------------------------------------------------

  /**
   * Read compaction Plan from instant file.
   * @param instant Instant to read from.
   * @return
   */
  public Option<byte[]> readCompactionPlanAsBytes(HoodieInstant instant);

  /**
   * Read Index Plan from instant file.
   * @param instant Instant to read from.
   * @return
   */
  public Option<byte[]> readIndexPlanAsBytes(HoodieInstant instant);

  /**
   * Revert instant state from inflight to requested.
   *
   * @param inflightInstant Inflight Instant
   * @return requested instant
   */
  public HoodieInstant revertInstantFromInflightToRequested(HoodieInstant inflightInstant);

  /**
   * TODO: This method is not needed, since log compaction plan is not a immutable plan.
   * Revert logcompaction State from inflight to requested.
   *
   * @param inflightInstant Inflight Instant
   * @return requested instant
   */
  public HoodieInstant revertLogCompactionInflightToRequested(HoodieInstant inflightInstant);

  /**
   * Transition Compaction State from requested to inflight.
   *
   * @param requestedInstant Requested instant
   * @return inflight instant
   */
  public HoodieInstant transitionCompactionRequestedToInflight(HoodieInstant requestedInstant);

  /**
   * Transition LogCompaction State from requested to inflight.
   *
   * @param requestedInstant Requested instant
   * @return inflight instant
   */
  public HoodieInstant transitionLogCompactionRequestedToInflight(HoodieInstant requestedInstant);

  /**
   * Transition Compaction State from inflight to Committed.
   *
   * @param shouldLock Whether to hold the lock when performing transition
   * @param inflightInstant Inflight instant
   * @param data Extra Metadata
   * @return commit instant
   */
  public HoodieInstant transitionCompactionInflightToComplete(boolean shouldLock, HoodieInstant inflightInstant,
                                                              Option<byte[]> data);

  /**
   * Transition Log Compaction State from inflight to Committed.
   *
   * @param shouldLock Whether to hold the lock when performing transition
   * @param inflightInstant Inflight instant
   * @param data Extra Metadata
   * @return commit instant
   */
  public HoodieInstant transitionLogCompactionInflightToComplete(boolean shouldLock,
                                                                 HoodieInstant inflightInstant, Option<byte[]> data);

  //-----------------------------------------------------------------
  //      END - COMPACTION RELATED META-DATA MANAGEMENT
  //-----------------------------------------------------------------

  /**
   * Transition Clean State from inflight to Committed.
   *
   * @param shouldLock Whether to hold the lock when performing transition
   * @param inflightInstant Inflight instant
   * @param data Extra Metadata
   * @return commit instant
   */
  public HoodieInstant transitionCleanInflightToComplete(boolean shouldLock, HoodieInstant inflightInstant,
                                                         Option<byte[]> data);

  /**
   * Transition Clean State from requested to inflight.
   *
   * @param requestedInstant requested instant
   * @param data Optional data to be stored
   * @return commit instant
   */
  public HoodieInstant transitionCleanRequestedToInflight(HoodieInstant requestedInstant, Option<byte[]> data);

  /**
   * Transition Rollback State from inflight to Committed.
   *
   * @param shouldLock Whether to hold the lock when performing transition
   * @param inflightInstant Inflight instant
   * @param data Extra Metadata
   * @return commit instant
   */
  public HoodieInstant transitionRollbackInflightToComplete(boolean shouldLock,
                                                            HoodieInstant inflightInstant, Option<byte[]> data);

  /**
   * Transition Rollback State from requested to inflight.
   *
   * @param requestedInstant requested instant
   * @return commit instant
   */
  public HoodieInstant transitionRollbackRequestedToInflight(HoodieInstant requestedInstant);

  /**
   * Transition Restore State from requested to inflight.
   *
   * @param requestedInstant requested instant
   * @return commit instant
   */
  public HoodieInstant transitionRestoreRequestedToInflight(HoodieInstant requestedInstant);

  /**
   * Transition replace requested file to replace inflight.
   *
   * @param requestedInstant Requested instant
   * @param data Extra Metadata
   * @return inflight instant
   */
  public HoodieInstant transitionReplaceRequestedToInflight(HoodieInstant requestedInstant, Option<byte[]> data);

  /**
   * Transition cluster requested file to cluster inflight.
   *
   * @param requestedInstant Requested instant
   * @param data Extra Metadata
   * @return inflight instant
   */
  public HoodieInstant transitionClusterRequestedToInflight(HoodieInstant requestedInstant, Option<byte[]> data);

  /**
   * Transition replace inflight to Committed.
   *
   * @param shouldLock Whether to hold the lock when performing transition
   * @param inflightInstant Inflight instant
   * @param data Extra Metadata
   * @return commit instant
   */
  public HoodieInstant transitionReplaceInflightToComplete(boolean shouldLock,
                                                           HoodieInstant inflightInstant, Option<byte[]> data);

  /**
   * Transition cluster inflight to replace committed.
   *
   * @param shouldLock Whether to hold the lock when performing transition
   * @param inflightInstant Inflight instant
   * @param data Extra Metadata
   * @return commit instant
   */
  public HoodieInstant transitionClusterInflightToComplete(boolean shouldLock,
                                                           HoodieInstant inflightInstant, Option<byte[]> data);

  /**
   * Save Restore requested instant with metadata.
   * @param commitType Instant type.
   * @param inFlightInstant Instant timestamp.
   */
  public void transitionRequestedToInflight(String commitType, String inFlightInstant);

  /**
   * Save Restore requested instant with metadata.
   * @param requested Instant to save.
   * @param content Metadata to be stored in instant file.
   */
  public void transitionRequestedToInflight(HoodieInstant requested, Option<byte[]> content);

  /**
   * Save Restore requested instant with metadata.
   * @param requested Instant to save.
   * @param content Metadata to be stored in instant file.
   */
  public void transitionRequestedToInflight(HoodieInstant requested, Option<byte[]> content,
      boolean allowRedundantTransitions);

  /**
   * Save Compaction requested instant with metadata.
   * @param instant Instant to save.
   * @param content Metadata to be stored in instant file.
   */
  public void saveToCompactionRequested(HoodieInstant instant, Option<byte[]> content);

  /**
   * Save Compaction requested instant with metadata.
   * @param instant Instant to save.
   * @param content Metadata to be stored in instant file.
   * @param overwrite Overwrite existing instant file.
   */
  public void saveToCompactionRequested(HoodieInstant instant, Option<byte[]> content, boolean overwrite);

  /**
   * Save Log Compaction requested instant with metadata.
   * @param instant Instant to save.
   * @param content Metadata to be stored in instant file.
   */
  public void saveToLogCompactionRequested(HoodieInstant instant, Option<byte[]> content);

  /**
   * Save Log Compaction requested instant with metadata.
   * @param instant Instant to save.
   * @param content Metadata to be stored in instant file.
   * @param overwrite Overwrite existing instant file.
   */
  public void saveToLogCompactionRequested(HoodieInstant instant, Option<byte[]> content, boolean overwrite);

  /**
   * Save pending replace instant with metadata.
   * @param instant Instant to save.
   * @param content Metadata to be stored in instant file.
   */
  public void saveToPendingReplaceCommit(HoodieInstant instant, Option<byte[]> content);

  /**
   * Save pending cluster instant with metadata.
   * @param instant Instant to save.
   * @param content Metadata to be stored in instant file.
   */
  public void saveToPendingClusterCommit(HoodieInstant instant, Option<byte[]> content);

  /**
   * Save clean requested instant with metadata.
   * @param instant Instant to save.
   * @param content Metadata to be stored in instant file.
   */
  public void saveToCleanRequested(HoodieInstant instant, Option<byte[]> content);

  /**
   * Save rollback requested instant with metadata.
   * @param instant Instant to save.
   * @param content Metadata to be stored in instant file.
   */
  public void saveToRollbackRequested(HoodieInstant instant, Option<byte[]> content);

  /**
   * Save Restore requested instant with metadata.
   * @param instant Instant to save.
   * @param content Metadata to be stored in instant file.
   */
  public void saveToRestoreRequested(HoodieInstant instant, Option<byte[]> content);

  /**
   * Transition index instant state from requested to inflight.
   *
   * @param requestedInstant Inflight Instant
   * @return inflight instant
   */
  public HoodieInstant transitionIndexRequestedToInflight(HoodieInstant requestedInstant, Option<byte[]> data);

  /**
   * Transition index instant state from inflight to completed.
   *
   * @param shouldLock Whether to hold the lock when performing transition
   * @param inflightInstant Inflight Instant
   * @return completed instant
   */
  public HoodieInstant transitionIndexInflightToComplete(boolean shouldLock,
                                                         HoodieInstant inflightInstant, Option<byte[]> data);

  /**
   * Revert index instant state from inflight to requested.
   * @param inflightInstant Inflight Instant
   * @return requested instant
   */
  public HoodieInstant revertIndexInflightToRequested(HoodieInstant inflightInstant);

  /**
   * Save content for inflight/requested index instant.
   */
  public void saveToPendingIndexAction(HoodieInstant instant, Option<byte[]> content);

  /**
   * Reloads timeline from storage
   * @return
   */
  public HoodieActiveTimeline reload();

  /**
   * Copies instant file from active timeline to destination directory.
   * @param instant Instant to copy.
   * @param dstDir Destination location.
   */
  public void copyInstant(HoodieInstant instant, StoragePath dstDir);

  /**
   * Valid Extensions in active timeline.
   * @return
   */
  Set<String> getValidExtensions();
}
