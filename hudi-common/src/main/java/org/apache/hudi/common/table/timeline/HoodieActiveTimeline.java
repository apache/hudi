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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieIndexPlan;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieRestorePlan;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
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

  String createCompletionTime();

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

  <T> void saveAsComplete(boolean shouldLock, HoodieInstant instant, Option<T> metadata, Option<String> completionTimeOpt);

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
  HoodieInstant transitionCompactionInflightToComplete(boolean shouldLock, HoodieInstant inflightInstant, HoodieCommitMetadata metadata);

  /**
   * Transition Log Compaction State from inflight to Committed.
   *
   * @param shouldLock Whether to hold the lock when performing transition
   * @param inflightInstant Inflight instant
   * @param metadata metadata to write into the instant file
   * @return commit instant
   */
  HoodieInstant transitionLogCompactionInflightToComplete(boolean shouldLock, HoodieInstant inflightInstant, HoodieCommitMetadata metadata);

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
  HoodieInstant transitionCleanInflightToComplete(boolean shouldLock, HoodieInstant inflightInstant, Option<HoodieCleanMetadata> metadata);

  /**
   * Transition Clean State from requested to inflight.
   *
   * @param requestedInstant requested instant
   * @param metadata metadata to write into the instant file
   * @return commit instant
   */
  HoodieInstant transitionCleanRequestedToInflight(HoodieInstant requestedInstant, Option<HoodieCleanerPlan> metadata);

  /**
   * Transition Rollback State from inflight to Committed.
   *
   * @param shouldLock Whether to hold the lock when performing transition
   * @param inflightInstant Inflight instant
   * @param metadata metadata to write into the instant file
   * @return commit instant
   */
  HoodieInstant transitionRollbackInflightToComplete(boolean shouldLock, HoodieInstant inflightInstant, HoodieRollbackMetadata metadata);

  /**
   * Transition Rollback State from requested to inflight.
   *
   * @param requestedInstant requested instant
   * @return commit instant
   */
  HoodieInstant transitionRollbackRequestedToInflight(HoodieInstant requestedInstant);

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
  HoodieInstant transitionReplaceInflightToComplete(boolean shouldLock, HoodieInstant inflightInstant, HoodieReplaceCommitMetadata metadata);

  /**
   * Transition cluster inflight to replace committed.
   *
   * @param shouldLock Whether to hold the lock when performing transition
   * @param inflightInstant Inflight instant
   * @param metadata metadata to write into the instant file
   * @return commit instant
   */
  HoodieInstant transitionClusterInflightToComplete(boolean shouldLock, HoodieInstant inflightInstant, HoodieReplaceCommitMetadata metadata);
  /*=======
  public HoodieInstant transitionClusterInflightToComplete(boolean shouldLock,
                                                           HoodieInstant inflightInstant, Option<byte[]> data) {
    ValidationUtils.checkArgument(inflightInstant.getAction().equals(HoodieTimeline.CLUSTERING_ACTION));
    ValidationUtils.checkArgument(inflightInstant.isInflight());
    HoodieInstant commitInstant = new HoodieInstant(State.COMPLETED, REPLACE_COMMIT_ACTION, inflightInstant.getTimestamp());
    // Then write to timeline
    transitionStateToComplete(shouldLock, inflightInstant, commitInstant, data);
    return commitInstant;
  }

  private void transitionPendingState(HoodieInstant fromInstant, HoodieInstant toInstant, Option<byte[]> data) {
    transitionPendingState(fromInstant, toInstant, data, false);
  }

  protected void transitionStateToComplete(boolean shouldLock, HoodieInstant fromInstant,
                                           HoodieInstant toInstant, Option<byte[]> data) {
    this.transitionStateToComplete(shouldLock, fromInstant, toInstant, data, Option.empty());
  }

  protected void transitionStateToComplete(boolean shouldLock, HoodieInstant fromInstant,
                                           HoodieInstant toInstant, Option<byte[]> data,
                                           Option<String> completionTimeOpt) {
    ValidationUtils.checkArgument(fromInstant.getTimestamp().equals(toInstant.getTimestamp()), String.format("%s and %s are not consistent when transition state.", fromInstant, toInstant));
    String fromInstantFileName = fromInstant.getFileName();
    // Ensures old state exists in timeline
    LOG.info("Checking for file exists ?" + getInstantFileNamePath(fromInstantFileName));
    try {
      if (metaClient.getTimelineLayoutVersion().isNullVersion()) {
        // Re-create the .inflight file by opening a new file and write the commit metadata in
        createFileInMetaPath(fromInstantFileName, data, false);
        StoragePath fromInstantPath = getInstantFileNamePath(fromInstantFileName);
        HoodieInstant instantWithCompletionTime =
            new HoodieInstant(toInstant.getState(), toInstant.getAction(),
                toInstant.getTimestamp(), completionTimeOpt.map(entry -> entry).orElse(metaClient.createNewInstantTime(false)));
        StoragePath toInstantPath =
            getInstantFileNamePath(instantWithCompletionTime.getFileName());
        boolean success = metaClient.getStorage().rename(fromInstantPath, toInstantPath);
        if (!success) {
          throw new HoodieIOException(
              "Could not rename " + fromInstantPath + " to " + toInstantPath);
        }
      } else {
        ValidationUtils.checkArgument(
            metaClient.getStorage().exists(getInstantFileNamePath(fromInstantFileName)));
        createCompleteFileInMetaPath(shouldLock, toInstant, data);
      }
    } catch (IOException e) {
      throw new HoodieIOException("Could not complete " + fromInstant, e);
    }
  }

  protected void transitionPendingState(HoodieInstant fromInstant, HoodieInstant toInstant, Option<byte[]> data,
                                        boolean allowRedundantTransitions) {
    ValidationUtils.checkArgument(fromInstant.getTimestamp().equals(toInstant.getTimestamp()), String.format("%s and %s are not consistent when transition state.", fromInstant, toInstant));
    String fromInstantFileName = fromInstant.getFileName();
    String toInstantFileName = toInstant.getFileName();
    try {
      HoodieStorage storage = metaClient.getStorage();
      if (metaClient.getTimelineLayoutVersion().isNullVersion()) {
        // Re-create the .inflight file by opening a new file and write the commit metadata in
        createFileInMetaPath(fromInstantFileName, data, allowRedundantTransitions);
        StoragePath fromInstantPath = getInstantFileNamePath(fromInstantFileName);
        StoragePath toInstantPath = getInstantFileNamePath(toInstantFileName);
        boolean success = storage.rename(fromInstantPath, toInstantPath);
        if (!success) {
          throw new HoodieIOException("Could not rename " + fromInstantPath + " to " + toInstantPath);
        }
      } else {
        // Ensures old state exists in timeline
        ValidationUtils.checkArgument(storage.exists(getInstantFileNamePath(fromInstantFileName)),
            "File " + getInstantFileNamePath(fromInstantFileName) + " does not exist!");
        // Use Write Once to create Target File
        if (allowRedundantTransitions) {
          FileIOUtils.createFileInPath(storage, getInstantFileNamePath(toInstantFileName), data);
        } else {
          storage.createImmutableFileInPath(getInstantFileNamePath(toInstantFileName), data);
        }
        LOG.info("Create new file for toInstant ?" + getInstantFileNamePath(toInstantFileName));
      }
    } catch (IOException e) {
      throw new HoodieIOException("Could not complete " + fromInstant, e);
    }
  }

  protected void revertCompleteToInflight(HoodieInstant completed, HoodieInstant inflight) {
    ValidationUtils.checkArgument(completed.isCompleted());
    ValidationUtils.checkArgument(inflight.isInflight());
    ValidationUtils.checkArgument(completed.getTimestamp().equals(inflight.getTimestamp()));
    StoragePath inflightFilePath = getInstantFileNamePath(inflight.getFileName());
    StoragePath completedFilePath = getInstantFileNamePath(getInstantFileName(completed));
    try {
      if (metaClient.getTimelineLayoutVersion().isNullVersion()) {
        if (!metaClient.getStorage().exists(inflightFilePath)) {
          boolean success = metaClient.getStorage().rename(completedFilePath, inflightFilePath);
          if (!success) {
            throw new HoodieIOException(
                "Could not rename " + completedFilePath + " to " + inflightFilePath);
          }
        }
      } else {
        StoragePath requestedInstantFilePath = getInstantFileNamePath(new HoodieInstant(State.REQUESTED, inflight.getAction(),
            inflight.getTimestamp()).getFileName());

        // If inflight and requested files do not exist, create one
        if (!metaClient.getStorage().exists(requestedInstantFilePath)) {
          metaClient.getStorage().create(requestedInstantFilePath, false).close();
        }

        if (!metaClient.getStorage().exists(inflightFilePath)) {
          metaClient.getStorage().create(inflightFilePath, false).close();
        }

        boolean success = metaClient.getStorage().deleteFile(completedFilePath);
        ValidationUtils.checkArgument(success, "State Reverting failed");
      }
    } catch (IOException e) {
      throw new HoodieIOException("Could not complete revert " + completed, e);
    }
  }

  private StoragePath getInstantFileNamePath(String fileName) {
    return new StoragePath(fileName.contains(SCHEMA_COMMIT_ACTION) ? metaClient.getSchemaFolderName() : metaClient.getMetaPath().toString(), fileName);
  }

  public void transitionRequestedToInflight(String commitType, String inFlightInstant) {
    HoodieInstant requested = new HoodieInstant(HoodieInstant.State.REQUESTED, commitType, inFlightInstant);
    transitionRequestedToInflight(requested, Option.empty(), false);
  }

  public void transitionRequestedToInflight(HoodieInstant requested, Option<byte[]> content) {
    transitionRequestedToInflight(requested, content, false);
  }

  public void transitionRequestedToInflight(HoodieInstant requested, Option<byte[]> content,
      boolean allowRedundantTransitions) {
    HoodieInstant inflight = new HoodieInstant(State.INFLIGHT, requested.getAction(), requested.getTimestamp());
    ValidationUtils.checkArgument(requested.isRequested(), "Instant " + requested + " in wrong state");
    transitionPendingState(requested, inflight, content, allowRedundantTransitions);
  }

  public void saveToCompactionRequested(HoodieInstant instant, Option<byte[]> content) {
    saveToCompactionRequested(instant, content, false);
  }

  public void saveToCompactionRequested(HoodieInstant instant, Option<byte[]> content, boolean overwrite) {
    ValidationUtils.checkArgument(instant.getAction().equals(HoodieTimeline.COMPACTION_ACTION));
    createFileInMetaPath(instant.getFileName(), content, overwrite);
  }

  public void saveToLogCompactionRequested(HoodieInstant instant, Option<byte[]> content) {
    saveToLogCompactionRequested(instant, content, false);
  }

  public void saveToLogCompactionRequested(HoodieInstant instant, Option<byte[]> content, boolean overwrite) {
    ValidationUtils.checkArgument(instant.getAction().equals(HoodieTimeline.LOG_COMPACTION_ACTION));
    createFileInMetaPath(instant.getFileName(), content, overwrite);
  }
  >>>>>>> ddf00dd470a (Fixing CreateHandle and MergeHandle to generate col stats and stitch it with HoodieWriteStat) */

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
  void saveToCompactionRequested(HoodieInstant instant, HoodieCompactionPlan metadata);

  /**
   * Save Compaction requested instant with metadata.
   * @param instant Instant to save.
   * @param metadata metadata to write into the instant file
   * @param overwrite Overwrite existing instant file.
   */
  void saveToCompactionRequested(HoodieInstant instant, HoodieCompactionPlan metadata, boolean overwrite);

  /**
   * Save Log Compaction requested instant with metadata.
   * @param instant Instant to save.
   * @param metadata metadata to write into the instant file
   */
  void saveToLogCompactionRequested(HoodieInstant instant, HoodieCompactionPlan metadata);

  /**
   * Save Log Compaction requested instant with metadata.
   * @param instant Instant to save.
   * @param metadata metadata to write into the instant file
   * @param overwrite Overwrite existing instant file.
   */
  void saveToLogCompactionRequested(HoodieInstant instant, HoodieCompactionPlan metadata, boolean overwrite);

  /**
   * Save pending replace instant with metadata.
   * @param instant Instant to save.
   * @param metadata metadata to write into the instant file
   */
  void saveToPendingReplaceCommit(HoodieInstant instant, HoodieRequestedReplaceMetadata metadata);

  /**
   * Save pending cluster instant with metadata.
   * @param instant Instant to save.
   * @param metadata metadata to write into the instant file
   */
  void saveToPendingClusterCommit(HoodieInstant instant, HoodieRequestedReplaceMetadata metadata);

  /**
   * Save clean requested instant with metadata.
   * @param instant Instant to save.
   * @param metadata metadata to write into the instant file
   */
  void saveToCleanRequested(HoodieInstant instant, Option<HoodieCleanerPlan> metadata);

  /**
   * Save rollback requested instant with metadata.
   * @param instant Instant to save.
   * @param metadata metadata to write into the instant file
   */
  void saveToRollbackRequested(HoodieInstant instant, HoodieRollbackPlan metadata);

  /**
   * Save Restore requested instant with metadata.
   * @param instant Instant to save.
   * @param metadata metadata to write into the instant file
   */
  void saveToRestoreRequested(HoodieInstant instant, HoodieRestorePlan metadata);

  /**
   * Transition index instant state from requested to inflight.
   *
   * @param requestedInstant Inflight Instant
   * @return inflight instant
   */
  HoodieInstant transitionIndexRequestedToInflight(HoodieInstant requestedInstant);

  /**
   * Save content for inflight/requested index instant.
   */
  void saveToPendingIndexAction(HoodieInstant instant, HoodieIndexPlan metadata);

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
