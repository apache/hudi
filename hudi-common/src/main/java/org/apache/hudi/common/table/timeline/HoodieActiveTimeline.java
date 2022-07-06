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

import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Represents the Active Timeline for the Hoodie table. Instants for the last 12 hours (configurable) is in the
 * ActiveTimeline and the rest are Archived. ActiveTimeline is a special timeline that allows for creation of instants
 * on the timeline.
 * <p>
 * </p>
 * The timeline is not automatically reloaded on any mutation operation, clients have to manually call reload() so that
 * they can chain multiple mutations to the timeline and then call reload() once.
 * <p>
 * </p>
 * This class can be serialized and de-serialized and on de-serialization the FileSystem is re-initialized.
 */
public class HoodieActiveTimeline extends HoodieDefaultTimeline {

  public static final Set<String> VALID_EXTENSIONS_IN_ACTIVE_TIMELINE = new HashSet<>(Arrays.asList(
      COMMIT_EXTENSION, INFLIGHT_COMMIT_EXTENSION, REQUESTED_COMMIT_EXTENSION,
      DELTA_COMMIT_EXTENSION, INFLIGHT_DELTA_COMMIT_EXTENSION, REQUESTED_DELTA_COMMIT_EXTENSION,
      SAVEPOINT_EXTENSION, INFLIGHT_SAVEPOINT_EXTENSION,
      CLEAN_EXTENSION, REQUESTED_CLEAN_EXTENSION, INFLIGHT_CLEAN_EXTENSION,
      INFLIGHT_COMPACTION_EXTENSION, REQUESTED_COMPACTION_EXTENSION,
      REQUESTED_RESTORE_EXTENSION, INFLIGHT_RESTORE_EXTENSION, RESTORE_EXTENSION,
      ROLLBACK_EXTENSION, REQUESTED_ROLLBACK_EXTENSION, INFLIGHT_ROLLBACK_EXTENSION,
      REQUESTED_REPLACE_COMMIT_EXTENSION, INFLIGHT_REPLACE_COMMIT_EXTENSION, REPLACE_COMMIT_EXTENSION,
      REQUESTED_INDEX_COMMIT_EXTENSION, INFLIGHT_INDEX_COMMIT_EXTENSION, INDEX_COMMIT_EXTENSION,
      REQUESTED_SAVE_SCHEMA_ACTION_EXTENSION, INFLIGHT_SAVE_SCHEMA_ACTION_EXTENSION, SAVE_SCHEMA_ACTION_EXTENSION));
  private static final Logger LOG = LogManager.getLogger(HoodieActiveTimeline.class);
  protected HoodieTableMetaClient metaClient;

  /**
   * Parse the timestamp of an Instant and return a {@code Date}.
   */
  public static Date parseDateFromInstantTime(String timestamp) throws ParseException {
    return HoodieInstantTimeGenerator.parseDateFromInstantTime(timestamp);
  }

  /**
   * Format the Date to a String representing the timestamp of a Hoodie Instant.
   */
  public static String formatDate(Date timestamp) {
    return HoodieInstantTimeGenerator.formatDate(timestamp);
  }

  /**
   * Returns next instant time in the correct format.
   * Ensures each instant time is atleast 1 second apart since we create instant times at second granularity
   */
  public static String createNewInstantTime() {
    return HoodieInstantTimeGenerator.createNewInstantTime(0);
  }

  /**
   * Returns next instant time that adds N milliseconds to current time.
   * Ensures each instant time is atleast 1 second apart since we create instant times at second granularity
   *
   * @param milliseconds Milliseconds to add to current time while generating the new instant time
   */
  public static String createNewInstantTime(long milliseconds) {
    return HoodieInstantTimeGenerator.createNewInstantTime(milliseconds);
  }

  protected HoodieActiveTimeline(HoodieTableMetaClient metaClient, Set<String> includedExtensions) {
    this(metaClient, includedExtensions, true);
  }

  protected HoodieActiveTimeline(HoodieTableMetaClient metaClient, Set<String> includedExtensions,
      boolean applyLayoutFilters) {
    // Filter all the filter in the metapath and include only the extensions passed and
    // convert them into HoodieInstant
    try {
      this.setInstants(metaClient.scanHoodieInstantsFromFileSystem(includedExtensions, applyLayoutFilters));
    } catch (IOException e) {
      throw new HoodieIOException("Failed to scan metadata", e);
    }
    this.metaClient = metaClient;
    // multiple casts will make this lambda serializable -
    // http://docs.oracle.com/javase/specs/jls/se8/html/jls-15.html#jls-15.16
    this.details = (Function<HoodieInstant, Option<byte[]>> & Serializable) this::getInstantDetails;
    LOG.info("Loaded instants upto : " + lastInstant());
  }

  public HoodieActiveTimeline(HoodieTableMetaClient metaClient) {
    this(metaClient, Collections.unmodifiableSet(VALID_EXTENSIONS_IN_ACTIVE_TIMELINE));
  }

  public HoodieActiveTimeline(HoodieTableMetaClient metaClient, boolean applyLayoutFilter) {
    this(metaClient, Collections.unmodifiableSet(VALID_EXTENSIONS_IN_ACTIVE_TIMELINE), applyLayoutFilter);
  }

  /**
   * For serialization and de-serialization only.
   *
   * @deprecated
   */
  @Deprecated
  public HoodieActiveTimeline() {
  }

  /**
   * This method is only used when this object is deserialized in a spark executor.
   *
   * @deprecated
   */
  @Deprecated
  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
  }

  public void createNewInstant(HoodieInstant instant) {
    LOG.info("Creating a new instant " + instant);
    // Create the in-flight file
    createFileInMetaPath(instant.getFileName(), Option.empty(), false);
  }

  public void createRequestedReplaceCommit(String instantTime, String actionType) {
    try {
      HoodieInstant instant = new HoodieInstant(State.REQUESTED, actionType, instantTime);
      LOG.info("Creating a new instant " + instant);
      // Create the request replace file
      createFileInMetaPath(instant.getFileName(),
              TimelineMetadataUtils.serializeRequestedReplaceMetadata(new HoodieRequestedReplaceMetadata()), false);
    } catch (IOException e) {
      throw new HoodieIOException("Error create requested replace commit ", e);
    }
  }

  public void saveAsComplete(HoodieInstant instant, Option<byte[]> data) {
    LOG.info("Marking instant complete " + instant);
    ValidationUtils.checkArgument(instant.isInflight(),
        "Could not mark an already completed instant as complete again " + instant);
    transitionState(instant, HoodieTimeline.getCompletedInstant(instant), data);
    LOG.info("Completed " + instant);
  }

  public HoodieInstant revertToInflight(HoodieInstant instant) {
    LOG.info("Reverting instant to inflight " + instant);
    HoodieInstant inflight = HoodieTimeline.getInflightInstant(instant, metaClient.getTableType());
    revertCompleteToInflight(instant, inflight);
    LOG.info("Reverted " + instant + " to inflight " + inflight);
    return inflight;
  }

  public void deleteInflight(HoodieInstant instant) {
    ValidationUtils.checkArgument(instant.isInflight());
    deleteInstantFile(instant);
  }

  public void deletePending(HoodieInstant instant) {
    ValidationUtils.checkArgument(!instant.isCompleted());
    deleteInstantFile(instant);
  }

  public void deleteCompletedRollback(HoodieInstant instant) {
    ValidationUtils.checkArgument(instant.isCompleted());
    deleteInstantFile(instant);
  }

  public static void deleteInstantFile(FileSystem fs, String metaPath, HoodieInstant instant) {
    try {
      fs.delete(new Path(metaPath, instant.getFileName()), false);
    } catch (IOException e) {
      throw new HoodieIOException("Could not delete instant file" + instant.getFileName(), e);
    }
  }

  public void deleteEmptyInstantIfExists(HoodieInstant instant) {
    ValidationUtils.checkArgument(isEmpty(instant));
    deleteInstantFileIfExists(instant);
  }

  public void deleteCompactionRequested(HoodieInstant instant) {
    ValidationUtils.checkArgument(instant.isRequested());
    ValidationUtils.checkArgument(Objects.equals(instant.getAction(), HoodieTimeline.COMPACTION_ACTION));
    deleteInstantFile(instant);
  }

  public void deleteInstantFileIfExists(HoodieInstant instant) {
    LOG.info("Deleting instant " + instant);
    Path inFlightCommitFilePath = getInstantFileNamePath(instant.getFileName());
    try {
      if (metaClient.getFs().exists(inFlightCommitFilePath)) {
        boolean result = metaClient.getFs().delete(inFlightCommitFilePath, false);
        if (result) {
          LOG.info("Removed instant " + instant);
        } else {
          throw new HoodieIOException("Could not delete instant " + instant);
        }
      } else {
        LOG.warn("The commit " + inFlightCommitFilePath + " to remove does not exist");
      }
    } catch (IOException e) {
      throw new HoodieIOException("Could not remove inflight commit " + inFlightCommitFilePath, e);
    }
  }

  public void deleteInstantFile(HoodieInstant instant) {
    LOG.info("Deleting instant " + instant);
    Path inFlightCommitFilePath = getInstantFileNamePath(instant.getFileName());
    try {
      boolean result = metaClient.getFs().delete(inFlightCommitFilePath, false);
      if (result) {
        LOG.info("Removed instant " + instant);
      } else {
        throw new HoodieIOException("Could not delete instant " + instant);
      }
    } catch (IOException e) {
      throw new HoodieIOException("Could not remove inflight commit " + inFlightCommitFilePath, e);
    }
  }

  @Override
  public Option<byte[]> getInstantDetails(HoodieInstant instant) {
    Path detailPath = getInstantFileNamePath(instant.getFileName());
    return readDataFromPath(detailPath);
  }

  public HoodieInstant getCompletedInstantForTimestamp(String timestamp) {
    return filterCompletedInstants()
            .getInstants()
            .filter(i -> timestamp.equals(i.getTimestamp()))
            .findFirst()
            .orElseThrow(() -> new HoodieIOException(String.format("No completed instant with timestamp %s ", timestamp)));
  }

  /**
   * Returns most recent instant having valid schema in its {@link HoodieCommitMetadata}
   */
  public Option<Pair<HoodieInstant, HoodieCommitMetadata>> getLastCommitMetadataWithValidSchema() {
    return Option.fromJavaOptional(
        getCommitMetadataStream()
            .filter(instantCommitMetadataPair ->
                !StringUtils.isNullOrEmpty(instantCommitMetadataPair.getValue().getMetadata(HoodieCommitMetadata.SCHEMA_KEY)))
            .findFirst()
    );
  }

  /**
   * Get the last instant with valid data, and convert this to HoodieCommitMetadata
   */
  public Option<Pair<HoodieInstant, HoodieCommitMetadata>> getLastCommitMetadataWithValidData() {
    return Option.fromJavaOptional(
        getCommitMetadataStream()
            .filter(instantCommitMetadataPair ->
                !instantCommitMetadataPair.getValue().getFileIdAndRelativePaths().isEmpty())
            .findFirst()
    );
  }

  /**
   * Returns stream of {@link HoodieCommitMetadata} in order reverse to chronological (ie most
   * recent metadata being the first element)
   */
  private Stream<Pair<HoodieInstant, HoodieCommitMetadata>> getCommitMetadataStream() {
    // NOTE: Streams are lazy
    return getCommitsTimeline().filterCompletedInstants()
        .getInstants()
        .sorted(Comparator.comparing(HoodieInstant::getTimestamp).reversed())
        .map(instant -> {
          try {
            HoodieCommitMetadata commitMetadata =
                HoodieCommitMetadata.fromBytes(getInstantDetails(instant).get(), HoodieCommitMetadata.class);
            return Pair.of(instant, commitMetadata);
          } catch (IOException e) {
            throw new HoodieIOException(String.format("Failed to fetch HoodieCommitMetadata for instant (%s)", instant), e);
          }
        });
  }

  public Option<byte[]> readCleanerInfoAsBytes(HoodieInstant instant) {
    // Cleaner metadata are always stored only in timeline .hoodie
    return readDataFromPath(getInstantFileNamePath(instant.getFileName()));
  }

  public Option<byte[]> readRollbackInfoAsBytes(HoodieInstant instant) {
    // Rollback metadata are always stored only in timeline .hoodie
    return readDataFromPath(getInstantFileNamePath(instant.getFileName()));
  }

  public Option<byte[]> readRestoreInfoAsBytes(HoodieInstant instant) {
    // Rollback metadata are always stored only in timeline .hoodie
    return readDataFromPath(new Path(metaClient.getMetaPath(), instant.getFileName()));
  }

  //-----------------------------------------------------------------
  //      BEGIN - COMPACTION RELATED META-DATA MANAGEMENT.
  //-----------------------------------------------------------------

  public Option<byte[]> readCompactionPlanAsBytes(HoodieInstant instant) {
    try {
      // Reading from auxiliary path first. In future release, we will cleanup compaction management
      // to only write to timeline and skip auxiliary and this code will be able to handle it.
      return readDataFromPath(new Path(metaClient.getMetaAuxiliaryPath(), instant.getFileName()));
    } catch (HoodieIOException e) {
      // This will be removed in future release. See HUDI-546
      if (e.getIOException() instanceof FileNotFoundException) {
        return readDataFromPath(new Path(metaClient.getMetaPath(), instant.getFileName()));
      } else {
        throw e;
      }
    }
  }

  public Option<byte[]> readIndexPlanAsBytes(HoodieInstant instant) {
    return readDataFromPath(new Path(metaClient.getMetaPath(), instant.getFileName()));
  }

  /**
   * Revert instant state from inflight to requested.
   *
   * @param inflightInstant Inflight Instant
   * @return requested instant
   */
  public HoodieInstant revertInstantFromInflightToRequested(HoodieInstant inflightInstant) {
    ValidationUtils.checkArgument(inflightInstant.isInflight());
    HoodieInstant requestedInstant =
        new HoodieInstant(State.REQUESTED, inflightInstant.getAction(), inflightInstant.getTimestamp());
    if (metaClient.getTimelineLayoutVersion().isNullVersion()) {
      // Pass empty data since it is read from the corresponding .aux/.compaction instant file
      transitionState(inflightInstant, requestedInstant, Option.empty());
    } else {
      deleteInflight(inflightInstant);
    }
    return requestedInstant;
  }

  /**
   * Transition Compaction State from requested to inflight.
   *
   * @param requestedInstant Requested instant
   * @return inflight instant
   */
  public HoodieInstant transitionCompactionRequestedToInflight(HoodieInstant requestedInstant) {
    ValidationUtils.checkArgument(requestedInstant.getAction().equals(HoodieTimeline.COMPACTION_ACTION));
    ValidationUtils.checkArgument(requestedInstant.isRequested());
    HoodieInstant inflightInstant =
        new HoodieInstant(State.INFLIGHT, COMPACTION_ACTION, requestedInstant.getTimestamp());
    transitionState(requestedInstant, inflightInstant, Option.empty());
    return inflightInstant;
  }

  /**
   * Transition Compaction State from inflight to Committed.
   *
   * @param inflightInstant Inflight instant
   * @param data Extra Metadata
   * @return commit instant
   */
  public HoodieInstant transitionCompactionInflightToComplete(HoodieInstant inflightInstant, Option<byte[]> data) {
    ValidationUtils.checkArgument(inflightInstant.getAction().equals(HoodieTimeline.COMPACTION_ACTION));
    ValidationUtils.checkArgument(inflightInstant.isInflight());
    HoodieInstant commitInstant = new HoodieInstant(State.COMPLETED, COMMIT_ACTION, inflightInstant.getTimestamp());
    transitionState(inflightInstant, commitInstant, data);
    return commitInstant;
  }

  private void createFileInAuxiliaryFolder(HoodieInstant instant, Option<byte[]> data) {
    // This will be removed in future release. See HUDI-546
    Path fullPath = new Path(metaClient.getMetaAuxiliaryPath(), instant.getFileName());
    FileIOUtils.createFileInPath(metaClient.getFs(), fullPath, data);
  }

  //-----------------------------------------------------------------
  //      END - COMPACTION RELATED META-DATA MANAGEMENT
  //-----------------------------------------------------------------

  /**
   * Transition Clean State from inflight to Committed.
   *
   * @param inflightInstant Inflight instant
   * @param data Extra Metadata
   * @return commit instant
   */
  public HoodieInstant transitionCleanInflightToComplete(HoodieInstant inflightInstant, Option<byte[]> data) {
    ValidationUtils.checkArgument(inflightInstant.getAction().equals(HoodieTimeline.CLEAN_ACTION));
    ValidationUtils.checkArgument(inflightInstant.isInflight());
    HoodieInstant commitInstant = new HoodieInstant(State.COMPLETED, CLEAN_ACTION, inflightInstant.getTimestamp());
    // Then write to timeline
    transitionState(inflightInstant, commitInstant, data);
    return commitInstant;
  }

  /**
   * Transition Clean State from requested to inflight.
   *
   * @param requestedInstant requested instant
   * @param data Optional data to be stored
   * @return commit instant
   */
  public HoodieInstant transitionCleanRequestedToInflight(HoodieInstant requestedInstant, Option<byte[]> data) {
    ValidationUtils.checkArgument(requestedInstant.getAction().equals(HoodieTimeline.CLEAN_ACTION));
    ValidationUtils.checkArgument(requestedInstant.isRequested());
    HoodieInstant inflight = new HoodieInstant(State.INFLIGHT, CLEAN_ACTION, requestedInstant.getTimestamp());
    transitionState(requestedInstant, inflight, data);
    return inflight;
  }

  /**
   * Transition Rollback State from inflight to Committed.
   *
   * @param inflightInstant Inflight instant
   * @param data Extra Metadata
   * @return commit instant
   */
  public HoodieInstant transitionRollbackInflightToComplete(HoodieInstant inflightInstant, Option<byte[]> data) {
    ValidationUtils.checkArgument(inflightInstant.getAction().equals(HoodieTimeline.ROLLBACK_ACTION));
    ValidationUtils.checkArgument(inflightInstant.isInflight());
    HoodieInstant commitInstant = new HoodieInstant(State.COMPLETED, ROLLBACK_ACTION, inflightInstant.getTimestamp());
    // Then write to timeline
    transitionState(inflightInstant, commitInstant, data);
    return commitInstant;
  }

  /**
   * Transition Rollback State from requested to inflight.
   *
   * @param requestedInstant requested instant
   * @return commit instant
   */
  public HoodieInstant transitionRollbackRequestedToInflight(HoodieInstant requestedInstant) {
    ValidationUtils.checkArgument(requestedInstant.getAction().equals(HoodieTimeline.ROLLBACK_ACTION));
    ValidationUtils.checkArgument(requestedInstant.isRequested());
    HoodieInstant inflight = new HoodieInstant(State.INFLIGHT, ROLLBACK_ACTION, requestedInstant.getTimestamp());
    transitionState(requestedInstant, inflight, Option.empty());
    return inflight;
  }

  /**
   * Transition Restore State from requested to inflight.
   *
   * @param requestedInstant requested instant
   * @return commit instant
   */
  public HoodieInstant transitionRestoreRequestedToInflight(HoodieInstant requestedInstant) {
    ValidationUtils.checkArgument(requestedInstant.getAction().equals(HoodieTimeline.RESTORE_ACTION), "Transition to inflight requested for a restore instant with diff action "
        + requestedInstant.toString());
    ValidationUtils.checkArgument(requestedInstant.isRequested(), "Transition to inflight requested for an instant not in requested state " + requestedInstant.toString());
    HoodieInstant inflight = new HoodieInstant(State.INFLIGHT, RESTORE_ACTION, requestedInstant.getTimestamp());
    transitionState(requestedInstant, inflight, Option.empty());
    return inflight;
  }

  /**
   * Transition replace requested file to replace inflight.
   *
   * @param requestedInstant Requested instant
   * @param data Extra Metadata
   * @return inflight instant
   */
  public HoodieInstant transitionReplaceRequestedToInflight(HoodieInstant requestedInstant, Option<byte[]> data) {
    ValidationUtils.checkArgument(requestedInstant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION));
    ValidationUtils.checkArgument(requestedInstant.isRequested());
    HoodieInstant inflightInstant = new HoodieInstant(State.INFLIGHT, REPLACE_COMMIT_ACTION, requestedInstant.getTimestamp());
    // Then write to timeline
    transitionState(requestedInstant, inflightInstant, data);
    return inflightInstant;
  }

  /**
   * Transition replace inflight to Committed.
   *
   * @param inflightInstant Inflight instant
   * @param data Extra Metadata
   * @return commit instant
   */
  public HoodieInstant transitionReplaceInflightToComplete(HoodieInstant inflightInstant, Option<byte[]> data) {
    ValidationUtils.checkArgument(inflightInstant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION));
    ValidationUtils.checkArgument(inflightInstant.isInflight());
    HoodieInstant commitInstant = new HoodieInstant(State.COMPLETED, REPLACE_COMMIT_ACTION, inflightInstant.getTimestamp());
    // Then write to timeline
    transitionState(inflightInstant, commitInstant, data);
    return commitInstant;
  }

  private void transitionState(HoodieInstant fromInstant, HoodieInstant toInstant, Option<byte[]> data) {
    transitionState(fromInstant, toInstant, data, false);
  }

  protected void transitionState(HoodieInstant fromInstant, HoodieInstant toInstant, Option<byte[]> data,
       boolean allowRedundantTransitions) {
    ValidationUtils.checkArgument(fromInstant.getTimestamp().equals(toInstant.getTimestamp()));
    try {
      if (metaClient.getTimelineLayoutVersion().isNullVersion()) {
        // Re-create the .inflight file by opening a new file and write the commit metadata in
        createFileInMetaPath(fromInstant.getFileName(), data, allowRedundantTransitions);
        Path fromInstantPath = getInstantFileNamePath(fromInstant.getFileName());
        Path toInstantPath = getInstantFileNamePath(toInstant.getFileName());
        boolean success = metaClient.getFs().rename(fromInstantPath, toInstantPath);
        if (!success) {
          throw new HoodieIOException("Could not rename " + fromInstantPath + " to " + toInstantPath);
        }
      } else {
        // Ensures old state exists in timeline
        LOG.info("Checking for file exists ?" + getInstantFileNamePath(fromInstant.getFileName()));
        ValidationUtils.checkArgument(metaClient.getFs().exists(getInstantFileNamePath(fromInstant.getFileName())));
        // Use Write Once to create Target File
        if (allowRedundantTransitions) {
          FileIOUtils.createFileInPath(metaClient.getFs(), getInstantFileNamePath(toInstant.getFileName()), data);
        } else {
          metaClient.getFs().createImmutableFileInPath(getInstantFileNamePath(toInstant.getFileName()), data);
        }
        LOG.info("Create new file for toInstant ?" + getInstantFileNamePath(toInstant.getFileName()));
      }
    } catch (IOException e) {
      throw new HoodieIOException("Could not complete " + fromInstant, e);
    }
  }

  protected void revertCompleteToInflight(HoodieInstant completed, HoodieInstant inflight) {
    ValidationUtils.checkArgument(completed.getTimestamp().equals(inflight.getTimestamp()));
    Path inFlightCommitFilePath = getInstantFileNamePath(inflight.getFileName());
    Path commitFilePath = getInstantFileNamePath(completed.getFileName());
    try {
      if (metaClient.getTimelineLayoutVersion().isNullVersion()) {
        if (!metaClient.getFs().exists(inFlightCommitFilePath)) {
          boolean success = metaClient.getFs().rename(commitFilePath, inFlightCommitFilePath);
          if (!success) {
            throw new HoodieIOException(
                "Could not rename " + commitFilePath + " to " + inFlightCommitFilePath);
          }
        }
      } else {
        Path requestedInstantFilePath = getInstantFileNamePath(new HoodieInstant(State.REQUESTED,
            inflight.getAction(), inflight.getTimestamp()).getFileName());

        // If inflight and requested files do not exist, create one
        if (!metaClient.getFs().exists(requestedInstantFilePath)) {
          metaClient.getFs().create(requestedInstantFilePath, false).close();
        }

        if (!metaClient.getFs().exists(inFlightCommitFilePath)) {
          metaClient.getFs().create(inFlightCommitFilePath, false).close();
        }

        boolean success = metaClient.getFs().delete(commitFilePath, false);
        ValidationUtils.checkArgument(success, "State Reverting failed");
      }
    } catch (IOException e) {
      throw new HoodieIOException("Could not complete revert " + completed, e);
    }
  }

  private Path getInstantFileNamePath(String fileName) {
    return new Path(fileName.contains(SCHEMA_COMMIT_ACTION) ? metaClient.getSchemaFolderName() : metaClient.getMetaPath(), fileName);
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
    transitionState(requested, inflight, content, allowRedundantTransitions);
  }

  public void saveToCompactionRequested(HoodieInstant instant, Option<byte[]> content) {
    saveToCompactionRequested(instant, content, false);
  }

  public void saveToCompactionRequested(HoodieInstant instant, Option<byte[]> content, boolean overwrite) {
    ValidationUtils.checkArgument(instant.getAction().equals(HoodieTimeline.COMPACTION_ACTION));
    // Write workload to auxiliary folder
    createFileInAuxiliaryFolder(instant, content);
    createFileInMetaPath(instant.getFileName(), content, overwrite);
  }

  /**
   * Saves content for requested REPLACE instant.
   */
  public void saveToPendingReplaceCommit(HoodieInstant instant, Option<byte[]> content) {
    ValidationUtils.checkArgument(instant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION));
    createFileInMetaPath(instant.getFileName(), content, false);
  }

  public void saveToCleanRequested(HoodieInstant instant, Option<byte[]> content) {
    ValidationUtils.checkArgument(instant.getAction().equals(HoodieTimeline.CLEAN_ACTION));
    ValidationUtils.checkArgument(instant.getState().equals(State.REQUESTED));
    // Plan is stored in meta path
    createFileInMetaPath(instant.getFileName(), content, false);
  }

  public void saveToRollbackRequested(HoodieInstant instant, Option<byte[]> content) {
    ValidationUtils.checkArgument(instant.getAction().equals(HoodieTimeline.ROLLBACK_ACTION));
    ValidationUtils.checkArgument(instant.getState().equals(State.REQUESTED));
    // Plan is stored in meta path
    createFileInMetaPath(instant.getFileName(), content, false);
  }

  public void saveToRestoreRequested(HoodieInstant instant, Option<byte[]> content) {
    ValidationUtils.checkArgument(instant.getAction().equals(HoodieTimeline.RESTORE_ACTION));
    ValidationUtils.checkArgument(instant.getState().equals(State.REQUESTED));
    // Plan is stored in meta path
    createFileInMetaPath(instant.getFileName(), content, false);
  }

  /**
   * Transition index instant state from requested to inflight.
   *
   * @param requestedInstant Inflight Instant
   * @return inflight instant
   */
  public HoodieInstant transitionIndexRequestedToInflight(HoodieInstant requestedInstant, Option<byte[]> data) {
    ValidationUtils.checkArgument(requestedInstant.getAction().equals(HoodieTimeline.INDEXING_ACTION),
        String.format("%s is not equal to %s action", requestedInstant.getAction(), INDEXING_ACTION));
    ValidationUtils.checkArgument(requestedInstant.isRequested(),
        String.format("Instant %s not in requested state", requestedInstant.getTimestamp()));
    HoodieInstant inflightInstant = new HoodieInstant(State.INFLIGHT, INDEXING_ACTION, requestedInstant.getTimestamp());
    transitionState(requestedInstant, inflightInstant, data);
    return inflightInstant;
  }

  /**
   * Transition index instant state from inflight to completed.
   * @param inflightInstant Inflight Instant
   * @return completed instant
   */
  public HoodieInstant transitionIndexInflightToComplete(HoodieInstant inflightInstant, Option<byte[]> data) {
    ValidationUtils.checkArgument(inflightInstant.getAction().equals(HoodieTimeline.INDEXING_ACTION),
        String.format("%s is not equal to %s action", inflightInstant.getAction(), INDEXING_ACTION));
    ValidationUtils.checkArgument(inflightInstant.isInflight(),
        String.format("Instant %s not inflight", inflightInstant.getTimestamp()));
    HoodieInstant commitInstant = new HoodieInstant(State.COMPLETED, INDEXING_ACTION, inflightInstant.getTimestamp());
    transitionState(inflightInstant, commitInstant, data);
    return commitInstant;
  }

  /**
   * Revert index instant state from inflight to requested.
   * @param inflightInstant Inflight Instant
   * @return requested instant
   */
  public HoodieInstant revertIndexInflightToRequested(HoodieInstant inflightInstant) {
    ValidationUtils.checkArgument(inflightInstant.getAction().equals(HoodieTimeline.INDEXING_ACTION),
        String.format("%s is not equal to %s action", inflightInstant.getAction(), INDEXING_ACTION));
    ValidationUtils.checkArgument(inflightInstant.isInflight(),
        String.format("Instant %s not inflight", inflightInstant.getTimestamp()));
    HoodieInstant requestedInstant = new HoodieInstant(State.REQUESTED, INDEXING_ACTION, inflightInstant.getTimestamp());
    if (metaClient.getTimelineLayoutVersion().isNullVersion()) {
      transitionState(inflightInstant, requestedInstant, Option.empty());
    } else {
      deleteInflight(inflightInstant);
    }
    return requestedInstant;
  }

  /**
   * Save content for inflight/requested index instant.
   */
  public void saveToPendingIndexAction(HoodieInstant instant, Option<byte[]> content) {
    ValidationUtils.checkArgument(instant.getAction().equals(HoodieTimeline.INDEXING_ACTION),
        String.format("%s is not equal to %s action", instant.getAction(), INDEXING_ACTION));
    createFileInMetaPath(instant.getFileName(), content, false);
  }

  protected void createFileInMetaPath(String filename, Option<byte[]> content, boolean allowOverwrite) {
    Path fullPath = getInstantFileNamePath(filename);
    if (allowOverwrite || metaClient.getTimelineLayoutVersion().isNullVersion()) {
      FileIOUtils.createFileInPath(metaClient.getFs(), fullPath, content);
    } else {
      metaClient.getFs().createImmutableFileInPath(fullPath, content);
    }
  }

  private Option<byte[]> readDataFromPath(Path detailPath) {
    try (FSDataInputStream is = metaClient.getFs().open(detailPath)) {
      return Option.of(FileIOUtils.readAsByteArray(is));
    } catch (IOException e) {
      throw new HoodieIOException("Could not read commit details from " + detailPath, e);
    }
  }

  public HoodieActiveTimeline reload() {
    return new HoodieActiveTimeline(metaClient);
  }
}
