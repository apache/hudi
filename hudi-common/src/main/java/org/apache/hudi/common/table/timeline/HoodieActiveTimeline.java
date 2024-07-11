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
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
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
      INFLIGHT_LOG_COMPACTION_EXTENSION, REQUESTED_LOG_COMPACTION_EXTENSION,
      ROLLBACK_EXTENSION, REQUESTED_ROLLBACK_EXTENSION, INFLIGHT_ROLLBACK_EXTENSION,
      REQUESTED_REPLACE_COMMIT_EXTENSION, INFLIGHT_REPLACE_COMMIT_EXTENSION, REPLACE_COMMIT_EXTENSION,
      REQUESTED_INDEX_COMMIT_EXTENSION, INFLIGHT_INDEX_COMMIT_EXTENSION, INDEX_COMMIT_EXTENSION,
      REQUESTED_SAVE_SCHEMA_ACTION_EXTENSION, INFLIGHT_SAVE_SCHEMA_ACTION_EXTENSION, SAVE_SCHEMA_ACTION_EXTENSION,
      REQUESTED_CLUSTERING_COMMIT_EXTENSION, INFLIGHT_CLUSTERING_COMMIT_EXTENSION));

  public static final Set<String> NOT_PARSABLE_TIMESTAMPS = new HashSet<String>(3) {{
      add(HoodieTimeline.INIT_INSTANT_TS);
      add(HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS);
      add(HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS);
    }};

  private static final Logger LOG = LoggerFactory.getLogger(HoodieActiveTimeline.class);
  protected HoodieTableMetaClient metaClient;

  /**
   * Parse the timestamp of an Instant and return a {@code Date}.
   * Throw ParseException if timestamp is not valid format as
   *  {@link org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator#SECS_INSTANT_TIMESTAMP_FORMAT}.
   *
   * @param timestamp a timestamp String which follow pattern as
   *  {@link org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator#SECS_INSTANT_TIMESTAMP_FORMAT}.
   * @return Date of instant timestamp
   */
  public static Date parseDateFromInstantTime(String timestamp) throws ParseException {
    return HoodieInstantTimeGenerator.parseDateFromInstantTime(timestamp);
  }

  /**
   * The same parsing method as above, but this method will mute ParseException.
   * If the given timestamp is invalid, returns {@code Option.empty}.
   * Or a corresponding Date value if these timestamp strings are provided
   *  {@link org.apache.hudi.common.table.timeline.HoodieTimeline#INIT_INSTANT_TS},
   *  {@link org.apache.hudi.common.table.timeline.HoodieTimeline#METADATA_BOOTSTRAP_INSTANT_TS},
   *  {@link org.apache.hudi.common.table.timeline.HoodieTimeline#FULL_BOOTSTRAP_INSTANT_TS}.
   * This method is useful when parsing timestamp for metrics
   *
   * @param timestamp a timestamp String which follow pattern as
   *  {@link org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator#SECS_INSTANT_TIMESTAMP_FORMAT}.
   * @return {@code Option<Date>} of instant timestamp, {@code Option.empty} if invalid timestamp
   */
  public static Option<Date> parseDateFromInstantTimeSafely(String timestamp) {
    Option<Date> parsedDate;
    try {
      parsedDate = Option.of(HoodieInstantTimeGenerator.parseDateFromInstantTime(timestamp));
    } catch (ParseException e) {
      if (NOT_PARSABLE_TIMESTAMPS.contains(timestamp)) {
        parsedDate = Option.of(new Date(Integer.parseInt(timestamp)));
      } else {
        LOG.warn("Failed to parse timestamp " + timestamp + ": " + e.getMessage());
        parsedDate = Option.empty();
      }
    }
    return parsedDate;
  }

  /**
   * Format the Date to a String representing the timestamp of a Hoodie Instant.
   */
  public static String formatDate(Date timestamp) {
    return HoodieInstantTimeGenerator.formatDate(timestamp);
  }

  /**
   * Returns next instant time in the correct format.
   * Ensures each instant time is at least 1 millisecond apart since we create instant times at millisecond granularity.
   *
   * @param shouldLock whether the lock should be enabled to get the instant time.
   * @param timeGenerator TimeGenerator used to generate the instant time.
   */
  public static String createNewInstantTime(boolean shouldLock, TimeGenerator timeGenerator) {
    return createNewInstantTime(shouldLock, timeGenerator, 0L);
  }

  /**
   * Returns next instant time in the correct format.
   * Ensures each instant time is at least 1 millisecond apart since we create instant times at millisecond granularity.
   *
   * @param shouldLock whether the lock should be enabled to get the instant time.
   * @param timeGenerator TimeGenerator used to generate the instant time.
   * @param milliseconds Milliseconds to add to current time while generating the new instant time
   */
  public static String createNewInstantTime(boolean shouldLock, TimeGenerator timeGenerator, long milliseconds) {
    return HoodieInstantTimeGenerator.createNewInstantTime(shouldLock, timeGenerator, milliseconds);
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

  /**
   * Create a complete instant and save to storage with a completion time.
   * @param instant the complete instant.
   */
  public void createCompleteInstant(HoodieInstant instant) {
    LOG.info("Creating a new complete instant " + instant);
    createCompleteFileInMetaPath(true, instant, Option.empty());
  }

  /**
   * Create a pending instant and save to storage.
   * @param instant the pending instant.
   */
  public void createNewInstant(HoodieInstant instant) {
    LOG.info("Creating a new instant " + instant);
    ValidationUtils.checkArgument(!instant.isCompleted());
    createFileInMetaPath(instant.getFileName(), Option.empty(), false);
  }

  public void createRequestedCommitWithReplaceMetadata(String instantTime, String actionType) {
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
    saveAsComplete(true, instant, data);
  }

  public void saveAsComplete(boolean shouldLock, HoodieInstant instant, Option<byte[]> data) {
    LOG.info("Marking instant complete " + instant);
    ValidationUtils.checkArgument(instant.isInflight(),
        "Could not mark an already completed instant as complete again " + instant);
    HoodieInstant commitInstant = new HoodieInstant(State.COMPLETED, instant.getAction(), instant.getTimestamp());
    transitionStateToComplete(shouldLock, instant, commitInstant, data);
    LOG.info("Completed " + instant);
  }

  public HoodieInstant revertToInflight(HoodieInstant instant) {
    LOG.info("Reverting instant to inflight " + instant);
    HoodieInstant inflight = HoodieTimeline.getInflightInstant(instant, metaClient);
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
    ValidationUtils.checkArgument(Objects.equals(instant.getAction(), HoodieTimeline.ROLLBACK_ACTION));
    deleteInstantFile(instant);
  }

  public static void deleteInstantFile(HoodieStorage storage, StoragePath metaPath, HoodieInstant instant) {
    try {
      storage.deleteFile(new StoragePath(metaPath, instant.getFileName()));
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

  /**
   * Note: This method should only be used in the case that delete requested/inflight instant or empty clean instant,
   * and completed commit instant in an archive operation.
   */
  public void deleteInstantFileIfExists(HoodieInstant instant) {
    LOG.info("Deleting instant " + instant);
    StoragePath commitFilePath = getInstantFileNamePath(instant.getFileName());
    try {
      if (metaClient.getStorage().exists(commitFilePath)) {
        boolean result = metaClient.getStorage().deleteFile(commitFilePath);
        if (result) {
          LOG.info("Removed instant " + instant);
        } else {
          throw new HoodieIOException("Could not delete instant " + instant + " with path " + commitFilePath);
        }
      } else {
        LOG.warn("The commit " + commitFilePath + " to remove does not exist");
      }
    } catch (IOException e) {
      throw new HoodieIOException("Could not remove commit " + commitFilePath, e);
    }
  }

  protected void deleteInstantFile(HoodieInstant instant) {
    LOG.info("Deleting instant " + instant);
    StoragePath filePath = getInstantFileNamePath(instant.getFileName());
    try {
      boolean result = metaClient.getStorage().deleteFile(filePath);
      if (result) {
        LOG.info("Removed instant " + instant);
      } else {
        throw new HoodieIOException("Could not delete instant " + instant + " with path " + filePath);
      }
    } catch (IOException e) {
      throw new HoodieIOException("Could not remove inflight commit " + filePath, e);
    }
  }

  /**
   * Many callers might not pass completionTime, here we have to search
   * timeline to get completionTime, the impact should be minor since
   * 1. It appeals only tests pass instant without completion time
   * 2. we already holds all instants in memory, the cost should be minor.
   *
   * <p>TODO: [HUDI-6885] Depreciate HoodieActiveTimeline#getInstantFileName and fix related tests.
   */
  protected String getInstantFileName(HoodieInstant instant) {
    if (instant.isCompleted() && instant.getCompletionTime() == null) {
      return getInstantsAsStream().filter(s -> s.equals(instant))
          .findFirst().orElseThrow(() -> new HoodieIOException("Cannot find the instant" + instant))
          .getFileName();
    }
    return instant.getFileName();
  }

  @Override
  public Option<byte[]> getInstantDetails(HoodieInstant instant) {
    StoragePath detailPath = getInstantFileNamePath(getInstantFileName(instant));
    return readDataFromPath(detailPath);
  }

  /**
   * Returns most recent instant having valid schema in its {@link HoodieCommitMetadata}
   */
  public Option<Pair<HoodieInstant, HoodieCommitMetadata>> getLastCommitMetadataWithValidSchema() {
    return Option.fromJavaOptional(
        getCommitMetadataStream()
            .filter(instantCommitMetadataPair ->
                WriteOperationType.canUpdateSchema(instantCommitMetadataPair.getRight().getOperationType())
                    && !StringUtils.isNullOrEmpty(instantCommitMetadataPair.getValue().getMetadata(HoodieCommitMetadata.SCHEMA_KEY)))
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
        .getInstantsAsStream()
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
    return readDataFromPath(getInstantFileNamePath(getInstantFileName(instant)));
  }

  public Option<byte[]> readRollbackInfoAsBytes(HoodieInstant instant) {
    // Rollback metadata are always stored only in timeline .hoodie
    return readDataFromPath(getInstantFileNamePath(getInstantFileName(instant)));
  }

  public Option<byte[]> readRestoreInfoAsBytes(HoodieInstant instant) {
    // Rollback metadata are always stored only in timeline .hoodie
    return readDataFromPath(getInstantFileNamePath(getInstantFileName(instant)));
  }

  //-----------------------------------------------------------------
  //      BEGIN - COMPACTION RELATED META-DATA MANAGEMENT.
  //-----------------------------------------------------------------

  public Option<byte[]> readCompactionPlanAsBytes(HoodieInstant instant) {
    return readDataFromPath(new StoragePath(metaClient.getMetaPath(), getInstantFileName(instant)));
  }

  public Option<byte[]> readIndexPlanAsBytes(HoodieInstant instant) {
    return readDataFromPath(new StoragePath(metaClient.getMetaPath(), getInstantFileName(instant)));
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
      transitionPendingState(inflightInstant, requestedInstant, Option.empty());
    } else {
      deleteInflight(inflightInstant);
    }
    return requestedInstant;
  }

  /**
   * TODO: This method is not needed, since log compaction plan is not a immutable plan.
   * Revert logcompaction State from inflight to requested.
   *
   * @param inflightInstant Inflight Instant
   * @return requested instant
   */
  public HoodieInstant revertLogCompactionInflightToRequested(HoodieInstant inflightInstant) {
    ValidationUtils.checkArgument(inflightInstant.getAction().equals(HoodieTimeline.LOG_COMPACTION_ACTION));
    ValidationUtils.checkArgument(inflightInstant.isInflight());
    HoodieInstant requestedInstant =
        new HoodieInstant(State.REQUESTED, LOG_COMPACTION_ACTION, inflightInstant.getTimestamp());
    if (metaClient.getTimelineLayoutVersion().isNullVersion()) {
      // Pass empty data since it is read from the corresponding .aux/.compaction instant file
      transitionPendingState(inflightInstant, requestedInstant, Option.empty());
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
    transitionPendingState(requestedInstant, inflightInstant, Option.empty());
    return inflightInstant;
  }

  /**
   * Transition LogCompaction State from requested to inflight.
   *
   * @param requestedInstant Requested instant
   * @return inflight instant
   */
  public HoodieInstant transitionLogCompactionRequestedToInflight(HoodieInstant requestedInstant) {
    ValidationUtils.checkArgument(requestedInstant.getAction().equals(HoodieTimeline.LOG_COMPACTION_ACTION));
    ValidationUtils.checkArgument(requestedInstant.isRequested());
    HoodieInstant inflightInstant =
        new HoodieInstant(State.INFLIGHT, LOG_COMPACTION_ACTION, requestedInstant.getTimestamp());
    transitionPendingState(requestedInstant, inflightInstant, Option.empty());
    return inflightInstant;
  }

  /**
   * Transition Compaction State from inflight to Committed.
   *
   * @param shouldLock Whether to hold the lock when performing transition
   * @param inflightInstant Inflight instant
   * @param data Extra Metadata
   * @return commit instant
   */
  public HoodieInstant transitionCompactionInflightToComplete(boolean shouldLock, HoodieInstant inflightInstant,
                                                              Option<byte[]> data) {
    ValidationUtils.checkArgument(inflightInstant.getAction().equals(HoodieTimeline.COMPACTION_ACTION));
    ValidationUtils.checkArgument(inflightInstant.isInflight());
    HoodieInstant commitInstant = new HoodieInstant(State.COMPLETED, COMMIT_ACTION, inflightInstant.getTimestamp());
    transitionStateToComplete(shouldLock, inflightInstant, commitInstant, data);
    return commitInstant;
  }

  /**
   * Transition Log Compaction State from inflight to Committed.
   *
   * @param shouldLock Whether to hold the lock when performing transition
   * @param inflightInstant Inflight instant
   * @param data Extra Metadata
   * @return commit instant
   */
  public HoodieInstant transitionLogCompactionInflightToComplete(boolean shouldLock,
                                                                 HoodieInstant inflightInstant, Option<byte[]> data) {
    ValidationUtils.checkArgument(inflightInstant.getAction().equals(HoodieTimeline.LOG_COMPACTION_ACTION));
    ValidationUtils.checkArgument(inflightInstant.isInflight());
    HoodieInstant commitInstant = new HoodieInstant(State.COMPLETED, DELTA_COMMIT_ACTION, inflightInstant.getTimestamp());
    transitionStateToComplete(shouldLock, inflightInstant, commitInstant, data);
    return commitInstant;
  }

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
                                                         Option<byte[]> data) {
    ValidationUtils.checkArgument(inflightInstant.getAction().equals(HoodieTimeline.CLEAN_ACTION));
    ValidationUtils.checkArgument(inflightInstant.isInflight());
    HoodieInstant commitInstant = new HoodieInstant(State.COMPLETED, CLEAN_ACTION, inflightInstant.getTimestamp());
    // Then write to timeline
    transitionStateToComplete(shouldLock, inflightInstant, commitInstant, data);
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
    transitionPendingState(requestedInstant, inflight, data);
    return inflight;
  }

  /**
   * Transition Rollback State from inflight to Committed.
   *
   * @param shouldLock Whether to hold the lock when performing transition
   * @param inflightInstant Inflight instant
   * @param data Extra Metadata
   * @return commit instant
   */
  public HoodieInstant transitionRollbackInflightToComplete(boolean shouldLock,
                                                            HoodieInstant inflightInstant, Option<byte[]> data) {
    ValidationUtils.checkArgument(inflightInstant.getAction().equals(HoodieTimeline.ROLLBACK_ACTION));
    ValidationUtils.checkArgument(inflightInstant.isInflight());
    HoodieInstant commitInstant = new HoodieInstant(State.COMPLETED, ROLLBACK_ACTION, inflightInstant.getTimestamp());
    // Then write to timeline
    transitionStateToComplete(shouldLock, inflightInstant, commitInstant, data);
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
    transitionPendingState(requestedInstant, inflight, Option.empty());
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
        + requestedInstant);
    ValidationUtils.checkArgument(requestedInstant.isRequested(), "Transition to inflight requested for an instant not in requested state " + requestedInstant.toString());
    HoodieInstant inflight = new HoodieInstant(State.INFLIGHT, RESTORE_ACTION, requestedInstant.getTimestamp());
    transitionPendingState(requestedInstant, inflight, Option.empty());
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
    transitionPendingState(requestedInstant, inflightInstant, data);
    return inflightInstant;
  }

  /**
   * Transition cluster requested file to cluster inflight.
   *
   * @param requestedInstant Requested instant
   * @param data Extra Metadata
   * @return inflight instant
   */
  public HoodieInstant transitionClusterRequestedToInflight(HoodieInstant requestedInstant, Option<byte[]> data) {
    ValidationUtils.checkArgument(requestedInstant.getAction().equals(HoodieTimeline.CLUSTERING_ACTION));
    ValidationUtils.checkArgument(requestedInstant.isRequested());
    HoodieInstant inflightInstant = new HoodieInstant(State.INFLIGHT, CLUSTERING_ACTION, requestedInstant.getTimestamp());
    // Then write to timeline
    transitionPendingState(requestedInstant, inflightInstant, data);
    return inflightInstant;
  }

  /**
   * Transition replace inflight to Committed.
   *
   * @param shouldLock Whether to hold the lock when performing transition
   * @param inflightInstant Inflight instant
   * @param data Extra Metadata
   * @return commit instant
   */
  public HoodieInstant transitionReplaceInflightToComplete(boolean shouldLock,
                                                           HoodieInstant inflightInstant, Option<byte[]> data) {
    ValidationUtils.checkArgument(inflightInstant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION));
    ValidationUtils.checkArgument(inflightInstant.isInflight());
    HoodieInstant commitInstant = new HoodieInstant(State.COMPLETED, REPLACE_COMMIT_ACTION, inflightInstant.getTimestamp());
    // Then write to timeline
    transitionStateToComplete(shouldLock, inflightInstant, commitInstant, data);
    return commitInstant;
  }

  /**
   * Transition cluster inflight to replace committed.
   *
   * @param shouldLock Whether to hold the lock when performing transition
   * @param inflightInstant Inflight instant
   * @param data Extra Metadata
   * @return commit instant
   */
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
                toInstant.getTimestamp(), metaClient.createNewInstantTime(false));
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
    ValidationUtils.checkArgument(completed.getTimestamp().equals(inflight.getTimestamp()));
    StoragePath filePath = getInstantFileNamePath(inflight.getFileName());
    StoragePath commitFilePath = getInstantFileNamePath(getInstantFileName(completed));
    try {
      if (metaClient.getTimelineLayoutVersion().isNullVersion()) {
        if (!metaClient.getStorage().exists(filePath)) {
          boolean success = metaClient.getStorage().rename(commitFilePath, filePath);
          if (!success) {
            throw new HoodieIOException(
                "Could not rename " + commitFilePath + " to " + filePath);
          }
        }
      } else {
        StoragePath requestedInstantFilePath = getInstantFileNamePath(new HoodieInstant(State.REQUESTED, inflight.getAction(),
            inflight.getTimestamp()).getFileName());

        // If inflight and requested files do not exist, create one
        if (!metaClient.getStorage().exists(requestedInstantFilePath)) {
          metaClient.getStorage().create(requestedInstantFilePath, false).close();
        }

        if (!metaClient.getStorage().exists(filePath)) {
          metaClient.getStorage().create(filePath, false).close();
        }

        boolean success = metaClient.getStorage().deleteFile(commitFilePath);
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

  /**
   * Saves content for requested REPLACE instant.
   */
  public void saveToPendingReplaceCommit(HoodieInstant instant, Option<byte[]> content) {
    ValidationUtils.checkArgument(instant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION));
    createFileInMetaPath(instant.getFileName(), content, false);
  }

  /**
   * Saves content for requested CLUSTER instant.
   */
  public void saveToPendingClusterCommit(HoodieInstant instant, Option<byte[]> content) {
    ValidationUtils.checkArgument(instant.getAction().equals(HoodieTimeline.CLUSTERING_ACTION));
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
    transitionPendingState(requestedInstant, inflightInstant, data);
    return inflightInstant;
  }

  /**
   * Transition index instant state from inflight to completed.
   *
   * @param shouldLock Whether to hold the lock when performing transition
   * @param inflightInstant Inflight Instant
   * @return completed instant
   */
  public HoodieInstant transitionIndexInflightToComplete(boolean shouldLock,
                                                         HoodieInstant inflightInstant, Option<byte[]> data) {
    ValidationUtils.checkArgument(inflightInstant.getAction().equals(HoodieTimeline.INDEXING_ACTION),
        String.format("%s is not equal to %s action", inflightInstant.getAction(), INDEXING_ACTION));
    ValidationUtils.checkArgument(inflightInstant.isInflight(),
        String.format("Instant %s not inflight", inflightInstant.getTimestamp()));
    HoodieInstant commitInstant = new HoodieInstant(State.COMPLETED, INDEXING_ACTION, inflightInstant.getTimestamp());
    transitionStateToComplete(shouldLock, inflightInstant, commitInstant, data);
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
      transitionPendingState(inflightInstant, requestedInstant, Option.empty());
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
    StoragePath fullPath = getInstantFileNamePath(filename);
    if (allowOverwrite || metaClient.getTimelineLayoutVersion().isNullVersion()) {
      FileIOUtils.createFileInPath(metaClient.getStorage(), fullPath, content);
    } else {
      metaClient.getStorage().createImmutableFileInPath(fullPath, content);
    }
  }

  protected void createCompleteFileInMetaPath(boolean shouldLock, HoodieInstant instant, Option<byte[]> content) {
    TimeGenerator timeGenerator = TimeGenerators
        .getTimeGenerator(metaClient.getTimeGeneratorConfig(), metaClient.getStorageConf());
    timeGenerator.consumeTimestamp(!shouldLock, currentTimeMillis -> {
      String completionTime = HoodieInstantTimeGenerator.formatDate(new Date(currentTimeMillis));
      String fileName = instant.getFileName(completionTime);
      StoragePath fullPath = getInstantFileNamePath(fileName);
      if (metaClient.getTimelineLayoutVersion().isNullVersion()) {
        FileIOUtils.createFileInPath(metaClient.getStorage(), fullPath, content);
      } else {
        metaClient.getStorage().createImmutableFileInPath(fullPath, content);
      }
      LOG.info("Created new file for toInstant ?" + fullPath);
    });
  }

  protected Option<byte[]> readDataFromPath(StoragePath detailPath) {
    try (InputStream is = metaClient.getStorage().open(detailPath)) {
      return Option.of(FileIOUtils.readAsByteArray(is));
    } catch (IOException e) {
      throw new HoodieIOException("Could not read commit details from " + detailPath, e);
    }
  }

  public HoodieActiveTimeline reload() {
    return new HoodieActiveTimeline(metaClient);
  }

  public void copyInstant(HoodieInstant instant, StoragePath dstDir) {
    StoragePath srcPath = new StoragePath(metaClient.getMetaPath(), getInstantFileName(instant));
    StoragePath dstPath = new StoragePath(dstDir, getInstantFileName(instant));
    try {
      HoodieStorage storage = metaClient.getStorage();
      storage.createDirectory(dstDir);
      FileIOUtils.copy(storage, srcPath, storage, dstPath, false, true);
    } catch (IOException e) {
      throw new HoodieIOException("Could not copy instant from " + srcPath + " to " + dstPath, e);
    }
  }
}
