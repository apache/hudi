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

import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.ActiveTimelineUtils;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantFactory;
import org.apache.hudi.common.table.timeline.InstantFileNameFactory;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

public class ActiveTimelineV1 extends BaseTimelineV1 implements HoodieActiveTimeline {

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
      REQUESTED_SAVE_SCHEMA_ACTION_EXTENSION, INFLIGHT_SAVE_SCHEMA_ACTION_EXTENSION, SAVE_SCHEMA_ACTION_EXTENSION));

  private static final Logger LOG = LoggerFactory.getLogger(HoodieActiveTimeline.class);
  protected HoodieTableMetaClient metaClient;
  private final InstantFileNameFactory instantFileNameFactory = new InstantFileNameFactoryV1();
  private final InstantFactory instantFactory = new InstantFactoryV1();

  protected ActiveTimelineV1(HoodieTableMetaClient metaClient, Set<String> includedExtensions,
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

  public ActiveTimelineV1(HoodieTableMetaClient metaClient) {
    this(metaClient, Collections.unmodifiableSet(VALID_EXTENSIONS_IN_ACTIVE_TIMELINE), true);
  }

  public ActiveTimelineV1(HoodieTableMetaClient metaClient, boolean applyLayoutFilter) {
    this(metaClient, Collections.unmodifiableSet(VALID_EXTENSIONS_IN_ACTIVE_TIMELINE), applyLayoutFilter);
  }

  /**
   * For serialization and de-serialization only.
   *
   * @deprecated
   */
  @Deprecated
  public ActiveTimelineV1() {
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

  @Override
  public Set<String> getValidExtensionsInActiveTimeline() {
    return Collections.unmodifiableSet(VALID_EXTENSIONS_IN_ACTIVE_TIMELINE);
  }

  @Override
  public void createCompleteInstant(HoodieInstant instant) {
    LOG.info("Creating a new complete instant " + instant);
    createFileInMetaPath(instantFileNameFactory.getFileName(instant), Option.empty(), false);
  }

  @Override
  public void createNewInstant(HoodieInstant instant) {
    LOG.info("Creating a new instant " + instant);
    // Create the in-flight file
    createFileInMetaPath(instantFileNameFactory.getFileName(instant), Option.empty(), false);
  }

  @Override
  public void createRequestedCommitWithReplaceMetadata(String instantTime, String actionType) {
    try {
      HoodieInstant instant = instantFactory.createNewInstant(HoodieInstant.State.REQUESTED, actionType, instantTime);
      LOG.info("Creating a new instant " + instant);
      // Create the request replace file
      createFileInMetaPath(instantFileNameFactory.getFileName(instant),
          TimelineMetadataUtils.serializeRequestedReplaceMetadata(new HoodieRequestedReplaceMetadata()), false);
    } catch (IOException e) {
      throw new HoodieIOException("Error create requested replace commit ", e);
    }
  }

  @Override
  public void saveAsComplete(HoodieInstant instant, Option<byte[]> data) {
    LOG.info("Marking instant complete " + instant);
    ValidationUtils.checkArgument(instant.isInflight(),
        "Could not mark an already completed instant as complete again " + instant);
    transitionState(instant, instantFactory.createNewInstant(HoodieInstant.State.COMPLETED, instant.getAction(), instant.getRequestTime()), data);
    LOG.info("Completed " + instant);
  }

  @Override
  public void saveAsComplete(boolean shouldLock, HoodieInstant instant, Option<byte[]> data) {
    saveAsComplete(instant, data);
  }

  @Override
  public HoodieInstant revertToInflight(HoodieInstant instant) {
    LOG.info("Reverting instant to inflight " + instant);
    HoodieInstant inflight = ActiveTimelineUtils.getInflightInstant(instant, metaClient);
    revertCompleteToInflight(instant, inflight);
    LOG.info("Reverted " + instant + " to inflight " + inflight);
    return inflight;
  }

  @Override
  public void deleteInflight(HoodieInstant instant) {
    ValidationUtils.checkArgument(instant.isInflight());
    deleteInstantFile(instant);
  }

  @Override
  public void deletePending(HoodieInstant instant) {
    ValidationUtils.checkArgument(!instant.isCompleted());
    deleteInstantFile(instant);
  }

  @Override
  public void deleteCompletedRollback(HoodieInstant instant) {
    ValidationUtils.checkArgument(instant.isCompleted());
    deleteInstantFile(instant);
  }

  @Override
  public void deleteEmptyInstantIfExists(HoodieInstant instant) {
    ValidationUtils.checkArgument(isEmpty(instant));
    deleteInstantFileIfExists(instant);
  }

  @Override
  public void deleteCompactionRequested(HoodieInstant instant) {
    ValidationUtils.checkArgument(instant.isRequested());
    ValidationUtils.checkArgument(Objects.equals(instant.getAction(), HoodieTimeline.COMPACTION_ACTION));
    deleteInstantFile(instant);
  }

  @Override
  public void deleteInstantFileIfExists(HoodieInstant instant) {
    LOG.info("Deleting instant " + instant);
    StoragePath commitFilePath = getInstantFileNamePath(instantFileNameFactory.getFileName(instant));
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

  private void deleteInstantFile(HoodieInstant instant) {
    LOG.info("Deleting instant " + instant);
    StoragePath inFlightCommitFilePath = getInstantFileNamePath(instantFileNameFactory.getFileName(instant));
    try {
      boolean result = metaClient.getStorage().deleteFile(inFlightCommitFilePath);
      if (result) {
        LOG.info("Removed instant " + instant);
      } else {
        throw new HoodieIOException("Could not delete instant " + instant + " with path " + inFlightCommitFilePath);
      }
    } catch (IOException e) {
      throw new HoodieIOException("Could not remove inflight commit " + inFlightCommitFilePath, e);
    }
  }

  @Override
  public Option<byte[]> getInstantDetails(HoodieInstant instant) {
    StoragePath detailPath = getInstantFileNamePath(instantFileNameFactory.getFileName(instant));
    return readDataFromPath(detailPath);
  }

  @Override
  public Option<Pair<HoodieInstant, HoodieCommitMetadata>> getLastCommitMetadataWithValidSchema() {
    return Option.fromJavaOptional(
        getCommitMetadataStream()
            .filter(instantCommitMetadataPair ->
                WriteOperationType.canUpdateSchema(instantCommitMetadataPair.getRight().getOperationType())
                    && !StringUtils.isNullOrEmpty(instantCommitMetadataPair.getValue().getMetadata(HoodieCommitMetadata.SCHEMA_KEY)))
            .findFirst()
    );
  }

  @Override
  public Option<Pair<HoodieInstant, HoodieCommitMetadata>> getLastCommitMetadataWithValidData() {
    return Option.fromJavaOptional(
        getCommitMetadataStream()
            .filter(instantCommitMetadataPair ->
                !instantCommitMetadataPair.getValue().getFileIdAndRelativePaths().isEmpty())
            .findFirst()
    );
  }

  private Stream<Pair<HoodieInstant, HoodieCommitMetadata>> getCommitMetadataStream() {
    // NOTE: Streams are lazy
    return getCommitsTimeline().filterCompletedInstants()
        .getInstantsAsStream()
        .sorted(Comparator.comparing(HoodieInstant::getRequestTime).reversed())
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

  @Override
  public Option<byte[]> readCleanerInfoAsBytes(HoodieInstant instant) {
    // Cleaner metadata are always stored only in timeline .hoodie
    return readDataFromPath(getInstantFileNamePath(instantFileNameFactory.getFileName(instant)));
  }

  @Override
  public Option<byte[]> readRollbackInfoAsBytes(HoodieInstant instant) {
    // Rollback metadata are always stored only in timeline .hoodie
    return readDataFromPath(getInstantFileNamePath(instantFileNameFactory.getFileName(instant)));
  }

  @Override
  public Option<byte[]> readRestoreInfoAsBytes(HoodieInstant instant) {
    // Rollback metadata are always stored only in timeline .hoodie
    return readDataFromPath(new StoragePath(metaClient.getMetaPath(), instantFileNameFactory.getFileName(instant)));
  }

  //-----------------------------------------------------------------
  //      BEGIN - COMPACTION RELATED META-DATA MANAGEMENT.
  //-----------------------------------------------------------------
  @Override
  public Option<byte[]> readCompactionPlanAsBytes(HoodieInstant instant) {
    return readDataFromPath(new StoragePath(metaClient.getMetaPath(), instantFileNameFactory.getFileName(instant)));
  }

  @Override
  public Option<byte[]> readIndexPlanAsBytes(HoodieInstant instant) {
    return readDataFromPath(new StoragePath(metaClient.getMetaPath(), instantFileNameFactory.getFileName(instant)));
  }

  @Override
  public HoodieInstant revertInstantFromInflightToRequested(HoodieInstant inflightInstant) {
    ValidationUtils.checkArgument(inflightInstant.isInflight());
    HoodieInstant requestedInstant =
        instantFactory.createNewInstant(HoodieInstant.State.REQUESTED, inflightInstant.getAction(), inflightInstant.getRequestTime());
    if (metaClient.getTimelineLayoutVersion().isNullVersion()) {
      // Pass empty data since it is read from the corresponding .aux/.compaction instant file
      transitionState(inflightInstant, requestedInstant, Option.empty());
    } else {
      deleteInflight(inflightInstant);
    }
    return requestedInstant;
  }

  @Override
  public HoodieInstant revertLogCompactionInflightToRequested(HoodieInstant inflightInstant) {
    ValidationUtils.checkArgument(inflightInstant.getAction().equals(HoodieTimeline.LOG_COMPACTION_ACTION));
    ValidationUtils.checkArgument(inflightInstant.isInflight());
    HoodieInstant requestedInstant =
        instantFactory.createNewInstant(HoodieInstant.State.REQUESTED, LOG_COMPACTION_ACTION, inflightInstant.getRequestTime());
    if (metaClient.getTimelineLayoutVersion().isNullVersion()) {
      // Pass empty data since it is read from the corresponding .aux/.compaction instant file
      transitionState(inflightInstant, requestedInstant, Option.empty());
    } else {
      deleteInflight(inflightInstant);
    }
    return requestedInstant;
  }

  @Override
  public HoodieInstant transitionCompactionRequestedToInflight(HoodieInstant requestedInstant) {
    ValidationUtils.checkArgument(requestedInstant.getAction().equals(HoodieTimeline.COMPACTION_ACTION));
    ValidationUtils.checkArgument(requestedInstant.isRequested());
    HoodieInstant inflightInstant =
        instantFactory.createNewInstant(HoodieInstant.State.INFLIGHT, COMPACTION_ACTION, requestedInstant.getRequestTime());
    transitionState(requestedInstant, inflightInstant, Option.empty());
    return inflightInstant;
  }

  @Override
  public HoodieInstant transitionLogCompactionRequestedToInflight(HoodieInstant requestedInstant) {
    ValidationUtils.checkArgument(requestedInstant.getAction().equals(HoodieTimeline.LOG_COMPACTION_ACTION));
    ValidationUtils.checkArgument(requestedInstant.isRequested());
    HoodieInstant inflightInstant =
        instantFactory.createNewInstant(HoodieInstant.State.INFLIGHT, LOG_COMPACTION_ACTION, requestedInstant.getRequestTime());
    transitionState(requestedInstant, inflightInstant, Option.empty());
    return inflightInstant;
  }

  @Override
  public HoodieInstant transitionCompactionInflightToComplete(boolean shouldLock, HoodieInstant inflightInstant,
                                                              Option<byte[]> data) {
    // Lock is not honored in 0.x mode.
    ValidationUtils.checkArgument(inflightInstant.getAction().equals(HoodieTimeline.COMPACTION_ACTION));
    ValidationUtils.checkArgument(inflightInstant.isInflight());
    HoodieInstant commitInstant = instantFactory.createNewInstant(HoodieInstant.State.COMPLETED, COMMIT_ACTION, inflightInstant.getRequestTime());
    transitionState(inflightInstant, commitInstant, data);
    return commitInstant;
  }

  @Override
  public HoodieInstant transitionLogCompactionInflightToComplete(boolean shouldLock,
                                                                 HoodieInstant inflightInstant, Option<byte[]> data) {
    // Lock is not honored in 0.x mode.
    ValidationUtils.checkArgument(inflightInstant.getAction().equals(HoodieTimeline.LOG_COMPACTION_ACTION));
    ValidationUtils.checkArgument(inflightInstant.isInflight());
    HoodieInstant commitInstant = instantFactory.createNewInstant(HoodieInstant.State.COMPLETED, DELTA_COMMIT_ACTION, inflightInstant.getRequestTime());
    transitionState(inflightInstant, commitInstant, data);
    return commitInstant;
  }
  //-----------------------------------------------------------------
  //      END - COMPACTION RELATED META-DATA MANAGEMENT
  //-----------------------------------------------------------------

  @Override
  public HoodieInstant transitionCleanInflightToComplete(boolean shouldLock, HoodieInstant inflightInstant,
                                                         Option<byte[]> data) {
    ValidationUtils.checkArgument(inflightInstant.getAction().equals(HoodieTimeline.CLEAN_ACTION));
    ValidationUtils.checkArgument(inflightInstant.isInflight());
    HoodieInstant commitInstant = instantFactory.createNewInstant(HoodieInstant.State.COMPLETED, CLEAN_ACTION, inflightInstant.getRequestTime());
    // Then write to timeline
    transitionState(inflightInstant, commitInstant, data);
    return commitInstant;
  }

  @Override
  public HoodieInstant transitionCleanRequestedToInflight(HoodieInstant requestedInstant, Option<byte[]> data) {
    ValidationUtils.checkArgument(requestedInstant.getAction().equals(HoodieTimeline.CLEAN_ACTION));
    ValidationUtils.checkArgument(requestedInstant.isRequested());
    HoodieInstant inflight = instantFactory.createNewInstant(HoodieInstant.State.INFLIGHT, CLEAN_ACTION, requestedInstant.getRequestTime());
    transitionState(requestedInstant, inflight, data);
    return inflight;
  }

  @Override
  public HoodieInstant transitionRollbackInflightToComplete(boolean shouldLock,
                                                            HoodieInstant inflightInstant, Option<byte[]> data) {
    ValidationUtils.checkArgument(inflightInstant.getAction().equals(HoodieTimeline.ROLLBACK_ACTION));
    ValidationUtils.checkArgument(inflightInstant.isInflight());
    HoodieInstant commitInstant = instantFactory.createNewInstant(HoodieInstant.State.COMPLETED, ROLLBACK_ACTION, inflightInstant.getRequestTime());
    // Then write to timeline
    transitionState(inflightInstant, commitInstant, data);
    return commitInstant;
  }

  @Override
  public HoodieInstant transitionRollbackRequestedToInflight(HoodieInstant requestedInstant) {
    ValidationUtils.checkArgument(requestedInstant.getAction().equals(HoodieTimeline.ROLLBACK_ACTION));
    ValidationUtils.checkArgument(requestedInstant.isRequested());
    HoodieInstant inflight = instantFactory.createNewInstant(HoodieInstant.State.INFLIGHT, ROLLBACK_ACTION, requestedInstant.getRequestTime());
    transitionState(requestedInstant, inflight, Option.empty());
    return inflight;
  }

  @Override
  public HoodieInstant transitionRestoreRequestedToInflight(HoodieInstant requestedInstant) {
    ValidationUtils.checkArgument(requestedInstant.getAction().equals(HoodieTimeline.RESTORE_ACTION), "Transition to inflight requested for a restore instant with diff action "
        + requestedInstant.toString());
    ValidationUtils.checkArgument(requestedInstant.isRequested(), "Transition to inflight requested for an instant not in requested state " + requestedInstant.toString());
    HoodieInstant inflight = instantFactory.createNewInstant(HoodieInstant.State.INFLIGHT, RESTORE_ACTION, requestedInstant.getRequestTime());
    transitionState(requestedInstant, inflight, Option.empty());
    return inflight;
  }

  @Override
  public HoodieInstant transitionReplaceRequestedToInflight(HoodieInstant requestedInstant, Option<byte[]> data) {
    ValidationUtils.checkArgument(requestedInstant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION));
    ValidationUtils.checkArgument(requestedInstant.isRequested());
    HoodieInstant inflightInstant = instantFactory.createNewInstant(HoodieInstant.State.INFLIGHT, REPLACE_COMMIT_ACTION, requestedInstant.getRequestTime());
    // Then write to timeline
    transitionState(requestedInstant, inflightInstant, data);
    return inflightInstant;
  }

  @Override
  public HoodieInstant transitionClusterRequestedToInflight(HoodieInstant requestedInstant, Option<byte[]> data) {
    // In 0.x, no separate clustering action, reuse replace action.
    return transitionReplaceRequestedToInflight(requestedInstant, data);
  }

  @Override
  public HoodieInstant transitionReplaceInflightToComplete(boolean shouldLock,
                                                           HoodieInstant inflightInstant, Option<byte[]> data) {
    ValidationUtils.checkArgument(inflightInstant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION));
    ValidationUtils.checkArgument(inflightInstant.isInflight());
    HoodieInstant commitInstant = instantFactory.createNewInstant(HoodieInstant.State.COMPLETED, REPLACE_COMMIT_ACTION, inflightInstant.getRequestTime());
    // Then write to timeline
    transitionState(inflightInstant, commitInstant, data);
    return commitInstant;
  }

  @Override
  public HoodieInstant transitionClusterInflightToComplete(boolean shouldLock, HoodieInstant inflightInstant, Option<byte[]> data) {
    // In 0.x, no separate clustering action, reuse replace action.
    return transitionReplaceInflightToComplete(shouldLock, inflightInstant, data);
  }

  private void transitionState(HoodieInstant fromInstant, HoodieInstant toInstant, Option<byte[]> data) {
    transitionState(fromInstant, toInstant, data, false);
  }

  protected void transitionState(HoodieInstant fromInstant, HoodieInstant toInstant, Option<byte[]> data,
                                 boolean allowRedundantTransitions) {
    ValidationUtils.checkArgument(fromInstant.getRequestTime().equals(toInstant.getRequestTime()), String.format("%s and %s are not consistent when transition state.", fromInstant, toInstant));
    try {
      HoodieStorage storage = metaClient.getStorage();
      if (metaClient.getTimelineLayoutVersion().isNullVersion()) {
        // Re-create the .inflight file by opening a new file and write the commit metadata in
        createFileInMetaPath(instantFileNameFactory.getFileName(fromInstant), data, allowRedundantTransitions);
        StoragePath fromInstantPath = getInstantFileNamePath(instantFileNameFactory.getFileName(fromInstant));
        StoragePath toInstantPath = getInstantFileNamePath(instantFileNameFactory.getFileName(toInstant));
        boolean success = storage.rename(fromInstantPath, toInstantPath);
        if (!success) {
          throw new HoodieIOException("Could not rename " + fromInstantPath + " to " + toInstantPath);
        }
      } else {
        // Ensures old state exists in timeline
        ValidationUtils.checkArgument(storage.exists(getInstantFileNamePath(instantFileNameFactory.getFileName(fromInstant))),
            "File " + getInstantFileNamePath(instantFileNameFactory.getFileName(fromInstant)) + " does not exist!");
        // Use Write Once to create Target File
        if (allowRedundantTransitions) {
          FileIOUtils.createFileInPath(storage, getInstantFileNamePath(instantFileNameFactory.getFileName(toInstant)), data);
        } else {
          storage.createImmutableFileInPath(getInstantFileNamePath(instantFileNameFactory.getFileName(toInstant)), data);
        }
        LOG.info("Create new file for toInstant ?" + getInstantFileNamePath(instantFileNameFactory.getFileName(toInstant)));
      }
    } catch (IOException e) {
      throw new HoodieIOException("Could not complete " + fromInstant, e);
    }
  }

  protected void revertCompleteToInflight(HoodieInstant completed, HoodieInstant inflight) {
    ValidationUtils.checkArgument(completed.getRequestTime().equals(inflight.getRequestTime()));
    StoragePath inFlightCommitFilePath = getInstantFileNamePath(instantFileNameFactory.getFileName(inflight));
    StoragePath commitFilePath = getInstantFileNamePath(instantFileNameFactory.getFileName(completed));
    try {
      if (metaClient.getTimelineLayoutVersion().isNullVersion()) {
        if (!metaClient.getStorage().exists(inFlightCommitFilePath)) {
          boolean success = metaClient.getStorage().rename(commitFilePath, inFlightCommitFilePath);
          if (!success) {
            throw new HoodieIOException(
                "Could not rename " + commitFilePath + " to " + inFlightCommitFilePath);
          }
        }
      } else {
        StoragePath requestedInstantFilePath = getInstantFileNamePath(
            instantFileNameFactory.getFileName(instantFactory.createNewInstant(HoodieInstant.State.REQUESTED,
                inflight.getAction(), inflight.getRequestTime())));

        // If inflight and requested files do not exist, create one
        if (!metaClient.getStorage().exists(requestedInstantFilePath)) {
          metaClient.getStorage().create(requestedInstantFilePath, false).close();
        }

        if (!metaClient.getStorage().exists(inFlightCommitFilePath)) {
          metaClient.getStorage().create(inFlightCommitFilePath, false).close();
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

  @Override
  public void transitionRequestedToInflight(String commitType, String inFlightInstant) {
    HoodieInstant requested = instantFactory.createNewInstant(HoodieInstant.State.REQUESTED, commitType, inFlightInstant);
    transitionRequestedToInflight(requested, Option.empty(), false);
  }

  @Override
  public void transitionRequestedToInflight(HoodieInstant requested, Option<byte[]> content) {
    transitionRequestedToInflight(requested, content, false);
  }

  @Override
  public void transitionRequestedToInflight(HoodieInstant requested, Option<byte[]> content,
                                            boolean allowRedundantTransitions) {
    HoodieInstant inflight = instantFactory.createNewInstant(HoodieInstant.State.INFLIGHT, requested.getAction(), requested.getRequestTime());
    ValidationUtils.checkArgument(requested.isRequested(), "Instant " + requested + " in wrong state");
    transitionState(requested, inflight, content, allowRedundantTransitions);
  }

  @Override
  public void saveToCompactionRequested(HoodieInstant instant, Option<byte[]> content) {
    saveToCompactionRequested(instant, content, false);
  }

  @Override
  public void saveToCompactionRequested(HoodieInstant instant, Option<byte[]> content, boolean overwrite) {
    ValidationUtils.checkArgument(instant.getAction().equals(HoodieTimeline.COMPACTION_ACTION));
    createFileInMetaPath(instantFileNameFactory.getFileName(instant), content, overwrite);
  }

  @Override
  public void saveToLogCompactionRequested(HoodieInstant instant, Option<byte[]> content) {
    saveToLogCompactionRequested(instant, content, false);
  }

  @Override
  public void saveToLogCompactionRequested(HoodieInstant instant, Option<byte[]> content, boolean overwrite) {
    ValidationUtils.checkArgument(instant.getAction().equals(HoodieTimeline.LOG_COMPACTION_ACTION));
    createFileInMetaPath(instantFileNameFactory.getFileName(instant), content, overwrite);
  }

  @Override
  public void saveToPendingReplaceCommit(HoodieInstant instant, Option<byte[]> content) {
    ValidationUtils.checkArgument(instant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION));
    createFileInMetaPath(instantFileNameFactory.getFileName(instant), content, false);
  }

  @Override
  public void saveToPendingClusterCommit(HoodieInstant instant, Option<byte[]> content) {
    // In 0.x, no separate clustering action, reuse replace action.
    saveToPendingReplaceCommit(instant, content);
  }

  @Override
  public void saveToCleanRequested(HoodieInstant instant, Option<byte[]> content) {
    ValidationUtils.checkArgument(instant.getAction().equals(HoodieTimeline.CLEAN_ACTION));
    ValidationUtils.checkArgument(instant.getState().equals(HoodieInstant.State.REQUESTED));
    // Plan is stored in meta path
    createFileInMetaPath(instantFileNameFactory.getFileName(instant), content, false);
  }

  @Override
  public void saveToRollbackRequested(HoodieInstant instant, Option<byte[]> content) {
    ValidationUtils.checkArgument(instant.getAction().equals(HoodieTimeline.ROLLBACK_ACTION));
    ValidationUtils.checkArgument(instant.getState().equals(HoodieInstant.State.REQUESTED));
    // Plan is stored in meta path
    createFileInMetaPath(instantFileNameFactory.getFileName(instant), content, false);
  }

  @Override
  public void saveToRestoreRequested(HoodieInstant instant, Option<byte[]> content) {
    ValidationUtils.checkArgument(instant.getAction().equals(HoodieTimeline.RESTORE_ACTION));
    ValidationUtils.checkArgument(instant.getState().equals(HoodieInstant.State.REQUESTED));
    // Plan is stored in meta path
    createFileInMetaPath(instantFileNameFactory.getFileName(instant), content, false);
  }

  @Override
  public HoodieInstant transitionIndexRequestedToInflight(HoodieInstant requestedInstant, Option<byte[]> data) {
    ValidationUtils.checkArgument(requestedInstant.getAction().equals(HoodieTimeline.INDEXING_ACTION),
        String.format("%s is not equal to %s action", requestedInstant.getAction(), INDEXING_ACTION));
    ValidationUtils.checkArgument(requestedInstant.isRequested(),
        String.format("Instant %s not in requested state", requestedInstant.getRequestTime()));
    HoodieInstant inflightInstant = instantFactory.createNewInstant(HoodieInstant.State.INFLIGHT, INDEXING_ACTION, requestedInstant.getRequestTime());
    transitionState(requestedInstant, inflightInstant, data);
    return inflightInstant;
  }

  @Override
  public HoodieInstant transitionIndexInflightToComplete(boolean shouldLock,
                                                         HoodieInstant inflightInstant, Option<byte[]> data) {
    ValidationUtils.checkArgument(inflightInstant.getAction().equals(HoodieTimeline.INDEXING_ACTION),
        String.format("%s is not equal to %s action", inflightInstant.getAction(), INDEXING_ACTION));
    ValidationUtils.checkArgument(inflightInstant.isInflight(),
        String.format("Instant %s not inflight", inflightInstant.getRequestTime()));
    HoodieInstant commitInstant = instantFactory.createNewInstant(HoodieInstant.State.COMPLETED, INDEXING_ACTION, inflightInstant.getRequestTime());
    transitionState(inflightInstant, commitInstant, data);
    return commitInstant;
  }

  @Override
  public HoodieInstant revertIndexInflightToRequested(HoodieInstant inflightInstant) {
    ValidationUtils.checkArgument(inflightInstant.getAction().equals(HoodieTimeline.INDEXING_ACTION),
        String.format("%s is not equal to %s action", inflightInstant.getAction(), INDEXING_ACTION));
    ValidationUtils.checkArgument(inflightInstant.isInflight(),
        String.format("Instant %s not inflight", inflightInstant.getRequestTime()));
    HoodieInstant requestedInstant = instantFactory.createNewInstant(HoodieInstant.State.REQUESTED, INDEXING_ACTION, inflightInstant.getRequestTime());
    if (metaClient.getTimelineLayoutVersion().isNullVersion()) {
      transitionState(inflightInstant, requestedInstant, Option.empty());
    } else {
      deleteInflight(inflightInstant);
    }
    return requestedInstant;
  }

  @Override
  public void saveToPendingIndexAction(HoodieInstant instant, Option<byte[]> content) {
    ValidationUtils.checkArgument(instant.getAction().equals(HoodieTimeline.INDEXING_ACTION),
        String.format("%s is not equal to %s action", instant.getAction(), INDEXING_ACTION));
    createFileInMetaPath(instantFileNameFactory.getFileName(instant), content, false);
  }

  protected void createFileInMetaPath(String filename, Option<byte[]> content, boolean allowOverwrite) {
    StoragePath fullPath = getInstantFileNamePath(filename);
    if (allowOverwrite || metaClient.getTimelineLayoutVersion().isNullVersion()) {
      FileIOUtils.createFileInPath(metaClient.getStorage(), fullPath, content);
    } else {
      metaClient.getStorage().createImmutableFileInPath(fullPath, content);
    }
  }

  protected Option<byte[]> readDataFromPath(StoragePath detailPath) {
    try (InputStream is = metaClient.getStorage().open(detailPath)) {
      return Option.of(FileIOUtils.readAsByteArray(is));
    } catch (IOException e) {
      throw new HoodieIOException("Could not read commit details from " + detailPath, e);
    }
  }

  @Override
  public HoodieActiveTimeline reload() {
    return new ActiveTimelineV1(metaClient);
  }

  @Override
  public void copyInstant(HoodieInstant instant, StoragePath dstDir) {
    StoragePath srcPath = new StoragePath(metaClient.getMetaPath(), instantFileNameFactory.getFileName(instant));
    StoragePath dstPath = new StoragePath(dstDir, instantFileNameFactory.getFileName(instant));
    try {
      HoodieStorage storage = metaClient.getStorage();
      storage.createDirectory(dstDir);
      FileIOUtils.copy(storage, srcPath, storage, dstPath, false, true);
    } catch (IOException e) {
      throw new HoodieIOException("Could not copy instant from " + srcPath + " to " + dstPath, e);
    }
  }

  @Override
  public Set<String> getValidExtensions() {
    return Collections.emptySet();
  }
}
