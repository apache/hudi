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

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Represents the Active Timeline for the HoodieDataset. Instants for the last 12 hours (configurable) is in the
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

  public static final SimpleDateFormat COMMIT_FORMATTER = new SimpleDateFormat("yyyyMMddHHmmss");

  public static final Set<String> VALID_EXTENSIONS_IN_ACTIVE_TIMELINE = new HashSet<>(Arrays.asList(
          new String[]{COMMIT_EXTENSION, INFLIGHT_COMMIT_EXTENSION, REQUESTED_COMMIT_EXTENSION, DELTA_COMMIT_EXTENSION,
              INFLIGHT_DELTA_COMMIT_EXTENSION, REQUESTED_DELTA_COMMIT_EXTENSION, SAVEPOINT_EXTENSION,
              INFLIGHT_SAVEPOINT_EXTENSION, CLEAN_EXTENSION, REQUESTED_CLEAN_EXTENSION, INFLIGHT_CLEAN_EXTENSION,
              INFLIGHT_COMPACTION_EXTENSION, REQUESTED_COMPACTION_EXTENSION, INFLIGHT_RESTORE_EXTENSION, RESTORE_EXTENSION}));

  private static final Logger LOG = LoggerFactory.getLogger(HoodieActiveTimeline.class);
  protected HoodieTableMetaClient metaClient;
  private static AtomicReference<String> lastInstantTime = new AtomicReference<>(String.valueOf(Integer.MIN_VALUE));

  /**
   * Returns next instant time in the {@link #COMMIT_FORMATTER} format.
   * Ensures each instant time is atleast 1 second apart since we create instant times at second granularity
   */
  public static String createNewInstantTime() {
    return lastInstantTime.updateAndGet((oldVal) -> {
      String newCommitTime = null;
      do {
        newCommitTime = HoodieActiveTimeline.COMMIT_FORMATTER.format(new Date());
      } while (HoodieTimeline.compareTimestamps(newCommitTime, oldVal, LESSER_OR_EQUAL));
      return newCommitTime;
    });
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
    LOG.info("Loaded instants {}", getInstants().collect(Collectors.toList()));
  }

  public HoodieActiveTimeline(HoodieTableMetaClient metaClient) {
    this(metaClient, new ImmutableSet.Builder<String>().addAll(VALID_EXTENSIONS_IN_ACTIVE_TIMELINE).build());
  }

  public HoodieActiveTimeline(HoodieTableMetaClient metaClient, boolean applyLayoutFilter) {
    this(metaClient,
        new ImmutableSet.Builder<String>()
            .addAll(VALID_EXTENSIONS_IN_ACTIVE_TIMELINE).build(), applyLayoutFilter);
  }

  /**
   * For serialization and de-serialization only.
   *
   * @deprecated
   */
  public HoodieActiveTimeline() {
  }

  /**
   * This method is only used when this object is deserialized in a spark executor.
   *
   * @deprecated
   */
  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
  }

  /**
   * Get all instants (commits, delta commits) that produce new data, in the active timeline.
   */
  public HoodieTimeline getCommitsTimeline() {
    return getTimelineOfActions(Sets.newHashSet(COMMIT_ACTION, DELTA_COMMIT_ACTION));
  }

  /**
   * Get all instants (commits, delta commits, in-flight/request compaction) that produce new data, in the active
   * timeline * With Async compaction a requested/inflight compaction-instant is a valid baseInstant for a file-slice as
   * there could be delta-commits with that baseInstant.
   */
  @Override
  public HoodieTimeline getCommitsAndCompactionTimeline() {
    return getTimelineOfActions(Sets.newHashSet(COMMIT_ACTION, DELTA_COMMIT_ACTION, COMPACTION_ACTION));
  }

  /**
   * Get all instants (commits, delta commits, clean, savepoint, rollback) that result in actions, in the active
   * timeline.
   */
  public HoodieTimeline getAllCommitsTimeline() {
    return getTimelineOfActions(Sets.newHashSet(COMMIT_ACTION, DELTA_COMMIT_ACTION, CLEAN_ACTION, COMPACTION_ACTION,
        SAVEPOINT_ACTION, ROLLBACK_ACTION));
  }

  /**
   * Get only pure commits (inflight and completed) in the active timeline.
   */
  public HoodieTimeline getCommitTimeline() {
    return getTimelineOfActions(Sets.newHashSet(COMMIT_ACTION));
  }

  /**
   * Get only the delta commits (inflight and completed) in the active timeline.
   */
  public HoodieTimeline getDeltaCommitTimeline() {
    return new HoodieDefaultTimeline(filterInstantsByAction(DELTA_COMMIT_ACTION),
        (Function<HoodieInstant, Option<byte[]>> & Serializable) this::getInstantDetails);
  }

  /**
   * Get a timeline of a specific set of actions. useful to create a merged timeline of multiple actions.
   *
   * @param actions actions allowed in the timeline
   */
  public HoodieTimeline getTimelineOfActions(Set<String> actions) {
    return new HoodieDefaultTimeline(getInstants().filter(s -> actions.contains(s.getAction())),
        (Function<HoodieInstant, Option<byte[]>> & Serializable) this::getInstantDetails);
  }

  /**
   * Get only the cleaner action (inflight and completed) in the active timeline.
   */
  public HoodieTimeline getCleanerTimeline() {
    return new HoodieDefaultTimeline(filterInstantsByAction(CLEAN_ACTION),
        (Function<HoodieInstant, Option<byte[]>> & Serializable) this::getInstantDetails);
  }

  /**
   * Get only the rollback action (inflight and completed) in the active timeline.
   */
  public HoodieTimeline getRollbackTimeline() {
    return new HoodieDefaultTimeline(filterInstantsByAction(ROLLBACK_ACTION),
        (Function<HoodieInstant, Option<byte[]>> & Serializable) this::getInstantDetails);
  }

  /**
   * Get only the save point action (inflight and completed) in the active timeline.
   */
  public HoodieTimeline getSavePointTimeline() {
    return new HoodieDefaultTimeline(filterInstantsByAction(SAVEPOINT_ACTION),
        (Function<HoodieInstant, Option<byte[]>> & Serializable) this::getInstantDetails);
  }

  /**
   * Get only the restore action (inflight and completed) in the active timeline.
   */
  public HoodieTimeline getRestoreTimeline() {
    return new HoodieDefaultTimeline(filterInstantsByAction(RESTORE_ACTION),
        (Function<HoodieInstant, Option<byte[]>> & Serializable) this::getInstantDetails);
  }

  protected Stream<HoodieInstant> filterInstantsByAction(String action) {
    return getInstants().filter(s -> s.getAction().equals(action));
  }

  public void createNewInstant(HoodieInstant instant) {
    LOG.info("Creating a new instant {}", instant);
    // Create the in-flight file
    createFileInMetaPath(instant.getFileName(), Option.empty(), false);
  }

  public void saveAsComplete(HoodieInstant instant, Option<byte[]> data) {
    LOG.info("Marking instant complete {}", instant);
    Preconditions.checkArgument(instant.isInflight(),
            "Could not mark an already completed instant as complete again " + instant);
    transitionState(instant, HoodieTimeline.getCompletedInstant(instant), data);
    LOG.info("Completed {}", instant);
  }

  public HoodieInstant revertToInflight(HoodieInstant instant) {
    LOG.info("Reverting instant to inflight {}", instant);
    HoodieInstant inflight = HoodieTimeline.getInflightInstant(instant, metaClient.getTableType());
    revertCompleteToInflight(instant, inflight);
    LOG.info("Reverted {} to inflight {}", instant, inflight);
    return inflight;
  }

  public void deleteInflight(HoodieInstant instant) {
    Preconditions.checkArgument(instant.isInflight());
    deleteInstantFile(instant);
  }

  public void deletePending(HoodieInstant instant) {
    Preconditions.checkArgument(!instant.isCompleted());
    deleteInstantFile(instant);
  }

  public void deleteCompactionRequested(HoodieInstant instant) {
    Preconditions.checkArgument(instant.isRequested());
    Preconditions.checkArgument(instant.getAction() == HoodieTimeline.COMPACTION_ACTION);
    deleteInstantFile(instant);
  }

  private void deleteInstantFile(HoodieInstant instant) {
    LOG.info("Deleting instant {}", instant);
    Path inFlightCommitFilePath = new Path(metaClient.getMetaPath(), instant.getFileName());
    try {
      boolean result = metaClient.getFs().delete(inFlightCommitFilePath, false);
      if (result) {
        LOG.info("Removed instant {}", instant);
      } else {
        throw new HoodieIOException("Could not delete instant " + instant);
      }
    } catch (IOException e) {
      throw new HoodieIOException("Could not remove inflight commit " + inFlightCommitFilePath, e);
    }
  }

  @Override
  public Option<byte[]> getInstantDetails(HoodieInstant instant) {
    Path detailPath = new Path(metaClient.getMetaPath(), instant.getFileName());
    return readDataFromPath(detailPath);
  }

  //-----------------------------------------------------------------
  //      BEGIN - COMPACTION RELATED META-DATA MANAGEMENT.
  //-----------------------------------------------------------------

  public Option<byte[]> readPlanAsBytes(HoodieInstant instant) {
    Path detailPath = null;
    if (metaClient.getTimelineLayoutVersion().isNullVersion()) {
      detailPath = new Path(metaClient.getMetaAuxiliaryPath(), instant.getFileName());
    } else {
      detailPath = new Path(metaClient.getMetaPath(), instant.getFileName());
    }
    return readDataFromPath(detailPath);
  }

  /**
   * Revert compaction State from inflight to requested.
   *
   * @param inflightInstant Inflight Instant
   * @return requested instant
   */
  public HoodieInstant revertCompactionInflightToRequested(HoodieInstant inflightInstant) {
    Preconditions.checkArgument(inflightInstant.getAction().equals(HoodieTimeline.COMPACTION_ACTION));
    Preconditions.checkArgument(inflightInstant.isInflight());
    HoodieInstant requestedInstant =
        new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, inflightInstant.getTimestamp());
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
    Preconditions.checkArgument(requestedInstant.getAction().equals(HoodieTimeline.COMPACTION_ACTION));
    Preconditions.checkArgument(requestedInstant.isRequested());
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
    Preconditions.checkArgument(inflightInstant.getAction().equals(HoodieTimeline.COMPACTION_ACTION));
    Preconditions.checkArgument(inflightInstant.isInflight());
    HoodieInstant commitInstant = new HoodieInstant(State.COMPLETED, COMMIT_ACTION, inflightInstant.getTimestamp());
    transitionState(inflightInstant, commitInstant, data);
    return commitInstant;
  }

  private void createFileInAuxiliaryFolder(HoodieInstant instant, Option<byte[]> data) {
    if (metaClient.getTimelineLayoutVersion().isNullVersion()) {
      /**
       *  For latest version, since we write immutable files directly in timeline directory, there is no need to write
       *  additional immutable files in .aux folder
       */
      Path fullPath = new Path(metaClient.getMetaAuxiliaryPath(), instant.getFileName());
      createFileInPath(fullPath, data);
    }
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
    Preconditions.checkArgument(inflightInstant.getAction().equals(HoodieTimeline.CLEAN_ACTION));
    Preconditions.checkArgument(inflightInstant.isInflight());
    HoodieInstant commitInstant = new HoodieInstant(State.COMPLETED, CLEAN_ACTION, inflightInstant.getTimestamp());
    // First write metadata to aux folder
    createFileInAuxiliaryFolder(commitInstant, data);
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
    Preconditions.checkArgument(requestedInstant.getAction().equals(HoodieTimeline.CLEAN_ACTION));
    Preconditions.checkArgument(requestedInstant.isRequested());
    HoodieInstant inflight = new HoodieInstant(State.INFLIGHT, CLEAN_ACTION, requestedInstant.getTimestamp());
    transitionState(requestedInstant, inflight, data);
    return inflight;
  }

  private void transitionState(HoodieInstant fromInstant, HoodieInstant toInstant, Option<byte[]> data) {
    Preconditions.checkArgument(fromInstant.getTimestamp().equals(toInstant.getTimestamp()));
    try {
      if (metaClient.getTimelineLayoutVersion().isNullVersion()) {
        // Re-create the .inflight file by opening a new file and write the commit metadata in
        createFileInMetaPath(fromInstant.getFileName(), data, false);
        Path fromInstantPath = new Path(metaClient.getMetaPath(), fromInstant.getFileName());
        Path toInstantPath = new Path(metaClient.getMetaPath(), toInstant.getFileName());
        boolean success = metaClient.getFs().rename(fromInstantPath, toInstantPath);
        if (!success) {
          throw new HoodieIOException("Could not rename " + fromInstantPath + " to " + toInstantPath);
        }
      } else {
        // Ensures old state exists in timeline
        LOG.info("Checking for file exists ?{}", new Path(metaClient.getMetaPath(), fromInstant.getFileName()));
        Preconditions.checkArgument(metaClient.getFs().exists(new Path(metaClient.getMetaPath(),
            fromInstant.getFileName())));
        // Use Write Once to create Target File
        createImmutableFileInPath(new Path(metaClient.getMetaPath(), toInstant.getFileName()), data);
        LOG.info("Create new file for toInstant ?{}", new Path(metaClient.getMetaPath(), toInstant.getFileName()));
      }
    } catch (IOException e) {
      throw new HoodieIOException("Could not complete " + fromInstant, e);
    }
  }

  private void revertCompleteToInflight(HoodieInstant completed, HoodieInstant inflight) {
    Preconditions.checkArgument(completed.getTimestamp().equals(inflight.getTimestamp()));
    Path inFlightCommitFilePath = new Path(metaClient.getMetaPath(), inflight.getFileName());
    Path commitFilePath = new Path(metaClient.getMetaPath(), completed.getFileName());
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
        Path requestedInstantFilePath = new Path(metaClient.getMetaPath(),
            new HoodieInstant(State.REQUESTED, inflight.getAction(), inflight.getTimestamp()).getFileName());

        // If inflight and requested files do not exist, create one
        if (!metaClient.getFs().exists(requestedInstantFilePath)) {
          metaClient.getFs().create(requestedInstantFilePath, false).close();
        }

        if (!metaClient.getFs().exists(inFlightCommitFilePath)) {
          metaClient.getFs().create(inFlightCommitFilePath, false).close();
        }

        boolean success = metaClient.getFs().delete(commitFilePath, false);
        Preconditions.checkArgument(success, "State Reverting failed");
      }
    } catch (IOException e) {
      throw new HoodieIOException("Could not complete revert " + completed, e);
    }
  }

  public void transitionRequestedToInflight(HoodieInstant requested, Option<byte[]> content) {
    HoodieInstant inflight = new HoodieInstant(State.INFLIGHT, requested.getAction(), requested.getTimestamp());
    Preconditions.checkArgument(requested.isRequested(), "Instant " + requested + " in wrong state");
    transitionState(requested, inflight, content);
  }

  public void saveToCompactionRequested(HoodieInstant instant, Option<byte[]> content) {
    saveToCompactionRequested(instant, content, false);
  }

  public void saveToCompactionRequested(HoodieInstant instant, Option<byte[]> content, boolean overwrite) {
    Preconditions.checkArgument(instant.getAction().equals(HoodieTimeline.COMPACTION_ACTION));
    // Write workload to auxiliary folder
    createFileInAuxiliaryFolder(instant, content);
    createFileInMetaPath(instant.getFileName(), content, overwrite);
  }

  public void saveToCleanRequested(HoodieInstant instant, Option<byte[]> content) {
    Preconditions.checkArgument(instant.getAction().equals(HoodieTimeline.CLEAN_ACTION));
    Preconditions.checkArgument(instant.getState().equals(State.REQUESTED));
    // Write workload to auxiliary folder
    createFileInAuxiliaryFolder(instant, content);
    // Plan is stored in meta path
    createFileInMetaPath(instant.getFileName(), content, false);
  }

  private void createFileInMetaPath(String filename, Option<byte[]> content, boolean allowOverwrite) {
    Path fullPath = new Path(metaClient.getMetaPath(), filename);
    if (allowOverwrite || metaClient.getTimelineLayoutVersion().isNullVersion()) {
      createFileInPath(fullPath, content);
    } else {
      createImmutableFileInPath(fullPath, content);
    }
  }

  private void createFileInPath(Path fullPath, Option<byte[]> content) {
    try {
      // If the path does not exist, create it first
      if (!metaClient.getFs().exists(fullPath)) {
        if (metaClient.getFs().createNewFile(fullPath)) {
          LOG.info("Created a new file in meta path: {}", fullPath);
        } else {
          throw new HoodieIOException("Failed to create file " + fullPath);
        }
      }

      if (content.isPresent()) {
        FSDataOutputStream fsout = metaClient.getFs().create(fullPath, true);
        fsout.write(content.get());
        fsout.close();
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to create file " + fullPath, e);
    }
  }

  /**
   * Creates a new file in timeline with overwrite set to false. This ensures
   * files are created only once and never rewritten
   * @param fullPath File Path
   * @param content Content to be stored
   */
  private void createImmutableFileInPath(Path fullPath, Option<byte[]> content) {
    FSDataOutputStream fsout = null;
    try {
      fsout = metaClient.getFs().create(fullPath, false);
      if (content.isPresent()) {
        fsout.write(content.get());
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to create file " + fullPath, e);
    } finally {
      try {
        if (null != fsout) {
          fsout.close();
        }
      } catch (IOException e) {
        throw new HoodieIOException("Failed to close file " + fullPath, e);
      }
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
