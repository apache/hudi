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
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
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

  public static final Set<String> VALID_EXTENSIONS_IN_ACTIVE_TIMELINE =
      new HashSet<>(Arrays.asList(new String[] {COMMIT_EXTENSION, INFLIGHT_COMMIT_EXTENSION, DELTA_COMMIT_EXTENSION,
          INFLIGHT_DELTA_COMMIT_EXTENSION, SAVEPOINT_EXTENSION, INFLIGHT_SAVEPOINT_EXTENSION, CLEAN_EXTENSION,
          INFLIGHT_CLEAN_EXTENSION, REQUESTED_CLEAN_EXTENSION, INFLIGHT_COMPACTION_EXTENSION,
          REQUESTED_COMPACTION_EXTENSION, INFLIGHT_RESTORE_EXTENSION, RESTORE_EXTENSION}));

  private static final transient Logger LOG = LogManager.getLogger(HoodieActiveTimeline.class);
  protected HoodieTableMetaClient metaClient;
  private static AtomicReference<String> lastInstantTime = new AtomicReference<>(String.valueOf(Integer.MIN_VALUE));

  /**
   * Returns next commit time in the {@link #COMMIT_FORMATTER} format.
   * Ensures each commit time is atleast 1 second apart since we create COMMIT times at second granularity
   */
  public static String createNewCommitTime() {
    lastInstantTime.updateAndGet((oldVal) -> {
      String newCommitTime = null;
      do {
        newCommitTime = HoodieActiveTimeline.COMMIT_FORMATTER.format(new Date());
      } while (HoodieTimeline.compareTimestamps(newCommitTime, oldVal, LESSER_OR_EQUAL));
      return newCommitTime;
    });
    return lastInstantTime.get();
  }

  protected HoodieActiveTimeline(HoodieTableMetaClient metaClient, Set<String> includedExtensions) {
    // Filter all the filter in the metapath and include only the extensions passed and
    // convert them into HoodieInstant
    try {
      this.setInstants(HoodieTableMetaClient.scanHoodieInstantsFromFileSystem(metaClient.getFs(),
          new Path(metaClient.getMetaPath()), includedExtensions));
      LOG.info("Loaded instants " + getInstants());
    } catch (IOException e) {
      throw new HoodieIOException("Failed to scan metadata", e);
    }
    this.metaClient = metaClient;
    // multiple casts will make this lambda serializable -
    // http://docs.oracle.com/javase/specs/jls/se8/html/jls-15.html#jls-15.16
    this.details = (Function<HoodieInstant, Option<byte[]>> & Serializable) this::getInstantDetails;
  }

  public HoodieActiveTimeline(HoodieTableMetaClient metaClient) {
    this(metaClient, new ImmutableSet.Builder<String>().addAll(VALID_EXTENSIONS_IN_ACTIVE_TIMELINE).build());
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

  public void createInflight(HoodieInstant instant) {
    LOG.info("Creating a new in-flight instant " + instant);
    // Create the in-flight file
    createFileInMetaPath(instant.getFileName(), Option.empty());
  }

  public void saveAsComplete(HoodieInstant instant, Option<byte[]> data) {
    LOG.info("Marking instant complete " + instant);
    Preconditions.checkArgument(instant.isInflight(),
        "Could not mark an already completed instant as complete again " + instant);
    transitionState(instant, HoodieTimeline.getCompletedInstant(instant), data);
    LOG.info("Completed " + instant);
  }

  public void revertToInflight(HoodieInstant instant) {
    LOG.info("Reverting " + instant + " to inflight ");
    revertStateTransition(instant, HoodieTimeline.getInflightInstant(instant));
    LOG.info("Reverted " + instant + " to inflight");
  }

  public HoodieInstant revertToRequested(HoodieInstant instant) {
    LOG.warn("Reverting " + instant + " to requested ");
    HoodieInstant requestedInstant = HoodieTimeline.getRequestedInstant(instant);
    revertStateTransition(instant, HoodieTimeline.getRequestedInstant(instant));
    LOG.warn("Reverted " + instant + " to requested");
    return requestedInstant;
  }

  public void deleteInflight(HoodieInstant instant) {
    Preconditions.checkArgument(instant.isInflight());
    deleteInstantFile(instant);
  }

  public void deleteCompactionRequested(HoodieInstant instant) {
    Preconditions.checkArgument(instant.isRequested());
    Preconditions.checkArgument(instant.getAction() == HoodieTimeline.COMPACTION_ACTION);
    deleteInstantFile(instant);
  }

  private void deleteInstantFile(HoodieInstant instant) {
    LOG.info("Deleting instant " + instant);
    Path inFlightCommitFilePath = new Path(metaClient.getMetaPath(), instant.getFileName());
    try {
      boolean result = metaClient.getFs().delete(inFlightCommitFilePath, false);
      if (result) {
        LOG.info("Removed in-flight " + instant);
      } else {
        throw new HoodieIOException("Could not delete in-flight instant " + instant);
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

  public Option<byte[]> getInstantAuxiliaryDetails(HoodieInstant instant) {
    Path detailPath = new Path(metaClient.getMetaAuxiliaryPath(), instant.getFileName());
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
    // Pass empty data since it is read from the corresponding .aux/.compaction instant file
    transitionState(inflightInstant, requestedInstant, Option.empty());
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
    Path fullPath = new Path(metaClient.getMetaAuxiliaryPath(), instant.getFileName());
    createFileInPath(fullPath, data);
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
   * @return commit instant
   */
  public HoodieInstant transitionCleanRequestedToInflight(HoodieInstant requestedInstant) {
    Preconditions.checkArgument(requestedInstant.getAction().equals(HoodieTimeline.CLEAN_ACTION));
    Preconditions.checkArgument(requestedInstant.isRequested());
    HoodieInstant inflight = new HoodieInstant(State.INFLIGHT, CLEAN_ACTION, requestedInstant.getTimestamp());
    transitionState(requestedInstant, inflight, Option.empty());
    return inflight;
  }

  private void transitionState(HoodieInstant fromInstant, HoodieInstant toInstant, Option<byte[]> data) {
    Preconditions.checkArgument(fromInstant.getTimestamp().equals(toInstant.getTimestamp()));
    Path commitFilePath = new Path(metaClient.getMetaPath(), toInstant.getFileName());
    try {
      // Re-create the .inflight file by opening a new file and write the commit metadata in
      Path inflightCommitFile = new Path(metaClient.getMetaPath(), fromInstant.getFileName());
      createFileInMetaPath(fromInstant.getFileName(), data);
      boolean success = metaClient.getFs().rename(inflightCommitFile, commitFilePath);
      if (!success) {
        throw new HoodieIOException("Could not rename " + inflightCommitFile + " to " + commitFilePath);
      }
    } catch (IOException e) {
      throw new HoodieIOException("Could not complete " + fromInstant, e);
    }
  }

  private void revertStateTransition(HoodieInstant curr, HoodieInstant revert) {
    Preconditions.checkArgument(curr.getTimestamp().equals(revert.getTimestamp()));
    Path revertFilePath = new Path(metaClient.getMetaPath(), revert.getFileName());
    try {
      if (!metaClient.getFs().exists(revertFilePath)) {
        Path currFilePath = new Path(metaClient.getMetaPath(), curr.getFileName());
        boolean success = metaClient.getFs().rename(currFilePath, revertFilePath);
        if (!success) {
          throw new HoodieIOException("Could not rename " + currFilePath + " to " + revertFilePath);
        }
        LOG.info("Renamed " + currFilePath + " to " + revertFilePath);
      }
    } catch (IOException e) {
      throw new HoodieIOException("Could not complete revert " + curr, e);
    }
  }

  public void saveToInflight(HoodieInstant instant, Option<byte[]> content) {
    Preconditions.checkArgument(instant.isInflight());
    createFileInMetaPath(instant.getFileName(), content);
  }

  public void saveToCompactionRequested(HoodieInstant instant, Option<byte[]> content) {
    Preconditions.checkArgument(instant.getAction().equals(HoodieTimeline.COMPACTION_ACTION));
    // Write workload to auxiliary folder
    createFileInAuxiliaryFolder(instant, content);
    createFileInMetaPath(instant.getFileName(), content);
  }

  public void saveToCleanRequested(HoodieInstant instant, Option<byte[]> content) {
    Preconditions.checkArgument(instant.getAction().equals(HoodieTimeline.CLEAN_ACTION));
    Preconditions.checkArgument(instant.getState().equals(State.REQUESTED));
    // Write workload to auxiliary folder
    createFileInAuxiliaryFolder(instant, content);
    // Plan is only stored in auxiliary folder
    createFileInMetaPath(instant.getFileName(), Option.empty());
  }

  private void createFileInMetaPath(String filename, Option<byte[]> content) {
    Path fullPath = new Path(metaClient.getMetaPath(), filename);
    createFileInPath(fullPath, content);
  }

  private void createFileInPath(Path fullPath, Option<byte[]> content) {
    try {
      // If the path does not exist, create it first
      if (!metaClient.getFs().exists(fullPath)) {
        if (metaClient.getFs().createNewFile(fullPath)) {
          LOG.info("Created a new file in meta path: " + fullPath);
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
