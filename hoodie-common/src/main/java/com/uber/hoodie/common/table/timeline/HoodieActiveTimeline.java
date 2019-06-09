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

package com.uber.hoodie.common.table.timeline;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant.State;
import com.uber.hoodie.exception.HoodieIOException;
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Represents the Active Timeline for the HoodieDataset. Instants for the last 12 hours
 * (configurable) is in the ActiveTimeline and the rest are Archived. ActiveTimeline is a special
 * timeline that allows for creation of instants on the timeline. <p></p> The timeline is not
 * automatically reloaded on any mutation operation, clients have to manually call reload() so that
 * they can chain multiple mutations to the timeline and then call reload() once. <p></p> This class
 * can be serialized and de-serialized and on de-serialization the FileSystem is re-initialized.
 */
public class HoodieActiveTimeline extends HoodieDefaultTimeline {

  public static final SimpleDateFormat COMMIT_FORMATTER = new SimpleDateFormat("yyyyMMddHHmmss");

  public static final Set<String> VALID_EXTENSIONS_IN_ACTIVE_TIMELINE = new HashSet<>(Arrays.asList(
      new String[]{COMMIT_EXTENSION, INFLIGHT_COMMIT_EXTENSION, DELTA_COMMIT_EXTENSION,
          INFLIGHT_DELTA_COMMIT_EXTENSION, SAVEPOINT_EXTENSION, INFLIGHT_SAVEPOINT_EXTENSION,
          CLEAN_EXTENSION, INFLIGHT_CLEAN_EXTENSION, INFLIGHT_COMPACTION_EXTENSION, REQUESTED_COMPACTION_EXTENSION,
          INFLIGHT_RESTORE_EXTENSION, RESTORE_EXTENSION}));

  private static final transient Logger log = LogManager.getLogger(HoodieActiveTimeline.class);
  protected HoodieTableMetaClient metaClient;

  /**
   * Returns next commit time in the {@link #COMMIT_FORMATTER} format.
   */
  public static String createNewCommitTime() {
    return HoodieActiveTimeline.COMMIT_FORMATTER.format(new Date());
  }

  protected HoodieActiveTimeline(HoodieTableMetaClient metaClient, Set<String> includedExtensions) {
    // Filter all the filter in the metapath and include only the extensions passed and
    // convert them into HoodieInstant
    try {
      this.setInstants(HoodieTableMetaClient.scanHoodieInstantsFromFileSystem(metaClient.getFs(),
          new Path(metaClient.getMetaPath()), includedExtensions));
      log.info("Loaded instants " + getInstants());
    } catch (IOException e) {
      throw new HoodieIOException("Failed to scan metadata", e);
    }
    this.metaClient = metaClient;
    // multiple casts will make this lambda serializable -
    // http://docs.oracle.com/javase/specs/jls/se8/html/jls-15.html#jls-15.16
    this.details =
        (Function<HoodieInstant, Optional<byte[]>> & Serializable) this::getInstantDetails;
  }

  public HoodieActiveTimeline(HoodieTableMetaClient metaClient) {
    this(metaClient,
        new ImmutableSet.Builder<String>()
            .addAll(VALID_EXTENSIONS_IN_ACTIVE_TIMELINE).build());
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
  private void readObject(java.io.ObjectInputStream in)
      throws IOException, ClassNotFoundException {
    in.defaultReadObject();
  }

  /**
   * Get all instants (commits, delta commits) that produce new data, in the active timeline *
   *
   */
  public HoodieTimeline getCommitsTimeline() {
    return getTimelineOfActions(
        Sets.newHashSet(COMMIT_ACTION, DELTA_COMMIT_ACTION));
  }

  /**
   * Get all instants (commits, delta commits, in-flight/request compaction) that produce new data, in the active
   * timeline *
   * With Async compaction a requested/inflight compaction-instant is a valid baseInstant for a file-slice as there
   * could be delta-commits with that baseInstant.
   */
  public HoodieTimeline getCommitsAndCompactionTimeline() {
    return getTimelineOfActions(
        Sets.newHashSet(COMMIT_ACTION, DELTA_COMMIT_ACTION, COMPACTION_ACTION));
  }

  /**
   * Get all instants (commits, delta commits, clean, savepoint, rollback) that result in actions,
   * in the active timeline *
   */
  public HoodieTimeline getAllCommitsTimeline() {
    return getTimelineOfActions(
        Sets.newHashSet(COMMIT_ACTION, DELTA_COMMIT_ACTION, CLEAN_ACTION, COMPACTION_ACTION,
            SAVEPOINT_ACTION, ROLLBACK_ACTION));
  }

  /**
   * Get only pure commits (inflight and completed) in the active timeline
   */
  public HoodieTimeline getCommitTimeline() {
    return getTimelineOfActions(Sets.newHashSet(COMMIT_ACTION));
  }

  /**
   * Get only the delta commits (inflight and completed) in the active timeline
   */
  public HoodieTimeline getDeltaCommitTimeline() {
    return new HoodieDefaultTimeline(filterInstantsByAction(DELTA_COMMIT_ACTION),
        (Function<HoodieInstant, Optional<byte[]>> & Serializable) this::getInstantDetails);
  }

  /**
   * Get a timeline of a specific set of actions. useful to create a merged timeline of multiple
   * actions
   *
   * @param actions actions allowed in the timeline
   */
  public HoodieTimeline getTimelineOfActions(Set<String> actions) {
    return new HoodieDefaultTimeline(getInstants().filter(s -> actions.contains(s.getAction())),
        (Function<HoodieInstant, Optional<byte[]>> & Serializable) this::getInstantDetails);
  }


  /**
   * Get only the cleaner action (inflight and completed) in the active timeline
   */
  public HoodieTimeline getCleanerTimeline() {
    return new HoodieDefaultTimeline(filterInstantsByAction(CLEAN_ACTION),
        (Function<HoodieInstant, Optional<byte[]>> & Serializable) this::getInstantDetails);
  }

  /**
   * Get only the rollback action (inflight and completed) in the active timeline
   */
  public HoodieTimeline getRollbackTimeline() {
    return new HoodieDefaultTimeline(filterInstantsByAction(ROLLBACK_ACTION),
        (Function<HoodieInstant, Optional<byte[]>> & Serializable) this::getInstantDetails);
  }

  /**
   * Get only the save point action (inflight and completed) in the active timeline
   */
  public HoodieTimeline getSavePointTimeline() {
    return new HoodieDefaultTimeline(filterInstantsByAction(SAVEPOINT_ACTION),
        (Function<HoodieInstant, Optional<byte[]>> & Serializable) this::getInstantDetails);
  }

  /**
   * Get only the restore action (inflight and completed) in the active timeline
   */
  public HoodieTimeline getRestoreTimeline() {
    return new HoodieDefaultTimeline(filterInstantsByAction(RESTORE_ACTION),
        (Function<HoodieInstant, Optional<byte[]>> & Serializable) this::getInstantDetails);
  }

  protected Stream<HoodieInstant> filterInstantsByAction(String action) {
    return getInstants().filter(s -> s.getAction().equals(action));
  }

  public void createInflight(HoodieInstant instant) {
    log.info("Creating a new in-flight instant " + instant);
    // Create the in-flight file
    createFileInMetaPath(instant.getFileName(), Optional.empty());
  }

  public void saveAsComplete(HoodieInstant instant, Optional<byte[]> data) {
    log.info("Marking instant complete " + instant);
    Preconditions.checkArgument(instant.isInflight(),
        "Could not mark an already completed instant as complete again " + instant);
    transitionState(instant, HoodieTimeline.getCompletedInstant(instant), data);
    log.info("Completed " + instant);
  }

  public void revertToInflight(HoodieInstant instant) {
    log.info("Reverting instant to inflight " + instant);
    revertCompleteToInflight(instant, HoodieTimeline.getInflightInstant(instant));
    log.info("Reverted " + instant + " to inflight");
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
    log.info("Deleting instant " + instant);
    Path inFlightCommitFilePath = new Path(metaClient.getMetaPath(), instant.getFileName());
    try {
      boolean result = metaClient.getFs().delete(inFlightCommitFilePath, false);
      if (result) {
        log.info("Removed in-flight " + instant);
      } else {
        throw new HoodieIOException("Could not delete in-flight instant " + instant);
      }
    } catch (IOException e) {
      throw new HoodieIOException(
          "Could not remove inflight commit " + inFlightCommitFilePath, e);
    }
  }

  @Override
  public Optional<byte[]> getInstantDetails(HoodieInstant instant) {
    Path detailPath = new Path(metaClient.getMetaPath(), instant.getFileName());
    return readDataFromPath(detailPath);
  }

  /** BEGIN - COMPACTION RELATED META-DATA MANAGEMENT **/

  public Optional<byte[]> getInstantAuxiliaryDetails(HoodieInstant instant) {
    Path detailPath = new Path(metaClient.getMetaAuxiliaryPath(), instant.getFileName());
    return readDataFromPath(detailPath);
  }

  /**
   * Revert compaction State from inflight to requested
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
    transitionState(inflightInstant, requestedInstant, Optional.empty());
    return requestedInstant;
  }

  /**
   * Transition Compaction State from requested to inflight
   *
   * @param requestedInstant Requested instant
   * @return inflight instant
   */
  public HoodieInstant transitionCompactionRequestedToInflight(HoodieInstant requestedInstant) {
    Preconditions.checkArgument(requestedInstant.getAction().equals(HoodieTimeline.COMPACTION_ACTION));
    Preconditions.checkArgument(requestedInstant.isRequested());
    HoodieInstant inflightInstant =
        new HoodieInstant(State.INFLIGHT, COMPACTION_ACTION, requestedInstant.getTimestamp());
    transitionState(requestedInstant, inflightInstant, Optional.empty());
    return inflightInstant;
  }

  /**
   * Transition Compaction State from inflight to Committed
   *
   * @param inflightInstant Inflight instant
   * @param data            Extra Metadata
   * @return commit instant
   */
  public HoodieInstant transitionCompactionInflightToComplete(HoodieInstant inflightInstant, Optional<byte[]> data) {
    Preconditions.checkArgument(inflightInstant.getAction().equals(HoodieTimeline.COMPACTION_ACTION));
    Preconditions.checkArgument(inflightInstant.isInflight());
    HoodieInstant commitInstant = new HoodieInstant(State.COMPLETED, COMMIT_ACTION, inflightInstant.getTimestamp());
    transitionState(inflightInstant, commitInstant, data);
    return commitInstant;
  }

  private void createFileInAuxiliaryFolder(HoodieInstant instant, Optional<byte[]> data) {
    Path fullPath = new Path(metaClient.getMetaAuxiliaryPath(), instant.getFileName());
    createFileInPath(fullPath, data);
  }

  /**
   * END - COMPACTION RELATED META-DATA MANAGEMENT
   **/

  private void transitionState(HoodieInstant fromInstant, HoodieInstant toInstant,
      Optional<byte[]> data) {
    Preconditions.checkArgument(fromInstant.getTimestamp().equals(toInstant.getTimestamp()));
    Path commitFilePath = new Path(metaClient.getMetaPath(), toInstant.getFileName());
    try {
      // Re-create the .inflight file by opening a new file and write the commit metadata in
      Path inflightCommitFile = new Path(metaClient.getMetaPath(), fromInstant.getFileName());
      createFileInMetaPath(fromInstant.getFileName(), data);
      boolean success = metaClient.getFs().rename(inflightCommitFile, commitFilePath);
      if (!success) {
        throw new HoodieIOException(
            "Could not rename " + inflightCommitFile + " to " + commitFilePath);
      }
    } catch (IOException e) {
      throw new HoodieIOException("Could not complete " + fromInstant, e);
    }
  }

  private void revertCompleteToInflight(HoodieInstant completed, HoodieInstant inflight) {
    Preconditions.checkArgument(completed.getTimestamp().equals(inflight.getTimestamp()));
    Path inFlightCommitFilePath = new Path(metaClient.getMetaPath(), inflight.getFileName());
    try {
      if (!metaClient.getFs().exists(inFlightCommitFilePath)) {
        Path commitFilePath = new Path(metaClient.getMetaPath(), completed.getFileName());
        boolean success = metaClient.getFs().rename(commitFilePath, inFlightCommitFilePath);
        if (!success) {
          throw new HoodieIOException(
              "Could not rename " + commitFilePath + " to " + inFlightCommitFilePath);
        }
      }
    } catch (IOException e) {
      throw new HoodieIOException("Could not complete revert " + completed, e);
    }
  }

  public void saveToInflight(HoodieInstant instant, Optional<byte[]> content) {
    Preconditions.checkArgument(instant.isInflight());
    createFileInMetaPath(instant.getFileName(), content);
  }

  public void saveToCompactionRequested(HoodieInstant instant, Optional<byte[]> content) {
    Preconditions.checkArgument(instant.getAction().equals(HoodieTimeline.COMPACTION_ACTION));
    // Write workload to auxiliary folder
    createFileInAuxiliaryFolder(instant, content);
    createFileInMetaPath(instant.getFileName(), content);
  }

  private void createFileInMetaPath(String filename, Optional<byte[]> content) {
    Path fullPath = new Path(metaClient.getMetaPath(), filename);
    createFileInPath(fullPath, content);
  }

  private void createFileInPath(Path fullPath, Optional<byte[]> content) {
    try {
      // If the path does not exist, create it first
      if (!metaClient.getFs().exists(fullPath)) {
        if (metaClient.getFs().createNewFile(fullPath)) {
          log.info("Created a new file in meta path: " + fullPath);
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

  private Optional<byte[]> readDataFromPath(Path detailPath) {
    try (FSDataInputStream is = metaClient.getFs().open(detailPath)) {
      return Optional.of(IOUtils.toByteArray(is));
    } catch (IOException e) {
      throw new HoodieIOException("Could not read commit details from " + detailPath, e);
    }
  }

  public HoodieActiveTimeline reload() {
    return new HoodieActiveTimeline(metaClient);
  }
}
