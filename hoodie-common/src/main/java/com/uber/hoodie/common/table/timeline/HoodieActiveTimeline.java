/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.table.timeline;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.exception.HoodieIOException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.time.FastDateFormat;
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

  public static final FastDateFormat COMMIT_FORMATTER = FastDateFormat
      .getInstance("yyyyMMddHHmmss");

  private static final transient Logger log = LogManager.getLogger(HoodieActiveTimeline.class);
  private HoodieTableMetaClient metaClient;

  /**
   * Returns next commit time in the {@link #COMMIT_FORMATTER} format.
   */
  public static String createNewCommitTime() {
    return HoodieActiveTimeline.COMMIT_FORMATTER.format(new Date());
  }

  protected HoodieActiveTimeline(HoodieTableMetaClient metaClient, String[] includedExtensions) {
    // Filter all the filter in the metapath and include only the extensions passed and
    // convert them into HoodieInstant
    try {
      this.instants =
          Arrays.stream(
              HoodieTableMetaClient
                  .scanFiles(metaClient.getFs(), new Path(metaClient.getMetaPath()), path -> {
                    // Include only the meta files with extensions that needs to be included
                    String extension = FSUtils.getFileExtension(path.getName());
                    return Arrays.stream(includedExtensions).anyMatch(Predicate.isEqual(extension));
                  })).sorted(Comparator.comparing(
                    // Sort the meta-data by the instant time (first part of the file name)
                    fileStatus -> FSUtils.getInstantTime(fileStatus.getPath().getName())))
              // create HoodieInstantMarkers from FileStatus, which extracts properties
              .map(HoodieInstant::new).collect(Collectors.toList());
      log.info("Loaded instants " + instants);
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
        new String[] {COMMIT_EXTENSION, INFLIGHT_COMMIT_EXTENSION, DELTA_COMMIT_EXTENSION,
            INFLIGHT_DELTA_COMMIT_EXTENSION, SAVEPOINT_EXTENSION, INFLIGHT_SAVEPOINT_EXTENSION,
            CLEAN_EXTENSION, INFLIGHT_CLEAN_EXTENSION});
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
   */
  public HoodieTimeline getCommitsTimeline() {
    return getTimelineOfActions(
        Sets.newHashSet(COMMIT_ACTION, DELTA_COMMIT_ACTION));
  }

  /**
   * Get all instants (commits, delta commits, clean, savepoint, rollback) that result in actions,
   * in the active timeline *
   */
  public HoodieTimeline getAllCommitsTimeline() {
    return getTimelineOfActions(
        Sets.newHashSet(COMMIT_ACTION, DELTA_COMMIT_ACTION, CLEAN_ACTION,
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
    return new HoodieDefaultTimeline(instants.stream().filter(s -> actions.contains(s.getAction())),
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


  protected Stream<HoodieInstant> filterInstantsByAction(String action) {
    return instants.stream().filter(s -> s.getAction().equals(action));
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
    moveInflightToComplete(instant, HoodieTimeline.getCompletedInstant(instant), data);
    log.info("Completed " + instant);
  }

  public void revertToInflight(HoodieInstant instant) {
    log.info("Reverting instant to inflight " + instant);
    moveCompleteToInflight(instant, HoodieTimeline.getInflightInstant(instant));
    log.info("Reverted " + instant + " to inflight");
  }

  public void deleteInflight(HoodieInstant instant) {
    log.info("Deleting in-flight " + instant);
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

  protected void moveInflightToComplete(HoodieInstant inflight, HoodieInstant completed,
      Optional<byte[]> data) {
    Path commitFilePath = new Path(metaClient.getMetaPath(), completed.getFileName());
    try {
      // open a new file and write the commit metadata in
      Path inflightCommitFile = new Path(metaClient.getMetaPath(), inflight.getFileName());
      createFileInMetaPath(inflight.getFileName(), data);
      boolean success = metaClient.getFs().rename(inflightCommitFile, commitFilePath);
      if (!success) {
        throw new HoodieIOException(
            "Could not rename " + inflightCommitFile + " to " + commitFilePath);
      }
    } catch (IOException e) {
      throw new HoodieIOException("Could not complete " + inflight, e);
    }
  }

  protected void moveCompleteToInflight(HoodieInstant completed, HoodieInstant inflight) {
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
    createFileInMetaPath(instant.getFileName(), content);
  }

  public void createFileInMetaPath(String filename, Optional<byte[]> content) {
    Path fullPath = new Path(metaClient.getMetaPath(), filename);
    try {
      if (!content.isPresent()) {
        if (metaClient.getFs().createNewFile(fullPath)) {
          log.info("Created a new file in meta path: " + fullPath);
          return;
        }
      } else {
        FSDataOutputStream fsout = metaClient.getFs().create(fullPath, true);
        fsout.write(content.get());
        fsout.close();
        return;
      }
      throw new HoodieIOException("Failed to create file " + fullPath);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to create file " + fullPath, e);
    }
  }

  protected Optional<byte[]> readDataFromPath(Path detailPath) {
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
