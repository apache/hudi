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

package org.apache.hudi.table.action.rollback;

import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.bootstrap.index.BootstrapIndex;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.MarkerFiles;
import org.apache.hudi.table.action.BaseActionExecutor;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public abstract class BaseRollbackActionExecutor extends BaseActionExecutor<HoodieRollbackMetadata> {

  private static final Logger LOG = LogManager.getLogger(BaseRollbackActionExecutor.class);

  interface RollbackStrategy extends Serializable {

    List<HoodieRollbackStat> execute(HoodieInstant instantToRollback);
  }

  protected final HoodieInstant instantToRollback;
  protected final boolean deleteInstants;
  protected final boolean skipTimelinePublish;
  protected final boolean useMarkerBasedStrategy;

  public BaseRollbackActionExecutor(JavaSparkContext jsc,
      HoodieWriteConfig config,
      HoodieTable<?> table,
      String instantTime,
      HoodieInstant instantToRollback,
      boolean deleteInstants) {
    this(jsc, config, table, instantTime, instantToRollback, deleteInstants,
        false, config.shouldRollbackUsingMarkers());
  }

  public BaseRollbackActionExecutor(JavaSparkContext jsc,
      HoodieWriteConfig config,
      HoodieTable<?> table,
      String instantTime,
      HoodieInstant instantToRollback,
      boolean deleteInstants,
      boolean skipTimelinePublish,
      boolean useMarkerBasedStrategy) {
    super(jsc, config, table, instantTime);
    this.instantToRollback = instantToRollback;
    this.deleteInstants = deleteInstants;
    this.skipTimelinePublish = skipTimelinePublish;
    this.useMarkerBasedStrategy = useMarkerBasedStrategy;
    if (useMarkerBasedStrategy) {
      ValidationUtils.checkArgument(!instantToRollback.isCompleted(),
          "Cannot use marker based rollback strategy on completed instant:" + instantToRollback);
    }
  }

  protected RollbackStrategy getRollbackStrategy() {
    if (useMarkerBasedStrategy) {
      return new MarkerBasedRollbackStrategy(table, jsc, config, instantTime);
    } else {
      return this::executeRollbackUsingFileListing;
    }
  }

  protected abstract List<HoodieRollbackStat> executeRollback() throws IOException;

  protected abstract List<HoodieRollbackStat> executeRollbackUsingFileListing(HoodieInstant instantToRollback);

  @Override
  public HoodieRollbackMetadata execute() {
    HoodieTimer rollbackTimer = new HoodieTimer().startTimer();
    List<HoodieRollbackStat> stats = doRollbackAndGetStats();
    HoodieRollbackMetadata rollbackMetadata = TimelineMetadataUtils.convertRollbackMetadata(
        instantTime,
        Option.of(rollbackTimer.endTimer()),
        Collections.singletonList(instantToRollback.getTimestamp()),
        stats);
    if (!skipTimelinePublish) {
      finishRollback(rollbackMetadata);
    }

    // Finally, remove the marker files post rollback.
    new MarkerFiles(table, instantToRollback.getTimestamp()).quietDeleteMarkerDir(jsc, config.getMarkersDeleteParallelism());

    return rollbackMetadata;
  }

  private void validateSavepointRollbacks() {
    // Check if any of the commits is a savepoint - do not allow rollback on those commits
    List<String> savepoints = table.getCompletedSavepointTimeline().getInstants()
        .map(HoodieInstant::getTimestamp)
        .collect(Collectors.toList());
    savepoints.forEach(s -> {
      if (s.contains(instantToRollback.getTimestamp())) {
        throw new HoodieRollbackException(
            "Could not rollback a savepointed commit. Delete savepoint first before rolling back" + s);
      }
    });
  }

  private void validateRollbackCommitSequence() {
    final String instantTimeToRollback = instantToRollback.getTimestamp();
    HoodieTimeline commitTimeline = table.getCompletedCommitsTimeline();
    HoodieTimeline inflightAndRequestedCommitTimeline = table.getPendingCommitTimeline();
    // Make sure only the last n commits are being rolled back
    // If there is a commit in-between or after that is not rolled back, then abort
    if ((instantTimeToRollback != null) && !commitTimeline.empty()
        && !commitTimeline.findInstantsAfter(instantTimeToRollback, Integer.MAX_VALUE).empty()) {
      throw new HoodieRollbackException(
          "Found commits after time :" + instantTimeToRollback + ", please rollback greater commits first");
    }

    List<String> inflights = inflightAndRequestedCommitTimeline.getInstants().map(HoodieInstant::getTimestamp)
        .collect(Collectors.toList());
    if ((instantTimeToRollback != null) && !inflights.isEmpty()
        && (inflights.indexOf(instantTimeToRollback) != inflights.size() - 1)) {
      throw new HoodieRollbackException(
          "Found in-flight commits after time :" + instantTimeToRollback + ", please rollback greater commits first");
    }
  }

  private void rollBackIndex() {
    if (!table.getIndex().rollbackCommit(instantToRollback.getTimestamp())) {
      throw new HoodieRollbackException("Rollback index changes failed, for time :" + instantToRollback);
    }
    LOG.info("Index rolled back for commits " + instantToRollback);
  }

  public List<HoodieRollbackStat> doRollbackAndGetStats() {
    final String instantTimeToRollback = instantToRollback.getTimestamp();
    final boolean isPendingCompaction = Objects.equals(HoodieTimeline.COMPACTION_ACTION, instantToRollback.getAction())
        && !instantToRollback.isCompleted();
    validateSavepointRollbacks();
    if (!isPendingCompaction) {
      validateRollbackCommitSequence();
    }

    try {
      List<HoodieRollbackStat> stats = executeRollback();
      LOG.info("Rolled back inflight instant " + instantTimeToRollback);
      if (!isPendingCompaction) {
        rollBackIndex();
      }
      return stats;
    } catch (IOException e) {
      throw new HoodieIOException("Unable to execute rollback ", e);
    }
  }

  protected void finishRollback(HoodieRollbackMetadata rollbackMetadata) throws HoodieIOException {
    try {
      table.getActiveTimeline().createNewInstant(
          new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.ROLLBACK_ACTION, instantTime));
      table.getActiveTimeline().saveAsComplete(
          new HoodieInstant(true, HoodieTimeline.ROLLBACK_ACTION, instantTime),
          TimelineMetadataUtils.serializeRollbackMetadata(rollbackMetadata));
      LOG.info("Rollback of Commits " + rollbackMetadata.getCommitsRollback() + " is complete");
      if (!table.getActiveTimeline().getCleanerTimeline().empty()) {
        LOG.info("Cleaning up older rollback meta files");
        FSUtils.deleteOlderRollbackMetaFiles(table.getMetaClient().getFs(),
            table.getMetaClient().getMetaPath(),
            table.getActiveTimeline().getRollbackTimeline().getInstants());
      }
    } catch (IOException e) {
      throw new HoodieIOException("Error executing rollback at instant " + instantTime, e);
    }
  }

  /**
   * Delete Inflight instant if enabled.
   *
   * @param deleteInstant Enable Deletion of Inflight instant
   * @param activeTimeline Hoodie active timeline
   * @param instantToBeDeleted Instant to be deleted
   */
  protected void deleteInflightAndRequestedInstant(boolean deleteInstant,
      HoodieActiveTimeline activeTimeline,
      HoodieInstant instantToBeDeleted) {
    // Remove the rolled back inflight commits
    if (deleteInstant) {
      LOG.info("Deleting instant=" + instantToBeDeleted);
      activeTimeline.deletePending(instantToBeDeleted);
      if (instantToBeDeleted.isInflight() && !table.getMetaClient().getTimelineLayoutVersion().isNullVersion()) {
        // Delete corresponding requested instant
        instantToBeDeleted = new HoodieInstant(HoodieInstant.State.REQUESTED, instantToBeDeleted.getAction(),
            instantToBeDeleted.getTimestamp());
        activeTimeline.deletePending(instantToBeDeleted);
      }
      LOG.info("Deleted pending commit " + instantToBeDeleted);
    } else {
      LOG.warn("Rollback finished without deleting inflight instant file. Instant=" + instantToBeDeleted);
    }
  }

  protected void dropBootstrapIndexIfNeeded(HoodieInstant instantToRollback) {
    if (HoodieTimeline.compareTimestamps(instantToRollback.getTimestamp(), HoodieTimeline.EQUALS, HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS)) {
      LOG.info("Dropping bootstrap index as metadata bootstrap commit is getting rolled back !!");
      BootstrapIndex.getBootstrapIndex(table.getMetaClient()).dropIndex();
    }
  }
}
