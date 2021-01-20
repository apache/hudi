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

package org.apache.hudi.table.action.clean;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanFileInfo;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class BaseCleanActionExecutor<T extends HoodieRecordPayload, I, K, O> extends BaseActionExecutor<T, I, K, O, HoodieCleanMetadata> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LogManager.getLogger(BaseCleanActionExecutor.class);

  public BaseCleanActionExecutor(HoodieEngineContext context, HoodieWriteConfig config, HoodieTable<T, I, K, O> table, String instantTime) {
    super(context, config, table, instantTime);
  }

  /**
   * Generates List of files to be cleaned.
   *
   * @param context HoodieEngineContext
   * @return Cleaner Plan
   */
  HoodieCleanerPlan requestClean(HoodieEngineContext context) {
    try {
      CleanPlanner<T, I, K, O> planner = new CleanPlanner<>(context, table, config);
      Option<HoodieInstant> earliestInstant = planner.getEarliestCommitToRetain();
      List<String> partitionsToClean = planner.getPartitionPathsToClean(earliestInstant);

      if (partitionsToClean.isEmpty()) {
        LOG.info("Nothing to clean here.");
        return HoodieCleanerPlan.newBuilder().setPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS.name()).build();
      }
      LOG.info("Total Partitions to clean : " + partitionsToClean.size() + ", with policy " + config.getCleanerPolicy());
      int cleanerParallelism = Math.min(partitionsToClean.size(), config.getCleanerParallelism());
      LOG.info("Using cleanerParallelism: " + cleanerParallelism);

      context.setJobStatus(this.getClass().getSimpleName(), "Generates list of file slices to be cleaned");

      Map<String, List<HoodieCleanFileInfo>> cleanOps = context
          .map(partitionsToClean, partitionPathToClean -> Pair.of(partitionPathToClean, planner.getDeletePaths(partitionPathToClean)), cleanerParallelism)
          .stream()
          .collect(Collectors.toMap(Pair::getKey, y -> CleanerUtils.convertToHoodieCleanFileInfoList(y.getValue())));

      return new HoodieCleanerPlan(earliestInstant
          .map(x -> new HoodieActionInstant(x.getTimestamp(), x.getAction(), x.getState().name())).orElse(null),
          config.getCleanerPolicy().name(), CollectionUtils.createImmutableMap(),
          CleanPlanner.LATEST_CLEAN_PLAN_VERSION, cleanOps);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to schedule clean operation", e);
    }
  }

  protected static Boolean deleteFileAndGetResult(FileSystem fs, String deletePathStr) throws IOException {
    Path deletePath = new Path(deletePathStr);
    LOG.debug("Working on delete path :" + deletePath);
    try {
      boolean deleteResult = fs.delete(deletePath, false);
      if (deleteResult) {
        LOG.debug("Cleaned file at path :" + deletePath);
      }
      return deleteResult;
    } catch (FileNotFoundException fio) {
      // With cleanPlan being used for retried cleaning operations, its possible to clean a file twice
      return false;
    }
  }

  /**
   * Performs cleaning of partition paths according to cleaning policy and returns the number of files cleaned. Handles
   * skews in partitions to clean by making files to clean as the unit of task distribution.
   *
   * @throws IllegalArgumentException if unknown cleaning policy is provided
   */
  abstract List<HoodieCleanStat> clean(HoodieEngineContext context, HoodieCleanerPlan cleanerPlan);

  /**
   * Creates a Cleaner plan if there are files to be cleaned and stores them in instant file.
   * Cleaner Plan contains absolute file paths.
   *
   * @param startCleanTime Cleaner Instant Time
   * @return Cleaner Plan if generated
   */
  Option<HoodieCleanerPlan> requestClean(String startCleanTime) {
    final HoodieCleanerPlan cleanerPlan = requestClean(context);
    if ((cleanerPlan.getFilePathsToBeDeletedPerPartition() != null)
        && !cleanerPlan.getFilePathsToBeDeletedPerPartition().isEmpty()
        && cleanerPlan.getFilePathsToBeDeletedPerPartition().values().stream().mapToInt(List::size).sum() > 0) {
      // Only create cleaner plan which does some work
      final HoodieInstant cleanInstant = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLEAN_ACTION, startCleanTime);
      // Save to both aux and timeline folder
      try {
        table.getActiveTimeline().saveToCleanRequested(cleanInstant, TimelineMetadataUtils.serializeCleanerPlan(cleanerPlan));
        LOG.info("Requesting Cleaning with instant time " + cleanInstant);
      } catch (IOException e) {
        LOG.error("Got exception when saving cleaner requested file", e);
        throw new HoodieIOException(e.getMessage(), e);
      }
      return Option.of(cleanerPlan);
    }
    return Option.empty();
  }

  /**
   * Executes the Cleaner plan stored in the instant metadata.
   */
  void runPendingClean(HoodieTable<T, I, K, O> table, HoodieInstant cleanInstant) {
    try {
      HoodieCleanerPlan cleanerPlan = CleanerUtils.getCleanerPlan(table.getMetaClient(), cleanInstant);
      runClean(table, cleanInstant, cleanerPlan);
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
  }

  private HoodieCleanMetadata runClean(HoodieTable<T, I, K, O> table, HoodieInstant cleanInstant, HoodieCleanerPlan cleanerPlan) {
    ValidationUtils.checkArgument(cleanInstant.getState().equals(HoodieInstant.State.REQUESTED)
        || cleanInstant.getState().equals(HoodieInstant.State.INFLIGHT));

    try {
      final HoodieInstant inflightInstant;
      final HoodieTimer timer = new HoodieTimer();
      timer.startTimer();
      if (cleanInstant.isRequested()) {
        inflightInstant = table.getActiveTimeline().transitionCleanRequestedToInflight(cleanInstant,
            TimelineMetadataUtils.serializeCleanerPlan(cleanerPlan));
      } else {
        inflightInstant = cleanInstant;
      }

      List<HoodieCleanStat> cleanStats = clean(context, cleanerPlan);
      if (cleanStats.isEmpty()) {
        return HoodieCleanMetadata.newBuilder().build();
      }

      table.getMetaClient().reloadActiveTimeline();
      HoodieCleanMetadata metadata = CleanerUtils.convertCleanMetadata(
          inflightInstant.getTimestamp(),
          Option.of(timer.endTimer()),
          cleanStats
      );

      table.getActiveTimeline().transitionCleanInflightToComplete(inflightInstant,
          TimelineMetadataUtils.serializeCleanMetadata(metadata));
      LOG.info("Marked clean started on " + inflightInstant.getTimestamp() + " as complete");
      return metadata;
    } catch (IOException e) {
      throw new HoodieIOException("Failed to clean up after commit", e);
    }
  }

  @Override
  public HoodieCleanMetadata execute() {
    // If there are inflight(failed) or previously requested clean operation, first perform them
    List<HoodieInstant> pendingCleanInstants = table.getCleanTimeline()
        .filterInflightsAndRequested().getInstants().collect(Collectors.toList());
    if (pendingCleanInstants.size() > 0) {
      pendingCleanInstants.forEach(hoodieInstant -> {
        LOG.info("Finishing previously unfinished cleaner instant=" + hoodieInstant);
        try {
          runPendingClean(table, hoodieInstant);
        } catch (Exception e) {
          LOG.warn("Failed to perform previous clean operation, instant: " + hoodieInstant, e);
        }
      });
      table.getMetaClient().reloadActiveTimeline();
    }

    // Plan and execute a new clean action
    Option<HoodieCleanerPlan> cleanerPlanOpt = requestClean(instantTime);
    if (cleanerPlanOpt.isPresent()) {
      table.getMetaClient().reloadActiveTimeline();
      HoodieCleanerPlan cleanerPlan = cleanerPlanOpt.get();
      if ((cleanerPlan.getFilePathsToBeDeletedPerPartition() != null) && !cleanerPlan.getFilePathsToBeDeletedPerPartition().isEmpty()) {
        return runClean(table, HoodieTimeline.getCleanRequestedInstant(instantTime), cleanerPlan);
      }
    }
    return null;
  }
}
