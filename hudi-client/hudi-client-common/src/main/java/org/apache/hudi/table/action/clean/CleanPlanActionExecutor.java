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

import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanFileInfo;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.CleanFileInfo;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantComparatorV1;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.CleanerUtils.EARLIEST_COMMIT_TO_NOT_ARCHIVE;
import static org.apache.hudi.common.util.MapUtils.nonEmpty;
import static org.apache.hudi.common.util.CleanerUtils.SAVEPOINTED_TIMESTAMPS;

public class CleanPlanActionExecutor<T, I, K, O> extends BaseActionExecutor<T, I, K, O, Option<HoodieCleanerPlan>> {
  private static final Logger LOG = LoggerFactory.getLogger(CleanPlanActionExecutor.class);
  private final Option<Map<String, String>> extraMetadata;

  public CleanPlanActionExecutor(HoodieEngineContext context,
                                 HoodieWriteConfig config,
                                 HoodieTable<T, I, K, O> table,
                                 String instantTime,
                                 Option<Map<String, String>> extraMetadata) {
    super(context, config, table, instantTime);
    this.extraMetadata = extraMetadata;
  }

  private int getCommitsSinceLastCleaning() {
    Option<HoodieInstant> lastCleanInstant = table.getActiveTimeline().getCleanerTimeline().filterCompletedInstants().lastInstant();
    HoodieTimeline commitTimeline = table.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();

    int numCommits;
    if (lastCleanInstant.isPresent() && !table.getActiveTimeline().isEmpty(lastCleanInstant.get())) {
      try {
        HoodieCleanMetadata cleanMetadata = TimelineMetadataUtils
            .deserializeHoodieCleanMetadata(table.getActiveTimeline().getInstantDetails(lastCleanInstant.get()).get());
        String lastCompletedCommitTimestamp = cleanMetadata.getLastCompletedCommitTimestamp();
        numCommits = commitTimeline.findInstantsAfter(lastCompletedCommitTimestamp).countInstants();
      } catch (IOException e) {
        throw new HoodieIOException("Parsing of last clean instant " + lastCleanInstant.get() + " failed", e);
      }
    } else {
      numCommits = commitTimeline.countInstants();
    }

    return numCommits;
  }

  private boolean needsCleaning(CleaningTriggerStrategy strategy) {
    if (strategy == CleaningTriggerStrategy.NUM_COMMITS) {
      int numberOfCommits = getCommitsSinceLastCleaning();
      int maxInlineCommitsForNextClean = config.getCleaningMaxCommits();
      return numberOfCommits >= maxInlineCommitsForNextClean;
    } else {
      throw new HoodieException("Unsupported cleaning trigger strategy: " + config.getCleaningTriggerStrategy());
    }
  }

  private HoodieCleanerPlan getEmptyCleanerPlan(Option<HoodieInstant> earliestInstant, CleanPlanner<T, I, K, O> planner) throws IOException {
    HoodieCleanerPlan.Builder cleanBuilder = HoodieCleanerPlan.newBuilder()
        .setFilePathsToBeDeletedPerPartition(Collections.emptyMap())
        .setExtraMetadata(prepareExtraMetadata(planner.getSavepointedTimestamps(), planner.getEarliestCommitToNotArchive()));
    if (earliestInstant.isPresent()) {
      HoodieInstant hoodieInstant = earliestInstant.get();
      cleanBuilder.setPolicy(config.getCleanerPolicy().name())
          .setVersion(CleanPlanner.LATEST_CLEAN_PLAN_VERSION)
          .setEarliestInstantToRetain(new HoodieActionInstant(hoodieInstant.requestedTime(), hoodieInstant.getAction(), hoodieInstant.getState().name()))
          .setLastCompletedCommitTimestamp(planner.getLastCompletedCommitTimestamp());
    } else {
      cleanBuilder.setPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS.name());
    }
    return cleanBuilder.build();
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
      context.setJobStatus(this.getClass().getSimpleName(), "Obtaining list of partitions to be cleaned: " + config.getTableName());
      List<String> partitionsToClean = planner.getPartitionPathsToClean(earliestInstant);

      if (partitionsToClean.isEmpty()) {
        LOG.info("Partitions to clean returned empty. Checking to see if empty clean needs to be created.");
        return getEmptyCleanerPlan(earliestInstant, planner);
      }
      LOG.info("Earliest commit to retain for clean : {}", (earliestInstant.isPresent() ? earliestInstant.get().requestedTime() : "null"));
      LOG.info("Total partitions to clean : {}, with policy {}", partitionsToClean.size(), config.getCleanerPolicy());
      int cleanerParallelism = Math.min(partitionsToClean.size(), config.getCleanerParallelism());
      LOG.info("Using cleanerParallelism: {}", cleanerParallelism);

      context.setJobStatus(this.getClass().getSimpleName(), "Generating list of file slices to be cleaned: " + config.getTableName());

      Map<String, List<HoodieCleanFileInfo>> cleanOps = new HashMap<>();
      List<String> partitionsToDelete = new ArrayList<>();
      boolean shouldUseBatchLookup = table.getMetaClient().getTableConfig().isMetadataTableAvailable();
      for (int i = 0; i < partitionsToClean.size(); i += cleanerParallelism) {
        // Handles at most 'cleanerParallelism' number of partitions once at a time to avoid overlarge memory pressure to the timeline server
        // (remote or local embedded), thus to reduce the risk of an OOM exception.
        List<String> subPartitionsToClean = partitionsToClean.subList(i, Math.min(i + cleanerParallelism, partitionsToClean.size()));
        if (shouldUseBatchLookup) {
          LOG.info("Load partitions and files into file system view in advance. Paths: {}", subPartitionsToClean);
          table.getHoodieView().loadPartitions(subPartitionsToClean);
        }
        Map<String, Pair<Boolean, List<CleanFileInfo>>> cleanOpsWithPartitionMeta = context
            .map(subPartitionsToClean, partitionPathToClean -> Pair.of(partitionPathToClean, planner.getDeletePaths(partitionPathToClean, earliestInstant)), cleanerParallelism)
            .stream()
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

        cleanOps.putAll(cleanOpsWithPartitionMeta.entrySet().stream()
            .filter(e -> !e.getValue().getValue().isEmpty())
            //.filter(entry -> !entry.getValue().getValue().isEmpty())
            .collect(Collectors.toMap(Map.Entry::getKey, e -> CleanerUtils.convertToHoodieCleanFileInfoList(e.getValue().getValue()))));

        partitionsToDelete.addAll(cleanOpsWithPartitionMeta.entrySet().stream().filter(entry -> entry.getValue().getKey()).map(Map.Entry::getKey)
            .collect(Collectors.toList()));
      }

      return new HoodieCleanerPlan(
          earliestInstant.map(x -> new HoodieActionInstant(x.requestedTime(), x.getAction(), x.getState().name())).orElse(null),
          planner.getLastCompletedCommitTimestamp(), // Note: This is the start time of the last completed ingestion before this clean.
          config.getCleanerPolicy().name(),
          Collections.emptyMap(),
          CleanPlanner.LATEST_CLEAN_PLAN_VERSION,
          cleanOps,
          partitionsToDelete,
          prepareExtraMetadata(planner.getSavepointedTimestamps(), planner.getEarliestCommitToNotArchive()));
    } catch (IOException e) {
      throw new HoodieIOException("Failed to schedule clean operation", e);
    }
  }

  private Map<String, String> prepareExtraMetadata(List<String> savepointedTimestamps, Option<String> earliestCommitToNotArchive) {
    if (savepointedTimestamps.isEmpty() && !earliestCommitToNotArchive.isPresent()) {
      return Collections.emptyMap();
    } else {
      Map<String, String> extraMetadata = new HashMap<>();
      extraMetadata.put(SAVEPOINTED_TIMESTAMPS, savepointedTimestamps.stream().collect(Collectors.joining(",")));
      if (earliestCommitToNotArchive.isPresent()) {
        extraMetadata.put(EARLIEST_COMMIT_TO_NOT_ARCHIVE, earliestCommitToNotArchive.get());
      }
      return extraMetadata;
    }
  }

  /**
   * Creates a Cleaner plan if there are files to be cleaned and stores them in instant file.
   * Cleaner Plan contains absolute file paths.
   *
   * @param startCleanTime Cleaner Instant Time
   * @return Cleaner Plan if generated
   */
  protected Option<HoodieCleanerPlan> requestClean(String startCleanTime) {
    // Check if the last clean completed successfully and wrote out its metadata. If not, it should be retried.
    Option<HoodieInstant> lastClean = table.getCleanTimeline().filterCompletedInstants().lastInstant();
    if (lastClean.isPresent()) {
      HoodieInstant cleanInstant = lastClean.get();
      HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
      if (activeTimeline.isEmpty(cleanInstant)) {
        activeTimeline.deleteEmptyInstantIfExists(cleanInstant);
        HoodieInstant cleanPlanInstant = new HoodieInstant(HoodieInstant.State.INFLIGHT, cleanInstant.getAction(), cleanInstant.requestedTime(), InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
        try {
          Option<byte[]> content = activeTimeline.getInstantDetails(cleanPlanInstant);
          // Deserialize plan if it is non-empty
          if (content.map(bytes -> bytes.length > 0).orElse(false)) {
            return Option.of(TimelineMetadataUtils.deserializeCleanerPlan(content.get()));
          } else {
            return Option.of(new HoodieCleanerPlan());
          }
        } catch (IOException ex) {
          throw new HoodieIOException("Failed to parse cleaner plan", ex);
        }
      }
    }
    final HoodieCleanerPlan cleanerPlan = requestClean(context);
    if ((cleanerPlan.getPartitionsToBeDeleted() != null && !cleanerPlan.getPartitionsToBeDeleted().isEmpty())
        || (nonEmpty(cleanerPlan.getFilePathsToBeDeletedPerPartition())
        && cleanerPlan.getFilePathsToBeDeletedPerPartition().values().stream().mapToInt(List::size).sum() > 0)) {
      // Only create cleaner plan which does some work
      createCleanRequested(startCleanTime, cleanerPlan);
      return Option.of(cleanerPlan);
    }
    // If cleaner plan returned an empty list, incremental clean is enabled and there was no
    // completed clean created in the last X hours configured in MAX_DURATION_TO_CREATE_EMPTY_CLEAN,
    // create a dummy clean to avoid full scan in the future.
    // Note: For a dataset with incremental clean enabled, that does not receive any updates, cleaner plan always comes
    // with an empty list of files to be cleaned.  CleanActionExecutor would never be invoked for this dataset.
    // To avoid fullscan on the dataset with every ingestion run, empty clean commit is created here.
    if (config.incrementalCleanerModeEnabled() && cleanerPlan.getEarliestInstantToRetain() != null && config.maxDurationToCreateEmptyCleanMs() > 0) {
      // Only create an empty clean commit if earliestInstantToRetain is present in the plan
      boolean eligibleForEmptyCleanCommit = true;

      // if there is no previous clean instant or the previous clean instant was before the configured max duration, schedule an empty clean commit
      Option<HoodieInstant> lastCleanInstant = table.getCleanTimeline().lastInstant();
      if (lastCleanInstant.isPresent())  {
        try {
          long currentCleanTimeMs = HoodieInstantTimeGenerator.parseDateFromInstantTime(startCleanTime).toInstant().toEpochMilli();
          long lastCleanTimeMs = HoodieInstantTimeGenerator.parseDateFromInstantTime(lastCleanInstant.get().requestedTime()).toInstant().toEpochMilli();
          eligibleForEmptyCleanCommit = currentCleanTimeMs - lastCleanTimeMs > config.maxDurationToCreateEmptyCleanMs();
        } catch (ParseException ex) {
          LOG.warn("Unable to parse last clean commit time", ex);
        }
      }
      if (eligibleForEmptyCleanCommit) {
        LOG.warn("Creating a dummy clean instant {} with earliestCommitToRetain of {}", startCleanTime,
            cleanerPlan.getEarliestInstantToRetain().getTimestamp());
        createCleanRequested(startCleanTime, cleanerPlan);
        return Option.of(cleanerPlan);
      }
    }
    return Option.empty();
  }

  /**
   * Create the clean.requested instant.
   *
   * @param startCleanTime - instant time of the clean.requested
   * @param cleanerPlan - content to be written into the clean.requested.
   */
  private void createCleanRequested(String startCleanTime, HoodieCleanerPlan cleanerPlan) {
    final HoodieInstant cleanInstant = instantGenerator.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLEAN_ACTION, startCleanTime);
    // Save to both aux and timeline folder
    try {
      table.getActiveTimeline().saveToCleanRequested(cleanInstant, TimelineMetadataUtils.serializeCleanerPlan(cleanerPlan));
      LOG.info("Requesting Cleaning with instant time " + cleanInstant);
    } catch (IOException e) {
      LOG.error("Got exception when saving cleaner requested file", e);
      throw new HoodieIOException(e.getMessage(), e);
    }
  }

  @Override
  public Option<HoodieCleanerPlan> execute() {
    if (!needsCleaning(config.getCleaningTriggerStrategy())) {
      return Option.empty();
    }
    // Plan a new clean action
    return requestClean(instantTime);
  }

}
