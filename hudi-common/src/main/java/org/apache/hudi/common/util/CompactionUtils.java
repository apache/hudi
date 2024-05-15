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

package org.apache.hudi.common.util;

import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.timeline.versioning.compaction.CompactionPlanMigrator;
import org.apache.hudi.common.table.timeline.versioning.compaction.CompactionV1MigrationHandler;
import org.apache.hudi.common.table.timeline.versioning.compaction.CompactionV2MigrationHandler;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Helper class to generate compaction plan from FileGroup/FileSlice abstraction.
 */
public class CompactionUtils {

  public static final Integer COMPACTION_METADATA_VERSION_1 = CompactionV1MigrationHandler.VERSION;
  public static final Integer COMPACTION_METADATA_VERSION_2 = CompactionV2MigrationHandler.VERSION;
  public static final Integer LATEST_COMPACTION_METADATA_VERSION = COMPACTION_METADATA_VERSION_2;

  /**
   * Generate compaction operation from file-slice.
   *
   * @param partitionPath          Partition path
   * @param fileSlice              File Slice
   * @param metricsCaptureFunction Metrics Capture function
   * @return Compaction Operation
   */
  public static HoodieCompactionOperation buildFromFileSlice(String partitionPath, FileSlice fileSlice,
                                                             Option<Function<Pair<String, FileSlice>, Map<String, Double>>> metricsCaptureFunction) {
    HoodieCompactionOperation.Builder builder = HoodieCompactionOperation.newBuilder();
    builder.setPartitionPath(partitionPath);
    builder.setFileId(fileSlice.getFileId());
    builder.setBaseInstantTime(fileSlice.getBaseInstantTime());
    builder.setDeltaFilePaths(fileSlice.getLogFiles().map(lf -> lf.getPath().getName()).collect(Collectors.toList()));
    if (fileSlice.getBaseFile().isPresent()) {
      builder.setDataFilePath(fileSlice.getBaseFile().get().getFileName());
      builder.setBootstrapFilePath(fileSlice.getBaseFile().get().getBootstrapBaseFile().map(BaseFile::getPath)
          .orElse(null));
    }

    if (metricsCaptureFunction.isPresent()) {
      builder.setMetrics(metricsCaptureFunction.get().apply(Pair.of(partitionPath, fileSlice)));
    }
    return builder.build();
  }

  /**
   * Generate compaction plan from file-slices.
   *
   * @param partitionFileSlicePairs list of partition file-slice pairs
   * @param extraMetadata           Extra Metadata
   * @param metricsCaptureFunction  Metrics Capture function
   */
  public static HoodieCompactionPlan buildFromFileSlices(List<Pair<String, FileSlice>> partitionFileSlicePairs,
                                                         Option<Map<String, String>> extraMetadata,
                                                         Option<Function<Pair<String, FileSlice>, Map<String, Double>>> metricsCaptureFunction) {
    HoodieCompactionPlan.Builder builder = HoodieCompactionPlan.newBuilder();
    extraMetadata.ifPresent(builder::setExtraMetadata);

    builder.setOperations(partitionFileSlicePairs.stream()
        .map(pfPair -> buildFromFileSlice(pfPair.getKey(), pfPair.getValue(), metricsCaptureFunction))
        .collect(Collectors.toList()));
    builder.setVersion(LATEST_COMPACTION_METADATA_VERSION);
    return builder.build();
  }

  /**
   * Build Avro generated Compaction operation payload from compaction operation POJO for serialization.
   */
  public static HoodieCompactionOperation buildHoodieCompactionOperation(CompactionOperation op) {
    return HoodieCompactionOperation.newBuilder().setFileId(op.getFileId()).setBaseInstantTime(op.getBaseInstantTime())
        .setPartitionPath(op.getPartitionPath())
        .setBootstrapFilePath(op.getBootstrapFilePath().orElse(null))
        .setDataFilePath(op.getDataFileName().isPresent() ? op.getDataFileName().get() : null)
        .setDeltaFilePaths(op.getDeltaFileNames()).setMetrics(op.getMetrics()).build();
  }

  /**
   * Build Compaction operation payload from Avro version for using in Spark executors.
   *
   * @param hc HoodieCompactionOperation
   */
  public static CompactionOperation buildCompactionOperation(HoodieCompactionOperation hc) {
    return CompactionOperation.convertFromAvroRecordInstance(hc);
  }

  /**
   * Get all pending compaction plans along with their instants.
   * @param metaClient Hoodie Meta Client
   */
  public static List<Pair<HoodieInstant, HoodieCompactionPlan>> getAllPendingCompactionPlans(
      HoodieTableMetaClient metaClient) {
    // This function returns pending compaction timeline.
    Function<HoodieTableMetaClient, HoodieTimeline> getFilteredTimelineByActionType =
        (hoodieTableMetaClient) -> hoodieTableMetaClient.getActiveTimeline().filterPendingCompactionTimeline();
    // Hoodie requested instant supplier
    Function<String, HoodieInstant> requestedInstantSupplier = HoodieTimeline::getCompactionRequestedInstant;
    return getCompactionPlansByTimeline(metaClient, getFilteredTimelineByActionType, requestedInstantSupplier);
  }

  /**
   * Get all pending logcompaction plans along with their instants.
   * @param metaClient Hoodie Meta Client
   */
  public static List<Pair<HoodieInstant, HoodieCompactionPlan>> getAllPendingLogCompactionPlans(
      HoodieTableMetaClient metaClient) {
    // This function returns pending logcompaction timeline.
    Function<HoodieTableMetaClient, HoodieTimeline> filteredTimelineSupplier =
        (hoodieTableMetaClient) -> hoodieTableMetaClient.getActiveTimeline().filterPendingLogCompactionTimeline();
    // Hoodie requested instant supplier
    Function<String, HoodieInstant> requestedInstantSupplier = HoodieTimeline::getLogCompactionRequestedInstant;
    return getCompactionPlansByTimeline(metaClient, filteredTimelineSupplier, requestedInstantSupplier);
  }

  /**
   * Util method to get compaction plans by action_type(COMPACT or LOG_COMPACT)
   * @param metaClient HoodieTable's metaclient
   * @param filteredTimelineSupplier gives a timeline object, this can be either filtered to return pending compactions or log compaction instants.
   * @param requestedInstantWrapper function that gives a requested Hoodie instant.
   * @return List of pair of HoodieInstant and it's corresponding compaction plan.
   * Note here the compaction plan can be related to a compaction instant or log compaction instant.
   */
  private static List<Pair<HoodieInstant, HoodieCompactionPlan>> getCompactionPlansByTimeline(
      HoodieTableMetaClient metaClient, Function<HoodieTableMetaClient, HoodieTimeline> filteredTimelineSupplier,
      Function<String, HoodieInstant> requestedInstantWrapper) {
    List<HoodieInstant> filteredInstants = filteredTimelineSupplier.apply(metaClient).getInstants();
    return filteredInstants.stream()
        .map(instant -> Pair.of(instant, getCompactionPlan(metaClient, requestedInstantWrapper.apply(instant.getTimestamp()))))
        .collect(Collectors.toList());
  }

  /**
   * This method will serve only Compaction instants
   * because we use same HoodieCompactionPlan for both the operations.
   */
  public static HoodieCompactionPlan getCompactionPlan(HoodieTableMetaClient metaClient, String compactionInstant) {
    HoodieInstant compactionRequestedInstant = HoodieTimeline.getCompactionRequestedInstant(compactionInstant);
    return getCompactionPlan(metaClient, compactionRequestedInstant);
  }

  /**
   * This method will serve only log compaction instants,
   * because we use same HoodieCompactionPlan for both the operations.
   */
  public static HoodieCompactionPlan getLogCompactionPlan(HoodieTableMetaClient metaClient, String logCompactionInstant) {
    HoodieInstant logCompactionRequestedInstant = HoodieTimeline.getLogCompactionRequestedInstant(logCompactionInstant);
    return getCompactionPlan(metaClient, logCompactionRequestedInstant);
  }

  /**
   * Util method to fetch both compaction and log compaction plan from requestedInstant.
   */
  private static HoodieCompactionPlan getCompactionPlan(HoodieTableMetaClient metaClient, HoodieInstant requestedInstant) {
    return getCompactionPlan(metaClient, metaClient.getActiveTimeline().readCompactionPlanAsBytes(requestedInstant));
  }

  /**
   * Util method to fetch both compaction and log compaction plan from requestedInstant.
   */
  public static HoodieCompactionPlan getCompactionPlan(HoodieTableMetaClient metaClient, Option<byte[]> planContent) {
    CompactionPlanMigrator migrator = new CompactionPlanMigrator(metaClient);
    try {
      HoodieCompactionPlan compactionPlan = TimelineMetadataUtils.deserializeCompactionPlan(planContent.get());
      return migrator.upgradeToLatest(compactionPlan, compactionPlan.getVersion());
    } catch (IOException e) {
      throw new HoodieException(e);
    }
  }

  /**
   * Get all PartitionPath + file-ids with pending Compaction operations and their target compaction instant time.
   *
   * @param metaClient Hoodie Table Meta Client
   */
  public static Map<HoodieFileGroupId, Pair<String, HoodieCompactionOperation>> getAllPendingCompactionOperations(
      HoodieTableMetaClient metaClient) {
    List<Pair<HoodieInstant, HoodieCompactionPlan>> pendingCompactionPlanWithInstants =
        getAllPendingCompactionPlans(metaClient);
    return getAllPendingCompactionOperationsInPendingCompactionPlans(pendingCompactionPlanWithInstants);
  }

  /**
   * Get all partition + file Ids with pending Log Compaction operations and their target log compaction instant time.
   */
  public static Map<HoodieFileGroupId, Pair<String, HoodieCompactionOperation>> getAllPendingLogCompactionOperations(
      HoodieTableMetaClient metaClient) {
    List<Pair<HoodieInstant, HoodieCompactionPlan>> pendingLogCompactionPlanWithInstants =
        getAllPendingLogCompactionPlans(metaClient);
    return getAllPendingCompactionOperationsInPendingCompactionPlans(pendingLogCompactionPlanWithInstants);
  }

  /**
   * Get all partition + file Ids with pending Log Compaction operations and their target log compaction instant time.
   */
  public static Map<HoodieFileGroupId, Pair<String, HoodieCompactionOperation>> getAllPendingCompactionOperationsInPendingCompactionPlans(
      List<Pair<HoodieInstant, HoodieCompactionPlan>> pendingLogCompactionPlanWithInstants) {

    Map<HoodieFileGroupId, Pair<String, HoodieCompactionOperation>> fgIdToPendingCompactionsWithInstantMap = new HashMap<>();
    pendingLogCompactionPlanWithInstants.stream().flatMap(instantPlanPair ->
        getPendingCompactionOperations(instantPlanPair.getKey(), instantPlanPair.getValue())).forEach(pair -> {
          // Defensive check to ensure a single-fileId does not have more than one pending log compaction with different
          // file slices. If we find a full duplicate we assume it is caused by eventual nature of the move operation
          // on some DFSs.
          if (fgIdToPendingCompactionsWithInstantMap.containsKey(pair.getKey())) {
            HoodieCompactionOperation operation = pair.getValue().getValue();
            HoodieCompactionOperation anotherOperation = fgIdToPendingCompactionsWithInstantMap.get(pair.getKey()).getValue();

            if (!operation.equals(anotherOperation)) {
              String msg = "Hudi File Id (" + pair.getKey() + ") has more than 1 pending operation. Instants: "
                  + pair.getValue() + ", " + fgIdToPendingCompactionsWithInstantMap.get(pair.getKey());
              throw new IllegalStateException(msg);
            }
          }
          fgIdToPendingCompactionsWithInstantMap.put(pair.getKey(), pair.getValue());
        });
    return fgIdToPendingCompactionsWithInstantMap;
  }

  /**
   * Get pending compaction operations for both major and minor compaction.
   */
  public static Stream<Pair<HoodieFileGroupId, Pair<String, HoodieCompactionOperation>>> getPendingCompactionOperations(
      HoodieInstant instant, HoodieCompactionPlan compactionPlan) {
    List<HoodieCompactionOperation> ops = compactionPlan.getOperations();
    if (null != ops) {
      return ops.stream().map(op -> Pair.of(new HoodieFileGroupId(op.getPartitionPath(), op.getFileId()),
          Pair.of(instant.getTimestamp(), op)));
    } else {
      return Stream.empty();
    }
  }

  /**
   * Return all pending compaction instant times.
   *
   * @return
   */
  public static List<HoodieInstant> getPendingCompactionInstantTimes(HoodieTableMetaClient metaClient) {
    return metaClient.getActiveTimeline().filterPendingCompactionTimeline().getInstants();
  }

  /**
   * Returns a pair of (timeline containing the delta commits after the latest completed
   * compaction commit, the completed compaction commit instant), if the latest completed
   * compaction commit is present; a pair of (timeline containing all the delta commits,
   * the first delta commit instant), if there is no completed compaction commit.
   *
   * @param activeTimeline Active timeline of a table.
   * @return Pair of timeline containing delta commits and an instant.
   */
  public static Option<Pair<HoodieTimeline, HoodieInstant>> getDeltaCommitsSinceLatestCompaction(
      HoodieActiveTimeline activeTimeline) {
    Option<HoodieInstant> lastCompaction = activeTimeline.getCommitTimeline().filterCompletedInstants().lastInstant();
    HoodieTimeline deltaCommits = activeTimeline.getDeltaCommitTimeline();

    HoodieInstant latestInstant;
    if (lastCompaction.isPresent()) {
      latestInstant = lastCompaction.get();
      // timeline containing the delta commits after the latest completed compaction commit,
      // and the completed compaction commit instant
      return Option.of(Pair.of(deltaCommits.findInstantsAfter(
          latestInstant.getTimestamp(), Integer.MAX_VALUE), lastCompaction.get()));
    } else {
      if (deltaCommits.countInstants() > 0) {
        latestInstant = deltaCommits.firstInstant().get();
        // timeline containing all the delta commits, and the first delta commit instant
        return Option.of(Pair.of(deltaCommits.findInstantsAfterOrEquals(
            latestInstant.getTimestamp(), Integer.MAX_VALUE), latestInstant));
      } else {
        return Option.empty();
      }
    }
  }

  public static Option<Pair<HoodieTimeline, HoodieInstant>> getDeltaCommitsSinceLatestCompactionRequest(
        HoodieActiveTimeline activeTimeline) {
    Option<HoodieInstant> lastCompaction = activeTimeline.getCommitTimeline()
          .filterCompletedInstants().lastInstant();
    Option<HoodieInstant> lastRequestCompaction = activeTimeline.getAllCommitsTimeline()
          .filterPendingCompactionTimeline().lastInstant();
    if (lastRequestCompaction.isPresent()) {
      lastCompaction = lastRequestCompaction;
    }
    HoodieTimeline deltaCommits = activeTimeline.getDeltaCommitTimeline();

    HoodieInstant latestInstant;
    if (lastCompaction.isPresent()) {
      latestInstant = lastCompaction.get();
      // timeline containing the delta commits after the latest completed compaction commit,
      // and the completed compaction commit instant
      return Option.of(Pair.of(deltaCommits.findInstantsAfter(
            latestInstant.getTimestamp(), Integer.MAX_VALUE), lastCompaction.get()));
    } else {
      if (deltaCommits.countInstants() > 0) {
        latestInstant = deltaCommits.firstInstant().get();
        // timeline containing all the delta commits, and the first delta commit instant
        return Option.of(Pair.of(deltaCommits.findInstantsAfterOrEquals(
              latestInstant.getTimestamp(), Integer.MAX_VALUE), latestInstant));
      } else {
        return Option.empty();
      }
    }
  }

  /**
   * Gets the oldest instant to retain for MOR compaction.
   * If there is no completed compaction,
   * num delta commits >= "hoodie.compact.inline.max.delta.commits"
   * If there is a completed compaction,
   * num delta commits after latest completed compaction >= "hoodie.compact.inline.max.delta.commits"
   *
   * @param activeTimeline  Active timeline of a table.
   * @param maxDeltaCommits Maximum number of delta commits that trigger the compaction plan,
   *                        i.e., "hoodie.compact.inline.max.delta.commits".
   * @return the oldest instant to keep for MOR compaction.
   */
  public static Option<HoodieInstant> getOldestInstantToRetainForCompaction(
      HoodieActiveTimeline activeTimeline, int maxDeltaCommits) {
    Option<Pair<HoodieTimeline, HoodieInstant>> deltaCommitsInfoOption =
        CompactionUtils.getDeltaCommitsSinceLatestCompaction(activeTimeline);
    if (deltaCommitsInfoOption.isPresent()) {
      Pair<HoodieTimeline, HoodieInstant> deltaCommitsInfo = deltaCommitsInfoOption.get();
      HoodieTimeline deltaCommitTimeline = deltaCommitsInfo.getLeft();
      int numDeltaCommits = deltaCommitTimeline.countInstants();
      if (numDeltaCommits < maxDeltaCommits) {
        return Option.of(deltaCommitsInfo.getRight());
      } else {
        // delta commits with the last one to keep
        List<HoodieInstant> instants = deltaCommitTimeline.getInstantsAsStream()
            .limit(numDeltaCommits - maxDeltaCommits + 1).collect(Collectors.toList());
        return Option.of(instants.get(instants.size() - 1));
      }
    }
    return Option.empty();
  }
}
