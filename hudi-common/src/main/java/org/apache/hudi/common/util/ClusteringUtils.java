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

import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieClusteringStrategy;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieSliceInfo;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.table.timeline.TableFormatCompletionAction;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantGeneratorV2;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;

/**
 * Helper class to generate clustering plan from metadata.
 */
public class ClusteringUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ClusteringUtils.class);

  public static final String TOTAL_IO_READ_MB = "TOTAL_IO_READ_MB";
  public static final String TOTAL_LOG_FILE_SIZE = "TOTAL_LOG_FILES_SIZE";
  public static final String TOTAL_LOG_FILES = "TOTAL_LOG_FILES";

  /**
   * Get all pending clustering plans along with their instants.
   */
  public static Stream<Pair<HoodieInstant, HoodieClusteringPlan>> getAllPendingClusteringPlans(
      HoodieTableMetaClient metaClient) {
    List<HoodieInstant> pendingClusterInstants =
        metaClient.getActiveTimeline().filterPendingReplaceOrClusteringTimeline().getInstants();
    return pendingClusterInstants.stream().map(instant -> getClusteringPlan(metaClient, instant))
        .filter(Option::isPresent).map(Option::get);
  }

  /**
   * Returns the pending clustering instant. This can be older pending replace commit or a new
   * clustering inflight commit. After HUDI-7905, all the requested and inflight clustering instants
   * use clustering action instead of replacecommit.
   */
  public static Option<HoodieInstant> getInflightClusteringInstant(String timestamp, HoodieActiveTimeline activeTimeline, InstantGenerator factory) {
    HoodieTimeline pendingReplaceOrClusterTimeline = activeTimeline.filterPendingReplaceOrClusteringTimeline();
    HoodieInstant inflightInstant = factory.getClusteringCommitInflightInstant(timestamp);
    if (pendingReplaceOrClusterTimeline.containsInstant(inflightInstant)) {
      return Option.of(inflightInstant);
    }
    inflightInstant = factory.getReplaceCommitInflightInstant(timestamp);
    return Option.ofNullable(pendingReplaceOrClusterTimeline.containsInstant(inflightInstant) ? inflightInstant : null);
  }

  /**
   * Returns the pending clustering instant. This can be older requested replace commit or a new
   * clustering requested commit. After HUDI-7905, all the requested and inflight clustering instants
   * use clustering action instead of replacecommit.
   */
  public static Option<HoodieInstant> getRequestedClusteringInstant(String timestamp, HoodieActiveTimeline activeTimeline, InstantGenerator factory) {
    HoodieTimeline pendingReplaceOrClusterTimeline = activeTimeline.filterPendingReplaceOrClusteringTimeline();
    HoodieInstant requestedInstant = factory.getClusteringCommitRequestedInstant(timestamp);
    if (pendingReplaceOrClusterTimeline.containsInstant(timestamp)) {
      return Option.of(requestedInstant);
    }
    requestedInstant = factory.getReplaceCommitRequestedInstant(timestamp);
    return Option.ofNullable(pendingReplaceOrClusterTimeline.containsInstant(requestedInstant) ? requestedInstant : null);
  }

  /**
   * Transitions the provided clustering instant fron inflight to complete based on the clustering
   * action type. After HUDI-7905, the new clustering commits are written with clustering action.
   */
  public static <T> void transitionClusteringOrReplaceInflightToComplete(boolean shouldLock, HoodieInstant clusteringInstant,
                                                                         HoodieReplaceCommitMetadata metadata, HoodieActiveTimeline activeTimeline,
                                                                         TableFormatCompletionAction tableFormatCompletionAction) {
    if (clusteringInstant.getAction().equals(HoodieTimeline.CLUSTERING_ACTION)) {
      activeTimeline.transitionClusterInflightToComplete(shouldLock, clusteringInstant, metadata, tableFormatCompletionAction);
    } else {
      activeTimeline.transitionReplaceInflightToComplete(shouldLock, clusteringInstant, metadata, tableFormatCompletionAction);
    }
  }

  /**
   * Transitions the provided clustering instant fron requested to inflight based on the clustering
   * action type. After HUDI-7905, the new clustering commits are written with clustering action.
   */
  public static <T> void transitionClusteringOrReplaceRequestedToInflight(
      HoodieInstant requestedClusteringInstant, Option<T> metadata, HoodieActiveTimeline activeTimeline) {
    if (requestedClusteringInstant.getAction().equals(HoodieTimeline.CLUSTERING_ACTION)) {
      activeTimeline.transitionClusterRequestedToInflight(requestedClusteringInstant, metadata);
    } else {
      activeTimeline.transitionReplaceRequestedToInflight(requestedClusteringInstant, metadata);
    }
  }

  /**
   * Checks if the requested, inflight, or completed instant of replacecommit action
   * is a clustering operation, by checking whether the requested instant contains
   * a clustering plan.
   *
   * @param actionType      Action type
   * @return whether the action type is a clustering or replace commit action type
   */
  public static boolean isClusteringOrReplaceCommitAction(String actionType) {
    return actionType.equals(HoodieTimeline.CLUSTERING_ACTION) || actionType.equals(HoodieTimeline.REPLACE_COMMIT_ACTION);
  }

  /**
   * Checks if the action type is a clustering or replace commit action type.
   *
   * @param timeline       Hudi timeline.
   * @param replaceInstant the instant of replacecommit action to check.
   * @return whether the instant is a clustering operation.
   */
  public static boolean isClusteringInstant(HoodieTimeline timeline, HoodieInstant replaceInstant, InstantGenerator factory) {
    return replaceInstant.getAction().equals(HoodieTimeline.CLUSTERING_ACTION)
        || (replaceInstant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION) && getClusteringPlan(timeline, replaceInstant, factory).isPresent());
  }

  /**
   * Get requested replace metadata from timeline.
   *
   * @param timeline              used to get the bytes stored in the requested replace instant in the timeline
   * @param pendingReplaceOrClusterInstant can be in any state, because it will always be converted to requested state
   * @return option of the replace metadata if present, else empty
   * @throws IOException
   */
  private static Option<HoodieRequestedReplaceMetadata> getRequestedReplaceMetadata(HoodieTimeline timeline, HoodieInstant pendingReplaceOrClusterInstant,
                                                                                    InstantGenerator factory) throws IOException {
    HoodieInstant requestedInstant;
    if (pendingReplaceOrClusterInstant.isInflight()) {
      // inflight replacecommit files don't have clustering plan.
      // This is because replacecommit inflight can have workload profile for 'insert_overwrite'.
      // Get the plan from corresponding requested instant.
      requestedInstant = factory.createNewInstant(HoodieInstant.State.REQUESTED, pendingReplaceOrClusterInstant.getAction(), pendingReplaceOrClusterInstant.requestedTime());
    } else if (pendingReplaceOrClusterInstant.isRequested()) {
      requestedInstant = pendingReplaceOrClusterInstant;
    } else {
      String action = factory instanceof InstantGeneratorV2 ? HoodieTimeline.CLUSTERING_ACTION : HoodieTimeline.REPLACE_COMMIT_ACTION;
      requestedInstant = factory.createNewInstant(HoodieInstant.State.REQUESTED, action, pendingReplaceOrClusterInstant.requestedTime());
    }
    try {
      // First assume the instant file is not empty and parse it.
      return getRequestedReplaceMetadataOption(timeline, pendingReplaceOrClusterInstant, factory, requestedInstant);
    } catch (Exception ex) {
      // If anything goes wrong, check if this is empty file.
      if (isEmptyReplaceOrClusteringInstant(timeline, pendingReplaceOrClusterInstant, factory, requestedInstant)) {
        return Option.empty();
      }
      // If still no luck, throw the exception.
      throw ex;
    }
  }

  private static Option<HoodieRequestedReplaceMetadata> getRequestedReplaceMetadataOption(
      HoodieTimeline timeline, HoodieInstant pendingReplaceOrClusterInstant, InstantGenerator factory, HoodieInstant requestedInstant) throws IOException {
    try {
      return Option.of(timeline.readRequestedReplaceMetadata(requestedInstant));
    } catch (HoodieIOException e) {
      if (e.getCause() instanceof FileNotFoundException && pendingReplaceOrClusterInstant.isCompleted()) {
        // For clustering instants, completed instant is also a replace commit instant. For input replace commit instant,
        // it is not known whether requested instant is CLUSTER or REPLACE_COMMIT_ACTION. So we need to query both.
        requestedInstant = factory.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, pendingReplaceOrClusterInstant.requestedTime());
        return Option.of(timeline.readRequestedReplaceMetadata(requestedInstant));
      }
      throw e;
    }
  }

  private static boolean isEmptyReplaceOrClusteringInstant(HoodieTimeline timeline,
                                                           HoodieInstant pendingReplaceOrClusterInstant,
                                                           InstantGenerator instantGenerator,
                                                           HoodieInstant requestedInstant) {
    try {
      return timeline.isEmpty(requestedInstant);
    } catch (HoodieIOException e) {
      if (e.getCause() instanceof FileNotFoundException && pendingReplaceOrClusterInstant.isCompleted()) {
        // For clustering instants, completed instant is also a replace commit instant. For input replace commit instant,
        // it is not known whether requested instant is CLUSTER or REPLACE_COMMIT_ACTION. So we need to query both.
        requestedInstant = instantGenerator.createNewInstant(
            HoodieInstant.State.REQUESTED,
            HoodieTimeline.REPLACE_COMMIT_ACTION,
            pendingReplaceOrClusterInstant.requestedTime());
        return timeline.isEmpty(requestedInstant);
      }
      throw e;
    }
  }

  /**
   * Get Clustering plan from timeline.
   * @param metaClient used to get the active timeline
   * @param pendingReplaceInstant can be in any state, because it will always be converted to requested state
   * @return option of the replace metadata if present, else empty
   */
  public static Option<Pair<HoodieInstant, HoodieClusteringPlan>> getClusteringPlan(HoodieTableMetaClient metaClient, HoodieInstant pendingReplaceInstant) {
    return getClusteringPlan(metaClient.getActiveTimeline(), pendingReplaceInstant, metaClient.getInstantGenerator());
  }

  /**
   * Returns clustering plan for pending clustering operation with given clustering timestamp
   * @param metaClient HoodieTableMetaClient
   * @param clusteringCommitTime Timestamp of clustering operation
   * @return HoodieClusteringPlan for the corresponding clustering operation
   */
  public static HoodieClusteringPlan getPendingClusteringPlan(HoodieTableMetaClient metaClient, String clusteringCommitTime) {
    return getClusteringPlan(
        metaClient, getRequestedClusteringInstant(clusteringCommitTime, metaClient.getActiveTimeline(), metaClient.getInstantGenerator()).get())
        .map(Pair::getRight).orElseThrow(() -> new HoodieClusteringException(
            "Unable to read clustering plan for instant: " + clusteringCommitTime));
  }

  /**
   * Get Clustering plan from timeline.
   * @param timeline
   * @param pendingReplaceInstant
   * @return
   */
  public static Option<Pair<HoodieInstant, HoodieClusteringPlan>> getClusteringPlan(HoodieTimeline timeline, HoodieInstant pendingReplaceInstant, InstantGenerator factory) {
    try {
      Option<HoodieRequestedReplaceMetadata> requestedReplaceMetadata = getRequestedReplaceMetadata(timeline, pendingReplaceInstant, factory);
      if (requestedReplaceMetadata.isPresent() && WriteOperationType.CLUSTER.name().equals(requestedReplaceMetadata.get().getOperationType())) {
        return Option.of(Pair.of(pendingReplaceInstant, requestedReplaceMetadata.get().getClusteringPlan()));
      }
      return Option.empty();
    } catch (IOException e) {
      throw new HoodieIOException("Error reading clustering plan " + pendingReplaceInstant.requestedTime(), e);
    }
  }

  /**
   * Get filegroups to pending clustering instant mapping for all pending clustering plans.
   * This includes all clustering operations in 'requested' and 'inflight' states.
   */
  public static Map<HoodieFileGroupId, HoodieInstant> getAllFileGroupsInPendingClusteringPlans(
      HoodieTableMetaClient metaClient) {
    Stream<Pair<HoodieInstant, HoodieClusteringPlan>> pendingClusteringPlans = getAllPendingClusteringPlans(metaClient);
    Stream<Map.Entry<HoodieFileGroupId, HoodieInstant>> resultStream = pendingClusteringPlans.flatMap(clusteringPlan ->
        // get all filegroups in the plan
        getFileGroupEntriesInClusteringPlan(clusteringPlan.getLeft(), clusteringPlan.getRight()));

    Map<HoodieFileGroupId, HoodieInstant> resultMap;
    try {
      resultMap = resultStream.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    } catch (Exception e) {
      if (e instanceof IllegalStateException && e.getMessage().contains("Duplicate key")) {
        throw new HoodieException("Found duplicate file groups pending clustering. If you're running deltastreamer in continuous mode, consider adding delay using --min-sync-interval-seconds. "
            + "Or consider setting write concurrency mode to OPTIMISTIC_CONCURRENCY_CONTROL.", e);
      }
      throw new HoodieException("Error getting all file groups in pending clustering", e);
    }
    LOG.info("Found " + resultMap.size() + " files in pending clustering operations");
    return resultMap;
  }

  public static Stream<Pair<HoodieFileGroupId, HoodieInstant>> getFileGroupsInPendingClusteringInstant(
      HoodieInstant instant, HoodieClusteringPlan clusteringPlan) {
    Stream<HoodieFileGroupId> partitionToFileIdLists = clusteringPlan.getInputGroups().stream().flatMap(ClusteringUtils::getFileGroupsFromClusteringGroup);
    return partitionToFileIdLists.map(e -> Pair.of(e, instant));
  }

  private static Stream<Map.Entry<HoodieFileGroupId, HoodieInstant>> getFileGroupEntriesInClusteringPlan(
      HoodieInstant instant, HoodieClusteringPlan clusteringPlan) {
    return getFileGroupsInPendingClusteringInstant(instant, clusteringPlan).map(entry ->
        new AbstractMap.SimpleEntry<>(entry.getLeft(), entry.getRight()));
  }

  public static Stream<HoodieFileGroupId> getFileGroupsFromClusteringPlan(HoodieClusteringPlan clusteringPlan) {
    return clusteringPlan.getInputGroups().stream().flatMap(ClusteringUtils::getFileGroupsFromClusteringGroup);
  }

  public static Stream<HoodieFileGroupId> getFileGroupsFromClusteringGroup(HoodieClusteringGroup group) {
    return group.getSlices().stream().map(slice -> new HoodieFileGroupId(slice.getPartitionPath(), slice.getFileId()));
  }

  /**
   * Create clustering plan from input fileSliceGroups.
   */
  public static HoodieClusteringPlan createClusteringPlan(String strategyClassName,
                                                          Map<String, String> strategyParams,
                                                          List<FileSlice>[] fileSliceGroups,
                                                          Map<String, String> extraMetadata) {
    List<HoodieClusteringGroup> clusteringGroups = Arrays.stream(fileSliceGroups).map(fileSliceGroup -> {
      Map<String, Double> groupMetrics = buildMetrics(fileSliceGroup);
      List<HoodieSliceInfo> sliceInfos = getFileSliceInfo(fileSliceGroup);
      return HoodieClusteringGroup.newBuilder().setSlices(sliceInfos).setMetrics(groupMetrics).build();
    }).collect(Collectors.toList());

    HoodieClusteringStrategy strategy = HoodieClusteringStrategy.newBuilder()
        .setStrategyClassName(strategyClassName).setStrategyParams(strategyParams)
        .build();

    return HoodieClusteringPlan.newBuilder()
        .setInputGroups(clusteringGroups)
        .setExtraMetadata(extraMetadata)
        .setStrategy(strategy)
        .setPreserveHoodieMetadata(true)
        .build();
  }

  private static List<HoodieSliceInfo> getFileSliceInfo(List<FileSlice> slices) {
    return slices.stream().map(slice -> HoodieSliceInfo.newBuilder()
        .setPartitionPath(slice.getPartitionPath())
        .setFileId(slice.getFileId())
        .setDataFilePath(slice.getBaseFile().map(BaseFile::getPath).orElse(null))
        .setDeltaFilePaths(slice.getLogFiles().map(f -> f.getPath().getName()).collect(Collectors.toList()))
        .setBootstrapFilePath(slice.getBaseFile().map(bf -> bf.getBootstrapBaseFile().map(BaseFile::getPath).orElse(null)).orElse(null))
        .build()).collect(Collectors.toList());
  }

  private static Map<String, Double> buildMetrics(List<FileSlice> fileSlices) {
    int numLogFiles = 0;
    long totalLogFileSize = 0;
    long totalIORead = 0;

    for (FileSlice slice : fileSlices) {
      numLogFiles +=  slice.getLogFiles().count();
      // Total size of all the log files
      totalLogFileSize += slice.getLogFiles().map(HoodieLogFile::getFileSize).filter(size -> size >= 0)
          .reduce(Long::sum).orElse(0L);
      // Total read will be the base file + all the log files
      totalIORead =
          FSUtils.getSizeInMB((slice.getBaseFile().isPresent() ? slice.getBaseFile().get().getFileSize() : 0L) + totalLogFileSize);
    }

    Map<String, Double> metrics = new HashMap<>();
    metrics.put(TOTAL_IO_READ_MB, (double) totalIORead);
    metrics.put(TOTAL_LOG_FILE_SIZE, (double) totalLogFileSize);
    metrics.put(TOTAL_LOG_FILES, (double) numLogFiles);
    return metrics;
  }

  public static List<HoodieInstant> getPendingClusteringInstantTimes(HoodieTableMetaClient metaClient) {
    InstantGenerator factory = metaClient.getInstantGenerator();
    return metaClient.getActiveTimeline().filterPendingReplaceOrClusteringTimeline().getInstantsAsStream()
        .filter(instant -> isClusteringInstant(metaClient.getActiveTimeline(), instant, factory))
        .collect(Collectors.toList());
  }

  /**
   * Returns the earliest instant to retain.
   * Make sure the clustering instant won't be archived before cleaned, and the earliest inflight clustering instant has a previous commit.
   *
   * @param activeTimeline               The active timeline
   * @param metaClient                   The meta client
   * @param cleanerPolicy                The hoodie cleaning policy
   * @return the earliest instant to retain for clustering
   */
  public static Option<HoodieInstant> getEarliestInstantToRetainForClustering(
      HoodieActiveTimeline activeTimeline, HoodieTableMetaClient metaClient, HoodieCleaningPolicy cleanerPolicy) throws IOException {
    Option<HoodieInstant> oldestInstantToRetain = Option.empty();
    HoodieTimeline replaceOrClusterTimeline = activeTimeline.getTimelineOfActions(CollectionUtils.createSet(HoodieTimeline.REPLACE_COMMIT_ACTION, HoodieTimeline.CLUSTERING_ACTION));
    if (!replaceOrClusterTimeline.empty()) {
      Option<HoodieInstant> cleanInstantOpt =
          activeTimeline.getCleanerTimeline().filterCompletedInstants().lastInstant();
      if (cleanInstantOpt.isPresent()) {
        // The first clustering instant of which timestamp is greater than or equal to the earliest commit to retain of
        // the clean metadata.
        HoodieInstant cleanInstant = cleanInstantOpt.get();
        HoodieCleanerPlan cleanerPlan = CleanerUtils.getCleanerPlan(metaClient, cleanInstant);
        Option<String> earliestInstantToRetain = Option.ofNullable(cleanerPlan.getEarliestInstantToRetain()).map(HoodieActionInstant::getTimestamp);
        String retainLowerBound;
        Option<String> earliestReplacedSavepointInClean = getEarliestReplacedSavepointInClean(activeTimeline, cleanerPolicy, cleanerPlan);
        if (earliestReplacedSavepointInClean.isPresent()) {
          retainLowerBound = earliestReplacedSavepointInClean.get();
        } else if (earliestInstantToRetain.isPresent() && !StringUtils.isNullOrEmpty(earliestInstantToRetain.get())) {
          retainLowerBound = earliestInstantToRetain.get();
        } else {
          // no earliestInstantToRetain, indicate KEEP_LATEST_FILE_VERSIONS clean policy,
          // retain first instant after clean instant.
          // For KEEP_LATEST_FILE_VERSIONS cleaner policy, file versions are only maintained for active file groups
          // not for replaced file groups. So, last clean instant can be considered as a lower bound, since
          // the cleaner would have removed all the file groups until then. But there is a catch to this logic,
          // while cleaner is running if there is a pending replacecommit then those files are not cleaned.
          // TODO: This case has to be handled. HUDI-6352
          retainLowerBound = cleanInstant.requestedTime();
        }
        oldestInstantToRetain = replaceOrClusterTimeline.findInstantsAfterOrEquals(retainLowerBound).firstInstant();
      } else {
        oldestInstantToRetain = replaceOrClusterTimeline.firstInstant();
      }
    }
    return oldestInstantToRetain;
  }

  public static Option<String> getEarliestReplacedSavepointInClean(HoodieActiveTimeline activeTimeline, HoodieCleaningPolicy cleanerPolicy,
                                                                   HoodieCleanerPlan cleanerPlan) {
    // EarliestSavepoint in clean is required to block archival when savepoint is deleted.
    // This ensures that archival is blocked until clean has cleaned up files retained due to savepoint.
    // If this guard is not present, the archiving of commits can lead to duplicates. Here is a scenario
    // illustrating the same. This scenario considers a case where EarliestSavepoint guard is not present
    // c1.dc - f1 (c1 deltacommit creates file with id f1)
    // c2.dc - f2 (c2 deltacommit creates file with id f2)
    // c2.sp - Savepoint at c2
    // c3.rc (replacing f2 -> f3) (Replace commit replacing file id f2 with f3)
    // c4.dc
    //
    // Lets say Incremental cleaner moved past the c3.rc without cleaning f2 since savepoint is created at c2.
    // Archival is blocked at c2 since there is a savepoint at c2.
    // Let's say the savepoint at c2 is now deleted, Archival would archive c3.rc since it is unblocked now.
    // Since c3 is archived and f2 has not been cleaned, the table view would be considering f2 as a valid
    // file id. This causes duplicates.

    String earliestInstantToRetain = Option.ofNullable(cleanerPlan.getEarliestInstantToRetain()).map(HoodieActionInstant::getTimestamp).orElse(null);
    Option<String[]> savepoints = Option.ofNullable(cleanerPlan.getExtraMetadata()).map(metadata -> metadata.getOrDefault(CleanerUtils.SAVEPOINTED_TIMESTAMPS, StringUtils.EMPTY_STRING).split(","));
    String earliestSavepoint = savepoints.flatMap(arr -> Option.fromJavaOptional(Arrays.stream(arr).sorted().findFirst())).orElse(null);
    if (cleanerPolicy != HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS) {
      if (!StringUtils.isNullOrEmpty(earliestInstantToRetain) && !StringUtils.isNullOrEmpty(earliestSavepoint)) {
        // When earliestToRetainTs is greater than first savepoint timestamp and there are no
        // replace commits between the first savepoint and the earliestToRetainTs, we can set the
        // earliestSavepointOpt to empty as there was no cleaning blocked due to savepoint
        if (compareTimestamps(earliestInstantToRetain, GREATER_THAN, earliestSavepoint)) {
          HoodieTimeline replaceTimeline = activeTimeline.getCompletedReplaceTimeline().findInstantsInClosedRange(earliestSavepoint, earliestInstantToRetain);
          if (!replaceTimeline.empty()) {
            return Option.of(earliestSavepoint);
          }
        }
      }
    }
    return Option.empty();
  }

  /**
   * @param instant  Hudi instant to check.
   * @param timeline Hudi timeline.
   * @return whether the given {@code instant} is a completed clustering operation.
   */
  public static boolean isCompletedClusteringInstant(HoodieInstant instant, HoodieTimeline timeline) {
    if (!instant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION)) {
      return false;
    }
    try {
      return TimelineUtils.getCommitMetadata(instant, timeline).getOperationType().equals(WriteOperationType.CLUSTER);
    } catch (IOException e) {
      throw new HoodieException("Resolve replace commit metadata error for instant: " + instant, e);
    }
  }

  /**
   * Returns whether the given instant {@code instant} is with insert overwrite operation.
   */
  public static boolean isInsertOverwriteInstant(HoodieInstant instant, HoodieTimeline timeline) {
    if (!instant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION)) {
      return false;
    }
    try {
      WriteOperationType opType = TimelineUtils.getCommitMetadata(instant, timeline).getOperationType();
      return opType.equals(WriteOperationType.INSERT_OVERWRITE) || opType.equals(WriteOperationType.INSERT_OVERWRITE_TABLE);
    } catch (IOException e) {
      throw new HoodieException("Resolve replace commit metadata error for instant: " + instant, e);
    }
  }

  public static Option<HoodieFileReader> getBaseFileReader(HoodieStorage storage, HoodieRecord.HoodieRecordType recordType, HoodieConfig config, String dataFilePath) {
    if (StringUtils.isNullOrEmpty(dataFilePath)) {
      return Option.empty();
    }
    try {
      return Option.of(HoodieIOFactory.getIOFactory(storage)
          .getReaderFactory(recordType)
          .getFileReader(config, new StoragePath(dataFilePath)));
    } catch (IOException e) {
      throw new HoodieIOException("Error reading base file " + dataFilePath, e);
    }
  }
}
