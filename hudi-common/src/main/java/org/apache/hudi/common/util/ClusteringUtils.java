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
import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieClusteringStrategy;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieSliceInfo;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    List<HoodieInstant> pendingReplaceInstants =
        metaClient.getActiveTimeline().filterPendingReplaceTimeline().getInstants();
    return pendingReplaceInstants.stream().map(instant -> getClusteringPlan(metaClient, instant))
        .filter(Option::isPresent).map(Option::get);
  }

  /**
   * Checks if the requested, inflight, or completed instant of replacecommit action
   * is a clustering operation, by checking whether the requested instant contains
   * a clustering plan.
   *
   * @param timeline       Hudi timeline.
   * @param replaceInstant the instant of replacecommit action to check.
   * @return whether the instant is a clustering operation.
   */
  public static boolean isClusteringInstant(HoodieTimeline timeline, HoodieInstant replaceInstant) {
    return getClusteringPlan(timeline, replaceInstant).isPresent();
  }

  /**
   * Get requested replace metadata from timeline.
   *
   * @param timeline              used to get the bytes stored in the requested replace instant in the timeline
   * @param pendingReplaceInstant can be in any state, because it will always be converted to requested state
   * @return option of the replace metadata if present, else empty
   * @throws IOException
   */
  private static Option<HoodieRequestedReplaceMetadata> getRequestedReplaceMetadata(HoodieTimeline timeline, HoodieInstant pendingReplaceInstant) throws IOException {
    final HoodieInstant requestedInstant;
    if (!pendingReplaceInstant.isRequested()) {
      // inflight replacecommit files don't have clustering plan.
      // This is because replacecommit inflight can have workload profile for 'insert_overwrite'.
      // Get the plan from corresponding requested instant.
      requestedInstant = HoodieTimeline.getReplaceCommitRequestedInstant(pendingReplaceInstant.getTimestamp());
    } else {
      requestedInstant = pendingReplaceInstant;
    }
    Option<byte[]> content = timeline.getInstantDetails(requestedInstant);
    if (!content.isPresent() || content.get().length == 0) {
      // few operations create requested file without any content. Assume these are not clustering
      return Option.empty();
    }
    return Option.of(TimelineMetadataUtils.deserializeRequestedReplaceMetadata(content.get()));
  }

  /**
   * Get Clustering plan from timeline.
   * @param metaClient used to get the active timeline
   * @param pendingReplaceInstant can be in any state, because it will always be converted to requested state
   * @return option of the replace metadata if present, else empty
   */
  public static Option<Pair<HoodieInstant, HoodieClusteringPlan>> getClusteringPlan(HoodieTableMetaClient metaClient, HoodieInstant pendingReplaceInstant) {
    return getClusteringPlan(metaClient.getActiveTimeline(), pendingReplaceInstant);
  }

  /**
   * Get Clustering plan from timeline.
   * @param timeline
   * @param pendingReplaceInstant
   * @return
   */
  public static Option<Pair<HoodieInstant, HoodieClusteringPlan>> getClusteringPlan(HoodieTimeline timeline, HoodieInstant pendingReplaceInstant) {
    try {
      Option<HoodieRequestedReplaceMetadata> requestedReplaceMetadata = getRequestedReplaceMetadata(timeline, pendingReplaceInstant);
      if (requestedReplaceMetadata.isPresent() && WriteOperationType.CLUSTER.name().equals(requestedReplaceMetadata.get().getOperationType())) {
        return Option.of(Pair.of(pendingReplaceInstant, requestedReplaceMetadata.get().getClusteringPlan()));
      }
      return Option.empty();
    } catch (IOException e) {
      throw new HoodieIOException("Error reading clustering plan " + pendingReplaceInstant.getTimestamp(), e);
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
    return metaClient.getActiveTimeline().filterPendingReplaceTimeline().getInstantsAsStream()
        .filter(instant -> isClusteringInstant(metaClient.getActiveTimeline(), instant))
        .collect(Collectors.toList());
  }

  /**
   * Returns the oldest instant to retain.
   * Make sure the clustering instant won't be archived before cleaned, and the oldest inflight clustering instant has a previous commit.
   *
   * @param activeTimeline The active timeline
   * @param metaClient     The meta client
   * @return the oldest instant to retain for clustering
   */
  public static Option<HoodieInstant> getOldestInstantToRetainForClustering(
      HoodieActiveTimeline activeTimeline, HoodieTableMetaClient metaClient) throws IOException {
    Option<HoodieInstant> oldestInstantToRetain = Option.empty();
    HoodieTimeline replaceTimeline = activeTimeline.getTimelineOfActions(CollectionUtils.createSet(HoodieTimeline.REPLACE_COMMIT_ACTION));
    if (!replaceTimeline.empty()) {
      Option<HoodieInstant> cleanInstantOpt =
          activeTimeline.getCleanerTimeline().filterCompletedInstants().lastInstant();
      if (cleanInstantOpt.isPresent()) {
        // The first clustering instant of which timestamp is greater than or equal to the earliest commit to retain of
        // the clean metadata.
        HoodieInstant cleanInstant = cleanInstantOpt.get();
        HoodieActionInstant earliestInstantToRetain = CleanerUtils.getCleanerPlan(metaClient, cleanInstant.isRequested()
                ? cleanInstant
                : HoodieTimeline.getCleanRequestedInstant(cleanInstant.getTimestamp()))
            .getEarliestInstantToRetain();
        String retainLowerBound;
        if (earliestInstantToRetain != null && !StringUtils.isNullOrEmpty(earliestInstantToRetain.getTimestamp())) {
          retainLowerBound = earliestInstantToRetain.getTimestamp();
        } else {
          // no earliestInstantToRetain, indicate KEEP_LATEST_FILE_VERSIONS clean policy,
          // retain first instant after clean instant.
          // For KEEP_LATEST_FILE_VERSIONS cleaner policy, file versions are only maintained for active file groups
          // not for replaced file groups. So, last clean instant can be considered as a lower bound, since
          // the cleaner would have removed all the file groups until then. But there is a catch to this logic,
          // while cleaner is running if there is a pending replacecommit then those files are not cleaned.
          // TODO: This case has to be handled. HUDI-6352
          retainLowerBound = cleanInstant.getTimestamp();
        }

        oldestInstantToRetain = replaceTimeline.filter(instant ->
                HoodieTimeline.compareTimestamps(
                    instant.getTimestamp(),
                    HoodieTimeline.GREATER_THAN_OR_EQUALS,
                    retainLowerBound))
            .firstInstant();
      } else {
        oldestInstantToRetain = replaceTimeline.firstInstant();
      }

      Option<HoodieInstant> pendingInstantOpt = replaceTimeline.filterInflights().firstInstant();
      if (pendingInstantOpt.isPresent()) {
        // Get the previous commit before the first inflight clustering instant.
        Option<HoodieInstant> beforePendingInstant = activeTimeline.getCommitsTimeline()
            .filterCompletedInstants()
            .findInstantsBefore(pendingInstantOpt.get().getTimestamp())
            .lastInstant();
        if (beforePendingInstant.isPresent()
            && oldestInstantToRetain.map(instant -> instant.compareTo(beforePendingInstant.get()) > 0).orElse(true)) {
          oldestInstantToRetain = beforePendingInstant;
        }
      }
    }
    return oldestInstantToRetain;
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
}
