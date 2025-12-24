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
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Helper class to generate clustering plan from metadata.
 */
public class ClusteringUtils {

  private static final Logger LOG = LogManager.getLogger(ClusteringUtils.class);

  public static final String TOTAL_IO_READ_MB = "TOTAL_IO_READ_MB";
  public static final String TOTAL_LOG_FILE_SIZE = "TOTAL_LOG_FILES_SIZE";
  public static final String TOTAL_LOG_FILES = "TOTAL_LOG_FILES";
  public static final String LSM_CLUSTERING_OUT_PUT_LEVEL = "hoodie.clustering.lsm.output.level";
  public static final String LSM_CLUSTERING_OUT_PUT_LEVEL_1 = "1";

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
   * Get requested replace metadata from timeline.
   * @param metaClient
   * @param pendingReplaceInstant
   * @return
   * @throws IOException
   */
  private static Option<HoodieRequestedReplaceMetadata> getRequestedReplaceMetadata(HoodieTableMetaClient metaClient, HoodieInstant pendingReplaceInstant) throws IOException {
    final HoodieInstant requestedInstant;
    if (!pendingReplaceInstant.isRequested()) {
      // inflight replacecommit files don't have clustering plan.
      // This is because replacecommit inflight can have workload profile for 'insert_overwrite'.
      // Get the plan from corresponding requested instant.
      requestedInstant = HoodieTimeline.getReplaceCommitRequestedInstant(pendingReplaceInstant.getTimestamp());
    } else {
      requestedInstant = pendingReplaceInstant;
    }
    Option<byte[]> content = metaClient.getActiveTimeline().getInstantDetails(requestedInstant);
    if (!content.isPresent() || content.get().length == 0) {
      // few operations create requested file without any content. Assume these are not clustering
      return Option.empty();
    }
    return Option.of(TimelineMetadataUtils.deserializeRequestedReplaceMetadata(content.get()));
  }

  /**
   * Get Clustering plan from timeline.
   * @param metaClient
   * @param pendingReplaceInstant
   * @return
   */
  public static Option<Pair<HoodieInstant, HoodieClusteringPlan>> getClusteringPlan(HoodieTableMetaClient metaClient, HoodieInstant pendingReplaceInstant) {
    try {
      Option<HoodieRequestedReplaceMetadata> requestedReplaceMetadata = getRequestedReplaceMetadata(metaClient, pendingReplaceInstant);
      if (requestedReplaceMetadata.isPresent() && WriteOperationType.CLUSTER.name().equals(requestedReplaceMetadata.get().getOperationType())) {
        return Option.of(Pair.of(pendingReplaceInstant, requestedReplaceMetadata.get().getClusteringPlan()));
      }
      return Option.empty();
    } catch (HoodieIOException hie) {
      if (hie.getCause() instanceof FileNotFoundException) {
        LOG.warn(hie);
        return Option.empty();
      }
      throw new HoodieException("Error reading clustering plan " + pendingReplaceInstant.getTimestamp(), hie);
    } catch (IOException e) {
      throw new HoodieIOException("Error reading clustering plan " + pendingReplaceInstant.getTimestamp(), e);
    }
  }

  /**
   * Get Clustering plan from timeline.
   *
   * @param metaClient
   * @param pendingReplaceInstant
   * @return
   */
  public static Option<Pair<HoodieInstant, HoodieClusteringPlan>> getClusteringPlan(HoodieTableMetaClient metaClient,
                                                                                    HoodieInstant pendingReplaceInstant,
                                                                                    boolean isCallProcedure) {
    try {
      Option<HoodieRequestedReplaceMetadata> requestedReplaceMetadata = getRequestedReplaceMetadata(metaClient, pendingReplaceInstant);
      boolean isClustering = WriteOperationType.CLUSTER.name().equals(requestedReplaceMetadata.get().getOperationType());
      if (isCallProcedure) {
        isClustering = true;
      }
      if (requestedReplaceMetadata.isPresent() && isClustering) {
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
   *
   * @return Pair of Map(HoodieFileGroupId, HoodieInstant) and Map(PartitionPath, LSM FileId which contains level 1 file)
   */
  public static Pair<Map<HoodieFileGroupId, HoodieInstant>, Map<String, Set<String>>> getAllFileGroupsInPendingClusteringPlans(
      HoodieTableMetaClient metaClient) {
    List<Pair<HoodieInstant, HoodieClusteringPlan>> pendingClusteringPlans = getAllPendingClusteringPlans(metaClient).collect(Collectors.toList());
    Stream<Map.Entry<HoodieFileGroupId, HoodieInstant>> resultStream = pendingClusteringPlans.stream().flatMap(clusteringPlan ->
        // get all filegroups in the plan
        getFileGroupEntriesInClusteringPlan(clusteringPlan.getLeft(), clusteringPlan.getRight()));

    Map<HoodieFileGroupId, HoodieInstant> resultMap;
    try {
      resultMap = resultStream.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    } catch (Exception e) {
      if (e instanceof IllegalStateException && e.getMessage().contains("Duplicate key")) {
        throw new HoodieException("Found duplicate file groups pending clustering. If you're running deltastreamer in continuous mode, consider adding delay using --min-sync-interval-seconds. "
            + "Or consider setting write concurrency mode to optimistic_concurrency_control.", e);
      }
      throw new HoodieException("Error getting all file groups in pending clustering", e);
    }
    LOG.info("Found " + resultMap.size() + " files in pending clustering operations");

    if (metaClient.getTableConfig().isLSMBasedLogFormat()) {
      // <String partitionPath, Set<String> fileId>
      Map<String, Set<String>> partitionPathToFileId = new HashMap<>();
      pendingClusteringPlans.forEach(clusteringPlan -> getLevel1ExistsInClusteringPlan(partitionPathToFileId, clusteringPlan.getRight()));
      LOG.info("Found " + partitionPathToFileId.size() + " partitions exists level 1 file in pending clustering");
      return Pair.of(resultMap, partitionPathToFileId);
    }

    return Pair.of(resultMap, Collections.emptyMap());
  }

  private static void getLevel1ExistsInClusteringPlan(Map<String, Set<String>> partitionPathToFileId, HoodieClusteringPlan clusteringPlan) {
    clusteringPlan.getInputGroups().stream().filter(inputGroup -> {
      if (inputGroup.getExtraMetadata() != null) {
        return LSM_CLUSTERING_OUT_PUT_LEVEL_1.equals(inputGroup.getExtraMetadata().get(LSM_CLUSTERING_OUT_PUT_LEVEL));
      }
      return false;
    }).forEach(inputGroup -> {
      List<HoodieSliceInfo> slices = inputGroup.getSlices();
      if (slices != null && slices.size() > 0) {
        // files in one slice have the same partition and fileId
        HoodieSliceInfo slice0Info = slices.get(0);
        String partitionPath = slice0Info.getPartitionPath();
        partitionPathToFileId.computeIfAbsent(partitionPath, p -> new HashSet<>()).add(FSUtils.getFileId(slice0Info.getFileId()));
      }
    });
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
      numLogFiles += slice.getLogFiles().count();
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
            .filter(instant -> isPendingClusteringInstant(metaClient, instant))
            .collect(Collectors.toList());
  }

  public static boolean isPendingClusteringInstant(HoodieTableMetaClient metaClient, HoodieInstant instant) {
    return getClusteringPlan(metaClient, instant).isPresent();
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
          // retain first instant after clean instant
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

      Option<HoodieInstant> pendingInstantOpt = replaceTimeline.filterInflightsAndRequested().firstInstant();
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
}
