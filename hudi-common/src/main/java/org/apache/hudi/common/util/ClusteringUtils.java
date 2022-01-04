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
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

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

  private static final Logger LOG = LogManager.getLogger(ClusteringUtils.class);

  public static final String TOTAL_IO_READ_MB = "TOTAL_IO_READ_MB";
  public static final String TOTAL_LOG_FILE_SIZE = "TOTAL_LOG_FILES_SIZE";
  public static final String TOTAL_LOG_FILES = "TOTAL_LOG_FILES";

  /**
   * Get all pending clustering plans along with their instants.
   */
  public static Stream<Pair<HoodieInstant, HoodieClusteringPlan>> getAllPendingClusteringPlans(
      HoodieTableMetaClient metaClient) {
    List<HoodieInstant> pendingReplaceInstants =
        metaClient.getActiveTimeline().filterPendingReplaceTimeline().getInstants().collect(Collectors.toList());
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
  public static Option<HoodieRequestedReplaceMetadata> getRequestedReplaceMetadata(HoodieTableMetaClient metaClient, HoodieInstant pendingReplaceInstant) throws IOException {
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
      LOG.warn("No content found in requested file for instant " + pendingReplaceInstant);
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
            + "Or consider setting write concurrency mode to optimistic_concurrency_control.", e);
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
    return metaClient.getActiveTimeline().filterPendingReplaceTimeline().getInstants().collect(Collectors.toList());
  }
}
