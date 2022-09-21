/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.util;

import org.apache.hudi.avro.model.HoodieBuildPlan;
import org.apache.hudi.avro.model.HoodieBuildTask;
import org.apache.hudi.common.model.BuildStatus;
import org.apache.hudi.common.model.HoodieBuildCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieBuildException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.secondary.index.HoodieSecondaryIndex;
import org.apache.hudi.secondary.index.SecondaryIndexUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.util.SerializationUtil;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.config.HoodieSecondaryIndexConfig.HOODIE_SECONDARY_INDEX_DATA;
import static org.apache.hudi.common.table.HoodieTableMetaClient.INDEX_FOLDER_NAME;
import static org.apache.hudi.common.table.HoodieTableMetaClient.METAFOLDER_NAME;

public class BuildUtils {

  public static String getIndexFolderPath(String basePath) {
    return new Path(new Path(basePath, METAFOLDER_NAME), INDEX_FOLDER_NAME).toString();
  }

  /**
   * Get index save path
   *
   * @param indexFolderPath Table base path
   * @param indexType       Index type name
   * @param fileName        File name
   * @return Index save dir
   */
  public static Path getIndexSaveDir(String indexFolderPath, String indexType, String fileName) {
    int index = fileName.indexOf(".");
    String fileNameWithoutExtension;
    if (index >= 0) {
      fileNameWithoutExtension = fileName.substring(0, index);
    } else {
      fileNameWithoutExtension = fileName;
    }

    return new Path(new Path(indexFolderPath, indexType), fileNameWithoutExtension);
  }

  public static Map<String, List<HoodieSecondaryIndex>> getBaseFileIndexInfo(Configuration config) {
    try {
      return SerializationUtil.readObjectFromConfAsBase64(HOODIE_SECONDARY_INDEX_DATA, config);
    } catch (IOException e) {
      throw new HoodieBuildException("Fail to get base file index info", e);
    }
  }

  public static Map<String, List<HoodieSecondaryIndex>> getBaseFileIndexInfo(
      HoodieTableMetaClient metaClient) {
    Map<HoodieSecondaryIndex, Set<String>> indexedBaseFiles = new HashMap<>();
    getAllCompletedBuildSecondaryIndexes(metaClient)
        .forEach((k, v) -> indexedBaseFiles.put(k, v.keySet()));

    return indexedBaseFiles.entrySet().stream()
        .flatMap(entry ->
            entry.getValue().stream().map(baseFile -> Pair.of(baseFile, entry.getKey())))
        .collect(Collectors.groupingBy(Pair::getLeft, Collectors.mapping(Pair::getRight, Collectors.toList())));
  }

  /**
   * Get all pending build secondary indexes
   *
   * @param metaClient HoodieTableMetaClient
   * @return All pending build secondary indexes
   */
  public static Map<HoodieSecondaryIndex, Map<String, HoodieInstant>> getAllPendingBuildSecondaryIndexes(
      HoodieTableMetaClient metaClient) {
    return getAllBuildSecondaryIndexes(metaClient, false);
  }

  /**
   * Get all completed build secondary indexes
   *
   * @param metaClient HoodieTableMetaClient
   * @return All completed build secondary indexes
   */
  public static Map<HoodieSecondaryIndex, Map<String, HoodieInstant>> getAllCompletedBuildSecondaryIndexes(
      HoodieTableMetaClient metaClient) {
    return getAllBuildSecondaryIndexes(metaClient, true);
  }

  /**
   * Get all completed or pending build secondary indexes
   *
   * @param metaClient  HoodieTableMetaClient
   * @param isCompleted Mark whether completed or pending build should be filtered out
   * @return All completed or pending build secondary indexes
   */
  private static Map<HoodieSecondaryIndex, Map<String, HoodieInstant>> getAllBuildSecondaryIndexes(
      HoodieTableMetaClient metaClient, boolean isCompleted) {
    Map<HoodieSecondaryIndex, Map<String, HoodieInstant>> xBuildSecondaryIndexes = new HashMap<>();
    getAllBuildPlans(metaClient, isCompleted).forEach(pair -> {
      HoodieInstant instant = pair.getLeft();
      pair.getRight().getTasks().forEach(task -> {
        String baseFilePath = task.getBaseFilePath();
        SecondaryIndexUtils.fromJsonString(task.getIndexMetas()).forEach(secondaryIndex -> {
          Map<String, HoodieInstant> baseFileToInstant = xBuildSecondaryIndexes.computeIfAbsent(secondaryIndex,
              HoodieSecondaryIndex -> new HashMap<>());
          baseFileToInstant.put(baseFilePath, instant);
        });
      });
    });

    return xBuildSecondaryIndexes;
  }

  /**
   * Get all pending build plans
   *
   * @param metaClient HoodieTableMetaClient
   * @return All pending build plans
   */
  public static Stream<Pair<HoodieInstant, HoodieBuildPlan>> getAllPendingBuildPlans(
      HoodieTableMetaClient metaClient) {
    return getAllBuildPlans(metaClient, false);
  }

  /**
   * Get all completed build plans
   *
   * @param metaClient HoodieTableMetaClient
   * @return All completed build plans
   */
  public static Stream<Pair<HoodieInstant, HoodieBuildPlan>> getAllCompletedBuildPlans(
      HoodieTableMetaClient metaClient) {
    return getAllBuildPlans(metaClient, true);
  }

  /**
   * Get all completed or pending build plans
   *
   * @param metaClient  HoodieTableMetaClient
   * @param isCompleted Mark whether completed or pending build should be filtered out
   * @return All completed or pending build plans
   */
  private static Stream<Pair<HoodieInstant, HoodieBuildPlan>> getAllBuildPlans(
      HoodieTableMetaClient metaClient, boolean isCompleted) {
    HoodieTimeline timeline;
    if (isCompleted) {
      timeline = metaClient.getActiveTimeline().filterCompletedBuildTimeline();
    } else {
      timeline = metaClient.getActiveTimeline().filterPendingBuildTimeline();
    }

    return timeline.getInstants()
        .map(instant -> getBuildPlan(metaClient, instant))
        .filter(Option::isPresent)
        .map(Option::get);
  }

  /**
   * Get pending build plan of passed-in instant
   *
   * @param metaClient       HoodieTableMetaClient
   * @param requestedInstant Requested hoodie instant
   * @return HoodieBuildPlan
   */
  public static Option<Pair<HoodieInstant, HoodieBuildPlan>> getBuildPlan(
      HoodieTableMetaClient metaClient,
      HoodieInstant requestedInstant) {
    HoodieInstant requestInstant = requestedInstant.isRequested() ? requestedInstant
        : HoodieTimeline.getRequestedInstant(requestedInstant);
    Option<byte[]> instantDetails = metaClient.getActiveTimeline().getInstantDetails(requestInstant);
    if (!instantDetails.isPresent() || instantDetails.get().length == 0) {
      return Option.empty();
    }
    HoodieBuildPlan buildPlan = null;
    try {
      buildPlan = TimelineMetadataUtils.deserializeBuildPlan(instantDetails.get());
    } catch (IOException e) {
      throw new HoodieIOException("Error reading build plan", e);
    }

    return Option.of(Pair.of(requestedInstant, buildPlan));
  }

  public static HoodieBuildCommitMetadata convertToCommitMetadata(List<BuildStatus> buildStatuses) {
    Map<String, Map<String, List<HoodieSecondaryIndex>>> committedIndexesInfo = new HashMap<>();
    buildStatuses.stream().collect(Collectors.groupingBy(BuildStatus::getPartition))
        .forEach((partition, value) -> {
          Map<String, List<HoodieSecondaryIndex>> fileToIndexes = new HashMap<>();
          value.forEach(buildStatus ->
              fileToIndexes.put(buildStatus.getFileName(), buildStatus.getSecondaryIndexes()));
          committedIndexesInfo.put(partition, fileToIndexes);
        });

    HoodieBuildCommitMetadata buildCommitMetadata = new HoodieBuildCommitMetadata();
    buildCommitMetadata.setOperationType(WriteOperationType.BUILD);
    buildCommitMetadata.setCommittedIndexesInfo(committedIndexesInfo);

    return buildCommitMetadata;
  }

  public static List<String> extractPartitions(List<HoodieBuildTask> buildTasks) {
    if (CollectionUtils.isNullOrEmpty(buildTasks)) {
      return Collections.emptyList();
    }

    return buildTasks.stream()
        .map(HoodieBuildTask::getPartition)
        .sorted()
        .collect(Collectors.toList());
  }
}
