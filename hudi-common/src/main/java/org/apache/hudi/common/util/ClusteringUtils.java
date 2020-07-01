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

import org.apache.hudi.avro.model.HoodieClusteringOperation;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.common.model.ClusteringOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

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
public class ClusteringUtils {

  private static final Logger LOG = LogManager.getLogger(ClusteringUtils.class);

  public static final Integer LATEST_CLUSTERING_METADATA_VERSION = 1;

  /**
   * Generate compaction operation from file-slice.
   *
   * @param partitionPath Partition path
   * @param fileSlice File Slice
   * @param metricsCaptureFunction Metrics Capture function
   * @return Compaction Operation
   */
  public static HoodieClusteringOperation buildFromFileSlice(String partitionPath, FileSlice fileSlice,
      Option<Function<Pair<String, FileSlice>, Map<String, Double>>> metricsCaptureFunction) {
    HoodieClusteringOperation.Builder builder = HoodieClusteringOperation.newBuilder();
    builder.setPartitionPath(partitionPath);

    if (metricsCaptureFunction.isPresent()) {
      builder.setMetrics(metricsCaptureFunction.get().apply(Pair.of(partitionPath, fileSlice)));
    }
    return builder.build();
  }

  /**
   * Generate compaction plan from file-slices.
   *
   * @param partitionFileSlicePairs list of partition file-slice pairs
   * @param extraMetadata Extra Metadata
   * @param metricsCaptureFunction Metrics Capture function
   */
  public static HoodieClusteringPlan buildFromFileSlices(List<Pair<String, FileSlice>> partitionFileSlicePairs,
      Option<Map<String, String>> extraMetadata,
      Option<Function<Pair<String, FileSlice>, Map<String, Double>>> metricsCaptureFunction) {
    HoodieClusteringPlan.Builder builder = HoodieClusteringPlan.newBuilder();
    extraMetadata.ifPresent(builder::setExtraMetadata);

    builder.setOperations(partitionFileSlicePairs.stream()
        .map(pfPair -> buildFromFileSlice(pfPair.getKey(), pfPair.getValue(), metricsCaptureFunction))
        .collect(Collectors.toList()));
    builder.setVersion(LATEST_CLUSTERING_METADATA_VERSION);
    return builder.build();
  }

  /**
   * Build Avro generated Clustering operation payload from clustering operation POJO for serialization.
   */
  public static HoodieClusteringOperation buildHoodieClusteringOperation(ClusteringOperation op) {
    return HoodieClusteringOperation.newBuilder()
            .setBaseFilePaths(op.getBaseFilePaths())
            .setMetrics(op.getMetrics())
            .setPartitionPath(op.getPartitionPath())
            .build();
  }

  /**
   * Build Compaction operation payload from Avro version for using in Spark executors.
   *
   * @param hc HoodieCompactionOperation
   */
  public static ClusteringOperation buildClusteringOperation(HoodieClusteringOperation hc) {
    return ClusteringOperation.convertFromAvroRecordInstance(hc);
  }

  /**
   * Get all pending compaction plans along with their instants.
   *
   * @param metaClient Hoodie Meta Client
   */
  public static List<Pair<HoodieInstant, HoodieClusteringPlan>> getAllPendingClusteringPlans(
      HoodieTableMetaClient metaClient) {
    List<HoodieInstant> pendingClusteringInstants =
        metaClient.getActiveTimeline().filterPendingClusteringTimeline().getInstants().collect(Collectors.toList());
    return pendingClusteringInstants.stream().map(instant -> {
      try {
        return Pair.of(instant, getClusteringPlan(metaClient, instant.getTimestamp()));
      } catch (IOException e) {
        throw new HoodieException(e);
      }
    }).collect(Collectors.toList());
  }

  public static HoodieClusteringPlan getClusteringPlan(HoodieTableMetaClient metaClient, String clusteringInstant)
      throws IOException {
    HoodieClusteringPlan clusteringPlan = TimelineMetadataUtils.deserializeClusteringPlan(
        metaClient.getActiveTimeline().readCompactionPlanAsBytes(
            HoodieTimeline.getClusteringRequestedInstant(clusteringInstant)).get());
    return clusteringPlan;
  }

  /**
   * Get all PartitionPath + file-ids with pending Compaction operations and their target compaction instant time.
   *
   * @param metaClient Hoodie Table Meta Client
   */
  public static Map<HoodieFileGroupId, Pair<String, HoodieClusteringOperation>> getAllPendingClusteringOperations(
      HoodieTableMetaClient metaClient) {
    List<Pair<HoodieInstant, HoodieClusteringPlan>> pendingClusteringPlanWithInstants =
        getAllPendingClusteringPlans(metaClient);

    Map<HoodieFileGroupId, Pair<String, HoodieClusteringOperation>> fgIdToPendingClusteringWithInstantMap =
        new HashMap<>();
    pendingClusteringPlanWithInstants.stream().flatMap(instantPlanPair ->
        getPendingClusteringOperations(instantPlanPair.getKey(), instantPlanPair.getValue())).forEach(pair -> {
          // Defensive check to ensure a single-fileId does not have more than one pending compaction with different
          // file slices. If we find a full duplicate we assume it is caused by eventual nature of the move operation
          // on some DFSs.
          if (fgIdToPendingClusteringWithInstantMap.containsKey(pair.getKey())) {
            HoodieClusteringOperation operation = pair.getValue().getValue();
            HoodieClusteringOperation anotherOperation = fgIdToPendingClusteringWithInstantMap.get(pair.getKey()).getValue();

            if (!operation.equals(anotherOperation)) {
              String msg = "Hudi File Id (" + pair.getKey() + ") has more than 1 pending compactions. Instants: "
                  + pair.getValue() + ", " + fgIdToPendingClusteringWithInstantMap.get(pair.getKey());
              throw new IllegalStateException(msg);
            }
          }
          fgIdToPendingClusteringWithInstantMap.put(pair.getKey(), pair.getValue());
        });
    return fgIdToPendingClusteringWithInstantMap;
  }

  public static Stream<Pair<HoodieFileGroupId, Pair<String, HoodieClusteringOperation>>> getPendingClusteringOperations(
      HoodieInstant instant, HoodieClusteringPlan clusteringPlan) {
    List<HoodieClusteringOperation> ops = clusteringPlan.getOperations();
    if (null != ops) {
      return ops.stream().flatMap(op -> {
        List<String> baseFilePaths = op.getBaseFilePaths();
        String partitionPath = op.getPartitionPath();
        return baseFilePaths.stream().map(path -> Pair.of(new HoodieFileGroupId(partitionPath, path),
               Pair.of(instant.getTimestamp(), op))).collect(Collectors.toList()).stream();
      });
    } else {
      return Stream.empty();
    }
  }

  /**
   * Return all pending clustering instant times.
   * 
   * @return
   */
  public static List<HoodieInstant> getPendingClusteringInstantTimes(HoodieTableMetaClient metaClient) {
    return metaClient.getActiveTimeline().filterPendingClusteringTimeline().getInstants().collect(Collectors.toList());
  }
}
