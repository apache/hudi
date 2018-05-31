/*
 *  Copyright (c) 2018 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.util;

import com.uber.hoodie.avro.model.HoodieCompactionOperation;
import com.uber.hoodie.avro.model.HoodieCompactionPlan;
import com.uber.hoodie.common.model.CompactionOperation;
import com.uber.hoodie.common.model.FileSlice;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.exception.HoodieException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Helper class to generate compaction plan from FileGroup/FileSlice abstraction
 */
public class CompactionUtils {

  /**
   * Generate compaction operation from file-slice
   *
   * @param partitionPath          Partition path
   * @param fileSlice              File Slice
   * @param metricsCaptureFunction Metrics Capture function
   * @return Compaction Operation
   */
  public static  HoodieCompactionOperation buildFromFileSlice(String partitionPath, FileSlice fileSlice,
      Optional<Function<Pair<String, FileSlice>, Map<String, Double>>> metricsCaptureFunction) {
    HoodieCompactionOperation.Builder builder = HoodieCompactionOperation.newBuilder();
    builder.setPartitionPath(partitionPath);
    builder.setFileId(fileSlice.getFileId());
    builder.setBaseInstantTime(fileSlice.getBaseInstantTime());
    builder.setDeltaFilePaths(fileSlice.getLogFiles().map(lf -> lf.getPath().toString()).collect(Collectors.toList()));
    if (fileSlice.getDataFile().isPresent()) {
      builder.setDataFilePath(fileSlice.getDataFile().get().getPath());
    }

    if (metricsCaptureFunction.isPresent()) {
      builder.setMetrics(metricsCaptureFunction.get().apply(Pair.of(partitionPath, fileSlice)));
    }
    return builder.build();
  }

  /**
   * Generate compaction plan from file-slices
   *
   * @param partitionFileSlicePairs list of partition file-slice pairs
   * @param extraMetadata           Extra Metadata
   * @param metricsCaptureFunction  Metrics Capture function
   */
  public static HoodieCompactionPlan buildFromFileSlices(
      List<Pair<String, FileSlice>> partitionFileSlicePairs,
      Optional<Map<String, String>> extraMetadata,
      Optional<Function<Pair<String, FileSlice>, Map<String, Double>>> metricsCaptureFunction) {
    HoodieCompactionPlan.Builder builder = HoodieCompactionPlan.newBuilder();
    extraMetadata.ifPresent(m -> builder.setExtraMetadata(m));
    builder.setOperations(partitionFileSlicePairs.stream().map(pfPair ->
        buildFromFileSlice(pfPair.getKey(), pfPair.getValue(), metricsCaptureFunction)).collect(Collectors.toList()));
    return builder.build();
  }

  /**
   * Build Avro generated Compaction operation payload from compaction operation POJO for serialization
   */
  public static HoodieCompactionOperation buildHoodieCompactionOperation(CompactionOperation op) {
    return HoodieCompactionOperation.newBuilder().setFileId(op.getFileId())
        .setBaseInstantTime(op.getBaseInstantTime())
        .setPartitionPath(op.getPartitionPath())
        .setDataFilePath(op.getDataFilePath().isPresent() ? op.getDataFilePath().get() : null)
        .setDeltaFilePaths(op.getDeltaFilePaths())
        .setMetrics(op.getMetrics()).build();
  }

  /**
   * Build Compaction operation payload from Avro version for using in Spark executors
   *
   * @param hc HoodieCompactionOperation
   */
  public static CompactionOperation buildCompactionOperation(HoodieCompactionOperation hc) {
    return CompactionOperation.convertFromAvroRecordInstance(hc);
  }

  /**
   * Get all pending compaction plans along with their instants
   *
   * @param metaClient Hoodie Meta Client
   */
  public static List<Pair<HoodieInstant, HoodieCompactionPlan>> getAllPendingCompactionPlans(
      HoodieTableMetaClient metaClient) {
    List<HoodieInstant> pendingCompactionInstants =
        metaClient.getActiveTimeline().filterPendingCompactionTimeline().getInstants().collect(Collectors.toList());
    return pendingCompactionInstants.stream().map(instant -> {
      try {
        HoodieCompactionPlan compactionPlan = AvroUtils.deserializeCompactionPlan(
            metaClient.getActiveTimeline().getInstantAuxiliaryDetails(
                HoodieTimeline.getCompactionRequestedInstant(instant.getTimestamp())).get());
        return Pair.of(instant, compactionPlan);
      } catch (IOException e) {
        throw new HoodieException(e);
      }
    }).collect(Collectors.toList());
  }

  /**
   * Get all file-ids with pending Compaction operations and their target compaction instant time
   *
   * @param metaClient Hoodie Table Meta Client
   */
  public static Map<String, Pair<String, HoodieCompactionOperation>> getAllPendingCompactionOperations(
      HoodieTableMetaClient metaClient) {
    List<Pair<HoodieInstant, HoodieCompactionPlan>> pendingCompactionPlanWithInstants =
        getAllPendingCompactionPlans(metaClient);

    Map<String, Pair<String, HoodieCompactionOperation>> fileIdToPendingCompactionWithInstantMap = new HashMap<>();
    pendingCompactionPlanWithInstants.stream().flatMap(instantPlanPair -> {
      HoodieInstant instant = instantPlanPair.getKey();
      HoodieCompactionPlan compactionPlan = instantPlanPair.getValue();
      List<HoodieCompactionOperation> ops = compactionPlan.getOperations();
      if (null != ops) {
        return ops.stream().map(op -> {
          return Pair.of(op.getFileId(), Pair.of(instant.getTimestamp(), op));
        });
      } else {
        return Stream.empty();
      }
    }).forEach(pair -> {
      // Defensive check to ensure a single-fileId does not have more than one pending compaction
      if (fileIdToPendingCompactionWithInstantMap.containsKey(pair.getKey())) {
        String msg = "Hoodie File Id (" + pair.getKey() + ") has more thant 1 pending compactions. Instants: "
            + pair.getValue() + ", " + fileIdToPendingCompactionWithInstantMap.get(pair.getKey());
        throw new IllegalStateException(msg);
      }
      fileIdToPendingCompactionWithInstantMap.put(pair.getKey(), pair.getValue());
    });
    return fileIdToPendingCompactionWithInstantMap;
  }
}
