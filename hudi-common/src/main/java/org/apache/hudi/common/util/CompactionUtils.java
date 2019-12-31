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
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.versioning.compaction.CompactionPlanMigrator;
import org.apache.hudi.common.versioning.compaction.CompactionV1MigrationHandler;
import org.apache.hudi.common.versioning.compaction.CompactionV2MigrationHandler;
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
   * @param partitionPath Partition path
   * @param fileSlice File Slice
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
    if (fileSlice.getDataFile().isPresent()) {
      builder.setDataFilePath(fileSlice.getDataFile().get().getFileName());
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
   * @param extraMetadata Extra Metadata
   * @param metricsCaptureFunction Metrics Capture function
   */
  public static HoodieCompactionPlan buildFromFileSlices(List<Pair<String, FileSlice>> partitionFileSlicePairs,
      Option<Map<String, String>> extraMetadata,
      Option<Function<Pair<String, FileSlice>, Map<String, Double>>> metricsCaptureFunction) {
    HoodieCompactionPlan.Builder builder = HoodieCompactionPlan.newBuilder();
    extraMetadata.ifPresent(m -> builder.setExtraMetadata(m));

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
   *
   * @param metaClient Hoodie Meta Client
   */
  public static List<Pair<HoodieInstant, HoodieCompactionPlan>> getAllPendingCompactionPlans(
      HoodieTableMetaClient metaClient) {
    List<HoodieInstant> pendingCompactionInstants =
        metaClient.getActiveTimeline().filterPendingCompactionTimeline().getInstants().collect(Collectors.toList());
    return pendingCompactionInstants.stream().map(instant -> {
      try {
        return Pair.of(instant, getCompactionPlan(metaClient, instant.getTimestamp()));
      } catch (IOException e) {
        throw new HoodieException(e);
      }
    }).collect(Collectors.toList());
  }

  public static HoodieCompactionPlan getCompactionPlan(HoodieTableMetaClient metaClient, String compactionInstant)
      throws IOException {
    CompactionPlanMigrator migrator = new CompactionPlanMigrator(metaClient);
    HoodieCompactionPlan compactionPlan = AvroUtils.deserializeCompactionPlan(
        metaClient.getActiveTimeline().readPlanAsBytes(
            HoodieTimeline.getCompactionRequestedInstant(compactionInstant)).get());
    return migrator.upgradeToLatest(compactionPlan, compactionPlan.getVersion());
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

    Map<HoodieFileGroupId, Pair<String, HoodieCompactionOperation>> fgIdToPendingCompactionWithInstantMap =
        new HashMap<>();
    pendingCompactionPlanWithInstants.stream().flatMap(instantPlanPair -> {
      return getPendingCompactionOperations(instantPlanPair.getKey(), instantPlanPair.getValue());
    }).forEach(pair -> {
      // Defensive check to ensure a single-fileId does not have more than one pending compaction with different
      // file slices. If we find a full duplicate we assume it is caused by eventual nature of the move operation
      // on some DFSs.
      if (fgIdToPendingCompactionWithInstantMap.containsKey(pair.getKey())) {
        HoodieCompactionOperation operation = pair.getValue().getValue();
        HoodieCompactionOperation anotherOperation =
            fgIdToPendingCompactionWithInstantMap.get(pair.getKey()).getValue();

        if (!operation.equals(anotherOperation)) {
          String msg = "Hudi File Id (" + pair.getKey() + ") has more than 1 pending compactions. Instants: "
              + pair.getValue() + ", " + fgIdToPendingCompactionWithInstantMap.get(pair.getKey());
          throw new IllegalStateException(msg);
        }
      }
      fgIdToPendingCompactionWithInstantMap.put(pair.getKey(), pair.getValue());
    });
    return fgIdToPendingCompactionWithInstantMap;
  }

  public static Stream<Pair<HoodieFileGroupId, Pair<String, HoodieCompactionOperation>>> getPendingCompactionOperations(
      HoodieInstant instant, HoodieCompactionPlan compactionPlan) {
    List<HoodieCompactionOperation> ops = compactionPlan.getOperations();
    if (null != ops) {
      return ops.stream().map(op -> {
        return Pair.of(new HoodieFileGroupId(op.getPartitionPath(), op.getFileId()),
            Pair.of(instant.getTimestamp(), op));
      });
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
    return metaClient.getActiveTimeline().filterPendingCompactionTimeline().getInstants().collect(Collectors.toList());
  }
}
