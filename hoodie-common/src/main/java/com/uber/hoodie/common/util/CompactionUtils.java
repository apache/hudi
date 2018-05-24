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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import javafx.util.Pair;

/**
 * Helper class to generate compaction workload from FileGroup/FileSlice abstraction
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
      Optional<Function<Pair<String, FileSlice>, Map<String, Long>>> metricsCaptureFunction) {
    HoodieCompactionOperation.Builder builder = HoodieCompactionOperation.newBuilder();
    builder.setPartitionPath(partitionPath);
    builder.setFileId(fileSlice.getFileId());
    builder.setBaseInstantTime(fileSlice.getBaseInstantTime());
    builder.setDeltaFilePaths(fileSlice.getLogFiles().map(lf -> lf.getPath().toString()).collect(Collectors.toList()));
    if (fileSlice.getDataFile().isPresent()) {
      builder.setDataFilePath(fileSlice.getDataFile().get().getPath());
    }

    if (metricsCaptureFunction.isPresent()) {
      builder.setMetrics(metricsCaptureFunction.get().apply(new Pair(partitionPath, fileSlice)));
    }
    return builder.build();
  }

  /**
   * Generate compaction workload from file-slices
   *
   * @param compactorId             Compactor Id to set
   * @param partitionFileSlicePairs list of partition file-slice pairs
   * @param extraMetadata           Extra Metadata
   * @param metricsCaptureFunction  Metrics Capture function
   */
  public static HoodieCompactionPlan buildFromFileSlices(String compactorId,
      List<Pair<String, FileSlice>> partitionFileSlicePairs,
      Optional<Map<String, String>> extraMetadata,
      Optional<Function<Pair<String, FileSlice>, Map<String, Long>>> metricsCaptureFunction) {
    HoodieCompactionPlan.Builder builder = HoodieCompactionPlan.newBuilder();
    builder.setCompactorId(compactorId);
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
}
