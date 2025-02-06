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

package org.apache.hudi.table.action.compact.strategy;

import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.utils.FileSliceMetricUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.action.IncrementalPartitionAwareStrategy;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Strategy for compaction. Pluggable implementation to define how compaction should be done. The over-ridden
 * implementations of this abstract class can capture the relevant metrics to order and filter the final list of
 * compaction operation to run in a single compaction. Implementation of CompactionStrategy cannot hold any state.
 * Difference instantiations can be passed in every time
 */
public abstract class CompactionStrategy implements IncrementalPartitionAwareStrategy, Serializable {

  public static final String TOTAL_IO_READ_MB = "TOTAL_IO_READ_MB";
  public static final String TOTAL_IO_WRITE_MB = "TOTAL_IO_WRITE_MB";
  public static final String TOTAL_IO_MB = "TOTAL_IO_MB";
  public static final String TOTAL_LOG_FILE_SIZE = "TOTAL_LOG_FILES_SIZE";
  public static final String TOTAL_LOG_FILES = "TOTAL_LOG_FILES";

  /**
   * Callback hook when a HoodieCompactionOperation is created. Individual strategies can capture the metrics they need
   * to decide on the priority.
   *
   * @param writeConfig write configuration.
   * @param slice fileSlice to capture metrics for.
   * @return Map[String, Object] - metrics captured
   */
  public Map<String, Double> captureMetrics(HoodieWriteConfig writeConfig, FileSlice slice) {
    Map<String, Double> metrics = new HashMap<>();
    long defaultMaxParquetFileSize = writeConfig.getParquetMaxFileSize();
    FileSliceMetricUtils.addFileSliceCommonMetrics(Collections.singletonList(slice), metrics, defaultMaxParquetFileSize);
    return metrics;
  }

  /**
   * Generate Compaction plan. Allows clients to order and filter the list of compactions to be set. The default
   * implementation takes care of setting compactor Id from configuration allowing subclasses to only worry about
   * ordering and filtering compaction operations
   *
   * @param writeConfig Hoodie Write Config
   * @param operations Compaction Operations to be ordered and filtered
   * @param pendingCompactionPlans Pending Compaction Plans for strategy to schedule next compaction plan
   * @return Compaction plan to be scheduled.
   */
  public HoodieCompactionPlan generateCompactionPlan(HoodieWriteConfig writeConfig,
      List<HoodieCompactionOperation> operations, List<HoodieCompactionPlan> pendingCompactionPlans, Map<String, String> params, Pair<List<String>, List<String>> partitionPair) {
    // Strategy implementation can overload this method to set specific compactor-id
    Pair<List<HoodieCompactionOperation>, List<String>> resPair = orderAndFilter(writeConfig, operations, pendingCompactionPlans);
    List<HoodieCompactionOperation> operationsToProcess = resPair.getLeft();
    List<String> missingPartitions = resPair.getRight();
    missingPartitions.addAll(partitionPair.getRight());
    List<String> res = writeConfig.isIncrementalTableServiceEnabled() ? missingPartitions.stream().distinct().collect(Collectors.toList()) : null;
    return HoodieCompactionPlan.newBuilder()
        .setOperations(operationsToProcess)
        .setMissingSchedulePartitions(res)
        .setVersion(CompactionUtils.LATEST_COMPACTION_METADATA_VERSION)
        .build();
  }

  /**
   * Order and Filter the list of compactions. Use the metrics captured with the captureMetrics to order and filter out
   * compactions
   *
   * @param writeConfig config for this compaction is passed in
   * @param operations list of compactions collected
   * @param pendingCompactionPlans Pending Compaction Plans for strategy to schedule next compaction plan
   * @return Pair of list of compactions to perform in this run and missing partitions.
   *         Filtered out operations related partitions are missing partitions.
   */
  public Pair<List<HoodieCompactionOperation>, List<String>> orderAndFilter(HoodieWriteConfig writeConfig,
      List<HoodieCompactionOperation> operations, List<HoodieCompactionPlan> pendingCompactionPlans) {
    return Pair.of(operations, Collections.emptyList());
  }

  /**
   * Filter the partition paths based on compaction strategy.
   * 
   * @param writeConfig
   * @param allPartitionPaths
   * @return
   */
  public Pair<List<String>, List<String>> filterPartitionPaths(HoodieWriteConfig writeConfig, List<String> allPartitionPaths) {
    return Pair.of(allPartitionPaths, Collections.emptyList());
  }
}
