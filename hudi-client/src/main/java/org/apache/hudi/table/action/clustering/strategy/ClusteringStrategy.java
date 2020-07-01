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

package org.apache.hudi.table.action.clustering.strategy;

import org.apache.hudi.avro.model.HoodieClusteringOperation;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.config.HoodieWriteConfig;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ClusteringStrategy implements Serializable {

  public static final String TOTAL_IO_READ_MB = "TOTAL_IO_READ_MB";
  public static final String TOTAL_IO_WRITE_MB = "TOTAL_IO_WRITE_MB";
  public static final String TOTAL_IO_MB = "TOTAL_IO_MB";

  /**
   * Callback hook when a HoodieClusteringOperation is created. Individual strategies can capture the metrics they need
   * to decide on the priority.
   *
   * @param dataFiles - Base file to compact
   * @param partitionPath - Partition path
   * @return Map[String, Object] - metrics captured
   */
  public Map<String, Double> captureMetrics(HoodieWriteConfig writeConfig, List<HoodieBaseFile> dataFiles,
      String partitionPath) {
    Map<String, Double> metrics = new HashMap<>();
    // Total read will be the base file + all the log files
    Long totalIORead = 0L;
    Long totalIOWrite = 0L;
    for (HoodieBaseFile baseFile : dataFiles) {
      totalIORead += baseFile.getFileSize();
      totalIOWrite += baseFile.getFileSize();
    }
    // Total IO will the the IO for read + write
    long totalIO = totalIORead + totalIOWrite;
    // Save these metrics and we will use during the filter
    metrics.put(TOTAL_IO_READ_MB, totalIORead.doubleValue());
    metrics.put(TOTAL_IO_WRITE_MB, totalIOWrite.doubleValue());
    metrics.put(TOTAL_IO_MB, (double) totalIO);
    return metrics;
  }

  /**
   * Generate Compaction plan. Allows clients to order and filter the list of compactions to be set. The default
   * implementation takes care of setting compactor Id from configuration allowing subclasses to only worry about
   * ordering and filtering compaction operations
   *
   * @param writeConfig Hoodie Write Config
   * @param operations Clustering Operations to be ordered and filtered
   * @param pendingClusteringPlans Pending Compaction Plans for strategy to schedule next compaction plan
   * @return Compaction plan to be scheduled.
   */
  public HoodieClusteringPlan generateClusteringPlan(HoodieWriteConfig writeConfig,
      List<HoodieClusteringOperation> operations, List<HoodieClusteringPlan> pendingClusteringPlans) {
    // Strategy implementation can overload this method to set specific clustering-id
    return HoodieClusteringPlan.newBuilder()
        .setOperations(orderAndFilter(writeConfig, operations, pendingClusteringPlans))
        .setVersion(ClusteringUtils.LATEST_CLUSTERING_METADATA_VERSION).build();
  }

  /**
   * Order and Filter the list of compactions. Use the metrics captured with the captureMetrics to order and filter out
   * compactions
   *
   * @param writeConfig config for this compaction is passed in
   * @param operations list of compactions collected
   * @param pendingClusteringPlans Pending Clustering Plans for strategy to schedule next clustering plan
   * @return list of compactions to perform in this run
   */
  public List<HoodieClusteringOperation> orderAndFilter(HoodieWriteConfig writeConfig,
      List<HoodieClusteringOperation> operations, List<HoodieClusteringPlan> pendingClusteringPlans) {
    return operations;
  }

  /**
   * Filter the partition paths based on compaction strategy.
   * 
   * @param writeConfig
   * @param allPartitionPaths
   * @return
   */
  public List<String> filterPartitionPaths(HoodieWriteConfig writeConfig, List<String> allPartitionPaths) {
    return allPartitionPaths;
  }
}
