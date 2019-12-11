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

package org.apache.hudi.io.compact.strategy;

import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.model.HoodieDataFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.compact.HoodieRealtimeTableCompactor;

import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Strategy for compaction. Pluggable implementation to define how compaction should be done. The over-ridden
 * implementations of this abstract class can capture the relevant metrics to order and filter the final list of
 * compaction operation to run in a single compaction. Implementation of CompactionStrategy cannot hold any state.
 * Difference instantiations can be passed in every time
 *
 * @see HoodieRealtimeTableCompactor
 */
public abstract class CompactionStrategy implements Serializable {

  public static final String TOTAL_IO_READ_MB = "TOTAL_IO_READ_MB";
  public static final String TOTAL_IO_WRITE_MB = "TOTAL_IO_WRITE_MB";
  public static final String TOTAL_IO_MB = "TOTAL_IO_MB";
  public static final String TOTAL_LOG_FILE_SIZE = "TOTAL_LOG_FILES_SIZE";
  public static final String TOTAL_LOG_FILES = "TOTAL_LOG_FILES";

  /**
   * Callback hook when a HoodieCompactionOperation is created. Individual strategies can capture the metrics they need
   * to decide on the priority.
   *
   * @param dataFile - Base file to compact
   * @param partitionPath - Partition path
   * @param logFiles - List of log files to compact with the base file
   * @return Map[String, Object] - metrics captured
   */
  public Map<String, Double> captureMetrics(HoodieWriteConfig writeConfig, Option<HoodieDataFile> dataFile,
      String partitionPath, List<HoodieLogFile> logFiles) {
    Map<String, Double> metrics = Maps.newHashMap();
    Long defaultMaxParquetFileSize = writeConfig.getParquetMaxFileSize();
    // Total size of all the log files
    Long totalLogFileSize = logFiles.stream().map(HoodieLogFile::getFileSize).filter(size -> size >= 0)
        .reduce((size1, size2) -> size1 + size2).orElse(0L);
    // Total read will be the base file + all the log files
    Long totalIORead =
        FSUtils.getSizeInMB((dataFile.isPresent() ? dataFile.get().getFileSize() : 0L) + totalLogFileSize);
    // Total write will be similar to the size of the base file
    Long totalIOWrite =
        FSUtils.getSizeInMB(dataFile.isPresent() ? dataFile.get().getFileSize() : defaultMaxParquetFileSize);
    // Total IO will the the IO for read + write
    Long totalIO = totalIORead + totalIOWrite;
    // Save these metrics and we will use during the filter
    metrics.put(TOTAL_IO_READ_MB, totalIORead.doubleValue());
    metrics.put(TOTAL_IO_WRITE_MB, totalIOWrite.doubleValue());
    metrics.put(TOTAL_IO_MB, totalIO.doubleValue());
    metrics.put(TOTAL_LOG_FILE_SIZE, totalLogFileSize.doubleValue());
    metrics.put(TOTAL_LOG_FILES, (double) logFiles.size());
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
      List<HoodieCompactionOperation> operations, List<HoodieCompactionPlan> pendingCompactionPlans) {
    // Strategy implementation can overload this method to set specific compactor-id
    return HoodieCompactionPlan.newBuilder()
        .setOperations(orderAndFilter(writeConfig, operations, pendingCompactionPlans))
        .setVersion(CompactionUtils.LATEST_COMPACTION_METADATA_VERSION).build();
  }

  /**
   * Order and Filter the list of compactions. Use the metrics captured with the captureMetrics to order and filter out
   * compactions
   *
   * @param writeConfig config for this compaction is passed in
   * @param operations list of compactions collected
   * @param pendingCompactionPlans Pending Compaction Plans for strategy to schedule next compaction plan
   * @return list of compactions to perform in this run
   */
  public List<HoodieCompactionOperation> orderAndFilter(HoodieWriteConfig writeConfig,
      List<HoodieCompactionOperation> operations, List<HoodieCompactionPlan> pendingCompactionPlans) {
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
