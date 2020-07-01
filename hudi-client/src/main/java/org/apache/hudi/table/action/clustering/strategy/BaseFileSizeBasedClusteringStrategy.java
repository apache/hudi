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
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.action.compact.strategy.BoundedIOCompactionStrategy;
import org.apache.hudi.table.action.compact.strategy.CompactionStrategy;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * LogFileSizeBasedClusteringStrategy orders the compactions based on the total log files size and limits the
 * clusterings within a configured IO bound.
 *
 * @see BoundedIOCompactionStrategy
 * @see CompactionStrategy
 */
public class BaseFileSizeBasedClusteringStrategy extends BoundedIOClusteringStrategy
    implements Comparator<HoodieClusteringOperation> {

  private static final String TOTAL_BASE_FILE_SIZE = "TOTAL_BASE_FILE_SIZE";

  @Override
  public Map<String, Double> captureMetrics(HoodieWriteConfig config, List<HoodieBaseFile> dataFile,
      String partitionPath) {
    Map<String, Double> metrics = super.captureMetrics(config, dataFile, partitionPath);

    // Total size of all the data files
    Long totalBaseFileSize = dataFile.stream().map(HoodieBaseFile::getFileSize).filter(size -> size >= 0)
        .reduce(Long::sum).orElse(0L);
    // save the metrics needed during the order
    metrics.put(TOTAL_BASE_FILE_SIZE, totalBaseFileSize.doubleValue());
    return metrics;
  }

  @Override
  public List<HoodieClusteringOperation> orderAndFilter(HoodieWriteConfig writeConfig,
      List<HoodieClusteringOperation> operations, List<HoodieClusteringPlan> pendingCompactionPlans) {
    // Order the operations based on the reverse size of the logs and limit them by the IO
    return super.orderAndFilter(writeConfig, operations.stream().sorted(this).collect(Collectors.toList()),
        pendingCompactionPlans);
  }

  @Override
  public int compare(HoodieClusteringOperation op1, HoodieClusteringOperation op2) {
    Long totalLogSize1 = op1.getMetrics().get(TOTAL_BASE_FILE_SIZE).longValue();
    Long totalLogSize2 = op2.getMetrics().get(TOTAL_BASE_FILE_SIZE).longValue();
    // Reverse the comparison order - so that larger log file size is compacted first
    return totalLogSize2.compareTo(totalLogSize1);
  }
}
