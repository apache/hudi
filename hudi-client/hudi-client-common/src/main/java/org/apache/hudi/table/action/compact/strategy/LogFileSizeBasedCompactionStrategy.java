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
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * LogFileSizeBasedCompactionStrategy orders the compactions based on the total log files size,
 * filters the file group which log files size is greater than the threshold and limits the
 * compactions within a configured IO bound.
 *
 * @see BoundedIOCompactionStrategy
 * @see CompactionStrategy
 */
public class LogFileSizeBasedCompactionStrategy extends BoundedIOCompactionStrategy
    implements Comparator<HoodieCompactionOperation> {

  @Override
  public Pair<List<HoodieCompactionOperation>, List<String>> orderAndFilter(HoodieWriteConfig writeConfig,
                                                                            List<HoodieCompactionOperation> operations, List<HoodieCompactionPlan> pendingCompactionPlans) {
    // Filter the file group which log files size is greater than the threshold in bytes.
    // Order the operations based on the reverse size of the logs and limit them by the IO
    long threshold = writeConfig.getCompactionLogFileSizeThreshold();
    ArrayList<String> missingPartitions = new ArrayList<>();
    boolean incrementalTableServiceEnabled = writeConfig.isIncrementalTableServiceEnabled();
    List<HoodieCompactionOperation> filterOperator = operations.stream()
        .filter(e -> {
          if (incrementalTableServiceEnabled && e.getMetrics().getOrDefault(TOTAL_LOG_FILE_SIZE, 0d) < threshold) {
            missingPartitions.add(e.getPartitionPath());
          }
          return e.getMetrics().getOrDefault(TOTAL_LOG_FILE_SIZE, 0d) >= threshold;
        }).sorted(this).collect(Collectors.toList());

    if (incrementalTableServiceEnabled) {
      Pair<List<HoodieCompactionOperation>, List<String>> resPair = super.orderAndFilter(writeConfig, filterOperator, pendingCompactionPlans);
      List<HoodieCompactionOperation> compactOperations = resPair.getLeft();
      List<String> innerMissingPartitions = resPair.getRight();
      missingPartitions.addAll(innerMissingPartitions);
      return Pair.of(compactOperations, missingPartitions);
    } else {
      return super.orderAndFilter(writeConfig, filterOperator, pendingCompactionPlans);
    }
  }

  @Override
  public int compare(HoodieCompactionOperation op1, HoodieCompactionOperation op2) {
    Long totalLogSize1 = op1.getMetrics().get(TOTAL_LOG_FILE_SIZE).longValue();
    Long totalLogSize2 = op2.getMetrics().get(TOTAL_LOG_FILE_SIZE).longValue();
    // Reverse the comparison order - so that larger log file size is compacted first
    return totalLogSize2.compareTo(totalLogSize1);
  }
}
