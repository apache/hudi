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
import org.apache.hudi.config.HoodieWriteConfig;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DayBasedAndBoundedIOCompactionStrategy extends DayBasedCompactionStrategy {

  private static final Comparator<HoodieCompactionOperation> LOG_SIZE_COMPARATOR = (op1, op2) -> {
    Long logSize1 = op1.getMetrics().get(TOTAL_LOG_FILE_SIZE).longValue();
    Long logSize2 = op2.getMetrics().get(TOTAL_LOG_FILE_SIZE).longValue();
    // Sorted by log file size from largest to smallest
    return logSize2.compareTo(logSize1);
  };

  @Override
  public List<HoodieCompactionOperation> orderAndFilter(HoodieWriteConfig writeConfig,
      List<HoodieCompactionOperation> operations,
      List<HoodieCompactionPlan> pendingCompactionPlans) {
    List<HoodieCompactionOperation> partitionedOperations = operations.stream()
        .collect(Collectors.groupingBy(HoodieCompactionOperation::getPartitionPath)).entrySet().stream()
        .sorted(Map.Entry.comparingByKey(comparator))
        .limit(writeConfig.getTargetPartitionsPerDayBasedCompaction())
        .flatMap(e -> e.getValue().stream()).collect(Collectors.toList());
    Long threshold = writeConfig.getCompactionLogFileSizeThreshold();
    // Get all operations with IO bounded
    return filterOperationsByBoundedIO(writeConfig, partitionedOperations.stream()
        .filter(op -> op.getMetrics().getOrDefault(TOTAL_LOG_FILE_SIZE, 0d) >= threshold)
        .sorted(LOG_SIZE_COMPARATOR)
        .collect(Collectors.toList()));
  }

  public List<HoodieCompactionOperation> filterOperationsByBoundedIO(HoodieWriteConfig writeConfig,
      List<HoodieCompactionOperation> partitionedOperations) {
    ArrayList<HoodieCompactionOperation> finalOperations = new ArrayList<>();
    Long targetIORemaining = writeConfig.getTargetIOPerCompactionInMB();
    for (HoodieCompactionOperation op : partitionedOperations) {
      Long opIo = op.getMetrics().get(TOTAL_IO_MB).longValue();
      targetIORemaining -= opIo;
      if (targetIORemaining <= 0) {
        return finalOperations;
      }
      finalOperations.add(op);
    }
    return finalOperations;
  }
}