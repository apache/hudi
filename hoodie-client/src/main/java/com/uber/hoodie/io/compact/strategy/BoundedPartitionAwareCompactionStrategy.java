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

package com.uber.hoodie.io.compact.strategy;

import com.uber.hoodie.avro.model.HoodieCompactionOperation;
import com.uber.hoodie.avro.model.HoodieCompactionPlan;
import com.uber.hoodie.config.HoodieWriteConfig;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.time.DateUtils;

/**
 * This strategy ensures that the last N partitions are picked up even if there are later partitions created for the
 * dataset. lastNPartitions is defined as the N partitions  before the currentDate.
 * currentDay = 2018/01/01
 * The dataset has partitions for 2018/02/02 and 2018/03/03 beyond the currentDay
 * This strategy will pick up the following partitions for compaction :
 * (2018/01/01, allPartitionsInRange[(2018/01/01 - lastNPartitions) to 2018/01/01), 2018/02/02, 2018/03/03)
 */
public class BoundedPartitionAwareCompactionStrategy extends DayBasedCompactionStrategy {

  SimpleDateFormat dateFormat = new SimpleDateFormat(datePartitionFormat);

  @Override
  public List<HoodieCompactionOperation> orderAndFilter(HoodieWriteConfig writeConfig,
      List<HoodieCompactionOperation> operations, List<HoodieCompactionPlan> pendingCompactionPlans) {
    // The earliest partition to compact - current day minus the target partitions limit
    String earliestPartitionPathToCompact = dateFormat.format(DateUtils.addDays(new Date(), -1 * writeConfig
            .getTargetPartitionsPerDayBasedCompaction()));
    // Filter out all partitions greater than earliestPartitionPathToCompact
    List<HoodieCompactionOperation> eligibleCompactionOperations = operations.stream()
        .collect(Collectors.groupingBy(HoodieCompactionOperation::getPartitionPath)).entrySet().stream()
        .sorted(Map.Entry.comparingByKey(comparator))
        .filter(e -> comparator.compare(earliestPartitionPathToCompact, e.getKey()) >= 0)
        .flatMap(e -> e.getValue().stream())
        .collect(Collectors.toList());

    return eligibleCompactionOperations;
  }

  @Override
  public List<String> filterPartitionPaths(HoodieWriteConfig writeConfig, List<String> partitionPaths) {
    // The earliest partition to compact - current day minus the target partitions limit
    String earliestPartitionPathToCompact = dateFormat.format(DateUtils.addDays(new Date(), -1 * writeConfig
        .getTargetPartitionsPerDayBasedCompaction()));
    // Get all partitions and sort them
    List<String> filteredPartitionPaths = partitionPaths.stream().map(partition -> partition.replace("/", "-"))
        .sorted(Comparator.reverseOrder()).map(partitionPath -> partitionPath.replace("-", "/"))
        .filter(e -> comparator.compare(earliestPartitionPathToCompact, e) >= 0)
        .collect(Collectors.toList());
    return filteredPartitionPaths;
  }
}
