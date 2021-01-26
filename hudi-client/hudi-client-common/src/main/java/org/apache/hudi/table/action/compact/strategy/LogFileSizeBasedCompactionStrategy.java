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

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * LogFileSizeBasedCompactionStrategy orders the compactions based on the total log files size and limits the
 * compactions within a configured IO bound.
 *
 * @see BoundedIOCompactionStrategy
 * @see CompactionStrategy
 */
public class LogFileSizeBasedCompactionStrategy extends BoundedIOCompactionStrategy
    implements Comparator<HoodieCompactionOperation> {

  @Override
  public List<HoodieCompactionOperation> orderAndFilter(HoodieWriteConfig writeConfig,
      List<HoodieCompactionOperation> operations, List<HoodieCompactionPlan> pendingCompactionPlans) {
    // Order the operations based on the reverse size of the logs and limit them by the IO
    return super.orderAndFilter(writeConfig, operations.stream().sorted(this).collect(Collectors.toList()),
        pendingCompactionPlans);
  }

  @Override
  public int compare(HoodieCompactionOperation op1, HoodieCompactionOperation op2) {
    Long totalLogSize1 = op1.getMetrics().get(TOTAL_LOG_FILE_SIZE).longValue();
    Long totalLogSize2 = op2.getMetrics().get(TOTAL_LOG_FILE_SIZE).longValue();
    // Reverse the comparison order - so that larger log file size is compacted first
    return totalLogSize2.compareTo(totalLogSize1);
  }
}
