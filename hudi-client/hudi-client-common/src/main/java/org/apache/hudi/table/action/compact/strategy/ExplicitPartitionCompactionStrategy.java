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
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.config.HoodieWriteConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Compaction strategy with explicit partition, it is based on the {@link LogFileSizeBasedCompactionStrategy}.
 */
public class ExplicitPartitionCompactionStrategy extends LogFileSizeBasedCompactionStrategy {

  @Override
  public Map<String, Double> captureMetrics(HoodieWriteConfig writeConfig, FileSlice slice) {
    return super.captureMetrics(writeConfig, slice);
  }

  @Override
  public HoodieCompactionPlan generateCompactionPlan(HoodieWriteConfig writeConfig, List<HoodieCompactionOperation> operations, List<HoodieCompactionPlan> pendingCompactionPlans) {
    return super.generateCompactionPlan(writeConfig, operations, pendingCompactionPlans);
  }

  @Override
  public List<HoodieCompactionOperation> orderAndFilter(HoodieWriteConfig writeConfig, List<HoodieCompactionOperation> operations, List<HoodieCompactionPlan> pendingCompactionPlans) {
    return super.orderAndFilter(writeConfig, operations, pendingCompactionPlans);
  }

  @Override
  public List<String> filterPartitionPaths(HoodieWriteConfig writeConfig, List<String> allPartitionPaths) {
    String itemPartition = writeConfig.getProps().getProperty("compaction.partition");
    List<String> filteredPartitionPaths = new ArrayList<>();
    for (String partitionPath : allPartitionPaths) {
      if (partitionPath.equals(itemPartition)) {
        filteredPartitionPaths.add(partitionPath);
      }
    }
    return filteredPartitionPaths;
  }
}
