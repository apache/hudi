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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * UnBoundedPartitionAwareCompactionStrategy is a custom UnBounded Strategy. This will filter all the partitions that
 * are eligible to be compacted by a {@link BoundedPartitionAwareCompactionStrategy} and return the result. This is done
 * so that a long running UnBoundedPartitionAwareCompactionStrategy does not step over partitions in a shorter running
 * BoundedPartitionAwareCompactionStrategy. Essentially, this is an inverse of the partitions chosen in
 * BoundedPartitionAwareCompactionStrategy
 *
 * @see CompactionStrategy
 */
public class UnBoundedPartitionAwareCompactionStrategy extends CompactionStrategy {

  @Override
  public Pair<List<HoodieCompactionOperation>, List<String>> orderAndFilter(HoodieWriteConfig config,
      final List<HoodieCompactionOperation> operations, final List<HoodieCompactionPlan> pendingCompactionWorkloads) {
    BoundedPartitionAwareCompactionStrategy boundedPartitionAwareCompactionStrategy =
        new BoundedPartitionAwareCompactionStrategy();
    List<HoodieCompactionOperation> operationsToExclude =
        boundedPartitionAwareCompactionStrategy.orderAndFilter(config, operations, pendingCompactionWorkloads).getLeft();
    List<HoodieCompactionOperation> allOperations = new ArrayList<>(operations);
    allOperations.removeAll(operationsToExclude);
    return Pair.of(allOperations, Collections.emptyList());
  }

  @Override
  public Pair<List<String>, List<String>> filterPartitionPaths(HoodieWriteConfig writeConfig, List<String> partitionPaths) {
    List<String> allPartitionPaths =
        partitionPaths.stream().map(partition -> partition.replace("/", "-")).sorted(Comparator.reverseOrder())
            .map(partitionPath -> partitionPath.replace("-", "/")).collect(Collectors.toList());
    BoundedPartitionAwareCompactionStrategy boundedPartitionAwareCompactionStrategy =
        new BoundedPartitionAwareCompactionStrategy();
    List<String> partitionsToExclude =
        boundedPartitionAwareCompactionStrategy.filterPartitionPaths(writeConfig, partitionPaths).getLeft();
    allPartitionPaths.removeAll(partitionsToExclude);
    return Pair.of(allPartitionPaths, Collections.emptyList());
  }
}
