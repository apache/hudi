/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table.action.compact.strategy;

import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * CompositeCompactionStrategy chains multiple compaction strategies together.
 * Multiple strategies perform like a pipeline with `and` condition instead of `or`.
 * The order of the strategies in the chain is important as the output of one strategy is passed as input to the next.
 */
public class CompositeCompactionStrategy extends CompactionStrategy {

  private final List<CompactionStrategy> strategies;

  public CompositeCompactionStrategy(List<CompactionStrategy> strategies) {
    this.strategies = strategies;
  }

  @Override
  public Pair<List<HoodieCompactionOperation>, List<String>> orderAndFilter(HoodieWriteConfig writeConfig,
                                                                            List<HoodieCompactionOperation> operations,
                                                                            List<HoodieCompactionPlan> pendingCompactionPlans) {
    List<HoodieCompactionOperation> finalOperations = operations;
    List<String> missingPartitions = new ArrayList<>();
    for (CompactionStrategy strategy : strategies) {
      Pair<List<HoodieCompactionOperation>, List<String>> resPair = strategy.orderAndFilter(writeConfig, finalOperations, pendingCompactionPlans);
      finalOperations = resPair.getLeft();
      missingPartitions.addAll(resPair.getRight());
    }
    return Pair.of(finalOperations, missingPartitions);
  }

  @Override
  public Pair<List<String>, List<String>> filterPartitionPaths(HoodieWriteConfig writeConfig, List<String> allPartitionPaths) {
    List<String> partitionsToProcess = allPartitionPaths;
    List<String> missingPartitions = new ArrayList<>();

    for (CompactionStrategy strategy : strategies) {
      Pair<List<String>, List<String>> innerRes = strategy.filterPartitionPaths(writeConfig, partitionsToProcess);
      partitionsToProcess = innerRes.getLeft();
      missingPartitions.addAll(innerRes.getRight());
    }
    return writeConfig.isIncrementalTableServiceEnabled() ? Pair.of(partitionsToProcess, missingPartitions) : Pair.of(partitionsToProcess, Collections.emptyList());
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("CompactionStrategyChain [");
    for (CompactionStrategy strategy : strategies) {
      builder.append(strategy.getClass());
      builder.append(" ===> ");
    }
    builder.append("]");
    return builder.toString();
  }
}
