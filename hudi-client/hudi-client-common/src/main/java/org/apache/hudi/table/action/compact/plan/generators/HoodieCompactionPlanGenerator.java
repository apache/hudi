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

package org.apache.hudi.table.action.compact.plan.generators;

import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseTableServicePlanActionExecutor;
import org.apache.hudi.table.action.compact.strategy.CompactionStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static java.util.stream.Collectors.toList;

public class HoodieCompactionPlanGenerator<T extends HoodieRecordPayload, I, K, O>
    extends BaseHoodieCompactionPlanGenerator<T, I, K, O> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieCompactionPlanGenerator.class);

  private final CompactionStrategy compactionStrategy;

  public HoodieCompactionPlanGenerator(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig,
                                       BaseTableServicePlanActionExecutor executor) {
    super(table, engineContext, writeConfig, executor);
    this.compactionStrategy = writeConfig.getCompactionStrategy();
    LOG.info("Compaction Strategy used is: " + compactionStrategy.toString());
  }

  @Override
  protected HoodieCompactionPlan getCompactionPlan(HoodieTableMetaClient metaClient, List<HoodieCompactionOperation> operations, Pair<List<String>, List<String>> partitionPair) {
    // Filter the compactions with the passed in filter. This lets us choose most effective
    // compactions only
    return compactionStrategy.generateCompactionPlan(writeConfig, operations,
        CompactionUtils.getAllPendingCompactionPlans(metaClient).stream().map(Pair::getValue).collect(toList()), getStrategyParams(), partitionPair);
  }

  @Override
  protected List<String> getPartitions() {
    return executor.getPartitions(compactionStrategy, TableServiceType.COMPACT);
  }

  @Override
  protected Pair<List<String>, List<String>> filterPartitionPathsByStrategy(List<String> partitionPaths) {
    return compactionStrategy.filterPartitionPaths(writeConfig, partitionPaths);
  }

  @Override
  protected boolean filterLogCompactionOperations() {
    return false;
  }
}
