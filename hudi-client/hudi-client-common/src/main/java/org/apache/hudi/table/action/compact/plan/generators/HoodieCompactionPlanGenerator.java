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
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class HoodieCompactionPlanGenerator<T extends HoodieRecordPayload, I, K, O>
    extends BaseHoodieCompactionPlanGenerator<T, I, K, O> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieCompactionPlanGenerator.class);

  public HoodieCompactionPlanGenerator(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  @Override
  protected HoodieCompactionPlan getCompactionPlan(HoodieTableMetaClient metaClient, List<HoodieCompactionOperation> operations) {
    // Filter the compactions with the passed in filter. This lets us choose most effective
    // compactions only
    return writeConfig.getCompactionStrategy().generateCompactionPlan(writeConfig, operations,
        CompactionUtils.getAllPendingCompactionPlans(metaClient).stream().map(Pair::getValue).collect(toList()));
  }

  @Override
  protected List<String> listPartitionsPaths(HoodieEngineContext engineContext, HoodieStorage storage, HoodieWriteConfig writeConfig, String basePathStr) {
    String compactionStrategy = writeConfig.getCompactionStrategy().getClass().getName();
    LOG.info("Compaction strategy is " + compactionStrategy);
    if (compactionStrategy.equals("com.heap.datalake.compaction.SpecificPartitionsCompactionStrategy")) {
      String[] partitions = writeConfig.getString("hoodie.compaction.include.partitions").split(",");
      if (partitions.length > 0) {
        LOG.info("Skipping listing all partitions in favor of partitions provided in config: " + Arrays.toString(partitions));
        return Arrays.asList(partitions);
      }
    }
    LOG.info("Defaulting to listing all partitions");
    return super.listPartitionsPaths(engineContext, storage, writeConfig, basePathStr);
  }

  @Override
  protected List<String> filterPartitionPathsByStrategy(HoodieWriteConfig writeConfig, List<String> partitionPaths) {
    return writeConfig.getCompactionStrategy().filterPartitionPaths(writeConfig, partitionPaths);
  }

  @Override
  protected boolean filterLogCompactionOperations() {
    return false;
  }
}
