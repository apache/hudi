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

package org.apache.hudi.table.action.cluster;

import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.clustering.run.strategy.SingleSparkJobConsistentHashingExecutionStrategy;
import org.apache.hudi.client.clustering.run.strategy.SparkBinaryCopyClusteringExecutionStrategy;
import org.apache.hudi.client.clustering.run.strategy.SparkConsistentBucketClusteringExecutionStrategy;
import org.apache.hudi.client.clustering.run.strategy.SparkSingleFileSortExecutionStrategy;
import org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy;
import org.apache.hudi.client.clustering.run.strategy.SparkStreamCopyClusteringExecutionStrategy;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.hudi.config.HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS;

public class SparkExecuteClusteringCommitActionExecutor<T>
    extends BaseSparkCommitActionExecutor<T> {

  private static final Set<String> LSM_SUPPORTED_EXECUTION_STRATEGIES = new HashSet<>(Arrays.asList(
      SparkSortAndSizeExecutionStrategy.class.getName(),
      SparkSingleFileSortExecutionStrategy.class.getName(),
      SparkBinaryCopyClusteringExecutionStrategy.class.getName(),
      SparkStreamCopyClusteringExecutionStrategy.class.getName(),
      SparkConsistentBucketClusteringExecutionStrategy.class.getName(),
      SingleSparkJobConsistentHashingExecutionStrategy.class.getName()));

  private final HoodieClusteringPlan clusteringPlan;

  public SparkExecuteClusteringCommitActionExecutor(HoodieEngineContext context,
                                                    HoodieWriteConfig config, HoodieTable table,
                                                    String instantTime) {
    super(context, config, table, instantTime, WriteOperationType.CLUSTER);
    this.clusteringPlan = ClusteringUtils.getClusteringPlan(
        table.getMetaClient(), ClusteringUtils.getRequestedClusteringInstant(instantTime, table.getActiveTimeline(), table.getInstantGenerator()).get())
        .map(Pair::getRight).orElseThrow(() -> new HoodieClusteringException(
            "Unable to read clustering plan for instant: " + instantTime));
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> execute() {
    validateLsmClustering();
    return executeClustering(clusteringPlan);
  }

  private void validateLsmClustering() {
    if (!table.getMetaClient().getTableConfig().isLSMTreeStorageLayout()) {
      return;
    }

    String executionStrategy = config.getClusteringExecutionStrategyClass();
    if (!LSM_SUPPORTED_EXECUTION_STRATEGIES.contains(executionStrategy)) {
      throw new HoodieClusteringException("Clustering execution strategy \"" + executionStrategy
          + "\" is not supported for LSM tables because its record-key ordering cannot be verified");
    }

    Map<String, String> strategyParams = clusteringPlan.getStrategy().getStrategyParams();
    String sortColumns = strategyParams == null ? null : strategyParams.get(PLAN_STRATEGY_SORT_COLUMNS.key());
    if (!StringUtils.isNullOrEmpty(sortColumns)) {
      throw new HoodieClusteringException("Custom clustering sort columns are not supported for LSM tables because "
          + "LSM files must be ordered by record key");
    }
  }

  @Override
  protected String getCommitActionType() {
    return HoodieTimeline.CLUSTERING_ACTION;
  }
}
