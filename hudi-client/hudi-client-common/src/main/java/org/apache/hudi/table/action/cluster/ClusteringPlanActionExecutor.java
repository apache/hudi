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
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseTableServicePlanActionExecutor;
import org.apache.hudi.table.action.cluster.strategy.ClusteringPlanStrategy;
import org.apache.hudi.util.Lazy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ClusteringPlanActionExecutor<T, I, K, O> extends BaseTableServicePlanActionExecutor<T, I, K, O, Option<HoodieClusteringPlan>> {

  private static final Logger LOG = LoggerFactory.getLogger(ClusteringPlanActionExecutor.class);

  private final Option<Map<String, String>> extraMetadata;

  public ClusteringPlanActionExecutor(HoodieEngineContext context,
                                      HoodieWriteConfig config,
                                      HoodieTable<T, I, K, O> table,
                                      String instantTime,
                                      Option<Map<String, String>> extraMetadata) {
    super(context, config, table, instantTime);
    this.extraMetadata = extraMetadata;
  }

  protected Option<HoodieClusteringPlan> createClusteringPlan() {
    LOG.info("Checking if clustering needs to be run on {}", config.getBasePath());
    Option<HoodieInstant> lastClusteringInstant =
        table.getActiveTimeline().getLastClusteringInstant();

    int commitsSinceLastClustering = table.getActiveTimeline().getCommitsTimeline().filterCompletedInstants()
        .findInstantsAfter(lastClusteringInstant.map(HoodieInstant::requestedTime).orElse("0"), Integer.MAX_VALUE)
        .countInstants();

    if (config.inlineClusteringEnabled() && config.getInlineClusterMaxCommits() > commitsSinceLastClustering) {
      LOG.info("Not scheduling inline clustering as only {} commits was found since last clustering {}. Waiting for {}",
          commitsSinceLastClustering, lastClusteringInstant,
          config.getInlineClusterMaxCommits());
      return Option.empty();
    }

    if ((config.isAsyncClusteringEnabled() || config.scheduleInlineClustering()) && config.getAsyncClusterMaxCommits() > commitsSinceLastClustering) {
      LOG.info("Not scheduling async clustering as only {} commits was found since last clustering {}. Waiting for {}",
          commitsSinceLastClustering, lastClusteringInstant, config.getAsyncClusterMaxCommits());
      return Option.empty();
    }

    LOG.info("Generating clustering plan for table {}", config.getBasePath());
    ClusteringPlanStrategy strategy = (ClusteringPlanStrategy) ReflectionUtils.loadClass(
        ClusteringPlanStrategy.checkAndGetClusteringPlanStrategy(config),
            new Class<?>[] {HoodieTable.class, HoodieEngineContext.class, HoodieWriteConfig.class}, table, context, config);

    Lazy<List<String>> partitions = Lazy.lazily(() -> getPartitions(strategy, TableServiceType.CLUSTER));

    return strategy.generateClusteringPlan(this, partitions);
  }

  @Override
  public Option<HoodieClusteringPlan> execute() {
    Option<HoodieClusteringPlan> planOption = createClusteringPlan();
    if (planOption.isPresent()) {
      // To support writing and reading with table version SIX, we need to allow instant action to be REPLACE_COMMIT_ACTION
      String action =  TimelineLayoutVersion.LAYOUT_VERSION_2.equals(table.getMetaClient().getTimelineLayoutVersion()) ? HoodieTimeline.CLUSTERING_ACTION : HoodieTimeline.REPLACE_COMMIT_ACTION;
      HoodieInstant clusteringInstant =
          instantGenerator.createNewInstant(HoodieInstant.State.REQUESTED, action, instantTime);
      HoodieRequestedReplaceMetadata requestedReplaceMetadata = HoodieRequestedReplaceMetadata.newBuilder()
          .setOperationType(WriteOperationType.CLUSTER.name())
          .setExtraMetadata(extraMetadata.orElse(Collections.emptyMap()))
          .setClusteringPlan(planOption.get())
          .build();
      table.getActiveTimeline().saveToPendingClusterCommit(clusteringInstant, requestedReplaceMetadata);
    }

    return planOption;
  }
}
