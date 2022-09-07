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
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;
import org.apache.hudi.table.action.cluster.strategy.ClusteringPlanStrategy;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class ClusteringPlanActionExecutor<T extends HoodieRecordPayload, I, K, O> extends BaseActionExecutor<T, I, K, O, Option<HoodieClusteringPlan>> {

  private static final Logger LOG = LogManager.getLogger(ClusteringPlanActionExecutor.class);

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
    LOG.info("Checking if clustering needs to be run on " + config.getBasePath());
    Option<HoodieInstant> lastClusteringInstant = table.getActiveTimeline().getCompletedReplaceTimeline().lastInstant();

    int commitsSinceLastClustering = table.getActiveTimeline().getCommitsTimeline().filterCompletedInstants()
        .findInstantsAfter(lastClusteringInstant.map(HoodieInstant::getTimestamp).orElse("0"), Integer.MAX_VALUE)
        .countInstants();

    if (table.getActiveTimeline().filterPendingReplaceTimeline().countInstants() != 0) {
      LOG.info("The last clustering is running,there is no need to generate a new clustering plan" + config.getBasePath());
      return Option.empty();
    }

    if (config.inlineClusteringEnabled() && config.getInlineClusterMaxCommits() > commitsSinceLastClustering) {
      LOG.info("Not scheduling inline clustering as only " + commitsSinceLastClustering
          + " commits was found since last clustering " + lastClusteringInstant + ". Waiting for "
          + config.getInlineClusterMaxCommits());
      return Option.empty();
    }

    if (config.isAsyncClusteringEnabled() && config.getAsyncClusterMaxCommits() > commitsSinceLastClustering) {
      LOG.info("Not scheduling async clustering as only " + commitsSinceLastClustering
          + " commits was found since last clustering " + lastClusteringInstant + ". Waiting for "
          + config.getAsyncClusterMaxCommits());
      return Option.empty();
    }

    LOG.info("Generating clustering plan for table " + config.getBasePath());
    ClusteringPlanStrategy strategy = (ClusteringPlanStrategy)
        ReflectionUtils.loadClass(ClusteringPlanStrategy.checkAndGetClusteringPlanStrategy(config), table, context, config);

    return strategy.generateClusteringPlan();
  }

  @Override
  public Option<HoodieClusteringPlan> execute() {
    Option<HoodieClusteringPlan> planOption = createClusteringPlan();
    if (planOption.isPresent()) {
      HoodieInstant clusteringInstant =
          new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, instantTime);
      try {
        HoodieRequestedReplaceMetadata requestedReplaceMetadata = HoodieRequestedReplaceMetadata.newBuilder()
            .setOperationType(WriteOperationType.CLUSTER.name())
            .setExtraMetadata(extraMetadata.orElse(Collections.emptyMap()))
            .setClusteringPlan(planOption.get())
            .build();
        table.getActiveTimeline().saveToPendingReplaceCommit(clusteringInstant,
            TimelineMetadataUtils.serializeRequestedReplaceMetadata(requestedReplaceMetadata));
      } catch (IOException ioe) {
        throw new HoodieIOException("Exception scheduling clustering", ioe);
      }
    }
    return planOption;
  }
}
