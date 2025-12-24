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

import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

public class SparkClusteringPlanActionExecutor<T, I, K, O> extends ClusteringPlanActionExecutor<T, I, K, O> {
  private static final Logger LOG = LogManager.getLogger(SparkClusteringPlanActionExecutor.class);
  private static final String SPARK_PERSIST_STORAGE_FULL_CLASS_NAME
      = "org.apache.hudi.client.clustering.plan.strategy.SparkSizeBasedClusteringPlanStrategyWithPersistStorage";
  public SparkClusteringPlanActionExecutor(HoodieEngineContext context, HoodieWriteConfig config, HoodieTable<T, I, K, O> table,
                                           String instantTime, Option<Map<String, String>> extraMetadata) {
    super(context, config, table, instantTime, extraMetadata);
  }

  @Override
  protected boolean canExecute(HoodieInstant instant) {
    boolean persistStorageConf = isPersistStorageConf(config);
    return persistStorageConf == isPersistInstant(instant, table.getMetaClient());
  }

  private boolean isPersistInstant(HoodieInstant instant, HoodieTableMetaClient metaClient) {
    try {
      HoodieRequestedReplaceMetadata replaceMetadata = TimelineMetadataUtils
          .deserializeRequestedReplaceMetadata(metaClient.getActiveTimeline().getInstantDetails(HoodieTimeline.getReplaceCommitRequestedInstant(instant.getTimestamp())).get());
      if (replaceMetadata.getClusteringPlan() == null) {
        return false;
      }
      Map<String, String> strategyParams = replaceMetadata.getClusteringPlan().getStrategy().getStrategyParams();
      return strategyParams.containsKey(HoodieClusteringConfig.PLAN_STRATEGY_CLASS_NAME.key())
          && SPARK_PERSIST_STORAGE_FULL_CLASS_NAME.equals(strategyParams.get(HoodieClusteringConfig.PLAN_STRATEGY_CLASS_NAME.key()));
    } catch (IOException e) {
      LOG.warn("failed to deserialize replacecommit metadata.", e);
      return false;
    }
  }

  private boolean isPersistStorageConf(HoodieWriteConfig config) {
    return config.contains(HoodieClusteringConfig.PLAN_STRATEGY_CLASS_NAME)
        && SPARK_PERSIST_STORAGE_FULL_CLASS_NAME.equals(config.getString(HoodieClusteringConfig.PLAN_STRATEGY_CLASS_NAME));
  }
}
