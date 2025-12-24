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

package org.apache.hudi.table.action.clean;

import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanFileInfo;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LSMCleanPlanActionExecutor<T, I, K, O> extends CleanPlanActionExecutor<T, I, K, O> {

  private static final Logger LOG = LogManager.getLogger(CleanPlanner.class);

  private final Option<Map<String, String>> extraMetadata;
  private final HoodieTableMetaClient metaClient;

  public LSMCleanPlanActionExecutor(HoodieEngineContext context,
                                    HoodieWriteConfig config,
                                    HoodieTable<T, I, K, O> table,
                                    String instantTime,
                                    Option<Map<String, String>> extraMetadata) {
    super(context, config, table, instantTime, extraMetadata);
    this.extraMetadata = extraMetadata;
    this.metaClient = table.getMetaClient();
  }

  /**
   * Generates List of files to be cleaned.
   *
   * @param context HoodieEngineContext
   * @return Cleaner Plan
   */
  @Override
  HoodieCleanerPlan requestClean(HoodieEngineContext context) {
    try {
      LSMCleanPlanner<T, I, K, O> planner = new LSMCleanPlanner<>(context, table, config);
      Option<HoodieInstant> earliestInstant = planner.getEarliestCommitToRetain();
      context.setJobStatus(this.getClass().getSimpleName(), "Obtaining list of partitions to be cleaned: " + config.getTableName());
      if (metaClient.getActiveTimeline().getCompletedReplaceTimeline().getInstants().isEmpty()) {
        LOG.info("Nothing to clean here. It is already clean");
        return HoodieCleanerPlan.newBuilder().setPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS.name()).build();
      }

      LOG.info("Earliest commit to retain for clean : " + (earliestInstant.isPresent() ? earliestInstant.get().getTimestamp() : "null"));
      Map<String, List<HoodieCleanFileInfo>> cleanOps = planner.getDeletePaths();
      return new HoodieCleanerPlan(earliestInstant
          .map(x -> new HoodieActionInstant(x.getTimestamp(), x.getAction(), x.getState().name())).orElse(null),
          planner.getLastCompletedCommitTimestamp(),
          config.getCleanerPolicy().name(), CollectionUtils.createImmutableMap(),
          CleanPlanner.LATEST_CLEAN_PLAN_VERSION, cleanOps, new ArrayList<>());
    } catch (IOException e) {
      throw new HoodieIOException("Failed to schedule lsm clean operation", e);
    }
  }

  @Override
  public Option<HoodieCleanerPlan> execute() {
    if (!needsCleaning(config.getCleaningTriggerStrategy())) {
      return Option.empty();
    }
    // Plan a new clean action
    return requestClean(instantTime);
  }
}
