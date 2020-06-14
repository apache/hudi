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

package org.apache.hudi.table.action.rollback;

import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CopyOnWriteRollbackActionExecutor extends BaseRollbackActionExecutor {

  private static final Logger LOG = LogManager.getLogger(CopyOnWriteRollbackActionExecutor.class);

  public CopyOnWriteRollbackActionExecutor(JavaSparkContext jsc,
                                           HoodieWriteConfig config,
                                           HoodieTable<?> table,
                                           String instantTime,
                                           HoodieInstant commitInstant,
                                           boolean deleteInstants) {
    super(jsc, config, table, instantTime, commitInstant, deleteInstants);
  }

  public CopyOnWriteRollbackActionExecutor(JavaSparkContext jsc,
                                           HoodieWriteConfig config,
                                           HoodieTable<?> table,
                                           String instantTime,
                                           HoodieInstant commitInstant,
                                           boolean deleteInstants,
                                           boolean skipTimelinePublish) {
    super(jsc, config, table, instantTime, commitInstant, deleteInstants, skipTimelinePublish);
  }

  @Override
  protected List<HoodieRollbackStat> executeRollback() throws IOException {
    long startTime = System.currentTimeMillis();
    List<HoodieRollbackStat> stats = new ArrayList<>();
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    HoodieInstant resolvedInstant = instantToRollback;

    if (instantToRollback.isCompleted()) {
      LOG.info("Unpublishing instant " + instantToRollback);
      resolvedInstant = activeTimeline.revertToInflight(instantToRollback);
      // reload meta-client to reflect latest timeline status
      table.getMetaClient().reloadActiveTimeline();
    }

    // For Requested State (like failure during index lookup), there is nothing to do rollback other than
    // deleting the timeline file
    if (!resolvedInstant.isRequested()) {
      // delete all the data files for this commit
      LOG.info("Clean out all parquet files generated for commit: " + resolvedInstant);
      List<RollbackRequest> rollbackRequests = generateRollbackRequests(resolvedInstant);

      //TODO: We need to persist this as rollback workload and use it in case of partial failures
      stats = new RollbackHelper(table.getMetaClient(), config).performRollback(jsc, resolvedInstant, rollbackRequests);
    }
    // Delete Inflight instant if enabled
    deleteInflightAndRequestedInstant(deleteInstants, activeTimeline, resolvedInstant);
    LOG.info("Time(in ms) taken to finish rollback " + (System.currentTimeMillis() - startTime));
    return stats;
  }

  private List<RollbackRequest> generateRollbackRequests(HoodieInstant instantToRollback)
      throws IOException {
    return FSUtils.getAllPartitionPaths(table.getMetaClient().getFs(), table.getMetaClient().getBasePath(),
        config.shouldAssumeDatePartitioning()).stream()
        .map(partitionPath -> RollbackRequest.createRollbackRequestWithDeleteDataAndLogFilesAction(partitionPath, instantToRollback))
        .collect(Collectors.toList());
  }
}
