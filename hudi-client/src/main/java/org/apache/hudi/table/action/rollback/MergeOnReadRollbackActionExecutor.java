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
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MergeOnReadRollbackActionExecutor extends BaseRollbackActionExecutor {

  private static final Logger LOG = LogManager.getLogger(MergeOnReadRollbackActionExecutor.class);

  public MergeOnReadRollbackActionExecutor(JavaSparkContext jsc,
      HoodieWriteConfig config,
      HoodieTable<?> table,
      String instantTime,
      HoodieInstant commitInstant,
      boolean deleteInstants) {
    super(jsc, config, table, instantTime, commitInstant, deleteInstants);
  }

  public MergeOnReadRollbackActionExecutor(JavaSparkContext jsc,
      HoodieWriteConfig config,
      HoodieTable<?> table,
      String instantTime,
      HoodieInstant commitInstant,
      boolean deleteInstants,
      boolean skipTimelinePublish,
      boolean useMarkerBasedStrategy) {
    super(jsc, config, table, instantTime, commitInstant, deleteInstants, skipTimelinePublish, useMarkerBasedStrategy);
  }

  @Override
  protected List<HoodieRollbackStat> executeRollback() throws IOException {
    HoodieTimer rollbackTimer = new HoodieTimer();
    rollbackTimer.startTimer();

    LOG.info("Rolling back instant " + instantToRollback);

    HoodieInstant resolvedInstant = instantToRollback;
    // Atomically un-publish all non-inflight commits
    if (instantToRollback.isCompleted()) {
      LOG.info("Un-publishing instant " + instantToRollback + ", deleteInstants=" + deleteInstants);
      resolvedInstant = table.getActiveTimeline().revertToInflight(instantToRollback);
      // reload meta-client to reflect latest timeline status
      table.getMetaClient().reloadActiveTimeline();
    }

    List<HoodieRollbackStat> allRollbackStats = new ArrayList<>();

    // At the moment, MOR table type does not support bulk nested rollbacks. Nested rollbacks is an experimental
    // feature that is expensive. To perform nested rollbacks, initiate multiple requests of client.rollback
    // (commitToRollback).
    // NOTE {@link HoodieCompactionConfig#withCompactionLazyBlockReadEnabled} needs to be set to TRUE. This is
    // required to avoid OOM when merging multiple LogBlocks performed during nested rollbacks.

    // For Requested State (like failure during index lookup), there is nothing to do rollback other than
    // deleting the timeline file
    if (!resolvedInstant.isRequested()) {
      LOG.info("Unpublished " + resolvedInstant);
      allRollbackStats = getRollbackStrategy().execute(resolvedInstant);
    }

    dropBootstrapIndexIfNeeded(resolvedInstant);

    // Delete Inflight instants if enabled
    deleteInflightAndRequestedInstant(deleteInstants, table.getActiveTimeline(), resolvedInstant);
    LOG.info("Time(in ms) taken to finish rollback " + rollbackTimer.endTimer());
    return allRollbackStats;
  }

  @Override
  protected List<HoodieRollbackStat> executeRollbackUsingFileListing(HoodieInstant resolvedInstant) {
    List<ListingBasedRollbackRequest> rollbackRequests;
    try {
      rollbackRequests = RollbackUtils.generateRollbackRequestsUsingFileListingMOR(resolvedInstant, table, jsc);
    } catch (IOException e) {
      throw new HoodieIOException("Error generating rollback requests by file listing.", e);
    }
    return new ListingBasedRollbackHelper(table.getMetaClient(), config).performRollback(jsc, resolvedInstant, rollbackRequests);
  }
}
