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

import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class CopyOnWriteRollbackActionExecutor<T extends HoodieRecordPayload, I, K, O> extends BaseRollbackActionExecutor<T, I, K, O> {

  private static final Logger LOG = LogManager.getLogger(CopyOnWriteRollbackActionExecutor.class);

  public CopyOnWriteRollbackActionExecutor(HoodieEngineContext context,
                                           HoodieWriteConfig config,
                                           HoodieTable<T, I, K, O> table,
                                           String instantTime,
                                           HoodieInstant commitInstant,
                                           boolean deleteInstants,
                                           boolean skipLocking) {
    super(context, config, table, instantTime, commitInstant, deleteInstants, skipLocking);
  }

  public CopyOnWriteRollbackActionExecutor(HoodieEngineContext context,
                                           HoodieWriteConfig config,
                                           HoodieTable<T, I, K, O> table,
                                           String instantTime,
                                           HoodieInstant commitInstant,
                                           boolean deleteInstants,
                                           boolean skipTimelinePublish,
                                           boolean useMarkerBasedStrategy,
                                           boolean skipLocking) {
    super(context, config, table, instantTime, commitInstant, deleteInstants, skipTimelinePublish, useMarkerBasedStrategy, skipLocking);
  }

  @Override
  protected List<HoodieRollbackStat> executeRollback(HoodieRollbackPlan hoodieRollbackPlan) {
    HoodieTimer rollbackTimer = new HoodieTimer();
    rollbackTimer.startTimer();

    List<HoodieRollbackStat> stats = new ArrayList<>();
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();

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
      LOG.info("Clean out all base files generated for commit: " + resolvedInstant);
      stats = executeRollback(resolvedInstant, hoodieRollbackPlan);
    }

    dropBootstrapIndexIfNeeded(instantToRollback);

    LOG.info("Time(in ms) taken to finish rollback " + rollbackTimer.endTimer());
    return stats;
  }
}
