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
import org.apache.hudi.common.NativeTableFormat;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class CopyOnWriteRollbackActionExecutor<T, I, K, O> extends BaseRollbackActionExecutor<T, I, K, O> {

  public CopyOnWriteRollbackActionExecutor(HoodieEngineContext context,
                                           HoodieWriteConfig config,
                                           HoodieTable<T, I, K, O> table,
                                           String instantTime,
                                           HoodieInstant commitInstant,
                                           boolean deleteInstants,
                                           boolean skipLocking) {
    super(context, config, table, instantTime, commitInstant, deleteInstants, skipLocking, false);
  }

  public CopyOnWriteRollbackActionExecutor(HoodieEngineContext context,
                                           HoodieWriteConfig config,
                                           HoodieTable<T, I, K, O> table,
                                           String instantTime,
                                           HoodieInstant commitInstant,
                                           boolean deleteInstants,
                                           boolean skipTimelinePublish,
                                           boolean skipLocking) {
    super(context, config, table, instantTime, commitInstant, deleteInstants, skipTimelinePublish, skipLocking, false);
  }

  @Override
  protected List<HoodieRollbackStat> executeRollback(HoodieRollbackPlan hoodieRollbackPlan) {
    HoodieTimer rollbackTimer = HoodieTimer.start();

    List<HoodieRollbackStat> stats = new ArrayList<>();
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();

    if (instantToRollback.isCompleted()) {
      log.info("Unpublishing instant " + instantToRollback);
      table.getMetaClient().getTableFormat().rollback(instantToRollback, table.getContext(), table.getMetaClient(), table.getViewManager());
      // Revert the completed instant to inflight in native format.
      resolvedInstant = activeTimeline.revertToInflight(instantToRollback);
      // reload meta-client to reflect latest timeline status
      table.getMetaClient().reloadActiveTimeline();
    }

    // If instant is inflight but marked as completed in native format, delete the completed instant from storage.
    if (instantToRollback.isInflight() && !table.getMetaClient().getTableFormat().getName().equals(NativeTableFormat.TABLE_FORMAT)) {
      HoodieActiveTimeline activeTimelineForNativeFormat = table.getMetaClient().getActiveTimelineForNativeFormat();
      Option<HoodieInstant> instantToRollbackInNativeFormat = activeTimelineForNativeFormat.filter(instant -> instant.requestedTime().equals(instantToRollback.requestedTime())).lastInstant();
      if (instantToRollbackInNativeFormat.isPresent() && instantToRollbackInNativeFormat.get().isCompleted()) {
        resolvedInstant = activeTimelineForNativeFormat.revertToInflight(instantToRollbackInNativeFormat.get());
        table.getMetaClient().reloadActiveTimeline();
      }
    }

    // For Requested State (like failure during index lookup), there is nothing to do rollback other than
    // deleting the timeline file
    if (!resolvedInstant.isRequested()) {
      // delete all the data files for this commit
      log.info("Clean out all base files generated for commit: " + resolvedInstant);
      stats = executeRollback(resolvedInstant, hoodieRollbackPlan);
    }

    dropBootstrapIndexIfNeeded(instantToRollback);

    log.info("Time(in ms) taken to finish rollback " + rollbackTimer.endTimer());
    return stats;
  }
}
