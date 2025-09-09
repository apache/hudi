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

package org.apache.hudi.table.action.restore;

import org.apache.hudi.avro.model.HoodieInstantInfo;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRestorePlan;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieRestoreException;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN;

public abstract class BaseRestoreActionExecutor<T, I, K, O> extends BaseActionExecutor<T, I, K, O, HoodieRestoreMetadata> {

  private static final Logger LOG = LoggerFactory.getLogger(BaseRestoreActionExecutor.class);

  private final String savepointToRestoreTimestamp;
  private final TransactionManager txnManager;

  public BaseRestoreActionExecutor(HoodieEngineContext context,
                                   HoodieWriteConfig config,
                                   HoodieTable<T, I, K, O> table,
                                   String instantTime,
                                   String savepointToRestoreTimestamp) {
    super(context, config, table, instantTime);
    this.savepointToRestoreTimestamp = savepointToRestoreTimestamp;
    this.txnManager = new TransactionManager(config, table.getStorage());
  }

  @Override
  public HoodieRestoreMetadata execute() {
    HoodieTimer restoreTimer = HoodieTimer.start();

    Option<HoodieInstant> restoreInstant = table.getRestoreTimeline()
        .filterInflightsAndRequested()
        .filter(instant -> instant.requestedTime().equals(instantTime))
        .firstInstant();
    if (!restoreInstant.isPresent()) {
      throw new HoodieRollbackException("No pending restore instants found to execute restore");
    }
    try {
      List<HoodieInstant> instantsToRollback = getInstantsToRollback(restoreInstant.get());
      ValidationUtils.checkArgument(restoreInstant.get().getState().equals(HoodieInstant.State.REQUESTED)
          || restoreInstant.get().getState().equals(HoodieInstant.State.INFLIGHT));
      Map<String, List<HoodieRollbackMetadata>> instantToMetadata = new HashMap<>();
      if (restoreInstant.get().isRequested()) {
        table.getActiveTimeline().transitionRestoreRequestedToInflight(restoreInstant.get());
      }

      instantsToRollback.forEach(instant -> {
        instantToMetadata.put(instant.requestedTime(), Collections.singletonList(rollbackInstant(instant)));
        LOG.info("Deleted instant " + instant);
      });

      return finishRestore(instantToMetadata,
          instantsToRollback,
          restoreTimer.endTimer()
      );
    } catch (IOException io) {
      throw new HoodieRestoreException("unable to Restore instant " + restoreInstant.get(), io);
    }
  }

  private List<HoodieInstant> getInstantsToRollback(HoodieInstant restoreInstant) throws IOException {
    List<HoodieInstant> instantsToRollback = new ArrayList<>();
    HoodieRestorePlan restorePlan = RestoreUtils.getRestorePlan(table.getMetaClient(), restoreInstant);
    for (HoodieInstantInfo instantInfo : restorePlan.getInstantsToRollback()) {
      // If restore crashed midway, there are chances that some commits are already rolled back,
      // but some are not. so, we can ignore those commits which are fully rolledback in previous attempt if any.
      Option<HoodieInstant> rollbackInstantOpt = table.getActiveTimeline().getWriteTimeline()
          .filter(instant -> instant.requestedTime().equals(instantInfo.getCommitTime()) && instant.getAction().equals(instantInfo.getAction())).firstInstant();
      if (rollbackInstantOpt.isPresent()) {
        instantsToRollback.add(rollbackInstantOpt.get());
      } else {
        LOG.warn("Ignoring already rolledback instant " + instantInfo.toString());
      }
    }
    return instantsToRollback;
  }

  protected abstract HoodieRollbackMetadata rollbackInstant(HoodieInstant rollbackInstant);

  private HoodieRestoreMetadata finishRestore(Map<String, List<HoodieRollbackMetadata>> instantToMetadata,
                                              List<HoodieInstant> instantsRolledBack,
                                              long durationInMs) throws IOException {

    HoodieRestoreMetadata restoreMetadata = TimelineMetadataUtils.convertRestoreMetadata(
        instantTime, durationInMs, instantsRolledBack, instantToMetadata);
    HoodieInstant restoreInflightInstant = instantGenerator.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.RESTORE_ACTION, instantTime);
    writeToMetadata(restoreMetadata, restoreInflightInstant);
    table.getActiveTimeline().saveAsComplete(
        true,
        restoreInflightInstant,
        Option.of(restoreMetadata),
        restoreCompletedInstant -> table.getMetaClient().getTableFormat().restore(restoreCompletedInstant, table.getContext(), table.getMetaClient(), table.getViewManager()));
    // get all pending rollbacks instants after restore instant time and delete them.
    // if not, rollbacks will be considered not completed and might hinder metadata table compaction.
    List<HoodieInstant> instantsToRollback = table.getActiveTimeline().getRollbackTimeline()
        .getReverseOrderedInstants()
        .filter(instant -> GREATER_THAN.test(instant.requestedTime(), savepointToRestoreTimestamp))
        .collect(Collectors.toList());
    instantsToRollback.forEach(entry -> {
      if (entry.isCompleted()) {
        table.getActiveTimeline().deleteCompletedRollback(entry);
      }
      table.getActiveTimeline().deletePending(instantGenerator.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.ROLLBACK_ACTION, entry.requestedTime()));
      table.getActiveTimeline().deletePending(instantGenerator.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.ROLLBACK_ACTION, entry.requestedTime()));
    });
    LOG.info("Commits " + instantsRolledBack + " rollback is complete. Restored table to " + savepointToRestoreTimestamp);
    return restoreMetadata;
  }

  /**
   * Update metadata table if available. Any update to metadata table happens within data table lock.
   *
   * @param restoreMetadata instance of {@link HoodieRestoreMetadata} to be applied to metadata.
   */
  private void writeToMetadata(HoodieRestoreMetadata restoreMetadata, HoodieInstant restoreInflightInstant) {
    try {
      this.txnManager.beginStateChange(Option.of(restoreInflightInstant), Option.empty());
      writeTableMetadata(restoreMetadata);
    } finally {
      this.txnManager.endStateChange(Option.of(restoreInflightInstant));
    }
  }
}
