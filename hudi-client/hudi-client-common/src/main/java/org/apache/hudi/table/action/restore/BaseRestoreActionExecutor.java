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

import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class BaseRestoreActionExecutor<T extends HoodieRecordPayload, I, K, O> extends BaseActionExecutor<T, I, K, O, HoodieRestoreMetadata> {

  private static final Logger LOG = LogManager.getLogger(BaseRestoreActionExecutor.class);

  private final String restoreInstantTime;
  private final TransactionManager txnManager;

  public BaseRestoreActionExecutor(HoodieEngineContext context,
                                   HoodieWriteConfig config,
                                   HoodieTable<T, I, K, O> table,
                                   String instantTime,
                                   String restoreInstantTime) {
    super(context, config, table, instantTime);
    this.restoreInstantTime = restoreInstantTime;
    this.txnManager = new TransactionManager(config, table.getMetaClient().getFs());
  }

  @Override
  public HoodieRestoreMetadata execute() {
    HoodieTimer restoreTimer = new HoodieTimer();
    restoreTimer.startTimer();

    // Get all the commits on the timeline after the provided commit time
    List<HoodieInstant> instantsToRollback = table.getActiveTimeline().getWriteTimeline()
        .getReverseOrderedInstants()
        .filter(instant -> HoodieActiveTimeline.GREATER_THAN.test(instant.getTimestamp(), restoreInstantTime))
        .collect(Collectors.toList());

    Map<String, List<HoodieRollbackMetadata>> instantToMetadata = new HashMap<>();
    table.getActiveTimeline().createNewInstant(new HoodieInstant(true, HoodieTimeline.RESTORE_ACTION, instantTime));
    instantsToRollback.forEach(instant -> {
      instantToMetadata.put(instant.getTimestamp(), Collections.singletonList(rollbackInstant(instant)));
      LOG.info("Deleted instant " + instant);
    });

    try {
      return finishRestore(instantToMetadata,
          instantsToRollback,
          restoreTimer.endTimer()
      );
    } catch (IOException io) {
      throw new HoodieRollbackException("unable to rollback instants " + instantsToRollback, io);
    }
  }

  protected abstract HoodieRollbackMetadata rollbackInstant(HoodieInstant rollbackInstant);

  private HoodieRestoreMetadata finishRestore(Map<String, List<HoodieRollbackMetadata>> instantToMetadata,
                                              List<HoodieInstant> instantsRolledBack,
                                              long durationInMs) throws IOException {

    HoodieRestoreMetadata restoreMetadata = TimelineMetadataUtils.convertRestoreMetadata(
        instantTime, durationInMs, instantsRolledBack, instantToMetadata);
    writeToMetadata(restoreMetadata);
    table.getActiveTimeline().saveAsComplete(new HoodieInstant(true, HoodieTimeline.RESTORE_ACTION, instantTime),
        TimelineMetadataUtils.serializeRestoreMetadata(restoreMetadata));
    LOG.info("Commits " + instantsRolledBack + " rollback is complete. Restored table to " + restoreInstantTime);
    return restoreMetadata;
  }

  /**
   * Update metadata table if available. Any update to metadata table happens within data table lock.
   * @param restoreMetadata instance of {@link HoodieRestoreMetadata} to be applied to metadata.
   */
  private void writeToMetadata(HoodieRestoreMetadata restoreMetadata) {
    try {
      this.txnManager.beginTransaction(Option.empty(), Option.empty());
      writeTableMetadata(restoreMetadata);
    } finally {
      this.txnManager.endTransaction(Option.empty());
    }
  }
}
