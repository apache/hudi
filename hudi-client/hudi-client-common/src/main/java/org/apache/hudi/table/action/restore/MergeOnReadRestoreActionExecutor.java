/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table.action.restore;

import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.rollback.MergeOnReadRollbackActionExecutor;

public class MergeOnReadRestoreActionExecutor<T, I, K, O>
    extends BaseRestoreActionExecutor<T, I, K, O> {
  public MergeOnReadRestoreActionExecutor(HoodieEngineContext context, HoodieWriteConfig config, HoodieTable<T, I, K, O> table,
                                          String instantTime, String savepointToRestoreTimestamp) {
    super(context, config, table, instantTime, savepointToRestoreTimestamp);
  }

  @Override
  protected HoodieRollbackMetadata rollbackInstant(HoodieInstant instantToRollback) {
    switch (instantToRollback.getAction()) {
      case HoodieTimeline.COMMIT_ACTION:
      case HoodieTimeline.DELTA_COMMIT_ACTION:
      case HoodieTimeline.COMPACTION_ACTION:
      case HoodieTimeline.REPLACE_COMMIT_ACTION:
      case HoodieTimeline.CLUSTERING_ACTION:
        // TODO : Get file status and create a rollback stat and file
        // TODO : Delete the .aux files along with the instant file, okay for now since the archival process will
        // delete these files when it does not see a corresponding instant file under .hoodie
        break;
      default:
        throw new IllegalArgumentException("invalid action name " + instantToRollback.getAction());
    }
    String newInstantTime;
    try (TransactionManager transactionManager = new TransactionManager(config, table.getStorage())) {
      newInstantTime = transactionManager.executeStateChangeWithInstant(instantTime -> {
        table.getMetaClient().reloadActiveTimeline();
        table.scheduleRollback(context, instantTime, instantToRollback, false, false, true);
        return instantTime;
      });
    }
    table.getMetaClient().reloadActiveTimeline();
    MergeOnReadRollbackActionExecutor rollbackActionExecutor = new MergeOnReadRollbackActionExecutor(
        context,
        config,
        table,
        newInstantTime,
        instantToRollback,
        true,
        true,
        false);

    // TODO : Get file status and create a rollback stat and file
    // TODO : Delete the .aux files along with the instant file, okay for now since the archival process will
    // delete these files when it does not see a corresponding instant file under .hoodie
    return rollbackActionExecutor.execute();
  }
}
