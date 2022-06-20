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

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.CkpMetadata;
import org.apache.hudi.table.HoodieTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class FlinkCopyOnWriteRollbackActionExecutor<T extends HoodieRecordPayload, I, K, O>
    extends CopyOnWriteRollbackActionExecutor<T, I, K, O> {

  private static final Logger LOG =
      LogManager.getLogger(FlinkCopyOnWriteRollbackActionExecutor.class);

  public FlinkCopyOnWriteRollbackActionExecutor(
      HoodieEngineContext context,
      HoodieWriteConfig config,
      HoodieTable<T, I, K, O> table,
      String instantTime,
      HoodieInstant commitInstant,
      boolean deleteInstants,
      boolean skipLocking) {

    super(context, config, table, instantTime, commitInstant, deleteInstants, skipLocking);
  }

  public FlinkCopyOnWriteRollbackActionExecutor(
      HoodieEngineContext context,
      HoodieWriteConfig config,
      HoodieTable<T, I, K, O> table,
      String instantTime,
      HoodieInstant commitInstant,
      boolean deleteInstants,
      boolean skipTimelinePublish,
      boolean skipLocking) {
    super(
        context,
        config,
        table,
        instantTime,
        commitInstant,
        deleteInstants,
        skipTimelinePublish,
        skipLocking);
  }

  @Override
  protected void deleteInflightAndRequestedInstant(
      boolean deleteInstant,
      HoodieActiveTimeline activeTimeline,
      HoodieInstant instantToBeDeleted) {
    super.deleteInflightAndRequestedInstant(deleteInstant, activeTimeline, instantToBeDeleted);
    // resolvedInstant uncompleted means that we will delete it during rollback
    if (deleteInstant && !instantToBeDeleted.isCompleted()) {
      CkpMetadata ckpMetadata =
          CkpMetadata.getInstance(
              table.getMetaClient().getFs(), table.getMetaClient().getBasePath());
      ckpMetadata.deleteInstant(instantToBeDeleted.getTimestamp());
      LOG.info("delete checkpoint metadata for for rollback instant: " + instantToBeDeleted);
    }
  }
}
