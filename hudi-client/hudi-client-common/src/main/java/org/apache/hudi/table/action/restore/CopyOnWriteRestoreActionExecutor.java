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
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.rollback.CopyOnWriteRollbackActionExecutor;

public class CopyOnWriteRestoreActionExecutor<T, I, K, O>
    extends BaseRestoreActionExecutor<T, I, K, O> {
  public CopyOnWriteRestoreActionExecutor(HoodieEngineContext context,
                                          HoodieWriteConfig config,
                                          HoodieTable table,
                                          String instantTime,
                                          String restoreInstantTime) {
    super(context, config, table, instantTime, restoreInstantTime);
  }

  @Override
  protected HoodieRollbackMetadata rollbackInstant(HoodieInstant instantToRollback) {
    if (!instantToRollback.getAction().equals(HoodieTimeline.COMMIT_ACTION)
        && !instantToRollback.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION)) {
      throw new HoodieRollbackException("Unsupported action in rollback instant:" + instantToRollback);
    }
    table.getMetaClient().reloadActiveTimeline();
    String newInstantTime = HoodieActiveTimeline.createNewInstantTime();
    table.scheduleRollback(context, newInstantTime, instantToRollback, false, false);
    table.getMetaClient().reloadActiveTimeline();
    CopyOnWriteRollbackActionExecutor rollbackActionExecutor = new CopyOnWriteRollbackActionExecutor(
        context,
        config,
        table,
        newInstantTime,
        instantToRollback,
        true,
        true,
        false);
    return rollbackActionExecutor.execute();
  }
}
