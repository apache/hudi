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

import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.BaseHoodieTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public abstract class BaseCopyOnWriteRollbackActionExecutor<T extends HoodieRecordPayload, I, K, O, P> extends BaseRollbackActionExecutor<T, I, K, O, P> {

  private static final Logger LOG = LogManager.getLogger(BaseCopyOnWriteRollbackActionExecutor.class);

  public BaseCopyOnWriteRollbackActionExecutor(HoodieEngineContext context,
                                               HoodieWriteConfig config,
                                               BaseHoodieTable<T, I, K, O, P> table,
                                               String instantTime,
                                               HoodieInstant commitInstant,
                                               boolean deleteInstants) {
    super(context, config, table, instantTime, commitInstant, deleteInstants);
  }

  public BaseCopyOnWriteRollbackActionExecutor(HoodieEngineContext context,
                                               HoodieWriteConfig config,
                                               BaseHoodieTable<T, I, K, O, P> table,
                                               String instantTime,
                                               HoodieInstant commitInstant,
                                               boolean deleteInstants,
                                               boolean skipTimelinePublish) {
    super(context, config, table, instantTime, commitInstant, deleteInstants, skipTimelinePublish);
  }

  protected List<RollbackRequest> generateRollbackRequests(HoodieInstant instantToRollback)
      throws IOException {
    return FSUtils.getAllPartitionPaths(table.getMetaClient().getFs(), table.getMetaClient().getBasePath(),
        config.shouldAssumeDatePartitioning()).stream()
        .map(partitionPath -> RollbackRequest.createRollbackRequestWithDeleteDataAndLogFilesAction(partitionPath, instantToRollback))
        .collect(Collectors.toList());
  }

}
