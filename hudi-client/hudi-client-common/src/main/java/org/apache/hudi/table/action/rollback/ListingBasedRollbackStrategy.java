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

import org.apache.hudi.avro.model.HoodieRollbackRequest;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.table.HoodieTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * Listing based rollback strategy to fetch list of {@link HoodieRollbackRequest}s.
 */
public class ListingBasedRollbackStrategy implements BaseRollbackPlanActionExecutor.RollbackStrategy {

  private static final Logger LOG = LogManager.getLogger(ListingBasedRollbackStrategy.class);

  protected final HoodieTable table;
  protected final HoodieEngineContext context;
  protected final HoodieWriteConfig config;
  protected final String instantTime;

  public ListingBasedRollbackStrategy(HoodieTable table,
                                      HoodieEngineContext context,
                                      HoodieWriteConfig config,
                                      String instantTime) {
    this.table = table;
    this.context = context;
    this.config = config;
    this.instantTime = instantTime;
  }

  @Override
  public List<HoodieRollbackRequest> getRollbackRequests(HoodieInstant instantToRollback) {
    try {
      List<ListingBasedRollbackRequest> rollbackRequests = null;
      if (table.getMetaClient().getTableType() == HoodieTableType.COPY_ON_WRITE) {
        rollbackRequests = RollbackUtils.generateRollbackRequestsByListingCOW(context,
            table.getMetaClient().getBasePath());
      } else {
        rollbackRequests = RollbackUtils
            .generateRollbackRequestsUsingFileListingMOR(instantToRollback, table, context);
      }
      List<HoodieRollbackRequest> listingBasedRollbackRequests = new ListingBasedRollbackHelper(table.getMetaClient(), config)
          .getRollbackRequestsForRollbackPlan(context, instantToRollback, rollbackRequests);
      return listingBasedRollbackRequests;
    } catch (IOException e) {
      LOG.error("Generating rollback requests failed for " + instantToRollback.getTimestamp(), e);
      throw new HoodieRollbackException("Generating rollback requests failed for " + instantToRollback.getTimestamp(), e);
    }
  }
}
