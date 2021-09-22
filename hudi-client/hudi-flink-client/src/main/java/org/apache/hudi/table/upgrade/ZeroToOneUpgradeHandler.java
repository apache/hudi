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

package org.apache.hudi.table.upgrade;

import org.apache.hudi.avro.model.HoodieRollbackRequest;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.rollback.BaseRollbackHelper;
import org.apache.hudi.table.action.rollback.ListingBasedRollbackHelper;
import org.apache.hudi.table.action.rollback.ListingBasedRollbackRequest;

import java.util.List;

/**
 * Upgrade handle to assist in upgrading hoodie table from version 0 to 1.
 */
public class ZeroToOneUpgradeHandler extends BaseZeroToOneUpgradeHandler {

  @Override
  HoodieTable getTable(HoodieWriteConfig config, HoodieEngineContext context) {
    return HoodieFlinkTable.create(config, (HoodieFlinkEngineContext) context);
  }

  @Override
  List<HoodieRollbackStat> getListBasedRollBackStats(HoodieTableMetaClient metaClient, HoodieWriteConfig config, HoodieEngineContext context, Option<HoodieInstant> commitInstantOpt,
                                                     List<ListingBasedRollbackRequest> rollbackRequests) {
    List<HoodieRollbackRequest> hoodieRollbackRequests = new ListingBasedRollbackHelper(metaClient, config)
        .getRollbackRequestsForRollbackPlan(context, commitInstantOpt.get(), rollbackRequests);
    return new BaseRollbackHelper(metaClient, config).collectRollbackStats(context, commitInstantOpt.get(), hoodieRollbackRequests);
  }
}
