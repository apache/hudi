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

package org.apache.hudi.table.upgrade;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.table.upgrade.UpgradeDowngradeUtils.rollbackFailedWritesAndCompact;

public class NineToEightDowngradeHandler implements DowngradeHandler {
  @Override
  public Pair<Map<ConfigProperty, String>, List<ConfigProperty>> downgrade(HoodieWriteConfig config,
                                                                           HoodieEngineContext context,
                                                                           String instantTime,
                                                                           SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    final HoodieTable table = upgradeDowngradeHelper.getTable(config, context);
    // Rollback and run compaction in one step
    rollbackFailedWritesAndCompact(
        table, context, config, upgradeDowngradeHelper,
        HoodieTableType.MERGE_ON_READ.equals(table.getMetaClient().getTableType()),
        HoodieTableVersion.NINE);
    UpgradeDowngradeUtils.dropNonV1SecondaryIndexPartitions(
        config, context, table, upgradeDowngradeHelper, "downgrading from table version nine to eight");
    return Pair.of(Collections.emptyMap(), Collections.emptyList());
  }
}
