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
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.EventTimeAvroPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.model.debezium.MySqlDebeziumAvroPayload;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.model.AWSDmsAvroPayload;
import org.apache.hudi.common.model.OverwriteNonDefaultsWithLatestAvroPayload;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hudi.common.table.HoodieTableConfig.MERGE_PROPERTIES;
import static org.apache.hudi.common.model.HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTIAL_UPDATE_MODE;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_MODE;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_STRATEGY_ID;
import static org.apache.hudi.table.upgrade.UpgradeDowngradeUtils.checkAndHandleMetadataTable;
import static org.apache.hudi.table.upgrade.UpgradeDowngradeUtils.rollbackFailedWritesAndCompact;

/**
 * Version 8 is the placeholder version from 1.0.0 to 1.0.2.
 * Version 9 is the placeholder version >= 1.1.0.
 * The major change introduced in version 9 is two table configurations for payload deprecation.
 * During the downgrade, we remove these two table configurations.
 */
public class NineToEightDowngradeHandler implements DowngradeHandler {
  private static final Set<String> PAYLOAD_CLASSES_TO_HANDLE = new HashSet<>(Arrays.asList(
      AWSDmsAvroPayload.class.getName(),
      EventTimeAvroPayload.class.getName(),
      MySqlDebeziumAvroPayload.class.getName(),
      OverwriteNonDefaultsWithLatestAvroPayload.class.getName(),
      PartialUpdateAvroPayload.class.getName(),
      PostgresDebeziumAvroPayload.class.getName()));

  @Override
  public Pair<Map<ConfigProperty, String>, List<ConfigProperty>> downgrade(HoodieWriteConfig config,
                                                                           HoodieEngineContext context,
                                                                           String instantTime,
                                                                           SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    final HoodieTable table = upgradeDowngradeHelper.getTable(config, context);
    HoodieTableMetaClient metaClient = table.getMetaClient();

    // If metadata is enabled for the data table, and
    // existing metadata table is behind the data table, then delete it
    checkAndHandleMetadataTable(context, table, config, metaClient);
    // Rollback and run compaction in one step
    rollbackFailedWritesAndCompact(
        table, context, config, upgradeDowngradeHelper,
        HoodieTableType.MERGE_ON_READ.equals(table.getMetaClient().getTableType()),
        HoodieTableVersion.NINE);
    HoodieTableMetaClient sd = HoodieTableMetaClient.builder()
        .setConf(context.getStorageConf().newInstance())
        .setBasePath(config.getBasePath())
        .build();
    // Prepare parameters.
    if (metaClient.getTableConfig().isMetadataTableAvailable()) {
      UpgradeDowngradeUtils.updateMetadataTableVersion(context, HoodieTableVersion.EIGHT, metaClient);
    }

    List<ConfigProperty> propertiesToRemove = new ArrayList<>();
    propertiesToRemove.add(MERGE_PROPERTIES);
    propertiesToRemove.add(PARTIAL_UPDATE_MODE);
    // For specified payload classes, add strategy id and custom merge mode.
    Map<ConfigProperty, String> propertiesToAdd = new HashMap<>();
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    String payloadClass = tableConfig.getPayloadClass();
    if (PAYLOAD_CLASSES_TO_HANDLE.contains(payloadClass)) {
      propertiesToAdd.put(RECORD_MERGE_STRATEGY_ID, PAYLOAD_BASED_MERGE_STRATEGY_UUID);
      propertiesToAdd.put(RECORD_MERGE_MODE, RecordMergeMode.CUSTOM.name());
    }

    return Pair.of(propertiesToAdd, propertiesToRemove);
  }
}
