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

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.model.AWSDmsAvroPayload;
import org.apache.hudi.common.model.OverwriteNonDefaultsWithLatestAvroPayload;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.table.HoodieTable;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_KEY;
import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_MARKER;
import static org.apache.hudi.common.model.HoodieRecordMerger.COMMIT_TIME_BASED_MERGE_STRATEGY_UUID;
import static org.apache.hudi.common.model.HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID;
import static org.apache.hudi.common.table.HoodieTableConfig.DEBEZIUM_UNAVAILABLE_VALUE;
import static org.apache.hudi.common.table.HoodieTableConfig.MERGE_PROPERTIES;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTIAL_UPDATE_CUSTOM_MARKER;
import static org.apache.hudi.table.upgrade.UpgradeDowngradeUtils.rollbackFailedWritesAndCompact;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTIAL_UPDATE_MODE;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_MODE;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_STRATEGY_ID;
import static org.apache.hudi.table.upgrade.SevenToEightUpgradeHandler.isMetadataTableBehindDataTable;
import static org.apache.hudi.table.upgrade.UpgradeDowngradeUtils.rollbackFailedWritesAndCompact;

public class EightToNineUpgradeHandler implements UpgradeHandler {
  @Override
  public Map<ConfigProperty, String> upgrade(HoodieWriteConfig config,
                                             HoodieEngineContext context,
                                             String instantTime,
                                             SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    final HoodieTable table = upgradeDowngradeHelper.getTable(config, context);
    // Rollback and run compaction in one step
    rollbackFailedWritesAndCompact(
        table, context, config, upgradeDowngradeHelper,
        HoodieTableType.MERGE_ON_READ.equals(table.getMetaClient().getTableType()),
        HoodieTableVersion.NINE);

    Map<ConfigProperty, String> tablePropsToAdd = new HashMap<>();
    HoodieTable table = upgradeDowngradeHelper.getTable(config, context);
    HoodieTableMetaClient metaClient = table.getMetaClient();
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    String payloadClass = tableConfig.getPayloadClass();

    // If auto upgrade is disabled, set writer version to 8 and return
    if (!config.autoUpgrade()) {
      config.setValue(
          HoodieWriteConfig.WRITE_TABLE_VERSION,
          String.valueOf(HoodieTableVersion.EIGHT.versionCode()));
      return tablePropsToAdd;
    }

    // If metadata is enabled for the data table, and
    // existing metadata table is behind the data table, then delete it
    if (!table.isMetadataTable()
        && config.isMetadataTableEnabled()
        && isMetadataTableBehindDataTable(config, metaClient)) {
      HoodieTableMetadataUtil.deleteMetadataTable(config.getBasePath(), context);
    }

    // Rollback and run compaction in one step
    rollbackFailedWritesAndCompact(
        table, context, config, upgradeDowngradeHelper,
        HoodieTableType.MERGE_ON_READ.equals(table.getMetaClient().getTableType()),
        HoodieTableVersion.EIGHT);

    // Handle table configs.
    String mergeProperties = tableConfig.getMergeProperties();
    if (!StringUtils.isNullOrEmpty(payloadClass)) {
      // Add MERGE Mode.
      if (payloadClass.equals(OverwriteNonDefaultsWithLatestAvroPayload.class.getName())
          || payloadClass.equals(AWSDmsAvroPayload.class.getName())) {
        tablePropsToAdd.put(RECORD_MERGE_STRATEGY_ID, COMMIT_TIME_BASED_MERGE_STRATEGY_UUID);
        tablePropsToAdd.put(RECORD_MERGE_MODE, RecordMergeMode.COMMIT_TIME_ORDERING.name());
      } else if (payloadClass.equals(PartialUpdateAvroPayload.class.getName())
          || payloadClass.equals(PostgresDebeziumAvroPayload.class.getName())) {
        tablePropsToAdd.put(RECORD_MERGE_STRATEGY_ID, EVENT_TIME_BASED_MERGE_STRATEGY_UUID);
        tablePropsToAdd.put(RECORD_MERGE_MODE, RecordMergeMode.EVENT_TIME_ORDERING.name());
      }
      // Add PARTIAL UPDATE Mode.
      if (payloadClass.equals(OverwriteNonDefaultsWithLatestAvroPayload.class.getName())
          || payloadClass.equals(PartialUpdateAvroPayload.class.getName())) {
        tablePropsToAdd.put(PARTIAL_UPDATE_MODE, PartialUpdateMode.IGNORE_DEFAULTS.name());
      } else if (payloadClass.equals(PostgresDebeziumAvroPayload.class.getName())) {
        tablePropsToAdd.put(PARTIAL_UPDATE_MODE, PartialUpdateMode.IGNORE_MARKERS.name());
      }
      // Add MERGE Properties.
      if (payloadClass.equals(AWSDmsAvroPayload.class.getName())) {
        String propertiesToAdd = DELETE_KEY + "=Op," + DELETE_MARKER + "=D";
        mergeProperties = StringUtils.isNullOrEmpty(mergeProperties)
            ? propertiesToAdd : mergeProperties + "," + mergeProperties;
      } else if (payloadClass.equals(PostgresDebeziumAvroPayload.class.getName())) {
        String propertiesToAdd =
            PARTIAL_UPDATE_CUSTOM_MARKER + "=" + DEBEZIUM_UNAVAILABLE_VALUE;
        mergeProperties = StringUtils.isNullOrEmpty(mergeProperties)
            ? propertiesToAdd : mergeProperties + "," + mergeProperties;
      }
    }
    tablePropsToAdd.put(MERGE_PROPERTIES, mergeProperties);
    return tablePropsToAdd;
  }
}
