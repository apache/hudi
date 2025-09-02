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
import org.apache.hudi.common.model.AWSDmsAvroPayload;
import org.apache.hudi.common.model.EventTimeAvroPayload;
import org.apache.hudi.common.model.HoodieIndexMetadata;
import org.apache.hudi.common.model.OverwriteNonDefaultsWithLatestAvroPayload;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.common.model.debezium.MySqlDebeziumAvroPayload;
import org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.table.HoodieTable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_KEY;
import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_MARKER;
import static org.apache.hudi.common.model.debezium.DebeziumConstants.FLATTENED_FILE_COL_NAME;
import static org.apache.hudi.common.model.debezium.DebeziumConstants.FLATTENED_POS_COL_NAME;
import static org.apache.hudi.common.model.HoodieRecordMerger.COMMIT_TIME_BASED_MERGE_STRATEGY_UUID;
import static org.apache.hudi.common.model.HoodieRecordMerger.CUSTOM_MERGE_STRATEGY_UUID;
import static org.apache.hudi.common.model.HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID;
import static org.apache.hudi.common.model.HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID;
import static org.apache.hudi.common.table.HoodieTableConfig.DEBEZIUM_UNAVAILABLE_VALUE;
import static org.apache.hudi.common.table.HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTIAL_UPDATE_UNAVAILABLE_VALUE;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTIAL_UPDATE_MODE;
import static org.apache.hudi.common.table.HoodieTableConfig.PAYLOAD_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_MODE;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_PROPERTY_PREFIX;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_STRATEGY_ID;
import static org.apache.hudi.table.upgrade.UpgradeDowngradeUtils.PAYLOAD_CLASSES_TO_HANDLE;

/**
 * Version 8 presents Hudi version from 1.0.0 to 1.0.2.
 * Version 9 presents Hudi version >= 1.1.0.
 * Major upgrade logic:
 *  Deprecate a given set of payload classes to prefer merge mode. That is,
 *   for table with payload class defined in RFC-97,
 *     remove hoodie.compaction.payload.class from table_configs
 *     add hoodie.legacy.payload.class=payload to table_configs
 *     set hoodie.table.partial.update.mode based on RFC-97
 *     set hoodie.table.merge.properties based on RFC-97
 *     set hoodie.record.merge.mode based on RFC-97
 *     set hoodie.record.merge.strategy.id based on RFC-97
 *   for table with event_time/commit_time merge mode,
 *     set hoodie.table.partial.update.mode to default value
 *   for certain payloads, we might need to set additional merge properties in table config to expose it to readers.
 *     set properties with hoodie.record.merge.property. as prefix as needed.
 *   for table with custom merger or payload,
 *     set hoodie.table.partial.update.mode to default value.
 */
public class EightToNineUpgradeHandler implements UpgradeHandler {
  private static final Set<String> PAYLOADS_MAPPED_TO_EVENT_TIME_MERGE_MODE = new HashSet<>(Arrays.asList(
      EventTimeAvroPayload.class.getName(),
      MySqlDebeziumAvroPayload.class.getName(),
      PartialUpdateAvroPayload.class.getName(),
      PostgresDebeziumAvroPayload.class.getName()));
  private static final Set<String> PAYLOADS_MAPPED_TO_COMMIT_TIME_MERGE_MODE = new HashSet<>(Arrays.asList(
      AWSDmsAvroPayload.class.getName(),
      OverwriteNonDefaultsWithLatestAvroPayload.class.getName()));
  public static final Set<String> BUILTIN_MERGE_STRATEGIES = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList(
          COMMIT_TIME_BASED_MERGE_STRATEGY_UUID,
          CUSTOM_MERGE_STRATEGY_UUID,
          EVENT_TIME_BASED_MERGE_STRATEGY_UUID,
          PAYLOAD_BASED_MERGE_STRATEGY_UUID)));

  @Override
  public UpgradeDowngrade.TableConfigChangeSet upgrade(HoodieWriteConfig config,
                                                       HoodieEngineContext context,
                                                       String instantTime,
                                                       SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    final HoodieTable table = upgradeDowngradeHelper.getTable(config, context);
    Map<ConfigProperty, String> tablePropsToAdd = new HashMap<>();
    Set<ConfigProperty> tablePropsToRemove = new HashSet<>();
    HoodieTableMetaClient metaClient = table.getMetaClient();
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    // Populate missing index versions indexes
    Option<HoodieIndexMetadata> indexMetadataOpt = metaClient.getIndexMetadata();
    if (indexMetadataOpt.isPresent()) {
      populateIndexVersionIfMissing(indexMetadataOpt);
      // Write the updated index metadata back to storage
      HoodieTableMetaClient.writeIndexMetadataToStorage(
          metaClient.getStorage(),
          metaClient.getIndexDefinitionPath(),
          indexMetadataOpt.get(),
          metaClient.getTableConfig().getTableVersion());
    }
    // Handle merge mode config.
    reconcileMergeModeConfig(tablePropsToAdd, tableConfig);
    // Handle partial update mode config.
    reconcilePartialUpdateModeConfig(tablePropsToAdd, tableConfig);
    // Handle merge properties config.
    reconcileMergePropertiesConfig(tablePropsToAdd, tableConfig);
    // Handle payload class configs.
    reconcilePayloadClassConfig(tablePropsToAdd, tablePropsToRemove, tableConfig);
    // Handle ordering fields config.
    reconcileOrderingFieldsConfig(tablePropsToAdd, tablePropsToRemove, tableConfig);
    return new UpgradeDowngrade.TableConfigChangeSet(tablePropsToAdd, tablePropsToRemove);
  }

  private void reconcileMergeModeConfig(Map<ConfigProperty, String> tablePropsToAdd,
                                        HoodieTableConfig tableConfig) {
    String payloadClass = tableConfig.getPayloadClass();
    String mergeStrategy = tableConfig.getRecordMergeStrategyId();
    if (!BUILTIN_MERGE_STRATEGIES.contains(mergeStrategy) || StringUtils.isNullOrEmpty(payloadClass)) {
      return;
    }
    if (PAYLOADS_MAPPED_TO_COMMIT_TIME_MERGE_MODE.contains(payloadClass)) {
      tablePropsToAdd.put(RECORD_MERGE_MODE, RecordMergeMode.COMMIT_TIME_ORDERING.name());
      tablePropsToAdd.put(RECORD_MERGE_STRATEGY_ID, COMMIT_TIME_BASED_MERGE_STRATEGY_UUID);
    } else if (PAYLOADS_MAPPED_TO_EVENT_TIME_MERGE_MODE.contains(payloadClass)) {
      tablePropsToAdd.put(RECORD_MERGE_MODE, RecordMergeMode.EVENT_TIME_ORDERING.name());
      tablePropsToAdd.put(RECORD_MERGE_STRATEGY_ID, EVENT_TIME_BASED_MERGE_STRATEGY_UUID);
    }
    // else: No op, which means merge strategy id and merge mode are not changed.
  }

  private void reconcilePayloadClassConfig(Map<ConfigProperty, String> tablePropsToAdd,
                                           Set<ConfigProperty> tablePropsToRemove,
                                           HoodieTableConfig tableConfig) {
    String payloadClass = tableConfig.getPayloadClass();
    String mergeStrategy = tableConfig.getRecordMergeStrategyId();
    if (!BUILTIN_MERGE_STRATEGIES.contains(mergeStrategy) || StringUtils.isNullOrEmpty(payloadClass)) {
      return;
    }
    if (PAYLOAD_CLASSES_TO_HANDLE.contains(payloadClass)) {
      tablePropsToAdd.put(LEGACY_PAYLOAD_CLASS_NAME, payloadClass);
      tablePropsToRemove.add(PAYLOAD_CLASS_NAME);
    }
  }

  private void reconcilePartialUpdateModeConfig(Map<ConfigProperty, String> tablePropsToAdd,
                                                HoodieTableConfig tableConfig) {
    String payloadClass = tableConfig.getPayloadClass();
    String mergeStrategy = tableConfig.getRecordMergeStrategyId();
    if (!BUILTIN_MERGE_STRATEGIES.contains(mergeStrategy) || StringUtils.isNullOrEmpty(payloadClass)) {
      return;
    }
    if (payloadClass.equals(OverwriteNonDefaultsWithLatestAvroPayload.class.getName())
        || payloadClass.equals(PartialUpdateAvroPayload.class.getName())) {
      tablePropsToAdd.put(PARTIAL_UPDATE_MODE, PartialUpdateMode.IGNORE_DEFAULTS.name());
    } else if (payloadClass.equals(PostgresDebeziumAvroPayload.class.getName())) {
      tablePropsToAdd.put(PARTIAL_UPDATE_MODE, PartialUpdateMode.FILL_UNAVAILABLE.name());
    }
  }

  private void reconcileMergePropertiesConfig(Map<ConfigProperty, String> tablePropsToAdd, HoodieTableConfig tableConfig) {
    String payloadClass = tableConfig.getPayloadClass();
    String mergeStrategy = tableConfig.getRecordMergeStrategyId();
    if (!BUILTIN_MERGE_STRATEGIES.contains(mergeStrategy) || StringUtils.isNullOrEmpty(payloadClass)) {
      return;
    }
    if (payloadClass.equals(AWSDmsAvroPayload.class.getName())) {
      tablePropsToAdd.put(
          ConfigProperty.key(RECORD_MERGE_PROPERTY_PREFIX + DELETE_KEY).noDefaultValue(),
          AWSDmsAvroPayload.OP_FIELD);
      tablePropsToAdd.put(
          ConfigProperty.key(RECORD_MERGE_PROPERTY_PREFIX + DELETE_MARKER).noDefaultValue(),
          AWSDmsAvroPayload.DELETE_OPERATION_VALUE);
    } else if (payloadClass.equals(PostgresDebeziumAvroPayload.class.getName())) {
      tablePropsToAdd.put(
          ConfigProperty.key(RECORD_MERGE_PROPERTY_PREFIX + PARTIAL_UPDATE_UNAVAILABLE_VALUE).noDefaultValue(), // // to be fixed once we land PR #13721.
          DEBEZIUM_UNAVAILABLE_VALUE);
    }
  }

  private void reconcileOrderingFieldsConfig(Map<ConfigProperty, String> tablePropsToAdd,
                                             Set<ConfigProperty> tablePropsToRemove,
                                             HoodieTableConfig tableConfig) {
    String payloadClass = tableConfig.getPayloadClass();
    Option<String> orderingFieldsOpt = MySqlDebeziumAvroPayload.class.getName().equals(payloadClass)
        ? Option.of(FLATTENED_FILE_COL_NAME + "," + FLATTENED_POS_COL_NAME)
        : tableConfig.getOrderingFieldsStr();
    orderingFieldsOpt.ifPresent(orderingFields -> {
      tablePropsToAdd.put(HoodieTableConfig.ORDERING_FIELDS, orderingFields);
      tablePropsToRemove.add(HoodieTableConfig.PRECOMBINE_FIELD);
    });
  }

  /**
   * Populates missing version attributes in index definitions based on table version.
   *
   * @param indexDefOption optional index metadata containing index definitions
   */
  static void populateIndexVersionIfMissing(Option<HoodieIndexMetadata> indexDefOption) {
    indexDefOption.ifPresent(idxDefs ->
        idxDefs.getIndexDefinitions().replaceAll((indexName, idxDef) -> {
          if (idxDef.getVersion() == null) {
            return idxDef.toBuilder().withVersion(HoodieIndexVersion.V1).build();
          } else {
            return idxDef;
          }
        }));
  }
}
