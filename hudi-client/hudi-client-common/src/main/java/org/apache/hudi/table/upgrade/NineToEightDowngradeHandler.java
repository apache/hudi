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
import org.apache.hudi.common.model.AWSDmsAvroPayload;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.EventTimeAvroPayload;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.debezium.DebeziumConstants;
import org.apache.hudi.common.model.debezium.MySqlDebeziumAvroPayload;
import org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpgradeDowngradeException;
import org.apache.hudi.table.HoodieTable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_KEY;
import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_MARKER;
import static org.apache.hudi.common.model.HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID;
import static org.apache.hudi.common.table.HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTIAL_UPDATE_MODE;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTIAL_UPDATE_UNAVAILABLE_VALUE;
import static org.apache.hudi.common.table.HoodieTableConfig.PAYLOAD_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_MODE;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_PROPERTY_PREFIX;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_STRATEGY_ID;
import static org.apache.hudi.keygen.KeyGenUtils.getComplexKeygenErrorMessage;
import static org.apache.hudi.keygen.KeyGenUtils.isComplexKeyGeneratorWithSingleRecordKeyField;
import static org.apache.hudi.table.upgrade.UpgradeDowngradeUtils.PAYLOAD_CLASSES_TO_HANDLE;

/**
 * Version 8 is the placeholder version from 1.0.0 to 1.0.2.
 * Version 9 is the placeholder version >= 1.1.0.
 * The major change introduced in version 9 is two table configurations for payload deprecation.
 * The main downgrade logic:
 *   for all tables:
 *     remove hoodie.table.partial.update.mode from table_configs
 *   for table with payload class defined in RFC-97,
 *     remove hoodie.legacy.payload.class from table_configs
 *     set hoodie.compaction.payload.class=payload
 *     set hoodie.record.merge.mode=CUSTOM
 *     set hoodie.record.merge.strategy.id accordingly
 *   remove any properties with prefix hoodie.record.merge.property.
 *   fix precombine field value for MySqlDebeziumAvroPayload.
 */
public class NineToEightDowngradeHandler implements DowngradeHandler {
  @Override
  public UpgradeDowngrade.TableConfigChangeSet downgrade(HoodieWriteConfig config,
                                                         HoodieEngineContext context,
                                                         String instantTime,
                                                         SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    final HoodieTable table = upgradeDowngradeHelper.getTable(config, context);
    HoodieTableMetaClient metaClient = table.getMetaClient();
    if (config.enableComplexKeygenValidation()
        && isComplexKeyGeneratorWithSingleRecordKeyField(metaClient.getTableConfig())) {
      throw new HoodieUpgradeDowngradeException(getComplexKeygenErrorMessage("downgrade"));
    }
    // Handle index Changes
    UpgradeDowngradeUtils.dropNonV1IndexPartitions(
        config, context, table, upgradeDowngradeHelper, "downgrading from table version nine to eight");
    // Update table properties.
    Set<ConfigProperty> propertiesToRemove = new HashSet<>();
    Map<ConfigProperty, String> propertiesToAdd = new HashMap<>();
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    reconcileMergeConfigs(propertiesToAdd, propertiesToRemove, tableConfig);
    reconcileOrderingFieldsConfig(propertiesToAdd, propertiesToRemove, tableConfig);
    return new UpgradeDowngrade.TableConfigChangeSet(propertiesToAdd, propertiesToRemove);
  }

  private void reconcileMergeConfigs(Map<ConfigProperty, String> propertiesToAdd,
                                     Set<ConfigProperty> propertiesToRemove,
                                     HoodieTableConfig tableConfig) {
    // Update table properties.
    propertiesToRemove.add(PARTIAL_UPDATE_MODE);
    // For specified payload classes, add strategy id and custom merge mode.
    String legacyPayloadClass = tableConfig.getLegacyPayloadClass();
    if (!StringUtils.isNullOrEmpty(legacyPayloadClass) && (PAYLOAD_CLASSES_TO_HANDLE.contains(legacyPayloadClass))) {
      propertiesToRemove.add(LEGACY_PAYLOAD_CLASS_NAME);
      propertiesToAdd.put(PAYLOAD_CLASS_NAME, legacyPayloadClass);
      if (!legacyPayloadClass.equals(OverwriteWithLatestAvroPayload.class.getName())
          && !legacyPayloadClass.equals(DefaultHoodieRecordPayload.class.getName())
          && !legacyPayloadClass.equals(EventTimeAvroPayload.class.getName())) {
        propertiesToAdd.put(RECORD_MERGE_STRATEGY_ID, PAYLOAD_BASED_MERGE_STRATEGY_UUID);
        propertiesToAdd.put(RECORD_MERGE_MODE, RecordMergeMode.CUSTOM.name());
      }
      if (legacyPayloadClass.equals(AWSDmsAvroPayload.class.getName())) {
        propertiesToRemove.add(
            ConfigProperty.key(RECORD_MERGE_PROPERTY_PREFIX + DELETE_KEY).noDefaultValue());
        propertiesToRemove.add(
            ConfigProperty.key(RECORD_MERGE_PROPERTY_PREFIX + DELETE_MARKER).noDefaultValue());
      }
      if (legacyPayloadClass.equals(PostgresDebeziumAvroPayload.class.getName())) {
        propertiesToRemove.add(
            ConfigProperty.key(RECORD_MERGE_PROPERTY_PREFIX + PARTIAL_UPDATE_UNAVAILABLE_VALUE).noDefaultValue());
      }
    }
  }

  private void reconcileOrderingFieldsConfig(Map<ConfigProperty, String> propertiesToAdd,
                                             Set<ConfigProperty> propertiesToRemove,
                                             HoodieTableConfig tableConfig) {
    Option<String> orderingFieldsOpt = MySqlDebeziumAvroPayload.class.getName().equals(tableConfig.getLegacyPayloadClass())
        ? Option.of(DebeziumConstants.ADDED_SEQ_COL_NAME)
        : tableConfig.getOrderingFieldsStr();
    orderingFieldsOpt.ifPresent(orderingFields -> {
      propertiesToAdd.put(HoodieTableConfig.PRECOMBINE_FIELD, orderingFields);
      propertiesToRemove.add(HoodieTableConfig.ORDERING_FIELDS);
    });
  }
}
