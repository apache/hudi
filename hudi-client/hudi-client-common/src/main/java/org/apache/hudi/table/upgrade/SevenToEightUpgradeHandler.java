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
import org.apache.hudi.common.model.BootstrapIndexType;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.InstantFileNameGenerator;
import org.apache.hudi.common.table.timeline.versioning.v1.ActiveTimelineV1;
import org.apache.hudi.common.table.timeline.versioning.v1.CommitMetadataSerDeV1;
import org.apache.hudi.common.table.timeline.versioning.v2.CommitMetadataSerDeV2;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.table.HoodieTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.table.upgrade.UpgradeDowngradeUtils.rollbackFailedWritesAndCompact;
import static org.apache.hudi.table.upgrade.UpgradeDowngradeUtils.upgradeActiveTimelineInstant;
import static org.apache.hudi.table.upgrade.UpgradeDowngradeUtils.upgradeToLSMTimeline;

/**
 * Version 6 is the table version for release 0.x (0.14.0 onwards).
 * Version 8 is the placeholder version to track 1.x.
 */
public class SevenToEightUpgradeHandler implements UpgradeHandler {

  @Override
  public Map<ConfigProperty, String> upgrade(HoodieWriteConfig config, HoodieEngineContext context,
                                             String instantTime, SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    config.setValue(HoodieWriteConfig.WRITE_TABLE_VERSION, String.valueOf(HoodieTableVersion.SIX.versionCode()));
    HoodieTable table = upgradeDowngradeHelper.getTable(config, context);
    HoodieTableMetaClient metaClient = table.getMetaClient();
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    // Rollback and run compaction in one step
    rollbackFailedWritesAndCompact(table, context, config, upgradeDowngradeHelper, HoodieTableType.MERGE_ON_READ.equals(table.getMetaClient().getTableType()));

    // handle table properties upgrade
    Map<ConfigProperty, String> tablePropsToAdd = new HashMap<>();
    upgradePartitionFields(config, tableConfig, tablePropsToAdd);
    setInitialVersion(config, tableConfig, tablePropsToAdd);
    setRecordMergeMode(config, tableConfig, tablePropsToAdd);
    upgradeKeyGeneratorType(config, tableConfig, tablePropsToAdd);
    upgradeBootstrapIndexType(config, tableConfig, tablePropsToAdd);

    // Handle timeline upgrade:
    //  - rename instants in active timeline to new format
    //  - convert archived timeline to new LSM timeline format
    List<HoodieInstant> instants = metaClient.getActiveTimeline().getInstants();
    if (!instants.isEmpty()) {
      InstantFileNameGenerator instantFileNameGenerator = metaClient.getInstantFileNameGenerator();
      CommitMetadataSerDeV2 commitMetadataSerDeV2 = new CommitMetadataSerDeV2();
      CommitMetadataSerDeV1 commitMetadataSerDeV1 = new CommitMetadataSerDeV1();
      ActiveTimelineV1 activeTimelineV1 = new ActiveTimelineV1(metaClient);
      context.map(instants, instant -> {
        String fileName = instantFileNameGenerator.getFileName(instant);
        return upgradeActiveTimelineInstant(instant, metaClient, fileName, commitMetadataSerDeV2, commitMetadataSerDeV1, activeTimelineV1);
      }, instants.size());
    }
    upgradeToLSMTimeline(table, context, config);

    return tablePropsToAdd;
  }

  static void upgradePartitionFields(HoodieWriteConfig config, HoodieTableConfig tableConfig, Map<ConfigProperty, String> tablePropsToAdd) {
    String keyGenerator = tableConfig.getKeyGeneratorClassName();
    String partitionPathField = config.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key());
    if (keyGenerator != null && partitionPathField != null
        && (keyGenerator.equals(KeyGeneratorType.CUSTOM.getClassName()) || keyGenerator.equals(KeyGeneratorType.CUSTOM_AVRO.getClassName()))) {
      tablePropsToAdd.put(HoodieTableConfig.PARTITION_FIELDS, partitionPathField);
    }
  }

  static void setInitialVersion(HoodieWriteConfig config, HoodieTableConfig tableConfig, Map<ConfigProperty, String> tablePropsToAdd) {
    tablePropsToAdd.put(HoodieTableConfig.INITIAL_VERSION, HoodieTableVersion.SIX.name());
  }

  static void setRecordMergeMode(HoodieWriteConfig config, HoodieTableConfig tableConfig, Map<ConfigProperty, String> tablePropsToAdd) {
    Triple<RecordMergeMode, String, String> mergingConfigs =
        HoodieTableConfig.inferCorrectMergingBehavior(config.getRecordMergeMode(), tableConfig.getPayloadClass(), tableConfig.getRecordMergeStrategyId());
    tablePropsToAdd.put(HoodieTableConfig.RECORD_MERGE_MODE, mergingConfigs.getLeft().name());
    if (StringUtils.nonEmpty(mergingConfigs.getMiddle())) {
      tablePropsToAdd.put(HoodieTableConfig.PAYLOAD_CLASS_NAME, mergingConfigs.getMiddle());
    }
    if (StringUtils.nonEmpty(mergingConfigs.getRight())) {
      tablePropsToAdd.put(HoodieTableConfig.RECORD_MERGE_STRATEGY_ID, mergingConfigs.getRight());
    }
  }

  static void upgradeBootstrapIndexType(HoodieWriteConfig config, HoodieTableConfig tableConfig, Map<ConfigProperty, String> tablePropsToAdd) {
    String bootstrapIndexClass = BootstrapIndexType.getBootstrapIndexClassName(tableConfig);
    if (StringUtils.nonEmpty(bootstrapIndexClass)) {
      tablePropsToAdd.put(HoodieTableConfig.BOOTSTRAP_INDEX_CLASS_NAME, bootstrapIndexClass);
      tablePropsToAdd.put(HoodieTableConfig.BOOTSTRAP_INDEX_TYPE, BootstrapIndexType.fromClassName(bootstrapIndexClass).name());
    }
  }

  static void upgradeKeyGeneratorType(HoodieWriteConfig config, HoodieTableConfig tableConfig, Map<ConfigProperty, String> tablePropsToAdd) {
    String keyGenerator = tableConfig.getKeyGeneratorClassName();
    if (StringUtils.nonEmpty(keyGenerator)) {
      tablePropsToAdd.put(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME, keyGenerator);
      tablePropsToAdd.put(HoodieTableConfig.KEY_GENERATOR_TYPE, KeyGeneratorType.fromClassName(keyGenerator).name());
    }
  }
}
