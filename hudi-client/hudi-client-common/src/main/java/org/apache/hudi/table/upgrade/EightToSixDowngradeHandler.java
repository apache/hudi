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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BootstrapIndexType;
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
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.HoodieTableConfig.TABLE_METADATA_PARTITIONS;
import static org.apache.hudi.metadata.MetadataPartitionType.BLOOM_FILTERS;
import static org.apache.hudi.metadata.MetadataPartitionType.COLUMN_STATS;
import static org.apache.hudi.metadata.MetadataPartitionType.FILES;
import static org.apache.hudi.metadata.MetadataPartitionType.RECORD_INDEX;
import static org.apache.hudi.table.upgrade.UpgradeDowngradeUtils.downgradeActiveTimelineInstant;
import static org.apache.hudi.table.upgrade.UpgradeDowngradeUtils.downgradeFromLSMTimeline;
import static org.apache.hudi.table.upgrade.UpgradeDowngradeUtils.rollbackFailedWritesAndCompact;

/**
 * Version 6 is the table version for release 0.x (0.14.0 onwards).
 * Version 8 is the placeholder version to track 1.x.
 */
public class EightToSixDowngradeHandler implements DowngradeHandler {

  private static final Set<String> SUPPORTED_METADATA_PARTITION_PATHS = getSupportedMetadataPartitionPaths();

  @Override
  public Map<ConfigProperty, String> downgrade(HoodieWriteConfig config, HoodieEngineContext context, String instantTime, SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    final HoodieTable table = upgradeDowngradeHelper.getTable(config, context);
    Map<ConfigProperty, String> tablePropsToAdd = new HashMap<>();
    // Rollback and run compaction in one step
    rollbackFailedWritesAndCompact(table, context, config, upgradeDowngradeHelper, true);

    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(context.getStorageConf().newInstance()).setBasePath(config.getBasePath()).build();
    // Handle timeline downgrade:
    //  - rename instants in active timeline to old format for table version 6
    //  - convert LSM timeline format to archived timeline for table version 6
    List<HoodieInstant> instants = metaClient.getActiveTimeline().getInstants();
    if (!instants.isEmpty()) {
      InstantFileNameGenerator instantFileNameGenerator = metaClient.getInstantFileNameGenerator();
      CommitMetadataSerDeV2 commitMetadataSerDeV2 = new CommitMetadataSerDeV2();
      CommitMetadataSerDeV1 commitMetadataSerDeV1 = new CommitMetadataSerDeV1();
      ActiveTimelineV1 activeTimelineV1 = new ActiveTimelineV1(metaClient);
      context.map(instants, instant -> {
        String fileName = instantFileNameGenerator.getFileName(instant);
        return downgradeActiveTimelineInstant(instant, metaClient, fileName, commitMetadataSerDeV2, commitMetadataSerDeV1, activeTimelineV1);
      }, instants.size());
    }
    downgradeFromLSMTimeline(table, context, config);

    // downgrade table properties
    downgradePartitionFields(config, context, upgradeDowngradeHelper, tablePropsToAdd);
    unsetInitialVersion(config, metaClient.getTableConfig(), tablePropsToAdd);
    unsetRecordMergeMode(config, metaClient.getTableConfig(), tablePropsToAdd);
    downgradeKeyGeneratorType(config, metaClient.getTableConfig(), tablePropsToAdd);
    downgradeBootstrapIndexType(config, metaClient.getTableConfig(), tablePropsToAdd);
    // Prepare parameters.
    if (metaClient.getTableConfig().isMetadataTableAvailable()) {
      // Delete unsupported metadata partitions in table version 7.
      downgradeMetadataPartitions(context, metaClient.getStorage(), metaClient, tablePropsToAdd);
      UpgradeDowngradeUtils.updateMetadataTableVersion(context, HoodieTableVersion.SIX, metaClient);
    }
    return tablePropsToAdd;
  }

  static void downgradePartitionFields(HoodieWriteConfig config,
                                       HoodieEngineContext context,
                                       SupportsUpgradeDowngrade upgradeDowngradeHelper,
                                       Map<ConfigProperty, String> tablePropsToAdd) {
    HoodieTableConfig tableConfig = upgradeDowngradeHelper.getTable(config, context).getMetaClient().getTableConfig();
    String keyGenerator = tableConfig.getKeyGeneratorClassName();
    String partitionPathField = config.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key());
    if (keyGenerator != null && partitionPathField != null
        && (keyGenerator.equals(KeyGeneratorType.CUSTOM.getClassName()) || keyGenerator.equals(KeyGeneratorType.CUSTOM_AVRO.getClassName()))) {
      tablePropsToAdd.put(HoodieTableConfig.PARTITION_FIELDS, tableConfig.getPartitionFieldProp());
    }
  }

  static void unsetInitialVersion(HoodieWriteConfig config, HoodieTableConfig tableConfig, Map<ConfigProperty, String> tablePropsToAdd) {
    tableConfig.getProps().remove(HoodieTableConfig.INITIAL_VERSION.key());
  }

  static void unsetRecordMergeMode(HoodieWriteConfig config, HoodieTableConfig tableConfig, Map<ConfigProperty, String> tablePropsToAdd) {
    Triple<RecordMergeMode, String, String> mergingConfigs =
        HoodieTableConfig.inferCorrectMergingBehavior(config.getRecordMergeMode(), tableConfig.getPayloadClass(), tableConfig.getRecordMergeStrategyId());
    if (StringUtils.nonEmpty(mergingConfigs.getMiddle())) {
      tablePropsToAdd.put(HoodieTableConfig.PAYLOAD_CLASS_NAME, mergingConfigs.getMiddle());
    }
    if (StringUtils.nonEmpty(mergingConfigs.getRight())) {
      tablePropsToAdd.put(HoodieTableConfig.RECORD_MERGE_STRATEGY_ID, mergingConfigs.getRight());
    }
    tableConfig.getProps().remove(HoodieTableConfig.RECORD_MERGE_MODE.key());
  }

  static void downgradeBootstrapIndexType(HoodieWriteConfig config,
                                          HoodieTableConfig tableConfig,
                                          Map<ConfigProperty, String> tablePropsToAdd) {
    String bootstrapIndexClassName = BootstrapIndexType.getBootstrapIndexClassName(config);
    if (StringUtils.nonEmpty(bootstrapIndexClassName)) {
      tablePropsToAdd.put(HoodieTableConfig.BOOTSTRAP_INDEX_CLASS_NAME, bootstrapIndexClassName);
    }
    tableConfig.getProps().remove(HoodieTableConfig.BOOTSTRAP_INDEX_TYPE.key());
  }

  static void downgradeKeyGeneratorType(HoodieWriteConfig config,
                                        HoodieTableConfig tableConfig,
                                        Map<ConfigProperty, String> tablePropsToAdd) {
    String keyGenerator = KeyGeneratorType.getKeyGeneratorClassName(config);
    if (StringUtils.nonEmpty(keyGenerator)) {
      keyGenerator = KeyGeneratorType.getKeyGeneratorClassName(config);
      tablePropsToAdd.put(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME, keyGenerator);
    }
    tableConfig.getProps().remove(HoodieTableConfig.KEY_GENERATOR_TYPE.key());
  }

  static void downgradeMetadataPartitions(HoodieEngineContext context,
                                          HoodieStorage hoodieStorage,
                                          HoodieTableMetaClient metaClient,
                                          Map<ConfigProperty, String> tablePropsToAdd) {
    // Get base path for metadata table.
    StoragePath metadataTableBasePath =
        HoodieTableMetadata.getMetadataTableBasePath(metaClient.getBasePath());

    // Fetch metadata partition paths.
    List<String> metadataPartitions = FSUtils.getAllPartitionPaths(context,
        hoodieStorage,
        metadataTableBasePath,
        false);

    // Delete partitions.
    List<String> validPartitionPaths = deleteMetadataPartition(context, metaClient, metadataPartitions);

    // Clean the configuration.
    tablePropsToAdd.put(TABLE_METADATA_PARTITIONS, String.join(",", validPartitionPaths));
  }

  static List<String> deleteMetadataPartition(HoodieEngineContext context,
                                              HoodieTableMetaClient metaClient,
                                              List<String> metadataPartitions) {
    metadataPartitions.stream()
        .filter(metadataPath -> !SUPPORTED_METADATA_PARTITION_PATHS.contains(metadataPath))
        .forEach(metadataPath ->
            HoodieTableMetadataUtil.deleteMetadataTablePartition(
                metaClient, context, metadataPath, true)
        );

    return metadataPartitions.stream()
        .filter(SUPPORTED_METADATA_PARTITION_PATHS::contains)
        .collect(Collectors.toList());
  }

  private static Set<String> getSupportedMetadataPartitionPaths() {
    Set<String> supportedPartitionPaths = new HashSet<>();
    supportedPartitionPaths.add(BLOOM_FILTERS.getPartitionPath());
    supportedPartitionPaths.add(COLUMN_STATS.getPartitionPath());
    supportedPartitionPaths.add(FILES.getPartitionPath());
    supportedPartitionPaths.add(RECORD_INDEX.getPartitionPath());
    return supportedPartitionPaths;
  }
}
