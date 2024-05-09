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

import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.compact.CompactionTriggerStrategy;
import org.apache.hudi.table.action.compact.strategy.UnBoundedCompactionStrategy;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.table.HoodieTableConfig.TABLE_METADATA_PARTITIONS;
import static org.apache.hudi.common.table.HoodieTableConfig.TABLE_METADATA_PARTITIONS_INFLIGHT;

/**
 * Downgrade handle to assist in downgrading hoodie table from version 6 to 5.
 * To ensure compatibility, we need recreate the compaction requested file to
 * .aux folder.
 * Since version 6 includes a new schema field for metadata table(MDT),
 * the MDT needs to be deleted during downgrade to avoid column drop error.
 * Also log block version was upgraded in version 6, therefore full compaction needs
 * to be completed during downgrade to avoid both read and future compaction failures.
 */
public class SixToFiveDowngradeHandler implements DowngradeHandler {

  @Override
  public Map<ConfigProperty, String> downgrade(HoodieWriteConfig config, HoodieEngineContext context, String instantTime, SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    final HoodieTable table = upgradeDowngradeHelper.getTable(config, context);

    // Since version 6 includes a new schema field for metadata table(MDT), the MDT needs to be deleted during downgrade to avoid column drop error.
    HoodieTableMetadataUtil.deleteMetadataTable(config.getBasePath(), context);
    // The log block version has been upgraded in version six so compaction is required for downgrade.
    runCompaction(table, context, config, upgradeDowngradeHelper);

    syncCompactionRequestedFileToAuxiliaryFolder(table);

    HoodieTableMetaClient metaClient = HoodieTableMetaClient.reload(table.getMetaClient());
    Map<ConfigProperty, String> updatedTableProps = new HashMap<>();
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    Option.ofNullable(tableConfig.getString(TABLE_METADATA_PARTITIONS))
        .ifPresent(v -> updatedTableProps.put(TABLE_METADATA_PARTITIONS, v));
    Option.ofNullable(tableConfig.getString(TABLE_METADATA_PARTITIONS_INFLIGHT))
        .ifPresent(v -> updatedTableProps.put(TABLE_METADATA_PARTITIONS_INFLIGHT, v));
    return updatedTableProps;
  }

  /**
   * Utility method to run compaction for MOR table as part of downgrade step.
   */
  private void runCompaction(HoodieTable table, HoodieEngineContext context, HoodieWriteConfig config,
                             SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    try {
      if (table.getMetaClient().getTableType() == HoodieTableType.MERGE_ON_READ) {
        // set required configs for scheduling compaction.
        HoodieInstantTimeGenerator.setCommitTimeZone(table.getMetaClient().getTableConfig().getTimelineTimezone());
        HoodieWriteConfig compactionConfig = HoodieWriteConfig.newBuilder().withProps(config.getProps()).build();
        compactionConfig.setValue(HoodieCompactionConfig.INLINE_COMPACT.key(), "true");
        compactionConfig.setValue(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "1");
        compactionConfig.setValue(HoodieCompactionConfig.INLINE_COMPACT_TRIGGER_STRATEGY.key(), CompactionTriggerStrategy.NUM_COMMITS.name());
        compactionConfig.setValue(HoodieCompactionConfig.COMPACTION_STRATEGY.key(), UnBoundedCompactionStrategy.class.getName());
        compactionConfig.setValue(HoodieMetadataConfig.ENABLE.key(), "false");
        try (BaseHoodieWriteClient writeClient = upgradeDowngradeHelper.getWriteClient(compactionConfig, context)) {
          Option<String> compactionInstantOpt = writeClient.scheduleCompaction(Option.empty());
          if (compactionInstantOpt.isPresent()) {
            writeClient.compact(compactionInstantOpt.get());
          }
        }
      }
    } catch (Exception e) {
      throw new HoodieException(e);
    }
  }

  /**
   * See HUDI-6040.
   */
  private static void syncCompactionRequestedFileToAuxiliaryFolder(HoodieTable table) {
    HoodieTableMetaClient metaClient = table.getMetaClient();
    HoodieTimeline compactionTimeline = new HoodieActiveTimeline(metaClient, false).filterPendingCompactionTimeline()
        .filter(instant -> instant.getState() == HoodieInstant.State.REQUESTED);
    compactionTimeline.getInstantsAsStream().forEach(instant -> {
      String fileName = instant.getFileName();
      FileIOUtils.copy(metaClient.getStorage(),
          new StoragePath(metaClient.getMetaPath(), fileName),
          new StoragePath(metaClient.getMetaAuxiliaryPath(), fileName));
    });
  }
}
