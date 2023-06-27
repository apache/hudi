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
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.fs.Path;

import java.util.Collections;
import java.util.Map;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.deleteMetadataTablePartition;
import static org.apache.hudi.table.upgrade.FiveToSixUpgradeHandler.checkUncompletedInstants;

/**
 * Downgrade handle to assist in downgrading hoodie table from version 6 to 5.
 * To ensure compatibility, we need recreate the compaction requested file to
 * .aux folder.
 */
public class SixToFiveDowngradeHandler implements DowngradeHandler {

  @Override
  public Map<ConfigProperty, String> downgrade(HoodieWriteConfig config, HoodieEngineContext context, String instantTime, SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    final HoodieTable table = upgradeDowngradeHelper.getTable(config, context);

    checkUncompletedInstants(table);
    removeRecordIndexIfNeeded(table, context);
    syncCompactionRequestedFileToAuxiliaryFolder(table);

    return Collections.emptyMap();
  }

  /**
   * Record-level index, a new partition in metadata table, was first added in
   * 0.14.0 ({@link HoodieTableVersion#SIX}. Any downgrade from this version
   * should remove this partition.
   */
  private static void removeRecordIndexIfNeeded(HoodieTable table, HoodieEngineContext context) {
    HoodieTableMetaClient metaClient = table.getMetaClient();
    deleteMetadataTablePartition(metaClient, context, MetadataPartitionType.RECORD_INDEX, false);
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
      FileIOUtils.copy(metaClient.getFs(),
          new Path(metaClient.getMetaPath(), fileName),
          new Path(metaClient.getMetaAuxiliaryPath(), fileName));
    });
  }
}
