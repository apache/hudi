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

package org.apache.hudi.metadata.index.partitionstats;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.BaseIndexer;
import org.apache.hudi.metadata.index.IndexInitializationContext;
import org.apache.hudi.metadata.index.model.IndexInitializationPlan;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.hudi.metadata.MetadataPartitionType.PARTITION_STATS;

/**
 * Implementation of {@link MetadataPartitionType#PARTITION_STATS} metadata
 */
@Slf4j
public class PartitionStatsIndexer extends BaseIndexer {
  public PartitionStatsIndexer(HoodieEngineContext engineContext, HoodieWriteConfig dataTableWriteConfig,
                                  HoodieTableMetaClient dataTableMetaClient) {
    super(engineContext, dataTableWriteConfig, dataTableMetaClient);
  }

  @Override
  public List<IndexInitializationPlan> buildInitialization(IndexInitializationContext context) throws IOException {
    // Partition stats index cannot be enabled for a non-partitioned table
    if (!dataTableMetaClient.getTableConfig().isTablePartitioned()) {
      return Collections.emptyList();
    }

    // For PARTITION_STATS, COLUMN_STATS should also be enabled
    if (!dataTableWriteConfig.isMetadataColumnStatsIndexEnabled()) {
      log.debug("Skipping partition stats initialization as column stats index is not enabled. Please enable {}",
          HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key());
      return Collections.emptyList();
    }

    HoodieData<HoodieRecord> records = HoodieTableMetadataUtil.convertFilesToPartitionStatsRecords(
        engineContext, context.latestFileSlices().get(), dataTableWriteConfig.getMetadataConfig(),
        dataTableMetaClient, context.tableSchema(), Option.of(dataTableWriteConfig.getRecordMerger().getRecordType()));
    final int fileGroupCount = dataTableWriteConfig.getMetadataConfig().getPartitionStatsIndexFileGroupCount();

    return Collections.singletonList(IndexInitializationPlan.of(fileGroupCount, PARTITION_STATS.getPartitionPath(), records));
  }
}
