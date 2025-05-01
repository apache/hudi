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
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.index.Indexer;
import org.apache.hudi.util.Lazy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.metadata.HoodieMetadataWriteUtils.getPartitionFileSlicePairs;
import static org.apache.hudi.metadata.MetadataPartitionType.PARTITION_STATS;

public class PartitionStatsIndexer implements Indexer {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionStatsIndexer.class);
  private final HoodieEngineContext engineContext;
  private final HoodieWriteConfig dataTableWriteConfig;
  private final HoodieTableMetaClient dataTableMetaClient;

  public PartitionStatsIndexer(HoodieEngineContext engineContext,
                               HoodieWriteConfig dataTableWriteConfig,
                               HoodieTableMetaClient dataTableMetaClient) {
    this.engineContext = engineContext;
    this.dataTableWriteConfig = dataTableWriteConfig;
    this.dataTableMetaClient = dataTableMetaClient;
  }

  @Override
  public List<InitialIndexPartitionData> initialize(
      List<HoodieTableMetadataUtil.DirectoryInfo> partitionInfoList,
      Map<String, Map<String, Long>> partitionToFilesMap,
      String createInstantTime,
      Lazy<HoodieTableFileSystemView> fsView,
      HoodieBackedTableMetadata metadata,
      String instantTimeForPartition) throws IOException {
    // For PARTITION_STATS, COLUMN_STATS should also be enabled
    if (!dataTableWriteConfig.isMetadataColumnStatsIndexEnabled()) {
      LOG.warn("Skipping partition stats initialization as column stats index is not enabled. Please enable {}",
          HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key());
      return Collections.emptyList();
    }
    HoodieData<HoodieRecord> records =
        HoodieTableMetadataUtil.convertFilesToPartitionStatsRecords(engineContext,
            getPartitionFileSlicePairs(dataTableMetaClient, metadata, fsView.get()),
            dataTableWriteConfig.getMetadataConfig(),
            dataTableMetaClient, Option.empty(), Option.of(dataTableWriteConfig.getRecordMerger().getRecordType()));
    final int numFileGroup = dataTableWriteConfig.getMetadataConfig().getPartitionStatsIndexFileGroupCount();
    return Collections.singletonList(InitialIndexPartitionData.of(
        numFileGroup, PARTITION_STATS.getPartitionPath(), records));
  }
}
