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
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.index.EngineIndexHelper;
import org.apache.hudi.metadata.index.IndexBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.metadata.HoodieMetadataWriteUtils.getPartitionFileSlicePairs;

public class PartitionStatsIndexBuilder implements IndexBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionStatsIndexBuilder.class);
  private final HoodieEngineContext engineContext;
  private final HoodieWriteConfig dataTableWriteConfig;
  private final HoodieTableMetaClient dataTableMetaClient;
  private final EngineIndexHelper indexHelper;

  public PartitionStatsIndexBuilder(HoodieEngineContext engineContext,
                                    HoodieWriteConfig dataTableWriteConfig,
                                    HoodieTableMetaClient dataTableMetaClient,
                                    EngineIndexHelper indexHelper) {
    this.engineContext = engineContext;
    this.dataTableWriteConfig = dataTableWriteConfig;
    this.dataTableMetaClient = dataTableMetaClient;
    this.indexHelper = indexHelper;
  }

  @Override
  public Pair<Integer, HoodieData<HoodieRecord>> createRecordsFromExistingFiles(
      List<HoodieTableMetadataUtil.DirectoryInfo> partitionInfoList,
      Map<String, Map<String, Long>> partitionToFilesMap,
      String createInstantTime,
      HoodieTableFileSystemView fsView,
      HoodieBackedTableMetadata metadata,
      String instantTimeForPartition) throws IOException {
    // For PARTITION_STATS, COLUMN_STATS should also be enabled
    if (!dataTableWriteConfig.isMetadataColumnStatsIndexEnabled()) {
      LOG.warn("Skipping partition stats initialization as column stats index is not enabled. Please enable {}",
          HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key());
      // TODO(yihua): avoid null and use a different way to indicate skipping
      return Pair.of(-1, null);
    }
    HoodieData<HoodieRecord> records =
        HoodieTableMetadataUtil.convertFilesToPartitionStatsRecords(engineContext,
            getPartitionFileSlicePairs(dataTableMetaClient, metadata, fsView),
            dataTableWriteConfig.getMetadataConfig(),
            dataTableMetaClient, Option.empty(), Option.of(dataTableWriteConfig.getRecordMerger().getRecordType()));
    final int fileGroupCount = dataTableWriteConfig.getMetadataConfig().getPartitionStatsIndexFileGroupCount();
    return Pair.of(fileGroupCount, records);
  }
}
