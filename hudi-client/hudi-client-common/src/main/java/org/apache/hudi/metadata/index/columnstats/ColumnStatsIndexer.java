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

package org.apache.hudi.metadata.index.columnstats;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.metadata.model.FileInfo;
import org.apache.hudi.metadata.model.FileSliceAndPartition;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.metadata.index.model.IndexPartitionInitialization;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.BaseIndexer;
import org.apache.hudi.util.Lazy;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.index.HoodieIndexUtils.register;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.existingIndexVersionOrDefault;
import static org.apache.hudi.metadata.MetadataPartitionType.COLUMN_STATS;

/**
 * Implementation of {@link MetadataPartitionType#COLUMN_STATS} metadata
 */
@Slf4j
public class ColumnStatsIndexer extends BaseIndexer {
  private final Lazy<List<String>> columnsToIndex;

  public ColumnStatsIndexer(HoodieEngineContext engineContext,
                               HoodieWriteConfig dataTableWriteConfig,
                               HoodieTableMetaClient dataTableMetaClient) {
    super(engineContext, dataTableWriteConfig, dataTableMetaClient);

    this.columnsToIndex = Lazy.lazily(() ->
        new ArrayList<>(HoodieTableMetadataUtil.getColumnsToIndex(
            dataTableMetaClient.getTableConfig(),
            dataTableWriteConfig.getMetadataConfig(),
            Lazy.lazily(() -> HoodieTableMetadataUtil.tryResolveSchemaForTable(dataTableMetaClient)),
            true,
            Option.of(dataTableWriteConfig.getRecordMerger().getRecordType()),
            existingIndexVersionOrDefault(PARTITION_NAME_COLUMN_STATS, dataTableMetaClient)).keySet()));
  }

  @Override
  public List<IndexPartitionInitialization> buildInitialization(
      String dataTableInstantTime,
      String instantTimeForPartition,
      Map<String, List<FileInfo>> partitionToAllFilesMap,
      Lazy<List<FileSliceAndPartition>> lazyPartitionFileSlices) throws IOException {
    final int fileGroupCount = dataTableWriteConfig.getMetadataConfig().getColumnStatsIndexFileGroupCount();
    // columnsToIndex can be empty if meta fields are disabled and cols to index is not explicitly overridden.
    if (partitionToAllFilesMap.isEmpty() || columnsToIndex.get().isEmpty()) {
      return Collections.singletonList(IndexPartitionInitialization.of(fileGroupCount, COLUMN_STATS.getPartitionPath(), engineContext.emptyHoodieData()));
    }

    log.info("Indexing {} columns for column stats index", columnsToIndex.get().size());
    // during initialization, we need stats for base and log files.
    HoodieData<HoodieRecord> records = HoodieTableMetadataUtil.convertFilesToColumnStatsRecords(
        engineContext, Collections.emptyMap(), partitionToAllFilesMap,
        dataTableMetaClient, dataTableWriteConfig.getColumnStatsIndexParallelism(),
        dataTableWriteConfig.getMetadataConfig().getMaxReaderBufferSize(),
        columnsToIndex.get());
    return Collections.singletonList(IndexPartitionInitialization.of(fileGroupCount, COLUMN_STATS.getPartitionPath(), records));
  }

  @Override
  public void postInitialization(HoodieTableMetaClient metadataMetaClient, HoodieData<HoodieRecord> records, int fileGroupCount, String relativePartitionPath) {
    List<String> indexColumns = records.isEmpty() ? Collections.emptyList() : columnsToIndex.get();
    HoodieIndexDefinition indexDefinition = HoodieIndexDefinition.newBuilder()
        .withIndexName(PARTITION_NAME_COLUMN_STATS)
        .withIndexType(PARTITION_NAME_COLUMN_STATS)
        .withIndexFunction(PARTITION_NAME_COLUMN_STATS)
        .withSourceFields(indexColumns)
        // Use the existing version if exists, otherwise fall back to the default version.
        .withVersion(existingIndexVersionOrDefault(PARTITION_NAME_COLUMN_STATS, dataTableMetaClient))
        .withIndexOptions(Collections.emptyMap())
        .build();
    log.info("Registering or updating index: {} of type: {}", indexDefinition.getIndexName(), indexDefinition.getIndexType());
    register(dataTableMetaClient, indexDefinition);

    super.postInitialization(metadataMetaClient, records, fileGroupCount, relativePartitionPath);
  }
}
