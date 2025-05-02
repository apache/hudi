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
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.collection.Tuple3;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.Indexer;
import org.apache.hudi.util.Lazy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.index.HoodieIndexUtils.register;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getColumnStatsRecords;
import static org.apache.hudi.metadata.MetadataPartitionType.COLUMN_STATS;

/**
 * Implementation of {@link MetadataPartitionType#COLUMN_STATS} metadata
 */
public class ColumnStatsIndexer implements Indexer {

  private static final Logger LOG = LoggerFactory.getLogger(ColumnStatsIndexer.class);
  private final HoodieEngineContext engineContext;
  private final HoodieWriteConfig dataTableWriteConfig;
  private final HoodieTableMetaClient dataTableMetaClient;
  private final Lazy<List<String>> columnsToIndex;

  public ColumnStatsIndexer(HoodieEngineContext engineContext,
                            HoodieWriteConfig dataTableWriteConfig,
                            HoodieTableMetaClient dataTableMetaClient) {
    this.engineContext = engineContext;
    this.dataTableWriteConfig = dataTableWriteConfig;
    this.dataTableMetaClient = dataTableMetaClient;
    this.columnsToIndex = Lazy.lazily(() ->
        new ArrayList<>(HoodieTableMetadataUtil.getColumnsToIndex(dataTableMetaClient.getTableConfig(),
            dataTableWriteConfig.getMetadataConfig(),
            Lazy.lazily(() -> HoodieTableMetadataUtil.tryResolveSchemaForTable(dataTableMetaClient)),
            true,
            Option.of(dataTableWriteConfig.getRecordMerger().getRecordType())).keySet()));
  }

  @Override
  public List<InitialIndexPartitionData> initialize(
      String dataTableInstantTime,
      Map<String, Map<String, Long>> partitionIdToAllFilesMap,
      Lazy<List<Pair<String, FileSlice>>> lazyLatestMergedPartitionFileSliceList) throws IOException {

    final int numFileGroup = dataTableWriteConfig.getMetadataConfig().getColumnStatsIndexFileGroupCount();
    if (partitionIdToAllFilesMap.isEmpty()) {
      return Collections.singletonList(InitialIndexPartitionData.of(
          numFileGroup, COLUMN_STATS.getPartitionPath(), engineContext.emptyHoodieData()));
    }

    if (columnsToIndex.get().isEmpty()) {
      // this can only happen if meta fields are disabled and cols to index is not explicitly overridden.
      return Collections.singletonList(InitialIndexPartitionData.of(
          numFileGroup, COLUMN_STATS.getPartitionPath(), engineContext.emptyHoodieData()));
    }

    LOG.info("Indexing {} columns for column stats index", columnsToIndex.get().size());

    // during initialization, we need stats for base and log files.
    int maxReaderBufferSize = dataTableWriteConfig.getMetadataConfig().getMaxReaderBufferSize();
    // Create the tuple (partition, filename, isDeleted) to handle both deletes and appends
    final List<Tuple3<String, String, Boolean>> partitionFileFlagTupleList =
        Indexer.fetchPartitionFileInfoTriplets(partitionIdToAllFilesMap);

    // Create records MDT
    int parallelism = Math.max(Math.min(partitionFileFlagTupleList.size(),
        dataTableWriteConfig.getColumnStatsIndexParallelism()), 1);
    List<String> columnListToIndex = columnsToIndex.get();
    // This meta client object has to be local to allow Spark to serialize the object
    // instead of the whole indexer object
    HoodieTableMetaClient metaClient = dataTableMetaClient;
    HoodieData<HoodieRecord> records = engineContext.parallelize(partitionFileFlagTupleList, parallelism)
        .flatMap(partitionFileFlagTuple -> {
          final String partitionPath = partitionFileFlagTuple.f0;
          final String filename = partitionFileFlagTuple.f1;
          final boolean isDeleted = partitionFileFlagTuple.f2;
          return getColumnStatsRecords(partitionPath, filename, metaClient, columnListToIndex,
              isDeleted, maxReaderBufferSize).iterator();
        });
    return Collections.singletonList(InitialIndexPartitionData.of(
        numFileGroup, COLUMN_STATS.getPartitionPath(), records));
  }

  @Override
  public void updateTableConfig() {
    HoodieIndexDefinition indexDefinition = HoodieIndexDefinition.newBuilder()
        .withIndexName(PARTITION_NAME_COLUMN_STATS)
        .withIndexType(PARTITION_NAME_COLUMN_STATS)
        .withIndexFunction(PARTITION_NAME_COLUMN_STATS)
        .withSourceFields(columnsToIndex.get())
        .withIndexOptions(Collections.EMPTY_MAP)
        .build();
    LOG.info("Registering Or Updating the index {}", PARTITION_NAME_COLUMN_STATS);
    register(dataTableMetaClient, indexDefinition);
  }
}
