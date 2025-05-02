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
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.Indexer;
import org.apache.hudi.util.Lazy;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getColumnsToIndex;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getFileStatsRangeMetadata;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.tryResolveSchemaForTable;
import static org.apache.hudi.metadata.MetadataPartitionType.PARTITION_STATS;

/**
 * Implementation of {@link MetadataPartitionType#PARTITION_STATS} metadata
 */
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
      String createInstantTime,
      String instantTimeForPartition,
      Map<String, Map<String, Long>> partitionIdToAllFilesMap,
      Lazy<List<Pair<String, FileSlice>>> lazyLatestMergedPartitionFileSliceList) throws IOException {
    // For PARTITION_STATS, COLUMN_STATS should also be enabled
    if (!dataTableWriteConfig.isMetadataColumnStatsIndexEnabled()) {
      LOG.warn("Skipping partition stats initialization as column stats index is not enabled. Please enable {}",
          HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key());
      return Collections.emptyList();
    }
    final int numFileGroup = dataTableWriteConfig.getMetadataConfig().getPartitionStatsIndexFileGroupCount();
    List<Pair<String, FileSlice>> partitionFileSliceList = lazyLatestMergedPartitionFileSliceList.get();
    if (partitionFileSliceList.isEmpty()) {
      return Collections.singletonList(InitialIndexPartitionData.of(
          numFileGroup, PARTITION_STATS.getPartitionPath(), engineContext.emptyHoodieData()));
    }
    HoodieMetadataConfig metadataConfig = dataTableWriteConfig.getMetadataConfig();
    Lazy<Option<Schema>> lazyWriterSchemaOpt = Lazy.lazily(() -> tryResolveSchemaForTable(dataTableMetaClient));
    final Map<String, Schema> columnsToIndexSchemaMap = getColumnsToIndex(
        dataTableMetaClient.getTableConfig(), metadataConfig, lazyWriterSchemaOpt,
        dataTableMetaClient.getActiveTimeline().getWriteTimeline().filterCompletedInstants().empty(),
        Option.of(dataTableWriteConfig.getRecordMerger().getRecordType()));
    if (columnsToIndexSchemaMap.isEmpty()) {
      LOG.warn("No columns to index for partition stats index");
      return Collections.singletonList(InitialIndexPartitionData.of(
          numFileGroup, PARTITION_STATS.getPartitionPath(), engineContext.emptyHoodieData()));
    }
    LOG.debug("Indexing following columns for partition stats index: {}", columnsToIndexSchemaMap);

    // Group by partition path and collect file names (BaseFile and LogFiles)
    List<Pair<String, Set<String>>> partitionToFileNames = partitionFileSliceList.stream()
        .collect(Collectors.groupingBy(Pair::getLeft,
            Collectors.mapping(pair -> extractFileNames(pair.getRight()), Collectors.toList())))
        .entrySet().stream()
        .map(entry -> Pair.of(entry.getKey(),
            entry.getValue().stream().flatMap(Set::stream).collect(Collectors.toSet())))
        .collect(Collectors.toList());

    // Create records for MDT
    int parallelism = Math.max(Math.min(partitionToFileNames.size(),
        metadataConfig.getPartitionStatsIndexParallelism()), 1);
    // This meta client object has to be local to allow Spark to serialize the object
    // instead of the whole indexer object
    HoodieTableMetaClient metaClient = dataTableMetaClient;
    HoodieData<HoodieRecord> records = engineContext.parallelize(partitionToFileNames, parallelism)
        .flatMap(partitionInfo -> {
          final String partitionPath = partitionInfo.getKey();
          // Step 1: Collect Column Metadata for Each File
          List<List<HoodieColumnRangeMetadata<Comparable>>> fileColumnMetadata =
              partitionInfo.getValue().stream()
                  .map(fileName -> getFileStatsRangeMetadata(
                      partitionPath, fileName, metaClient,
                      new ArrayList<>(columnsToIndexSchemaMap.keySet()), false,
                      metadataConfig.getMaxReaderBufferSize()))
                  .collect(Collectors.toList());
          return collectAndProcessColumnMetadata(
              fileColumnMetadata, partitionPath, true, columnsToIndexSchemaMap).iterator();
        });
    return Collections.singletonList(InitialIndexPartitionData.of(
        numFileGroup, PARTITION_STATS.getPartitionPath(), records));
  }

  private static Stream<HoodieRecord> collectAndProcessColumnMetadata(
      List<List<HoodieColumnRangeMetadata<Comparable>>> fileColumnMetadata,
      String partitionPath, boolean isTightBound,
      Map<String, Schema> colsToIndexSchemaMap
  ) {
    return HoodieTableMetadataUtil.collectAndProcessColumnMetadata(
        partitionPath, isTightBound, Option.empty(), fileColumnMetadata.stream().flatMap(List::stream), colsToIndexSchemaMap);
  }

  private static Set<String> extractFileNames(FileSlice fileSlice) {
    Set<String> fileNames = new HashSet<>();
    Option<HoodieBaseFile> baseFile = fileSlice.getBaseFile();
    baseFile.ifPresent(hoodieBaseFile -> fileNames.add(hoodieBaseFile.getFileName()));
    fileSlice.getLogFiles().forEach(hoodieLogFile -> fileNames.add(hoodieLogFile.getFileName()));
    return fileNames;
  }
}
