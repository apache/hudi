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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Lazy;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.BaseIndexer;
import org.apache.hudi.metadata.index.model.IndexCleanContext;
import org.apache.hudi.metadata.index.model.IndexInitializationContext;
import org.apache.hudi.metadata.index.model.IndexInitializationPlan;
import org.apache.hudi.metadata.index.model.IndexPartitionAndRecords;
import org.apache.hudi.metadata.index.model.IndexRestoreContext;
import org.apache.hudi.metadata.index.model.IndexUpdateContext;
import org.apache.hudi.metadata.model.FileInfo;
import org.apache.hudi.metadata.stats.HoodieColumnRangeMetadata;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.fs.FSUtils.getFileNameFromPath;
import static org.apache.hudi.index.HoodieIndexUtils.register;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.existingIndexVersionOrDefault;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getColumnStatsRecords;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getColumnsToIndex;
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
        new ArrayList<>(getColumnsToIndex(
            dataTableMetaClient.getTableConfig(),
            dataTableWriteConfig.getMetadataConfig(),
            Lazy.lazily(() -> HoodieTableMetadataUtil.tryResolveSchemaForTable(dataTableMetaClient)),
            true,
            Option.of(dataTableWriteConfig.getRecordMerger().getRecordType()),
            existingIndexVersionOrDefault(PARTITION_NAME_COLUMN_STATS, dataTableMetaClient)).keySet()));
  }

  @Override
  public List<IndexInitializationPlan> buildInitialization(IndexInitializationContext context) throws IOException {
    Map<String, List<FileInfo>> partitionToAllFilesMap = context.allFiles();
    final int fileGroupCount = dataTableWriteConfig.getMetadataConfig().getColumnStatsIndexFileGroupCount();
    // columnsToIndex can be empty if meta fields are disabled and cols to index is not explicitly overridden.
    if (partitionToAllFilesMap.isEmpty() || columnsToIndex.get().isEmpty()) {
      return Collections.singletonList(IndexInitializationPlan.of(fileGroupCount, COLUMN_STATS.getPartitionPath(), engineContext.emptyHoodieData()));
    }

    log.info("Indexing {} columns for column stats index", columnsToIndex.get().size());
    // during initialization, we need stats for base and log files.
    HoodieData<HoodieRecord> records = HoodieTableMetadataUtil.convertFilesToColumnStatsRecords(
        engineContext, Collections.emptyMap(), partitionToAllFilesMap,
        dataTableMetaClient, dataTableWriteConfig.getColumnStatsIndexParallelism(),
        dataTableWriteConfig.getMetadataConfig().getMaxReaderBufferSize(),
        columnsToIndex.get());
    return Collections.singletonList(IndexInitializationPlan.of(fileGroupCount, COLUMN_STATS.getPartitionPath(), records));
  }

  @Override
  public List<IndexPartitionAndRecords> buildUpdate(IndexUpdateContext context) {
    final HoodieData<HoodieRecord> records = convertMetadataToColumnStatsRecords(context.commitMetadata(), engineContext,
        dataTableMetaClient, dataTableWriteConfig.getMetadataConfig(), Option.of(dataTableWriteConfig.getRecordMerger().getRecordType()));
    return Collections.singletonList(IndexPartitionAndRecords.of(COLUMN_STATS.getPartitionPath(), records));
  }

  @Override
  public List<IndexPartitionAndRecords> buildClean(IndexCleanContext context) {
    final HoodieData<HoodieRecord> records =
        convertMetadataToColumnStatsRecords(context.cleanMetadata(), engineContext, dataTableMetaClient,
            dataTableWriteConfig.getMetadataConfig(), Option.of(dataTableWriteConfig.getRecordMerger().getRecordType()));
    return Collections.singletonList(IndexPartitionAndRecords.of(COLUMN_STATS.getPartitionPath(), records));
  }

  @Override
  public List<IndexPartitionAndRecords> buildRestore(IndexRestoreContext context) {
    if (context.filesDeleted().isEmpty() && context.filesAdded().isEmpty()) {
      return Collections.emptyList();
    }
    Lazy<Option<HoodieSchema>> tableSchema =
        Lazy.lazily(() -> HoodieTableMetadataUtil.tryResolveSchemaForTable(dataTableMetaClient));
    // Restore records are generated from data files, so meta fields are not included here.
    final List<String> restoreColumnsToIndex = new ArrayList<>(HoodieTableMetadataUtil.getColumnsToIndex(
        dataTableMetaClient.getTableConfig(),
        dataTableWriteConfig.getMetadataConfig(),
        tableSchema,
        false,
        Option.of(dataTableWriteConfig.getRecordMerger().getRecordType()),
        HoodieTableMetadataUtil.existingIndexVersionOrDefault(PARTITION_NAME_COLUMN_STATS, dataTableMetaClient)).keySet());
    if (restoreColumnsToIndex.isEmpty()) {
      log.info("Since there are no columns to index, stop to generate ColumnStats records.");
      return Collections.emptyList();
    }
    HoodieData<HoodieRecord> records = HoodieTableMetadataUtil.convertFilesToColumnStatsRecords(
        engineContext, context.filesDeleted(),
        context.filesAdded(), dataTableMetaClient,
        dataTableWriteConfig.getMetadataConfig().getColumnStatsIndexParallelism(),
        dataTableWriteConfig.getMetadataConfig().getMaxReaderBufferSize(),
        restoreColumnsToIndex);
    return Collections.singletonList(IndexPartitionAndRecords.of(COLUMN_STATS.getPartitionPath(), records));
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

  private static HoodieData<HoodieRecord> convertMetadataToColumnStatsRecords(
      HoodieCommitMetadata commitMetadata,
      HoodieEngineContext engineContext,
      HoodieTableMetaClient dataMetaClient,
      HoodieMetadataConfig metadataConfig,
      Option<HoodieRecord.HoodieRecordType> recordTypeOpt) {
    List<HoodieWriteStat> allWriteStats = commitMetadata.getPartitionToWriteStats().values().stream()
        .flatMap(Collection::stream).collect(Collectors.toList());

    if (allWriteStats.isEmpty()) {
      return engineContext.emptyHoodieData();
    }

    try {
      Map<String, HoodieSchema> columnsToIndexSchemaMap = getColumnsToIndex(commitMetadata, dataMetaClient, metadataConfig, recordTypeOpt);
      if (columnsToIndexSchemaMap.isEmpty()) {
        // In case there are no columns to index, bail
        return engineContext.emptyHoodieData();
      }
      List<String> columnsToIndex = new ArrayList<>(columnsToIndexSchemaMap.keySet());
      int parallelism = Math.max(Math.min(allWriteStats.size(), metadataConfig.getColumnStatsIndexParallelism()), 1);
      return engineContext.parallelize(allWriteStats, parallelism)
          .flatMap(writeStat ->
              translateWriteStatToColumnStats(writeStat, dataMetaClient, columnsToIndex).iterator());
    } catch (Exception e) {
      throw new HoodieException("Failed to generate column stats records for metadata table", e);
    }
  }

  /**
   * Convert clean metadata to column stats index records.
   *
   * @param cleanMetadata                    - Clean action metadata
   * @param engineContext                    - Engine context
   * @param dataMetaClient                   - HoodieTableMetaClient for data
   * @param metadataConfig                   - HoodieMetadataConfig
   * @return List of column stats index records for the clean metadata
   */
  public static HoodieData<HoodieRecord> convertMetadataToColumnStatsRecords(
      HoodieCleanMetadata cleanMetadata,
      HoodieEngineContext engineContext,
      HoodieTableMetaClient dataMetaClient,
      HoodieMetadataConfig metadataConfig,
      Option<HoodieRecord.HoodieRecordType> recordTypeOpt) {
    List<Pair<String, String>> deleteFileList = new ArrayList<>();
    cleanMetadata.getPartitionMetadata().forEach((partition, partitionMetadata) -> {
      // Files deleted from a partition
      List<String> deletedFiles = partitionMetadata.getDeletePathPatterns();
      deletedFiles.forEach(entry -> deleteFileList.add(Pair.of(partition, entry)));
    });
    if (deleteFileList.isEmpty()) {
      return engineContext.emptyHoodieData();
    }

    HoodieIndexVersion indexVersion = existingIndexVersionOrDefault(PARTITION_NAME_COLUMN_STATS, dataMetaClient);

    List<String> columnsToIndex = new ArrayList<>(getColumnsToIndex(dataMetaClient.getTableConfig(), metadataConfig,
        Lazy.lazily(() -> HoodieTableMetadataUtil.tryResolveSchemaForTable(dataMetaClient)), false, recordTypeOpt, indexVersion).keySet());

    if (columnsToIndex.isEmpty()) {
      // In case there are no columns to index, bail
      log.info("No columns to index for column stats index.");
      return engineContext.emptyHoodieData();
    }

    int parallelism = Math.max(Math.min(deleteFileList.size(), metadataConfig.getColumnStatsIndexParallelism()), 1);
    return engineContext.parallelize(deleteFileList, parallelism)
        .flatMap(deleteFileInfoPair -> {
          String partitionPath = deleteFileInfoPair.getLeft();
          String fileName = deleteFileInfoPair.getRight();
          return getColumnStatsRecords(partitionPath, fileName, dataMetaClient, columnsToIndex, true).iterator();
        });
  }

  private static Stream<HoodieRecord> translateWriteStatToColumnStats(
      HoodieWriteStat writeStat,
      HoodieTableMetaClient datasetMetaClient,
      List<String> columnsToIndex) {
    if (writeStat.getColumnStats().isPresent()) {
      Map<String, HoodieColumnRangeMetadata<Comparable>> columnRangeMap = writeStat.getColumnStats().get();
      Collection<HoodieColumnRangeMetadata<Comparable>> columnRangeMetadataList = columnRangeMap.values();
      return HoodieMetadataPayload.createColumnStatsRecords(writeStat.getPartitionPath(), columnRangeMetadataList, false);
    }

    String filePath = writeStat.getPath();
    return getColumnStatsRecords(writeStat.getPartitionPath(), getFileNameFromPath(filePath), datasetMetaClient, columnsToIndex, false);
  }
}
