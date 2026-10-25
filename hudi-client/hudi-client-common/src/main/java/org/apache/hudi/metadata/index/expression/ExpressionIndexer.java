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

package org.apache.hudi.metadata.index.expression;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieIndexMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.exception.HoodieSchemaNotFoundException;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.BaseIndexer;
import org.apache.hudi.metadata.index.EngineIndexerSupport;
import org.apache.hudi.metadata.index.EngineIndexerSupport.PartitionStatsRecordsFunction;
import org.apache.hudi.metadata.index.bloomfilters.BloomFiltersIndexer;
import org.apache.hudi.metadata.index.columnstats.ColumnStatsIndexer;
import org.apache.hudi.metadata.index.model.IndexCleanContext;
import org.apache.hudi.metadata.index.model.IndexInitializationContext;
import org.apache.hudi.metadata.index.model.IndexInitializationPlan;
import org.apache.hudi.metadata.index.model.IndexPartitionAndRecords;
import org.apache.hudi.metadata.index.model.IndexUpdateContext;
import org.apache.hudi.metadata.model.FileInfoAndPartition;
import org.apache.hudi.metadata.model.FileSliceAndPartition;
import org.apache.hudi.metadata.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.storage.StoragePath;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getExpressionIndexPartitionsToInit;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getProjectedSchemaForExpressionIndex;
import static org.apache.hudi.metadata.MetadataPartitionType.EXPRESSION_INDEX;
import static org.apache.hudi.metadata.MetadataPartitionType.fromPartitionPath;

/**
 * Implementation of {@link MetadataPartitionType#EXPRESSION_INDEX} index
 */
@Slf4j
public class ExpressionIndexer extends BaseIndexer {

  private final EngineIndexerSupport engineIndexerSupport;

  public ExpressionIndexer(
      HoodieEngineContext engineContext,
      HoodieWriteConfig dataTableWriteConfig,
      HoodieTableMetaClient dataTableMetaClient,
      EngineIndexerSupport engineIndexerSupport) {
    super(engineContext, dataTableWriteConfig, dataTableMetaClient);

    this.engineIndexerSupport = engineIndexerSupport;
  }

  @Override
  public List<IndexInitializationPlan> buildInitialization(IndexInitializationContext context) throws IOException {
    Set<String> expressionIndexPartitionsToInit = getExpressionIndexPartitionsToInit(
        EXPRESSION_INDEX, dataTableWriteConfig.getMetadataConfig(), dataTableMetaClient);
    if (expressionIndexPartitionsToInit.size() != 1) {
      if (expressionIndexPartitionsToInit.size() > 1) {
        log.warn("Skipping expression index initialization as only one expression index "
            + "bootstrap at a time is supported for now. Provided: {}", expressionIndexPartitionsToInit);
      }
      return Collections.emptyList();
    }

    String indexName = expressionIndexPartitionsToInit.iterator().next();
    HoodieIndexDefinition indexDefinition = HoodieTableMetadataUtil.getHoodieIndexDefinition(indexName, dataTableMetaClient);
    ValidationUtils.checkState(indexDefinition != null, "Expression Index definition is not present for index " + indexName);

    List<FileSliceAndPartition> fileSlices = context.latestFileSlices().get();
    List<FileInfoAndPartition> filesToIndex = new ArrayList<>();
    fileSlices.forEach(fsp -> {
      if (fsp.getFileSlice().getBaseFile().isPresent()) {
        filesToIndex.add(FileInfoAndPartition.of(fsp.getPartitionPath(), fsp.getFileSlice().getBaseFile().get().getPath(), fsp.getFileSlice().getBaseFile().get().getFileSize()));
      }
      fsp.getFileSlice().getLogFiles()
          .forEach(hoodieLogFile
              -> filesToIndex.add(FileInfoAndPartition.of(fsp.getPartitionPath(), hoodieLogFile.getPath().toString(), hoodieLogFile.getFileSize())));
    });

    int fileGroupCount = dataTableWriteConfig.getMetadataConfig().getExpressionIndexFileGroupCount();
    if (filesToIndex.isEmpty()) {
      return Collections.singletonList(IndexInitializationPlan.of(fileGroupCount, indexName, engineContext.emptyHoodieData()));
    }

    int parallelism = Math.min(filesToIndex.size(), dataTableWriteConfig.getMetadataConfig().getExpressionIndexParallelism());
    HoodieSchema tableSchema =
        context.tableSchema().get()
            .orElseThrow(() -> new HoodieMetadataException("Table schema is not available for expression index initialization"));
    HoodieSchema readerSchema = getProjectedSchemaForExpressionIndex(indexDefinition, dataTableMetaClient, tableSchema);

    HoodieData<HoodieRecord> records = engineIndexerSupport.generateExpressionIndexRecords(
        filesToIndex, indexDefinition, dataTableMetaClient, parallelism,
        tableSchema, readerSchema, engineContext.getStorageConf(), context.dataInstantTime(),
        getPartitionRecordsFunction(indexDefinition));

    return Collections.singletonList(IndexInitializationPlan.of(fileGroupCount, indexName, records));
  }

  @Override
  public List<IndexPartitionAndRecords> buildUpdate(IndexUpdateContext context) {
    if (!MetadataPartitionType.EXPRESSION_INDEX.isMetadataPartitionAvailable(dataTableMetaClient)) {
      log.info("Don't need to update expression index, since no expression index is available");
      return Collections.emptyList();
    }
    return dataTableMetaClient.getTableConfig().getMetadataPartitions()
        .stream()
        .filter(partition -> partition.startsWith(HoodieTableMetadataUtil.PARTITION_NAME_EXPRESSION_INDEX_PREFIX))
        .map(partition -> {
          HoodieData<HoodieRecord> expressionIndexRecords;
          try {
            expressionIndexRecords = buildExpressionIndexRecordsForUpdate(context.instantTime(), context.tableMetadata(), context.commitMetadata(), partition);
          } catch (Exception e) {
            throw new HoodieMetadataException(String.format("Failed to get expression index updates for partition %s", partition), e);
          }
          return IndexPartitionAndRecords.of(partition, expressionIndexRecords);
        }).collect(Collectors.toList());
  }

  private HoodieData<HoodieRecord> buildExpressionIndexRecordsForUpdate(
      String instantTime,
      HoodieBackedTableMetadata tableMetadata,
      HoodieCommitMetadata commitMetadata,
      String indexPartition) {
    // Keep update orchestration in the common indexer. Engine support only generates records
    // from the file/schema inputs and loads engine-specific column stats when needed.
    HoodieIndexDefinition indexDefinition = HoodieTableMetadataUtil.getHoodieIndexDefinition(indexPartition, dataTableMetaClient);
    ValidationUtils.checkState(indexDefinition != null, "Expression Index definition is not present for index " + indexPartition);

    // Step 1: Generate partition, file path and size descriptors from the newly created files in the commit metadata
    List<FileInfoAndPartition> filesToIndex = getFilesToIndex(commitMetadata);
    int parallelism = Math.min(filesToIndex.size(), dataTableWriteConfig.getMetadataConfig().getExpressionIndexParallelism());
    HoodieSchema tableSchema;
    try {
      tableSchema = new TableSchemaResolver(dataTableMetaClient).getTableSchema();
    } catch (Exception e) {
      throw new HoodieSchemaNotFoundException("No schema found for table at " + dataTableMetaClient.getBasePath(), e);
    }
    HoodieSchema readerSchema = getProjectedSchemaForExpressionIndex(indexDefinition, dataTableMetaClient, tableSchema);
    Option<PartitionStatsRecordsFunction> partitionRecordsFunctionOpt =
        getPartitionRecordsFunctionForUpdate(indexDefinition, indexPartition, commitMetadata, tableMetadata, instantTime);

    // Step 2: Compute the expression index column stat and partition stat records for these newly created files
    // partitionRecordsFunctionOpt - Function used to generate partition stats. These stats are generated only for expression index created using column stats
    //
    // In the partitionRecordsFunctionOpt function we merge the expression index records from the new files created in the commit metadata
    // with the expression index records from the unmodified files to get the new partition stat records
    return engineIndexerSupport.generateExpressionIndexRecords(
        filesToIndex, indexDefinition, dataTableMetaClient, parallelism,
        tableSchema, readerSchema, engineContext.getStorageConf(), instantTime, partitionRecordsFunctionOpt);
  }

  private List<FileInfoAndPartition> getFilesToIndex(HoodieCommitMetadata commitMetadata) {
    List<FileInfoAndPartition> filesToIndex = new ArrayList<>();
    commitMetadata.getPartitionToWriteStats().forEach((dataPartition, writeStats) -> writeStats.forEach(writeStat -> filesToIndex.add(
        FileInfoAndPartition.of(
            writeStat.getPartitionPath(),
            new StoragePath(dataTableMetaClient.getBasePath(), writeStat.getPath()).toString(),
            writeStat.getFileSizeInBytes()))));
    return filesToIndex;
  }

  private Option<PartitionStatsRecordsFunction>
      getPartitionRecordsFunction(HoodieIndexDefinition indexDefinition) {
    if (!PARTITION_NAME_COLUMN_STATS.equals(indexDefinition.getIndexType())) {
      return Option.empty();
    }
    return Option.of(rangeMetadata ->
        HoodieTableMetadataUtil.collectAndProcessExprIndexPartitionStatRecords(
            rangeMetadata, true, Option.of(indexDefinition.getIndexName())));
  }

  private Option<PartitionStatsRecordsFunction>
      getPartitionRecordsFunctionForUpdate(
          HoodieIndexDefinition indexDefinition,
          String indexPartition,
          HoodieCommitMetadata commitMetadata,
          HoodieBackedTableMetadata tableMetadata,
          String instantTime) {
    if (!PARTITION_NAME_COLUMN_STATS.equals(indexDefinition.getIndexType())) {
      return Option.empty();
    }

    // Partition stats for expression indexes need both newly written files and existing files
    // in affected partitions. The engine loads existing column stats; this function merges
    // them with the column stats generated from the current commit.
    HoodiePairData<String, HoodieColumnRangeMetadata<Comparable>> existingPartitionStats =
        engineIndexerSupport.loadExpressionIndexPartitionStats(
            dataTableMetaClient, tableMetadata, commitMetadata, indexDefinition, indexPartition, instantTime);
    return Option.of(rangeMetadata ->
        HoodieTableMetadataUtil.collectAndProcessExprIndexPartitionStatRecords(
            existingPartitionStats.union(rangeMetadata), true, Option.of(indexDefinition.getIndexName())));
  }

  @Override
  public List<IndexPartitionAndRecords> buildClean(IndexCleanContext context) {
    Option<HoodieIndexMetadata> indexMetadata = dataTableMetaClient.getIndexMetadata();
    if (indexMetadata.isEmpty()) {
      throw new HoodieMetadataException("Expression index metadata not found");
    }
    List<IndexPartitionAndRecords> indexRecordsList = new ArrayList<>();
    HoodieIndexMetadata metadata = indexMetadata.get();
    Map<String, HoodieIndexDefinition> indexDefinitions = metadata.getIndexDefinitions();
    if (indexDefinitions.isEmpty()) {
      throw new HoodieMetadataException("Expression index has no index definitions");
    }
    // iterate over each index definition and check:
    // if it is an expression index using column_stats, then follow the same approach as column_stats
    // if it is an expression index using bloom_filters, then follow the same approach as bloom_filters
    // else throw an exception
    for (Map.Entry<String, HoodieIndexDefinition> entry : indexDefinitions.entrySet()) {
      String indexName = entry.getKey();
      HoodieIndexDefinition indexDefinition = entry.getValue();
      if (MetadataPartitionType.EXPRESSION_INDEX.equals(fromPartitionPath(indexDefinition.getIndexName()))) {
        if (indexDefinition.getIndexType().equalsIgnoreCase(PARTITION_NAME_BLOOM_FILTERS)) {
          indexRecordsList.add(IndexPartitionAndRecords.of(indexName,
              BloomFiltersIndexer.convertMetadataToBloomFilterRecords(context.cleanMetadata(), engineContext, context.instantTime(), dataTableWriteConfig.getBloomIndexParallelism())));
        } else if (indexDefinition.getIndexType().equalsIgnoreCase(PARTITION_NAME_COLUMN_STATS)) {
          HoodieMetadataConfig modifiedMetadataConfig = HoodieMetadataConfig.newBuilder()
              .withProperties(dataTableWriteConfig.getMetadataConfig().getProps())
              .withColumnStatsIndexForColumns(String.join(",", indexDefinition.getSourceFields()))
              .build();
          indexRecordsList.add(IndexPartitionAndRecords.of(indexName, ColumnStatsIndexer.convertMetadataToColumnStatsRecords(
              context.cleanMetadata(), engineContext, dataTableMetaClient, modifiedMetadataConfig, Option.of(dataTableWriteConfig.getRecordMerger().getRecordType()))));
        } else {
          throw new HoodieMetadataException("Unsupported expression index type");
        }
      }
    }
    return indexRecordsList;
  }
}
