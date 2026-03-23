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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieIndexMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.index.ExpressionIndexRecordGenerator;
import org.apache.hudi.metadata.model.FileInfo;
import org.apache.hudi.metadata.index.bloomfilters.BloomFiltersIndexer;
import org.apache.hudi.metadata.index.columnstats.ColumnStatsIndexer;
import org.apache.hudi.metadata.index.model.IndexPartitionAndRecords;
import org.apache.hudi.metadata.model.FileSliceAndPartition;
import org.apache.hudi.metadata.model.FileInfoAndPartition;
import org.apache.hudi.metadata.index.model.IndexPartitionInitialization;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieMetadataException;
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

  private final ExpressionIndexRecordGenerator expressionIndexRecordGenerator;

  public ExpressionIndexer(
      HoodieEngineContext engineContext,
      HoodieWriteConfig dataTableWriteConfig,
      HoodieTableMetaClient dataTableMetaClient,
      ExpressionIndexRecordGenerator expressionIndexRecordGenerator) {
    super(engineContext, dataTableWriteConfig, dataTableMetaClient);

    this.expressionIndexRecordGenerator = expressionIndexRecordGenerator;
  }

  @Override
  public List<IndexPartitionInitialization> buildInitialization(
      String dataTableInstantTime,
      String instantTimeForPartition,
      Map<String, List<FileInfo>> partitionToAllFilesMap,
      Lazy<List<FileSliceAndPartition>> lazyPartitionFileSlices) throws IOException {
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

    List<FileSliceAndPartition> partitionFileSlicePairs = lazyPartitionFileSlices.get();
    List<FileInfoAndPartition> filesToIndex = new ArrayList<>();
    partitionFileSlicePairs.forEach(fsp -> {
      if (fsp.fileSlice().getBaseFile().isPresent()) {
        filesToIndex.add(FileInfoAndPartition.of(fsp.partitionPath(), fsp.fileSlice().getBaseFile().get().getPath(), fsp.fileSlice().getBaseFile().get().getFileSize()));
      }
      fsp.fileSlice().getLogFiles()
          .forEach(hoodieLogFile
              -> filesToIndex.add(FileInfoAndPartition.of(fsp.partitionPath(), hoodieLogFile.getPath().toString(), hoodieLogFile.getFileSize())));
    });

    int fileGroupCount = dataTableWriteConfig.getMetadataConfig().getExpressionIndexFileGroupCount();
    if (filesToIndex.isEmpty()) {
      return Collections.singletonList(IndexPartitionInitialization.of(fileGroupCount, indexName, engineContext.emptyHoodieData()));
    }

    int parallelism = Math.min(filesToIndex.size(), dataTableWriteConfig.getMetadataConfig().getExpressionIndexParallelism());
    HoodieSchema tableSchema =
        HoodieTableMetadataUtil.tryResolveSchemaForTable(dataTableMetaClient)
            .orElseThrow(() -> new HoodieMetadataException("Table schema is not available for expression index initialization"));
    HoodieSchema readerSchema = getProjectedSchemaForExpressionIndex(indexDefinition, dataTableMetaClient, tableSchema);

    HoodieData<HoodieRecord> records = expressionIndexRecordGenerator.buildInitialization(
        filesToIndex, indexDefinition, dataTableMetaClient, parallelism,
        tableSchema, readerSchema, engineContext.getStorageConf(), dataTableInstantTime);

    return Collections.singletonList(IndexPartitionInitialization.of(fileGroupCount, indexName, records));
  }

  @Override
  public List<IndexPartitionAndRecords> buildUpdate(String instantTime, HoodieBackedTableMetadata tableMetadata, Lazy<HoodieTableFileSystemView> lazyFileSystemView,
                                                    HoodieCommitMetadata commitMetadata) {
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
            expressionIndexRecords = expressionIndexRecordGenerator.buildUpdate(dataTableMetaClient, tableMetadata, commitMetadata, partition, instantTime);
          } catch (Exception e) {
            throw new HoodieMetadataException(String.format("Failed to get expression index updates for partition %s", partition), e);
          }
          return IndexPartitionAndRecords.of(partition, expressionIndexRecords);
        }).collect(Collectors.toList());
  }

  @Override
  public List<IndexPartitionAndRecords> buildClean(String instantTime, HoodieCleanMetadata cleanMetadata) {
    Option<HoodieIndexMetadata> indexMetadata = dataTableMetaClient.getIndexMetadata();
    if (indexMetadata.isEmpty()) {
      throw new HoodieMetadataException("Expression index metadata not found");
    }
    List<IndexPartitionAndRecords> indexRecordsList = new ArrayList<>();
    HoodieIndexMetadata metadata = indexMetadata.get();
    Map<String, HoodieIndexDefinition> indexDefinitions = metadata.getIndexDefinitions();
    if (indexDefinitions.isEmpty()) {
      throw new HoodieMetadataException("Expression index metadata not found");
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
              BloomFiltersIndexer.convertMetadataToBloomFilterRecords(cleanMetadata, engineContext, instantTime, dataTableWriteConfig.getBloomIndexParallelism())));
        } else if (indexDefinition.getIndexType().equalsIgnoreCase(PARTITION_NAME_COLUMN_STATS)) {
          HoodieMetadataConfig modifiedMetadataConfig = HoodieMetadataConfig.newBuilder()
              .withProperties(dataTableWriteConfig.getMetadataConfig().getProps())
              .withColumnStatsIndexForColumns(String.join(",", indexDefinition.getSourceFields()))
              .build();
          indexRecordsList.add(IndexPartitionAndRecords.of(indexName, ColumnStatsIndexer.convertMetadataToColumnStatsRecords(
              cleanMetadata, engineContext, dataTableMetaClient, modifiedMetadataConfig, Option.of(dataTableWriteConfig.getRecordMerger().getRecordType()))));
        } else {
          throw new HoodieMetadataException("Unsupported expression index type");
        }
      }
    }
    return indexRecordsList;
  }
}
