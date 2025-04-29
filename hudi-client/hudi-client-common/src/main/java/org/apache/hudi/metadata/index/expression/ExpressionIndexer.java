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
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieIndexMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.ExpressionIndexRecordGenerator;
import org.apache.hudi.metadata.index.Indexer;
import org.apache.hudi.util.Lazy;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.metadata.HoodieMetadataWriteUtils.getIndexDefinition;
import static org.apache.hudi.metadata.HoodieMetadataWriteUtils.getPartitionFileSlicePairs;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_EXPRESSION_INDEX_PREFIX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getIndexPartitionsToInit;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getProjectedSchemaForExpressionIndex;
import static org.apache.hudi.metadata.MetadataPartitionType.EXPRESSION_INDEX;
import static org.apache.hudi.metadata.MetadataPartitionType.isNewExpressionIndexDefinitionRequired;
import static org.apache.hudi.metadata.index.bloomfilters.BloomFiltersIndexer.convertMetadataToBloomFilterRecords;
import static org.apache.hudi.metadata.index.columnstats.ColumnStatsIndexer.convertMetadataToColumnStatsRecords;

public class ExpressionIndexer implements Indexer {

  private static final Logger LOG = LoggerFactory.getLogger(ExpressionIndexer.class);
  private final HoodieEngineContext engineContext;
  private final HoodieWriteConfig dataTableWriteConfig;
  private final HoodieTableMetaClient dataTableMetaClient;
  private final ExpressionIndexRecordGenerator indexHelper;
  private final Lazy<Set<String>> expressionIndexPartitionsToInit;

  public ExpressionIndexer(HoodieEngineContext engineContext,
                           HoodieWriteConfig dataTableWriteConfig,
                           HoodieTableMetaClient dataTableMetaClient,
                           ExpressionIndexRecordGenerator indexHelper) {
    this.engineContext = engineContext;
    this.dataTableWriteConfig = dataTableWriteConfig;
    this.dataTableMetaClient = dataTableMetaClient;
    this.indexHelper = indexHelper;
    this.expressionIndexPartitionsToInit = Lazy.lazily(() ->
        getExpressionIndexPartitionsToInit(
            dataTableWriteConfig.getMetadataConfig(), dataTableMetaClient));
  }

  @Override
  public List<InitialIndexPartitionData> build(
      List<HoodieTableMetadataUtil.DirectoryInfo> partitionInfoList,
      Map<String, Map<String, Long>> partitionToFilesMap,
      String createInstantTime,
      Lazy<HoodieTableFileSystemView> fsView,
      HoodieBackedTableMetadata metadata,
      String instantTimeForPartition) throws IOException {
    if (expressionIndexPartitionsToInit.get().size() != 1) {
      if (expressionIndexPartitionsToInit.get().size() > 1) {
        LOG.warn(
            "Skipping expression index initialization as only one expression index "
                + "bootstrap at a time is supported for now. Provided: {}",
            expressionIndexPartitionsToInit.get());
      }
      return Collections.emptyList();
    }
    String indexName = expressionIndexPartitionsToInit.get().iterator().next();

    HoodieIndexDefinition indexDefinition = getIndexDefinition(dataTableMetaClient, indexName);
    ValidationUtils.checkState(indexDefinition != null,
        "Expression Index definition is not present for index " + indexName);
    List<Pair<String, FileSlice>> partitionFileSlicePairs = getPartitionFileSlicePairs(
        dataTableMetaClient, metadata, fsView.get());
    List<ExpressionIndexRecordGenerator.FileToIndex> filesToIndex = new ArrayList<>();
    partitionFileSlicePairs.forEach(entry -> {
      if (entry.getValue().getBaseFile().isPresent()) {
        filesToIndex.add(ExpressionIndexRecordGenerator.FileToIndex.of(
            entry.getKey(), entry.getValue().getBaseFile().get().getPath(),
            entry.getValue().getBaseFile().get().getFileLen()));
      }
      entry.getValue().getLogFiles().forEach(hoodieLogFile -> {
        if (entry.getValue().getLogFiles().count() > 0) {
          entry.getValue().getLogFiles().forEach(logfile ->
              filesToIndex.add(ExpressionIndexRecordGenerator.FileToIndex.of(
                  entry.getKey(), logfile.getPath().toString(), logfile.getFileSize())));
        }
      });
    });

    int numFileGroup = dataTableWriteConfig.getMetadataConfig().getExpressionIndexFileGroupCount();
    int parallelism = Math.min(filesToIndex.size(),
        dataTableWriteConfig.getMetadataConfig().getExpressionIndexParallelism());
    Schema readerSchema = getProjectedSchemaForExpressionIndex(indexDefinition, dataTableMetaClient);
    return Collections.singletonList(InitialIndexPartitionData.of(numFileGroup,
        expressionIndexPartitionsToInit.get().iterator().next(),
        indexHelper.generate(
            filesToIndex, indexDefinition, dataTableMetaClient,
            parallelism,
            readerSchema, engineContext.getStorageConf(), instantTimeForPartition)));
  }

  @Override
  public List<IndexPartitionData> update(String instantTime,
                                         HoodieBackedTableMetadata tableMetadata,
                                         Lazy<HoodieTableFileSystemView> lazyFileSystemView,
                                         HoodieCommitMetadata commitMetadata) {
    return dataTableMetaClient.getTableConfig().getMetadataPartitions()
        .stream()
        .filter(partition -> partition.startsWith(HoodieTableMetadataUtil.PARTITION_NAME_EXPRESSION_INDEX_PREFIX))
        .map(partition -> {
          HoodieData<HoodieRecord> expressionIndexRecords;
          try {
            expressionIndexRecords = indexHelper.updateFromCommitMetadata(
                dataTableMetaClient, tableMetadata, commitMetadata, partition, instantTime);
          } catch (Exception e) {
            throw new HoodieMetadataException(String.format("Failed to get expression index updates for partition %s", partition), e);
          }
          return IndexPartitionData.of(partition, expressionIndexRecords);
        })
        .collect(Collectors.toList());
  }

  @Override
  public List<IndexPartitionData> clean(String instantTime, HoodieCleanMetadata cleanMetadata) {
    return convertMetadataToExpressionIndexRecords(engineContext, cleanMetadata, instantTime,
        dataTableMetaClient, dataTableWriteConfig.getMetadataConfig(),
        dataTableWriteConfig.getBloomIndexParallelism(),
        Option.of(dataTableWriteConfig.getRecordMerger().getRecordType()));
  }

  public static Set<String> getExpressionIndexPartitionsToInit(HoodieMetadataConfig metadataConfig,
                                                               HoodieTableMetaClient dataMetaClient) {
    return getIndexPartitionsToInit(
        EXPRESSION_INDEX,
        metadataConfig,
        dataMetaClient,
        () -> isNewExpressionIndexDefinitionRequired(metadataConfig, dataMetaClient),
        metadataConfig::getExpressionIndexColumn,
        metadataConfig::getExpressionIndexName,
        PARTITION_NAME_EXPRESSION_INDEX_PREFIX,
        metadataConfig.getExpressionIndexType()
    );
  }

  public static List<IndexPartitionData> convertMetadataToExpressionIndexRecords(
      HoodieEngineContext engineContext,
      HoodieCleanMetadata cleanMetadata,
      String instantTime, HoodieTableMetaClient dataMetaClient,
      HoodieMetadataConfig metadataConfig, int bloomIndexParallelism,
      Option<HoodieRecord.HoodieRecordType> recordTypeOpt) {
    List<IndexPartitionData> indexPartitionDataList = new ArrayList<>();
    Option<HoodieIndexMetadata> indexMetadata = dataMetaClient.getIndexMetadata();
    if (indexMetadata.isPresent()) {
      HoodieIndexMetadata metadata = indexMetadata.get();
      Map<String, HoodieIndexDefinition> indexDefinitions = metadata.getIndexDefinitions();
      if (indexDefinitions.isEmpty()) {
        throw new HoodieMetadataException("Expression index metadata not found");
      }
      // iterate over each index definition and check:
      // if it is a expression index using column_stats, then follow the same approach as column_stats
      // if it is a expression index using bloom_filters, then follow the same approach as bloom_filters
      // else throw an exception
      for (Map.Entry<String, HoodieIndexDefinition> entry : indexDefinitions.entrySet()) {
        String indexName = entry.getKey();
        HoodieIndexDefinition indexDefinition = entry.getValue();
        if (MetadataPartitionType.EXPRESSION_INDEX.equals(MetadataPartitionType.fromPartitionPath(indexDefinition.getIndexName()))) {
          if (indexDefinition.getIndexType().equalsIgnoreCase(PARTITION_NAME_BLOOM_FILTERS)) {
            indexPartitionDataList.add(IndexPartitionData.of(
                indexName, convertMetadataToBloomFilterRecords(
                    cleanMetadata, engineContext, instantTime, bloomIndexParallelism)));
          } else if (indexDefinition.getIndexType().equalsIgnoreCase(PARTITION_NAME_COLUMN_STATS)) {
            HoodieMetadataConfig modifiedMetadataConfig = HoodieMetadataConfig.newBuilder()
                .withProperties(metadataConfig.getProps())
                .withColumnStatsIndexForColumns(String.join(",", indexDefinition.getSourceFields()))
                .build();
            Option<HoodieData<HoodieRecord>> recordOption = convertMetadataToColumnStatsRecords(
                cleanMetadata, engineContext, dataMetaClient, modifiedMetadataConfig, recordTypeOpt);
            if (recordOption.isPresent()) {
              indexPartitionDataList.add(IndexPartitionData.of(indexName, recordOption.get()));
            }
          } else {
            throw new HoodieMetadataException("Unsupported expression index type");
          }
        }
      }
    } else {
      throw new HoodieMetadataException("Expression index metadata not found");
    }
    return indexPartitionDataList;
  }
}
