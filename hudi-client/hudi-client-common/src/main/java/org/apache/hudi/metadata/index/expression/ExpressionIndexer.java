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
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.expression.HoodieExpressionIndex;
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

import static org.apache.hudi.metadata.HoodieMetadataWriteUtils.getIndexDefinition;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_EXPRESSION_INDEX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_EXPRESSION_INDEX_PREFIX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getIndexPartitionsToInit;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getProjectedSchemaForExpressionIndex;
import static org.apache.hudi.metadata.MetadataPartitionType.EXPRESSION_INDEX;
import static org.apache.hudi.metadata.MetadataPartitionType.getIndexDefinitions;

/**
 * Implementation of {@link MetadataPartitionType#EXPRESSION_INDEX} index
 */
public class ExpressionIndexer implements Indexer {

  private static final Logger LOG = LoggerFactory.getLogger(ExpressionIndexer.class);
  private final HoodieEngineContext engineContext;
  private final HoodieWriteConfig dataTableWriteConfig;
  private final HoodieTableMetaClient dataTableMetaClient;
  private final ExpressionIndexRecordGenerator expressionIndexRecordGenerator;
  private final Lazy<Set<String>> expressionIndexPartitionsToInit;

  public ExpressionIndexer(HoodieEngineContext engineContext,
                           HoodieWriteConfig dataTableWriteConfig,
                           HoodieTableMetaClient dataTableMetaClient,
                           ExpressionIndexRecordGenerator expressionIndexRecordGenerator) {
    this.engineContext = engineContext;
    this.dataTableWriteConfig = dataTableWriteConfig;
    this.dataTableMetaClient = dataTableMetaClient;
    this.expressionIndexRecordGenerator = expressionIndexRecordGenerator;
    this.expressionIndexPartitionsToInit = Lazy.lazily(() -> {
      HoodieMetadataConfig metadataConfig = dataTableWriteConfig.getMetadataConfig();
      return getIndexPartitionsToInit(
          EXPRESSION_INDEX,
          metadataConfig,
          dataTableMetaClient,
          () -> isNewExpressionIndexDefinitionRequired(metadataConfig, dataTableMetaClient),
          metadataConfig::getExpressionIndexColumn,
          metadataConfig::getExpressionIndexName,
          PARTITION_NAME_EXPRESSION_INDEX_PREFIX,
          metadataConfig.getExpressionIndexType());
    });
  }

  @Override
  public List<InitialIndexPartitionData> initialize(
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
    List<Pair<String, FileSlice>> partitionFileSlicePairs = Indexer.getPartitionFileSlicePairs(
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
        expressionIndexRecordGenerator.generate(
            filesToIndex, indexDefinition, dataTableMetaClient,
            parallelism,
            readerSchema, engineContext.getStorageConf(), instantTimeForPartition)));
  }

  // TODO(yihua): move test and remove the static method
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

  /**
   * Given metadata config and table config, determine whether a new expression index definition is required.
   */
  public static boolean isNewExpressionIndexDefinitionRequired(HoodieMetadataConfig metadataConfig, HoodieTableMetaClient dataMetaClient) {
    String expressionIndexColumn = metadataConfig.getExpressionIndexColumn();
    if (StringUtils.isNullOrEmpty(expressionIndexColumn)) {
      return false;
    }

    // check that expr is present in index options
    Map<String, String> expressionIndexOptions = metadataConfig.getExpressionIndexOptions();
    if (expressionIndexOptions.isEmpty()) {
      return false;
    }

    // get all index definitions for this column and index type
    // check if none of the index definitions has index function matching the expression
    List<HoodieIndexDefinition> indexDefinitions = getIndexDefinitions(expressionIndexColumn, PARTITION_NAME_EXPRESSION_INDEX, dataMetaClient);
    return indexDefinitions.isEmpty()
        || indexDefinitions.stream().noneMatch(indexDefinition -> indexDefinition.getIndexFunction().equals(expressionIndexOptions.get(HoodieExpressionIndex.EXPRESSION_OPTION)));
  }
}
