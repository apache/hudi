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
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.index.ExpressionIndexRecordGenerator;
import org.apache.hudi.metadata.index.Indexer;
import org.apache.hudi.util.Lazy;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hudi.metadata.HoodieMetadataWriteUtils.getIndexDefinition;
import static org.apache.hudi.metadata.HoodieMetadataWriteUtils.getPartitionFileSlicePairs;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_EXPRESSION_INDEX_PREFIX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getIndexPartitionsToInit;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getProjectedSchemaForExpressionIndex;
import static org.apache.hudi.metadata.MetadataPartitionType.EXPRESSION_INDEX;
import static org.apache.hudi.metadata.MetadataPartitionType.isNewExpressionIndexDefinitionRequired;

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
  public String getPartitionName() {
    return expressionIndexPartitionsToInit.get().iterator().next();
  }

  @Override
  public InitialIndexData build(
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
      // TODO(yihua): avoid null and use a different way to indicate skipping
      return InitialIndexData.of(-1, null);
    }
    String indexName = expressionIndexPartitionsToInit.get().iterator().next();

    HoodieIndexDefinition indexDefinition = getIndexDefinition(dataTableMetaClient, indexName);
    ValidationUtils.checkState(indexDefinition != null,
        "Expression Index definition is not present for index " + indexName);
    List<Pair<String, FileSlice>> partitionFileSlicePairs = getPartitionFileSlicePairs(
        dataTableMetaClient, metadata, fsView.get());
    List<Pair<String, Pair<String, Long>>> partitionFilePathSizeTriplet = new ArrayList<>();
    partitionFileSlicePairs.forEach(entry -> {
      if (entry.getValue().getBaseFile().isPresent()) {
        partitionFilePathSizeTriplet.add(Pair.of(entry.getKey(), Pair.of(entry.getValue().getBaseFile().get().getPath(),
            entry.getValue().getBaseFile().get().getFileLen())));
      }
      entry.getValue().getLogFiles().forEach(hoodieLogFile -> {
        if (entry.getValue().getLogFiles().count() > 0) {
          entry.getValue().getLogFiles().forEach(logfile -> {
            partitionFilePathSizeTriplet.add(
                Pair.of(entry.getKey(), Pair.of(logfile.getPath().toString(), logfile.getFileSize())));
          });
        }
      });
    });

    int numFileGroup = dataTableWriteConfig.getMetadataConfig().getExpressionIndexFileGroupCount();
    int parallelism = Math.min(partitionFilePathSizeTriplet.size(),
        dataTableWriteConfig.getMetadataConfig().getExpressionIndexParallelism());
    Schema readerSchema = getProjectedSchemaForExpressionIndex(indexDefinition, dataTableMetaClient);
    return InitialIndexData.of(numFileGroup,
        indexHelper.generate(
            partitionFilePathSizeTriplet, indexDefinition, dataTableMetaClient,
            parallelism,
            readerSchema, engineContext.getStorageConf(), instantTimeForPartition));
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
}
