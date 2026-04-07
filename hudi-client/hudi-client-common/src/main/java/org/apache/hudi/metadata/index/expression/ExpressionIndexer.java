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

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.metadata.index.ExpressionIndexRecordGenerator;
import org.apache.hudi.metadata.model.FileInfo;
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

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getExpressionIndexPartitionsToInit;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getProjectedSchemaForExpressionIndex;
import static org.apache.hudi.metadata.MetadataPartitionType.EXPRESSION_INDEX;

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
    if (partitionFileSlicePairs.isEmpty()) {
      return Collections.singletonList(IndexPartitionInitialization.of(fileGroupCount, indexName, engineContext.emptyHoodieData()));
    }

    int parallelism = Math.min(filesToIndex.size(), dataTableWriteConfig.getMetadataConfig().getExpressionIndexParallelism());
    HoodieSchema tableSchema =
        HoodieTableMetadataUtil.tryResolveSchemaForTable(dataTableMetaClient)
            .orElseThrow(() -> new HoodieMetadataException("Table schema is not available for expression index initialization"));
    HoodieSchema readerSchema = getProjectedSchemaForExpressionIndex(indexDefinition, dataTableMetaClient, tableSchema);

    HoodieData<HoodieRecord> records = expressionIndexRecordGenerator.generate(
        filesToIndex, indexDefinition, dataTableMetaClient, parallelism,
        tableSchema, readerSchema, engineContext.getStorageConf(), dataTableInstantTime);

    return Collections.singletonList(IndexPartitionInitialization.of(fileGroupCount, indexName, records));
  }
}
