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

package org.apache.hudi.metadata.index.secondary;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.metadata.model.FileInfo;
import org.apache.hudi.metadata.model.FileSliceAndPartition;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.metadata.index.model.IndexPartitionInitialization;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.BaseIndexer;
import org.apache.hudi.util.Lazy;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getSecondaryIndexPartitionsToInit;
import static org.apache.hudi.metadata.MetadataPartitionType.RECORD_INDEX;
import static org.apache.hudi.metadata.MetadataPartitionType.SECONDARY_INDEX;
import static org.apache.hudi.metadata.SecondaryIndexRecordGenerationUtils.readSecondaryKeysFromFileSlices;

/**
 * Implementation of {@link MetadataPartitionType#SECONDARY_INDEX} index
 */
@Slf4j
public class SecondaryIndexer extends BaseIndexer {

  private static final int RECORD_INDEX_AVERAGE_RECORD_SIZE = 48;

  public SecondaryIndexer(
      HoodieEngineContext engineContext,
      HoodieWriteConfig dataTableWriteConfig,
      HoodieTableMetaClient dataTableMetaClient) {
    super(engineContext, dataTableWriteConfig, dataTableMetaClient);
  }

  @Override
  public List<IndexPartitionInitialization> buildInitialization(String dataTableInstantTime, String instantTimeForPartition, Map<String, List<FileInfo>> partitionToAllFilesMap,
                                                                Lazy<List<FileSliceAndPartition>> lazyLatestMergedPartitionFileSliceList) throws IOException {
    Set<String> secondaryIndexPartitionsToInit = getSecondaryIndexPartitionsToInit(SECONDARY_INDEX, dataTableWriteConfig.getMetadataConfig(), dataTableMetaClient);
    if (secondaryIndexPartitionsToInit.size() != 1) {
      if (secondaryIndexPartitionsToInit.size() > 1) {
        log.warn("Skipping secondary index initialization as only one secondary index bootstrap at a time is supported for now. Provided: {}", secondaryIndexPartitionsToInit);
      }
      return Collections.emptyList();
    }

    String indexName = secondaryIndexPartitionsToInit.iterator().next();
    HoodieIndexDefinition indexDefinition = HoodieTableMetadataUtil.getHoodieIndexDefinition(indexName, dataTableMetaClient);
    ValidationUtils.checkState(indexDefinition != null, "Secondary Index definition is not present for index " + indexName);

    List<FileSliceAndPartition> partitionFileSlicePairs = lazyLatestMergedPartitionFileSliceList.get();

    int parallelism = Math.min(partitionFileSlicePairs.size(), dataTableWriteConfig.getMetadataConfig().getSecondaryIndexParallelism());
    HoodieData<HoodieRecord> records = readSecondaryKeysFromFileSlices(
        engineContext,
        partitionFileSlicePairs,
        parallelism,
        this.getClass().getSimpleName(),
        dataTableMetaClient,
        indexDefinition,
        dataTableWriteConfig.getProps());

    // Initialize the file groups - using the same estimation logic as that of record index
    final int fileGroupCount = HoodieTableMetadataUtil.estimateFileGroupCount(RECORD_INDEX, records::count,
        RECORD_INDEX_AVERAGE_RECORD_SIZE, dataTableWriteConfig.getGlobalRecordLevelIndexMinFileGroupCount(),
        dataTableWriteConfig.getGlobalRecordLevelIndexMaxFileGroupCount(), dataTableWriteConfig.getRecordIndexGrowthFactor(),
        dataTableWriteConfig.getRecordIndexMaxFileGroupSizeBytes());

    return Collections.singletonList(IndexPartitionInitialization.of(fileGroupCount, indexName, records));
  }
}
