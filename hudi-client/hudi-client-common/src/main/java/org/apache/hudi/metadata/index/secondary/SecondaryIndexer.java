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
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.index.Indexer;
import org.apache.hudi.util.Lazy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hudi.metadata.HoodieMetadataWriteUtils.getIndexDefinition;
import static org.apache.hudi.metadata.HoodieMetadataWriteUtils.getPartitionFileSlicePairs;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getSecondaryIndexPartitionsToInit;
import static org.apache.hudi.metadata.MetadataPartitionType.RECORD_INDEX;
import static org.apache.hudi.metadata.MetadataPartitionType.SECONDARY_INDEX;
import static org.apache.hudi.metadata.SecondaryIndexRecordGenerationUtils.readSecondaryKeysFromFileSlices;
import static org.apache.hudi.metadata.index.record.RecordIndexer.RECORD_INDEX_AVERAGE_RECORD_SIZE;

/**
 * Implementation of {@link SECONDARY_INDEX} index
 */
public class SecondaryIndexer implements Indexer {
  private static final Logger LOG = LoggerFactory.getLogger(SecondaryIndexer.class);
  private final HoodieEngineContext engineContext;
  private final EngineType engineType;
  private final HoodieWriteConfig dataTableWriteConfig;
  private final HoodieTableMetaClient dataTableMetaClient;
  private final Lazy<Set<String>> secondaryIndexPartitionsToInit;

  public SecondaryIndexer(HoodieEngineContext engineContext,
                          EngineType engineType,
                          HoodieWriteConfig dataTableWriteConfig,
                          HoodieTableMetaClient dataTableMetaClient) {
    this.engineContext = engineContext;
    this.dataTableWriteConfig = dataTableWriteConfig;
    this.dataTableMetaClient = dataTableMetaClient;
    this.engineType = engineType;
    this.secondaryIndexPartitionsToInit = Lazy.lazily(() ->
        getSecondaryIndexPartitionsToInit(
            SECONDARY_INDEX, dataTableWriteConfig.getMetadataConfig(), dataTableMetaClient));
  }

  @Override
  public List<InitialIndexPartitionData> initialize(
      List<HoodieTableMetadataUtil.DirectoryInfo> partitionInfoList,
      Map<String, Map<String, Long>> partitionToFilesMap,
      String createInstantTime,
      Lazy<HoodieTableFileSystemView> fsView,
      HoodieBackedTableMetadata metadata,
      String instantTimeForPartition) throws IOException {
    if (secondaryIndexPartitionsToInit.get().size() != 1) {
      if (secondaryIndexPartitionsToInit.get().size() > 1) {
        LOG.warn("Skipping secondary index initialization as only one secondary index "
                + "bootstrap at a time is supported for now. Provided: {}",
            secondaryIndexPartitionsToInit.get());

      }
      return Collections.emptyList();
    }
    String indexName = secondaryIndexPartitionsToInit.get().iterator().next();

    HoodieIndexDefinition indexDefinition = getIndexDefinition(dataTableMetaClient, indexName);
    ValidationUtils.checkState(indexDefinition != null, "Secondary Index definition is not present for index " + indexName);
    List<Pair<String, FileSlice>> partitionFileSlicePairs = getPartitionFileSlicePairs(
        dataTableMetaClient, metadata, fsView.get());

    int parallelism = Math.min(partitionFileSlicePairs.size(),
        dataTableWriteConfig.getMetadataConfig().getSecondaryIndexParallelism());
    HoodieData<HoodieRecord> records = readSecondaryKeysFromFileSlices(
        engineContext,
        partitionFileSlicePairs,
        parallelism,
        this.getClass().getSimpleName(),
        dataTableMetaClient,
        engineType,
        indexDefinition);

    // Initialize the file groups - using the same estimation logic as that of record index
    final int numFileGroup = HoodieTableMetadataUtil.estimateFileGroupCount(
        RECORD_INDEX, records.count(), RECORD_INDEX_AVERAGE_RECORD_SIZE,
        dataTableWriteConfig.getRecordIndexMinFileGroupCount(),
        dataTableWriteConfig.getRecordIndexMaxFileGroupCount(),
        dataTableWriteConfig.getRecordIndexGrowthFactor(),
        dataTableWriteConfig.getRecordIndexMaxFileGroupSizeBytes());

    return Collections.singletonList(InitialIndexPartitionData.of(
        numFileGroup, secondaryIndexPartitionsToInit.get().iterator().next(), records));
  }
}
