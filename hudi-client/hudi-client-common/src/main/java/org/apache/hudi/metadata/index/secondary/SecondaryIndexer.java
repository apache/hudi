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
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.BaseIndexer;
import org.apache.hudi.metadata.index.model.IndexCleanContext;
import org.apache.hudi.metadata.index.model.IndexInitializationContext;
import org.apache.hudi.metadata.index.model.IndexInitializationPlan;
import org.apache.hudi.metadata.index.model.IndexPartitionAndRecords;
import org.apache.hudi.metadata.index.model.IndexUpdateContext;
import org.apache.hudi.metadata.model.FileSliceAndPartition;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX_PREFIX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.RECORD_INDEX_AVERAGE_RECORD_SIZE;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getSecondaryIndexPartitionsToInit;
import static org.apache.hudi.metadata.MetadataPartitionType.RECORD_INDEX;
import static org.apache.hudi.metadata.MetadataPartitionType.SECONDARY_INDEX;
import static org.apache.hudi.metadata.SecondaryIndexRecordGenerationUtils.convertWriteStatsToSecondaryIndexRecords;
import static org.apache.hudi.metadata.SecondaryIndexRecordGenerationUtils.readSecondaryKeysFromFileSlices;

/**
 * Implementation of {@link MetadataPartitionType#SECONDARY_INDEX} index
 */
@Slf4j
public class SecondaryIndexer extends BaseIndexer {

  public SecondaryIndexer(
      HoodieEngineContext engineContext,
      HoodieWriteConfig dataTableWriteConfig,
      HoodieTableMetaClient dataTableMetaClient) {
    super(engineContext, dataTableWriteConfig, dataTableMetaClient);
  }

  @Override
  public List<IndexInitializationPlan> buildInitialization(IndexInitializationContext context) throws IOException {
    Set<String> secondaryIndexPartitionsToInit = getSecondaryIndexPartitionsToInit(SECONDARY_INDEX, dataTableWriteConfig.getMetadataConfig(), dataTableMetaClient);
    if (secondaryIndexPartitionsToInit.size() > 1) {
      log.warn("Skipping secondary index initialization as only one secondary index bootstrap at a time is supported for now. Provided: {}", secondaryIndexPartitionsToInit);
      return Collections.emptyList();
    }
    if (secondaryIndexPartitionsToInit.isEmpty()) {
      return Collections.emptyList();
    }

    String indexName = secondaryIndexPartitionsToInit.iterator().next();
    HoodieIndexDefinition indexDefinition = HoodieTableMetadataUtil.getHoodieIndexDefinition(indexName, dataTableMetaClient);
    ValidationUtils.checkState(indexDefinition != null, "Secondary Index definition is not present for index " + indexName);

    List<FileSliceAndPartition> fileSlices = context.latestFileSlices().get();

    int parallelism = Math.min(fileSlices.size(), dataTableWriteConfig.getMetadataConfig().getSecondaryIndexParallelism());
    HoodieData<HoodieRecord> records = readSecondaryKeysFromFileSlices(
        engineContext,
        fileSlices,
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

    return Collections.singletonList(IndexInitializationPlan.of(fileGroupCount, indexName, records));
  }

  @Override
  public List<IndexPartitionAndRecords> buildUpdate(IndexUpdateContext context) {
    if (!SECONDARY_INDEX.isMetadataPartitionAvailable(dataTableMetaClient)) {
      return Collections.emptyList();
    }
    // If write operation type based on commit metadata is COMPACT or CLUSTER then no need to update,
    // because these operations do not change the secondary key - record key mapping.
    WriteOperationType operationType = context.commitMetadata().getOperationType();
    if (operationType.isInsertOverwriteOrDeletePartition()) {
      throw new HoodieIndexException(String.format("Can not perform operation %s on secondary index", operationType));
    } else if (operationType == WriteOperationType.COMPACT || operationType == WriteOperationType.CLUSTER) {
      return Collections.emptyList();
    }

    return dataTableMetaClient.getTableConfig().getMetadataPartitions()
        .stream()
        .filter(partition -> partition.startsWith(PARTITION_NAME_SECONDARY_INDEX_PREFIX))
        .map(partition -> {
          HoodieData<HoodieRecord> secondaryIndexRecords;
          try {
            secondaryIndexRecords = getSecondaryIndexUpdates(context.commitMetadata(), partition, context.instantTime());
          } catch (Exception e) {
            throw new HoodieMetadataException("Failed to get secondary index updates for partition " + partition, e);
          }
          return IndexPartitionAndRecords.of(partition, secondaryIndexRecords);
        }).collect(Collectors.toList());
  }

  @Override
  public List<IndexPartitionAndRecords> buildClean(IndexCleanContext context) {
    return Collections.emptyList();
  }

  private HoodieData<HoodieRecord> getSecondaryIndexUpdates(HoodieCommitMetadata commitMetadata, String indexPartition, String instantTime) {
    List<HoodieWriteStat> allWriteStats = commitMetadata.getPartitionToWriteStats().values().stream()
        .flatMap(Collection::stream).collect(Collectors.toList());
    // Return early if there are no write stats, or if this helper is reached for a table-service operation.
    if (allWriteStats.isEmpty() || WriteOperationType.isCompactionOrClustering(commitMetadata.getOperationType())) {
      return engineContext.emptyHoodieData();
    }
    HoodieIndexDefinition indexDefinition = HoodieTableMetadataUtil.getHoodieIndexDefinition(indexPartition, dataTableMetaClient);
    return convertWriteStatsToSecondaryIndexRecords(allWriteStats, instantTime, indexDefinition,
        dataTableWriteConfig.getMetadataConfig(), dataTableMetaClient, engineContext, dataTableWriteConfig);
  }
}
