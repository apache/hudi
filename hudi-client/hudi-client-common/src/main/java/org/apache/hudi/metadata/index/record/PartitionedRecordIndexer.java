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

package org.apache.hudi.metadata.index.record;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.model.DataPartitionAndRecords;
import org.apache.hudi.metadata.model.FileInfo;
import org.apache.hudi.metadata.model.FileSliceAndPartition;
import org.apache.hudi.metadata.index.model.IndexPartitionInitialization;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.record.HoodieRecordIndex;
import org.apache.hudi.metadata.BucketizedMetadataTableFileGroupIndexParser;
import org.apache.hudi.util.Lazy;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.createRecordIndexDefinition;
import static org.apache.hudi.metadata.MetadataPartitionType.RECORD_INDEX;

/**
 * Implementation of the partitioned {@link MetadataPartitionType#RECORD_INDEX} index
 */
@Slf4j
public class PartitionedRecordIndexer extends BaseRecordIndexer {
  public PartitionedRecordIndexer(HoodieEngineContext engineContext, HoodieWriteConfig dataTableWriteConfig,
                                     HoodieTableMetaClient dataTableMetaClient) {
    super(engineContext, dataTableWriteConfig, dataTableMetaClient);
  }

  @Override
  public List<IndexPartitionInitialization> buildInitialization(String dataTableInstantTime, String instantTimeForPartition, Map<String, List<FileInfo>> partitionToAllFilesMap,
                                                                Lazy<List<FileSliceAndPartition>> lazyPartitionFileSlices) throws IOException {
    createRecordIndexDefinition(dataTableMetaClient, Collections.singletonMap(HoodieRecordIndex.IS_PARTITIONED_OPTION, "true"));
    Map<String, List<FileSliceAndPartition>> partitionFileSlicePairsMap = lazyPartitionFileSlices.get().stream()
        .collect(Collectors.groupingBy(FileSliceAndPartition::partitionPath));
    Map<String, DataPartitionAndRecords> fileGroupCountAndRecordsPairMap = new HashMap<>(partitionFileSlicePairsMap.size());
    int maxParallelismPerHudiPartition = partitionFileSlicePairsMap.isEmpty()
        ? 1 : Math.max(1, dataTableWriteConfig.getMetadataConfig().getRecordIndexMaxParallelism() / partitionFileSlicePairsMap.size());
    int totalFileGroupCount = 0;
    for (Map.Entry<String, List<FileSliceAndPartition>> entry: partitionFileSlicePairsMap.entrySet()) {
      String partition = entry.getKey();
      List<FileSliceAndPartition> fileSliceAndPartitions = entry.getValue();
      log.info("Initializing partitioned record index from data partition {}", partition);
      DataPartitionAndRecords dataPartitionAndRecords = initializeRecordIndexPartition(partition, fileSliceAndPartitions, maxParallelismPerHudiPartition);
      fileGroupCountAndRecordsPairMap.put(partition, dataPartitionAndRecords);
      totalFileGroupCount += dataPartitionAndRecords.numFileGroups();
    }
    log.info("Initializing partitioned record index with {} mappings", totalFileGroupCount);

    List<DataPartitionAndRecords> initializationList = new ArrayList<>();
    // Generate the file groups
    TreeMap<String, Integer> partitionSizes = new TreeMap<>();
    for (Map.Entry<String, DataPartitionAndRecords> entry: fileGroupCountAndRecordsPairMap.entrySet()) {
      String dataPartition = entry.getKey();
      DataPartitionAndRecords dataPartitionAndRecords = entry.getValue();
      ValidationUtils.checkArgument(dataPartitionAndRecords.numFileGroups() > 0, "FileGroup count for partitioned RLI data partition " + dataPartition + " should be > 0");
      partitionSizes.put(dataPartition, dataPartitionAndRecords.numFileGroups());
      initializationList.add(dataPartitionAndRecords);
    }

    return Collections.singletonList(IndexPartitionInitialization.of(totalFileGroupCount,
        RECORD_INDEX.getPartitionPath(), new BucketizedMetadataTableFileGroupIndexParser(partitionSizes), initializationList));
  }
}
