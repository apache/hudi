/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.metadata.index.files;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.metadata.model.FileInfo;
import org.apache.hudi.metadata.model.FileSliceAndPartition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.metadata.index.model.IndexPartitionInitialization;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieMetadataPayload;
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

import static org.apache.hudi.metadata.MetadataPartitionType.FILES;

/**
 * Implementation of {@link MetadataPartitionType#FILES} metadata
 */
@Slf4j
public class FilesIndexer extends BaseIndexer {
  public FilesIndexer(HoodieEngineContext engineContext, HoodieWriteConfig dataTableWriteConfig,
                         HoodieTableMetaClient dataTableMetaClient) {
    super(engineContext, dataTableWriteConfig, dataTableMetaClient);
  }

  @Override
  public List<IndexPartitionInitialization> buildInitialization(String dataTableInstantTime, String instantTimeForPartition, Map<String, List<FileInfo>> partitionToAllFilesMap,
                                                                Lazy<List<FileSliceAndPartition>> lazyLatestMergedPartitionFileSliceList) throws IOException {
    // FILES partition uses a single file group
    final int fileGroupCount = 1;

    Set<String> partitions = partitionToAllFilesMap.keySet();
    final int totalDataFilesCount = partitionToAllFilesMap.values().stream().mapToInt(List::size).sum();
    log.info("Committing total {} partitions and {} files to metadata", partitions.size(), totalDataFilesCount);

    // Record which saves the list of all partitions
    HoodieRecord record = HoodieMetadataPayload.createPartitionListRecord(partitions);
    HoodieData<HoodieRecord> allPartitionsRecord = engineContext.parallelize(Collections.singletonList(record), 1);
    if (partitionToAllFilesMap.isEmpty()) {
      return Collections.singletonList(IndexPartitionInitialization.of(fileGroupCount, FILES.getPartitionPath(), allPartitionsRecord));
    }

    // Records which save the file listing of each partition
    engineContext.setJobStatus(this.getClass().getSimpleName(), "Creating records for metadata FILES partition");
    HoodieData<HoodieRecord> fileListRecords = engineContext.parallelize(
            new ArrayList<>(partitionToAllFilesMap.entrySet()), partitionToAllFilesMap.size())
        .map(partitionInfo -> {
          Map<String, Long> fileNameToSizeMap = partitionInfo.getValue().stream()
              .collect(Collectors.toMap(FileInfo::fileName, FileInfo::size));
          return HoodieMetadataPayload.createPartitionFilesRecord(
              partitionInfo.getKey(), fileNameToSizeMap, Collections.emptyList());
        });
    ValidationUtils.checkState(fileListRecords.count() == partitions.size());

    return Collections.singletonList(IndexPartitionInitialization.of(fileGroupCount, FILES.getPartitionPath(), allPartitionsRecord.union(fileListRecords)));
  }
}
