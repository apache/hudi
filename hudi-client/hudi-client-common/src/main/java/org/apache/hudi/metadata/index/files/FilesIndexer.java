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

package org.apache.hudi.metadata.index.files;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.Indexer;
import org.apache.hudi.metadata.model.FileSliceAndPartition;
import org.apache.hudi.util.Lazy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;

import static org.apache.hudi.metadata.MetadataPartitionType.FILES;

/**
 * Implementation of {@link MetadataPartitionType#FILES} metadata
 */
public class FilesIndexer implements Indexer {

  private static final Logger LOG = LoggerFactory.getLogger(FilesIndexer.class);
  private final HoodieEngineContext engineContext;

  public FilesIndexer(HoodieEngineContext engineContext) {
    this.engineContext = engineContext;
  }

  @Override
  public List<InitialIndexPartitionData> initialize(
      String dataTableInstantTime,
      Map<String, Map<String, Long>> partitionIdToAllFilesMap,
      Lazy<List<FileSliceAndPartition>> lazyLatestMergedPartitionFileSliceList) throws IOException {
    // FILES partition uses a single file group
    final int numFileGroup = 1;

    Set<String> partitions = partitionIdToAllFilesMap.keySet();
    final int totalDataFilesCount = partitionIdToAllFilesMap.values().stream().mapToInt(Map::size).sum();
    LOG.info("Committing total {} partitions and {} files to metadata", partitions.size(), totalDataFilesCount);

    // Record which saves the list of all partitions
    HoodieRecord record = HoodieMetadataPayload.createPartitionListRecord(partitions);
    HoodieData<HoodieRecord> allPartitionsRecord = engineContext.parallelize(Collections.singletonList(record), 1);
    if (partitionIdToAllFilesMap.isEmpty()) {
      return Collections.singletonList(InitialIndexPartitionData.of(
          numFileGroup, FILES.getPartitionPath(), allPartitionsRecord));
    }

    // Records which save the file listing of each partition
    engineContext.setJobStatus(this.getClass().getSimpleName(), "Creating records for metadata FILES partition");
    HoodieData<HoodieRecord> fileListRecords =
        engineContext.parallelize(new ArrayList<>(partitionIdToAllFilesMap.entrySet()), partitionIdToAllFilesMap.size())
            .map(partitionInfo -> {
              Map<String, Long> fileNameToSizeMap = partitionInfo.getValue();
              return HoodieMetadataPayload.createPartitionFilesRecord(
                  partitionInfo.getKey(), fileNameToSizeMap, Collections.emptyList());
            });
    ValidationUtils.checkState(fileListRecords.count() == partitions.size());

    return Collections.singletonList(InitialIndexPartitionData.of(
        numFileGroup, FILES.getPartitionPath(), allPartitionsRecord.union(fileListRecords)));
  }
}
