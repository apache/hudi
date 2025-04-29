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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.common.data.HoodieAccumulator;
import org.apache.hudi.common.data.HoodieAtomicLongAccumulator;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.index.Indexer;
import org.apache.hudi.util.Lazy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.metadata.MetadataPartitionType.FILES;

public class FilesIndexer implements Indexer {

  private static final Logger LOG = LoggerFactory.getLogger(FilesIndexer.class);
  private final HoodieEngineContext engineContext;

  public FilesIndexer(HoodieEngineContext engineContext) {
    this.engineContext = engineContext;
  }

  @Override
  public List<InitialIndexPartitionData> build(
      List<HoodieTableMetadataUtil.DirectoryInfo> partitionInfoList,
      Map<String, Map<String, Long>> partitionToFilesMap,
      String createInstantTime,
      Lazy<HoodieTableFileSystemView> fsView,
      HoodieBackedTableMetadata metadata,
      String instantTimeForPartition) throws IOException {
    // FILES partition uses a single file group
    final int numFileGroup = 1;

    List<String> partitions = partitionInfoList.stream()
        .map(p -> HoodieTableMetadataUtil.getPartitionIdentifierForFilesPartition(p.getRelativePath()))
        .collect(Collectors.toList());
    final int totalDataFilesCount =
        partitionInfoList.stream().mapToInt(HoodieTableMetadataUtil.DirectoryInfo::getTotalFiles).sum();
    LOG.info("Committing total {} partitions and {} files to metadata", partitions.size(), totalDataFilesCount);

    // Record which saves the list of all partitions
    HoodieRecord record = HoodieMetadataPayload.createPartitionListRecord(partitions);
    HoodieData<HoodieRecord> allPartitionsRecord = engineContext.parallelize(Collections.singletonList(record), 1);
    if (partitionInfoList.isEmpty()) {
      return Collections.singletonList(InitialIndexPartitionData.of(
          numFileGroup, FILES.getPartitionPath(), allPartitionsRecord));
    }

    // Records which save the file listing of each partition
    engineContext.setJobStatus(this.getClass().getSimpleName(), "Creating records for metadata FILES partition");
    HoodieData<HoodieRecord> fileListRecords =
        engineContext.parallelize(partitionInfoList, partitionInfoList.size()).map(partitionInfo -> {
          Map<String, Long> fileNameToSizeMap = partitionInfo.getFileNameToSizeMap();
          return HoodieMetadataPayload.createPartitionFilesRecord(partitionInfo.getRelativePath(), fileNameToSizeMap,
              Collections.emptyList());
        });
    ValidationUtils.checkState(fileListRecords.count() == partitions.size());

    return Collections.singletonList(InitialIndexPartitionData.of(
        numFileGroup, FILES.getPartitionPath(), allPartitionsRecord.union(fileListRecords)));
  }

  @Override
  public List<IndexPartitionData> update(
      String instantTime,
      HoodieBackedTableMetadata tableMetadata,
      Lazy<HoodieTableFileSystemView> lazyFileSystemView, HoodieCommitMetadata commitMetadata) {
    return Collections.singletonList(IndexPartitionData.of(
        FILES.getPartitionPath(), engineContext.parallelize(
            convertMetadataToFilesPartitionRecords(commitMetadata, instantTime), 1)));
  }

  @Override
  public List<IndexPartitionData> clean(
      String instantTime,
      HoodieCleanMetadata cleanMetadata) {
    return Collections.singletonList(IndexPartitionData.of(
        FILES.getPartitionPath(), engineContext.parallelize(
            convertMetadataToFilesPartitionRecords(cleanMetadata, instantTime), 1)));
  }

  /**
   * Finds all new files/partitions created as part of commit and creates metadata table records for them.
   *
   * @param commitMetadata - Commit action metadata
   * @param instantTime    - Commit action instant time
   * @return List of metadata table records
   */
  private static List<HoodieRecord> convertMetadataToFilesPartitionRecords(HoodieCommitMetadata commitMetadata,
                                                                           String instantTime) {
    List<HoodieRecord> records = new ArrayList<>(commitMetadata.getPartitionToWriteStats().size());

    // Add record bearing added partitions list
    List<String> partitionsAdded = getPartitionsAdded(commitMetadata);

    records.add(HoodieMetadataPayload.createPartitionListRecord(partitionsAdded));

    // Update files listing records for each individual partition
    HoodieAccumulator newFileCount = HoodieAtomicLongAccumulator.create();
    List<HoodieRecord<HoodieMetadataPayload>> updatedPartitionFilesRecords =
        commitMetadata.getPartitionToWriteStats().entrySet()
            .stream()
            .map(entry -> {
              String partitionStatName = entry.getKey();
              List<HoodieWriteStat> writeStats = entry.getValue();

              HashMap<String, Long> updatedFilesToSizesMapping =
                  writeStats.stream().reduce(new HashMap<>(writeStats.size()),
                      (map, stat) -> {
                        String pathWithPartition = stat.getPath();
                        if (pathWithPartition == null) {
                          // Empty partition
                          LOG.warn("Unable to find path in write stat to update metadata table {}", stat);
                          return map;
                        }

                        String fileName = FSUtils.getFileName(pathWithPartition, partitionStatName);

                        // Since write-stats are coming in no particular order, if the same
                        // file have previously been appended to w/in the txn, we simply pick max
                        // of the sizes as reported after every write, since file-sizes are
                        // monotonically increasing (ie file-size never goes down, unless deleted)
                        map.merge(fileName, stat.getFileSizeInBytes(), Math::max);

                        Map<String, Long> cdcPathAndSizes = stat.getCdcStats();
                        if (cdcPathAndSizes != null && !cdcPathAndSizes.isEmpty()) {
                          cdcPathAndSizes.forEach((key, value) -> map.put(FSUtils.getFileName(key, partitionStatName), value));
                        }
                        return map;
                      },
                      CollectionUtils::combine);

              newFileCount.add(updatedFilesToSizesMapping.size());
              return HoodieMetadataPayload.createPartitionFilesRecord(partitionStatName, updatedFilesToSizesMapping,
                  Collections.emptyList());
            })
            .collect(Collectors.toList());

    records.addAll(updatedPartitionFilesRecords);

    LOG.info("Updating at {} from Commit/{}. #partitions_updated={}, #files_added={}", instantTime, commitMetadata.getOperationType(),
        records.size(), newFileCount.value());

    return records;
  }

  /**
   * Finds all files that were deleted as part of a clean and creates metadata table records for them.
   *
   * @param cleanMetadata
   * @param instantTime
   * @return a list of metadata table records
   */
  public static List<HoodieRecord> convertMetadataToFilesPartitionRecords(HoodieCleanMetadata cleanMetadata,
                                                                          String instantTime) {
    List<HoodieRecord> records = new LinkedList<>();
    int[] fileDeleteCount = {0};
    List<String> deletedPartitions = new ArrayList<>();
    cleanMetadata.getPartitionMetadata().forEach((partitionName, partitionMetadata) -> {
      // Files deleted from a partition
      List<String> deletedFiles = partitionMetadata.getDeletePathPatterns();
      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partitionName, Collections.emptyMap(),
          deletedFiles);
      records.add(record);
      fileDeleteCount[0] += deletedFiles.size();
      boolean isPartitionDeleted = partitionMetadata.getIsPartitionDeleted();
      if (isPartitionDeleted) {
        deletedPartitions.add(partitionName);
      }
    });

    if (!deletedPartitions.isEmpty()) {
      // if there are partitions to be deleted, add them to delete list
      records.add(HoodieMetadataPayload.createPartitionListRecord(deletedPartitions, true));
    }
    LOG.info("Updating at {} from Clean. #partitions_updated={}, #files_deleted={}, #partitions_deleted={}",
        instantTime, records.size(), fileDeleteCount[0], deletedPartitions.size());
    return records;
  }

  private static List<String> getPartitionsAdded(HoodieCommitMetadata commitMetadata) {
    return commitMetadata.getPartitionToWriteStats().keySet().stream()
        // We need to make sure we properly handle case of non-partitioned tables
        .map(HoodieTableMetadataUtil::getPartitionIdentifierForFilesPartition)
        .collect(Collectors.toList());
  }
}
