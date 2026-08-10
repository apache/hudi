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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.common.data.HoodieAccumulator;
import org.apache.hudi.common.data.HoodieAtomicLongAccumulator;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.BaseIndexer;
import org.apache.hudi.metadata.index.model.IndexCleanContext;
import org.apache.hudi.metadata.index.model.IndexInitializationContext;
import org.apache.hudi.metadata.index.model.IndexInitializationPlan;
import org.apache.hudi.metadata.index.model.IndexPartitionAndRecords;
import org.apache.hudi.metadata.index.model.IndexRestoreContext;
import org.apache.hudi.metadata.index.model.IndexUpdateContext;
import org.apache.hudi.metadata.model.FileInfo;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
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
  public List<IndexInitializationPlan> buildInitialization(IndexInitializationContext context) throws IOException {
    Map<String, List<FileInfo>> partitionToAllFilesMap = context.allFiles();
    // FILES partition uses a single file group
    final int fileGroupCount = 1;

    Set<String> partitions = partitionToAllFilesMap.keySet();
    final int totalDataFilesCount = partitionToAllFilesMap.values().stream().mapToInt(List::size).sum();
    log.info("Committing total {} partitions and {} files to metadata", partitions.size(), totalDataFilesCount);

    // Record which saves the list of all partitions
    HoodieRecord record = HoodieMetadataPayload.createPartitionListRecord(partitions);
    HoodieData<HoodieRecord> allPartitionsRecord = engineContext.parallelize(Collections.singletonList(record), 1);
    if (partitionToAllFilesMap.isEmpty()) {
      return Collections.singletonList(IndexInitializationPlan.of(fileGroupCount, FILES.getPartitionPath(), allPartitionsRecord));
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

    return Collections.singletonList(IndexInitializationPlan.of(fileGroupCount, FILES.getPartitionPath(), allPartitionsRecord.union(fileListRecords)));
  }

  @Override
  public List<IndexPartitionAndRecords> buildUpdate(IndexUpdateContext context) {
    final HoodieData<HoodieRecord> records = engineContext.parallelize(
        convertMetadataToFilesPartitionRecords(context.commitMetadata(), context.instantTime()), 1);
    return Collections.singletonList(IndexPartitionAndRecords.of(FILES.getPartitionPath(), records));
  }

  @Override
  public List<IndexPartitionAndRecords> buildClean(IndexCleanContext context) {
    final HoodieData<HoodieRecord> records = engineContext.parallelize(
        convertMetadataToFilesPartitionRecords(context.cleanMetadata(), context.instantTime()), 1);
    return Collections.singletonList(IndexPartitionAndRecords.of(FILES.getPartitionPath(), records));
  }

  @Override
  public List<IndexPartitionAndRecords> buildRestore(IndexRestoreContext context) {
    List<HoodieRecord> records = new LinkedList<>();
    int[] fileDeleteCount = {0};
    int[] filesAddedCount = {0};

    context.filesAdded().forEach((partition, filesToAdd) -> {
      filesAddedCount[0] += filesToAdd.size();
      List<String> filesToDelete = context.filesDeleted().getOrDefault(partition, Collections.emptyList());
      fileDeleteCount[0] += filesToDelete.size();
      Map<String, Long> fileNameToSize = filesToAdd.stream().collect(Collectors.toMap(FileInfo::fileName, FileInfo::size));
      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partition, fileNameToSize, filesToDelete);
      records.add(record);
    });

    // There could be partitions that only appear in the delete map.
    context.filesDeleted().forEach((partition, filesToDelete) -> {
      if (!context.filesAdded().containsKey(partition)) {
        fileDeleteCount[0] += filesToDelete.size();
        HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partition, Collections.emptyMap(), filesToDelete);
        records.add(record);
      }
    });

    if (!context.deletedPartitions().isEmpty()) {
      // if there are partitions to be deleted, add them to delete list
      records.add(HoodieMetadataPayload.createPartitionListRecord(context.deletedPartitions(), true));
    }

    log.info("Re-adding missing records at {} during Restore. #partitions_updated={}, #files_added={}, #files_deleted={}, #partitions_deleted={}",
        context.instantTime(), records.size(), filesAddedCount[0], fileDeleteCount[0], context.deletedPartitions().size());
    return Collections.singletonList(IndexPartitionAndRecords.of(MetadataPartitionType.FILES.getPartitionPath(), engineContext.parallelize(records, 1)));
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

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

              HashMap<String, Long> updatedFilesToSizesMapping = new HashMap<>(writeStats.size());
              for (HoodieWriteStat stat : writeStats) {
                String pathWithPartition = stat.getPath();
                if (pathWithPartition == null) {
                  // Empty partition
                  log.warn("Unable to find path in write stat to update metadata table {}", stat);
                  continue;
                }

                String fileName = FSUtils.getFileName(pathWithPartition, partitionStatName);

                // Since write-stats are coming in no particular order, if the same
                // file have previously been appended to w/in the txn, we simply pick max
                // of the sizes as reported after every write, since file-sizes are
                // monotonically increasing (ie file-size never goes down, unless deleted)
                updatedFilesToSizesMapping.merge(fileName, stat.getFileSizeInBytes(), Math::max);

                Map<String, Long> cdcPathAndSizes = stat.getCdcStats();
                if (cdcPathAndSizes != null && !cdcPathAndSizes.isEmpty()) {
                  cdcPathAndSizes.forEach((key, value) ->
                      updatedFilesToSizesMapping.put(FSUtils.getFileName(key, partitionStatName), value));
                }
              }

              newFileCount.add(updatedFilesToSizesMapping.size());
              return HoodieMetadataPayload.createPartitionFilesRecord(partitionStatName, updatedFilesToSizesMapping,
                  Collections.emptyList());
            })
            .collect(Collectors.toList());

    records.addAll(updatedPartitionFilesRecords);

    log.info("Updating at {} from Commit/{}. #partitions_updated={}, #files_added={}", instantTime, commitMetadata.getOperationType(),
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
  private static List<HoodieRecord> convertMetadataToFilesPartitionRecords(HoodieCleanMetadata cleanMetadata,
                                                                          String instantTime) {
    List<HoodieRecord> records = new LinkedList<>();
    int[] fileDeleteCount = {0};
    List<String> deletedPartitions = new ArrayList<>();
    cleanMetadata.getPartitionMetadata().forEach((partitionName, partitionMetadata) -> {
      boolean isPartitionDeleted = partitionMetadata.getIsPartitionDeleted();
      // Files deleted from a partition
      List<String> deletedFiles = partitionMetadata.getDeletePathPatterns();
      records.add(HoodieMetadataPayload.createPartitionFilesRecord(partitionName, Collections.emptyMap(),
          deletedFiles, isPartitionDeleted));
      fileDeleteCount[0] += deletedFiles.size();
      if (isPartitionDeleted) {
        deletedPartitions.add(partitionName);
      }
    });

    if (!deletedPartitions.isEmpty()) {
      // if there are partitions to be deleted, add them to delete list
      records.add(HoodieMetadataPayload.createPartitionListRecord(deletedPartitions, true));
    }
    log.info("Updating at {} from Clean. #partitions_updated={}, #files_deleted={}, #partitions_deleted={}",
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
