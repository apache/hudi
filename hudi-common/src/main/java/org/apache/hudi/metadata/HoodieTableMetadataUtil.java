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

package org.apache.hudi.metadata;

import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.metadata.HoodieTableMetadata.EMPTY_PARTITION_NAME;
import static org.apache.hudi.metadata.HoodieTableMetadata.NON_PARTITIONED_NAME;

/**
 * A utility to convert timeline information to metadata table records.
 */
public class HoodieTableMetadataUtil {

  private static final Logger LOG = LogManager.getLogger(HoodieTableMetadataUtil.class);

  protected static final String PARTITION_NAME_FILES = "files";
  protected static final String PARTITION_NAME_COLUMN_STATS = "column_stats";
  protected static final String PARTITION_NAME_BLOOM_FILTERS = "bloom_filters";

  /**
   * Delete the metadata table for the dataset. This will be invoked during upgrade/downgrade operation during which
   * no other
   * process should be running.
   *
   * @param basePath base path of the dataset
   * @param context  instance of {@link HoodieEngineContext}.
   */
  public static void deleteMetadataTable(String basePath, HoodieEngineContext context) {
    final String metadataTablePath = HoodieTableMetadata.getMetadataTableBasePath(basePath);
    FileSystem fs = FSUtils.getFs(metadataTablePath, context.getHadoopConf().get());
    try {
      fs.delete(new Path(metadataTablePath), true);
    } catch (Exception e) {
      throw new HoodieMetadataException("Failed to remove metadata table from path " + metadataTablePath, e);
    }
  }

  /**
   * Convert commit action to metadata records for the enabled partition types.
   *
   * @param commitMetadata                      - Commit action metadata
   * @param dataMetaClient                      - Meta client for the data table
   * @param isMetaIndexColumnStatsForAllColumns - Do all columns need meta indexing?
   * @param instantTime                         - Action instant time
   * @return Map of partition to metadata records for the commit action
   */
  public static Map<MetadataPartitionType, HoodieData<HoodieRecord>> convertMetadataToRecords(
      HoodieEngineContext context, List<MetadataPartitionType> enabledPartitionTypes,
      HoodieCommitMetadata commitMetadata, HoodieTableMetaClient dataMetaClient,
      boolean isMetaIndexColumnStatsForAllColumns, String instantTime) {
    final Map<MetadataPartitionType, HoodieData<HoodieRecord>> partitionToRecordsMap = new HashMap<>();
    final HoodieData<HoodieRecord> filesPartitionRecordsRDD = context.parallelize(
        convertMetadataToFilesPartitionRecords(commitMetadata, instantTime), 1);
    partitionToRecordsMap.put(MetadataPartitionType.FILES, filesPartitionRecordsRDD);

    if (enabledPartitionTypes.contains(MetadataPartitionType.BLOOM_FILTERS)) {
      final List<HoodieRecord> metadataBloomFilterRecords = convertMetadataToBloomFilterRecords(commitMetadata,
          dataMetaClient, instantTime);
      if (!metadataBloomFilterRecords.isEmpty()) {
        final HoodieData<HoodieRecord> metadataBloomFilterRecordsRDD = context.parallelize(metadataBloomFilterRecords, 1);
        partitionToRecordsMap.put(MetadataPartitionType.BLOOM_FILTERS, metadataBloomFilterRecordsRDD);
      }
    }

    if (enabledPartitionTypes.contains(MetadataPartitionType.COLUMN_STATS)) {
      final List<HoodieRecord> metadataColumnStats = convertMetadataToColumnStatsRecords(commitMetadata, context,
          dataMetaClient, isMetaIndexColumnStatsForAllColumns, instantTime);
      if (!metadataColumnStats.isEmpty()) {
        final HoodieData<HoodieRecord> metadataColumnStatsRDD = context.parallelize(metadataColumnStats, 1);
        partitionToRecordsMap.put(MetadataPartitionType.COLUMN_STATS, metadataColumnStatsRDD);
      }
    }
    return partitionToRecordsMap;
  }

  /**
   * Finds all new files/partitions created as part of commit and creates metadata table records for them.
   *
   * @param commitMetadata - Commit action metadata
   * @param instantTime    - Commit action instant time
   * @return List of metadata table records
   */
  public static List<HoodieRecord> convertMetadataToFilesPartitionRecords(HoodieCommitMetadata commitMetadata,
                                                                          String instantTime) {
    List<HoodieRecord> records = new LinkedList<>();
    List<String> allPartitions = new LinkedList<>();
    commitMetadata.getPartitionToWriteStats().forEach((partitionStatName, writeStats) -> {
      final String partition = partitionStatName.equals(EMPTY_PARTITION_NAME) ? NON_PARTITIONED_NAME : partitionStatName;
      allPartitions.add(partition);

      Map<String, Long> newFiles = new HashMap<>(writeStats.size());
      writeStats.forEach(hoodieWriteStat -> {
        String pathWithPartition = hoodieWriteStat.getPath();
        if (pathWithPartition == null) {
          // Empty partition
          LOG.warn("Unable to find path in write stat to update metadata table " + hoodieWriteStat);
          return;
        }

        int offset = partition.equals(NON_PARTITIONED_NAME) ? (pathWithPartition.startsWith("/") ? 1 : 0) : partition.length() + 1;
        String filename = pathWithPartition.substring(offset);
        long totalWriteBytes = newFiles.containsKey(filename)
            ? newFiles.get(filename) + hoodieWriteStat.getTotalWriteBytes()
            : hoodieWriteStat.getTotalWriteBytes();
        newFiles.put(filename, totalWriteBytes);
      });
      // New files added to a partition
      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(
          partition, Option.of(newFiles), Option.empty());
      records.add(record);
    });

    // New partitions created
    HoodieRecord record = HoodieMetadataPayload.createPartitionListRecord(new ArrayList<>(allPartitions));
    records.add(record);

    LOG.info("Updating at " + instantTime + " from Commit/" + commitMetadata.getOperationType()
        + ". #partitions_updated=" + records.size());
    return records;
  }

  /**
   * Convert commit action metadata to bloom filter records.
   *
   * @param commitMetadata - Commit action metadata
   * @param dataMetaClient - Meta client for the data table
   * @param instantTime    - Action instant time
   * @return List of metadata table records
   */
  public static List<HoodieRecord> convertMetadataToBloomFilterRecords(HoodieCommitMetadata commitMetadata,
                                                                       HoodieTableMetaClient dataMetaClient,
                                                                       String instantTime) {
    List<HoodieRecord> records = new LinkedList<>();
    commitMetadata.getPartitionToWriteStats().forEach((partitionStatName, writeStats) -> {
      final String partition = partitionStatName.equals(EMPTY_PARTITION_NAME) ? NON_PARTITIONED_NAME : partitionStatName;
      Map<String, Long> newFiles = new HashMap<>(writeStats.size());
      writeStats.forEach(hoodieWriteStat -> {
        // No action for delta logs
        if (hoodieWriteStat instanceof HoodieDeltaWriteStat) {
          return;
        }

        String pathWithPartition = hoodieWriteStat.getPath();
        if (pathWithPartition == null) {
          // Empty partition
          LOG.error("Failed to find path in write stat to update metadata table " + hoodieWriteStat);
          return;
        }
        int offset = partition.equals(NON_PARTITIONED_NAME) ? (pathWithPartition.startsWith("/") ? 1 : 0) :
            partition.length() + 1;

        final String fileName = pathWithPartition.substring(offset);
        if (!FSUtils.isBaseFile(new Path(fileName))) {
          return;
        }
        ValidationUtils.checkState(!newFiles.containsKey(fileName), "Duplicate files in HoodieCommitMetadata");

        final Path writeFilePath = new Path(dataMetaClient.getBasePath(), pathWithPartition);
        try {
          HoodieFileReader<IndexedRecord> fileReader =
              HoodieFileReaderFactory.getFileReader(dataMetaClient.getHadoopConf(), writeFilePath);
          try {
            final BloomFilter fileBloomFilter = fileReader.readBloomFilter();
            if (fileBloomFilter == null) {
              LOG.error("Failed to read bloom filter for " + writeFilePath);
              return;
            }
            ByteBuffer bloomByteBuffer = ByteBuffer.wrap(fileBloomFilter.serializeToString().getBytes());
            HoodieRecord record = HoodieMetadataPayload.createBloomFilterMetadataRecord(
                partition, fileName, instantTime, bloomByteBuffer, false);
            records.add(record);
          } catch (Exception e) {
            LOG.error("Failed to read bloom filter for " + writeFilePath);
            return;
          }
          fileReader.close();
        } catch (IOException e) {
          LOG.error("Failed to get bloom filter for file: " + writeFilePath + ", write stat: " + hoodieWriteStat);
        }
      });
    });

    return records;
  }

  /**
   * Convert the clean action to metadata records.
   */
  public static Map<MetadataPartitionType, HoodieData<HoodieRecord>> convertMetadataToRecords(
      HoodieEngineContext engineContext, List<MetadataPartitionType> enabledPartitionTypes,
      HoodieCleanMetadata cleanMetadata, HoodieTableMetaClient dataMetaClient, String instantTime) {
    final Map<MetadataPartitionType, HoodieData<HoodieRecord>> partitionToRecordsMap = new HashMap<>();
    final HoodieData<HoodieRecord> filesPartitionRecordsRDD = engineContext.parallelize(
        convertMetadataToFilesPartitionRecords(cleanMetadata, instantTime), 1);
    partitionToRecordsMap.put(MetadataPartitionType.FILES, filesPartitionRecordsRDD);

    if (enabledPartitionTypes.contains(MetadataPartitionType.BLOOM_FILTERS)) {
      final List<HoodieRecord> metadataBloomFilterRecords = convertMetadataToBloomFilterRecords(cleanMetadata,
          engineContext, instantTime);
      if (!metadataBloomFilterRecords.isEmpty()) {
        final HoodieData<HoodieRecord> metadataBloomFilterRecordsRDD = engineContext.parallelize(metadataBloomFilterRecords, 1);
        partitionToRecordsMap.put(MetadataPartitionType.BLOOM_FILTERS, metadataBloomFilterRecordsRDD);
      }
    }

    if (enabledPartitionTypes.contains(MetadataPartitionType.COLUMN_STATS)) {
      final List<HoodieRecord> metadataColumnStats = convertMetadataToColumnStatsRecords(cleanMetadata, engineContext,
          dataMetaClient);
      if (!metadataColumnStats.isEmpty()) {
        final HoodieData<HoodieRecord> metadataColumnStatsRDD = engineContext.parallelize(metadataColumnStats, 1);
        partitionToRecordsMap.put(MetadataPartitionType.COLUMN_STATS, metadataColumnStatsRDD);
      }
    }

    return partitionToRecordsMap;
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
    cleanMetadata.getPartitionMetadata().forEach((partitionName, partitionMetadata) -> {
      final String partition = partitionName.equals(EMPTY_PARTITION_NAME) ? NON_PARTITIONED_NAME : partitionName;
      // Files deleted from a partition
      List<String> deletedFiles = partitionMetadata.getDeletePathPatterns();
      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partition, Option.empty(),
          Option.of(new ArrayList<>(deletedFiles)));

      records.add(record);
      fileDeleteCount[0] += deletedFiles.size();
    });

    LOG.info("Updating at " + instantTime + " from Clean. #partitions_updated=" + records.size()
        + ", #files_deleted=" + fileDeleteCount[0]);
    return records;
  }

  /**
   * Convert clean metadata to bloom filter index records.
   *
   * @param cleanMetadata - Clean action metadata
   * @param engineContext - Engine context
   * @param instantTime   - Clean action instant time
   * @return List of bloom filter index records for the clean metadata
   */
  public static List<HoodieRecord> convertMetadataToBloomFilterRecords(HoodieCleanMetadata cleanMetadata,
                                                                       HoodieEngineContext engineContext,
                                                                       String instantTime) {
    List<Pair<String, String>> deleteFileList = new ArrayList<>();
    cleanMetadata.getPartitionMetadata().forEach((partition, partitionMetadata) -> {
      // Files deleted from a partition
      List<String> deletedFiles = partitionMetadata.getDeletePathPatterns();
      deletedFiles.forEach(entry -> {
        final Path deletedFilePath = new Path(entry);
        if (FSUtils.isBaseFile(deletedFilePath)) {
          deleteFileList.add(Pair.of(partition, deletedFilePath.getName()));
        }
      });
    });

    return engineContext.map(deleteFileList, deleteFileInfo -> {
      return HoodieMetadataPayload.createBloomFilterMetadataRecord(
          deleteFileInfo.getLeft(), deleteFileInfo.getRight(), instantTime, ByteBuffer.allocate(0), true);
    }, 1).stream().collect(Collectors.toList());
  }

  /**
   * Convert clean metadata to column stats index records.
   *
   * @param cleanMetadata     - Clean action metadata
   * @param engineContext     - Engine context
   * @param datasetMetaClient - data table meta client
   * @return List of column stats index records for the clean metadata
   */
  public static List<HoodieRecord> convertMetadataToColumnStatsRecords(HoodieCleanMetadata cleanMetadata,
                                                                       HoodieEngineContext engineContext,
                                                                       HoodieTableMetaClient datasetMetaClient) {
    List<Pair<String, String>> deleteFileList = new ArrayList<>();
    cleanMetadata.getPartitionMetadata().forEach((partition, partitionMetadata) -> {
      // Files deleted from a partition
      List<String> deletedFiles = partitionMetadata.getDeletePathPatterns();
      deletedFiles.forEach(entry -> deleteFileList.add(Pair.of(partition, entry)));
    });

    List<String> latestColumns = getLatestColumns(datasetMetaClient);
    return engineContext.flatMap(deleteFileList,
        deleteFileInfo -> {
          if (deleteFileInfo.getRight().endsWith(HoodieFileFormat.PARQUET.getFileExtension())) {
            return getColumnStats(deleteFileInfo.getKey(), deleteFileInfo.getValue(), datasetMetaClient,
                latestColumns, true);
          }
          return Stream.empty();
        }, 1).stream().collect(Collectors.toList());
  }

  /**
   * Convert restore action metadata to metadata table records.
   */
  public static Map<MetadataPartitionType, HoodieData<HoodieRecord>> convertMetadataToRecords(
      HoodieEngineContext engineContext, List<MetadataPartitionType> enabledPartitionTypes,
      HoodieActiveTimeline metadataTableTimeline, HoodieRestoreMetadata restoreMetadata,
      HoodieTableMetaClient dataMetaClient, String instantTime, Option<String> lastSyncTs) {
    final Map<MetadataPartitionType, HoodieData<HoodieRecord>> partitionToRecordsMap = new HashMap<>();
    final Map<String, Map<String, Long>> partitionToAppendedFiles = new HashMap<>();
    final Map<String, List<String>> partitionToDeletedFiles = new HashMap<>();

    processRestoreMetadata(metadataTableTimeline, restoreMetadata,
        partitionToAppendedFiles, partitionToDeletedFiles, lastSyncTs);

    final HoodieData<HoodieRecord> filesPartitionRecordsRDD = engineContext.parallelize(
        convertFilesToFilesPartitionRecords(partitionToDeletedFiles,
            partitionToAppendedFiles, instantTime, "Restore"), 1);
    partitionToRecordsMap.put(MetadataPartitionType.FILES, filesPartitionRecordsRDD);

    if (enabledPartitionTypes.contains(MetadataPartitionType.BLOOM_FILTERS)) {
      final List<HoodieRecord> metadataBloomFilterRecords = convertFilesToBloomFilterRecords(
          engineContext, dataMetaClient, partitionToDeletedFiles, partitionToAppendedFiles, instantTime);
      if (!metadataBloomFilterRecords.isEmpty()) {
        final HoodieData<HoodieRecord> metadataBloomFilterRecordsRDD = engineContext.parallelize(metadataBloomFilterRecords, 1);
        partitionToRecordsMap.put(MetadataPartitionType.BLOOM_FILTERS, metadataBloomFilterRecordsRDD);
      }
    }

    if (enabledPartitionTypes.contains(MetadataPartitionType.COLUMN_STATS)) {
      final List<HoodieRecord> metadataColumnStats = convertFilesToColumnStatsRecords(
          engineContext, dataMetaClient, partitionToDeletedFiles, partitionToAppendedFiles, instantTime);
      if (!metadataColumnStats.isEmpty()) {
        final HoodieData<HoodieRecord> metadataColumnStatsRDD = engineContext.parallelize(metadataColumnStats, 1);
        partitionToRecordsMap.put(MetadataPartitionType.COLUMN_STATS, metadataColumnStatsRDD);
      }
    }

    return partitionToRecordsMap;
  }

  /**
   * Aggregates all files deleted and appended to from all rollbacks associated with a restore operation then
   * creates metadata table records for them.
   *
   * @param restoreMetadata - Restore action metadata
   * @return a list of metadata table records
   */
  private static void processRestoreMetadata(HoodieActiveTimeline metadataTableTimeline,
                                             HoodieRestoreMetadata restoreMetadata,
                                             Map<String, Map<String, Long>> partitionToAppendedFiles,
                                             Map<String, List<String>> partitionToDeletedFiles,
                                             Option<String> lastSyncTs) {
    restoreMetadata.getHoodieRestoreMetadata().values().forEach(rms -> {
      rms.forEach(rm -> processRollbackMetadata(metadataTableTimeline, rm,
          partitionToDeletedFiles, partitionToAppendedFiles, lastSyncTs));
    });
  }

  /**
   * Convert rollback action metadata to metadata table records.
   */
  public static Map<MetadataPartitionType, HoodieData<HoodieRecord>> convertMetadataToRecords(
      HoodieEngineContext engineContext, List<MetadataPartitionType> enabledPartitionTypes,
      HoodieActiveTimeline metadataTableTimeline, HoodieRollbackMetadata rollbackMetadata,
      HoodieTableMetaClient dataMetaClient, String instantTime, Option<String> lastSyncTs, boolean wasSynced) {
    final Map<MetadataPartitionType, HoodieData<HoodieRecord>> partitionToRecordsMap = new HashMap<>();

    Map<String, List<String>> partitionToDeletedFiles = new HashMap<>();
    Map<String, Map<String, Long>> partitionToAppendedFiles = new HashMap<>();
    List<HoodieRecord> filesPartitionRecords = convertMetadataToRollbackRecords(metadataTableTimeline, rollbackMetadata,
        partitionToDeletedFiles, partitionToAppendedFiles, instantTime, lastSyncTs, wasSynced);
    final HoodieData<HoodieRecord> rollbackRecordsRDD = engineContext.parallelize(filesPartitionRecords, 1);
    partitionToRecordsMap.put(MetadataPartitionType.FILES, rollbackRecordsRDD);

    if (enabledPartitionTypes.contains(MetadataPartitionType.BLOOM_FILTERS)) {
      final List<HoodieRecord> metadataBloomFilterRecords = convertFilesToBloomFilterRecords(
          engineContext, dataMetaClient, partitionToDeletedFiles, partitionToAppendedFiles, instantTime);
      if (!metadataBloomFilterRecords.isEmpty()) {
        final HoodieData<HoodieRecord> metadataBloomFilterRecordsRDD = engineContext.parallelize(metadataBloomFilterRecords, 1);
        partitionToRecordsMap.put(MetadataPartitionType.BLOOM_FILTERS, metadataBloomFilterRecordsRDD);
      }
    }

    if (enabledPartitionTypes.contains(MetadataPartitionType.COLUMN_STATS)) {
      final List<HoodieRecord> metadataColumnStats = convertFilesToColumnStatsRecords(
          engineContext, dataMetaClient, partitionToDeletedFiles, partitionToAppendedFiles, instantTime);
      if (!metadataColumnStats.isEmpty()) {
        final HoodieData<HoodieRecord> metadataColumnStatsRDD = engineContext.parallelize(metadataColumnStats, 1);
        partitionToRecordsMap.put(MetadataPartitionType.COLUMN_STATS, metadataColumnStatsRDD);
      }
    }

    return partitionToRecordsMap;
  }

  /**
   * Convert rollback action metadata to files partition records.
   */
  private static List<HoodieRecord> convertMetadataToRollbackRecords(HoodieActiveTimeline metadataTableTimeline,
                                                                     HoodieRollbackMetadata rollbackMetadata,
                                                                     Map<String, List<String>> partitionToDeletedFiles,
                                                                     Map<String, Map<String, Long>> partitionToAppendedFiles,
                                                                     String instantTime,
                                                                     Option<String> lastSyncTs, boolean wasSynced) {
    processRollbackMetadata(metadataTableTimeline, rollbackMetadata, partitionToDeletedFiles,
        partitionToAppendedFiles, lastSyncTs);
    if (!wasSynced) {
      // Since the instant-being-rolled-back was never committed to the metadata table, the files added there
      // need not be deleted. For MOR Table, the rollback appends logBlocks so we need to keep the appended files.
      partitionToDeletedFiles.clear();
    }
    return convertFilesToFilesPartitionRecords(partitionToDeletedFiles, partitionToAppendedFiles, instantTime, "Rollback");
  }

  /**
   * Extracts information about the deleted and append files from the {@code HoodieRollbackMetadata}.
   * <p>
   * During a rollback files may be deleted (COW, MOR) or rollback blocks be appended (MOR only) to files. This
   * function will extract this change file for each partition.
   *
   * @param metadataTableTimeline    Current timeline of the Metadata Table
   * @param rollbackMetadata         {@code HoodieRollbackMetadata}
   * @param partitionToDeletedFiles  The {@code Map} to fill with files deleted per partition.
   * @param partitionToAppendedFiles The {@code Map} to fill with files appended per partition and their sizes.
   */
  private static void processRollbackMetadata(HoodieActiveTimeline metadataTableTimeline,
                                              HoodieRollbackMetadata rollbackMetadata,
                                              Map<String, List<String>> partitionToDeletedFiles,
                                              Map<String, Map<String, Long>> partitionToAppendedFiles,
                                              Option<String> lastSyncTs) {
    rollbackMetadata.getPartitionMetadata().values().forEach(pm -> {
      final String instantToRollback = rollbackMetadata.getCommitsRollback().get(0);
      // Has this rollback produced new files?
      boolean hasRollbackLogFiles = pm.getRollbackLogFiles() != null && !pm.getRollbackLogFiles().isEmpty();
      boolean hasNonZeroRollbackLogFiles = hasRollbackLogFiles && pm.getRollbackLogFiles().values().stream().mapToLong(Long::longValue).sum() > 0;

      // If instant-to-rollback has not been synced to metadata table yet then there is no need to update metadata
      // This can happen in two cases:
      //  Case 1: Metadata Table timeline is behind the instant-to-rollback.
      boolean shouldSkip = lastSyncTs.isPresent()
          && HoodieTimeline.compareTimestamps(instantToRollback, HoodieTimeline.GREATER_THAN, lastSyncTs.get());

      if (!hasNonZeroRollbackLogFiles && shouldSkip) {
        LOG.info(String.format("Skipping syncing of rollbackMetadata at %s, given metadata table is already synced upto to %s",
            instantToRollback, lastSyncTs.get()));
        return;
      }

      // Case 2: The instant-to-rollback was never committed to Metadata Table. This can happen if the instant-to-rollback
      // was a failed commit (never completed) as only completed instants are synced to Metadata Table.
      // But the required Metadata Table instants should not have been archived
      HoodieInstant syncedInstant = new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION, instantToRollback);
      if (metadataTableTimeline.getCommitsTimeline().isBeforeTimelineStarts(syncedInstant.getTimestamp())) {
        throw new HoodieMetadataException(String.format("The instant %s required to sync rollback of %s has been archived",
            syncedInstant, instantToRollback));
      }

      shouldSkip = !metadataTableTimeline.containsInstant(syncedInstant);
      if (!hasNonZeroRollbackLogFiles && shouldSkip) {
        LOG.info(String.format("Skipping syncing of rollbackMetadata at %s, since this instant was never committed to Metadata Table",
            instantToRollback));
        return;
      }

      final String partition = pm.getPartitionPath();
      if ((!pm.getSuccessDeleteFiles().isEmpty() || !pm.getFailedDeleteFiles().isEmpty()) && !shouldSkip) {
        if (!partitionToDeletedFiles.containsKey(partition)) {
          partitionToDeletedFiles.put(partition, new ArrayList<>());
        }

        // Extract deleted file name from the absolute paths saved in getSuccessDeleteFiles()
        List<String> deletedFiles = pm.getSuccessDeleteFiles().stream().map(p -> new Path(p).getName())
            .collect(Collectors.toList());
        if (!pm.getFailedDeleteFiles().isEmpty()) {
          deletedFiles.addAll(pm.getFailedDeleteFiles().stream().map(p -> new Path(p).getName())
              .collect(Collectors.toList()));
        }
        partitionToDeletedFiles.get(partition).addAll(deletedFiles);
      }

      BiFunction<Long, Long, Long> fileMergeFn = (oldSize, newSizeCopy) -> {
        // if a file exists in both written log files and rollback log files, we want to pick the one that is higher
        // as rollback file could have been updated after written log files are computed.
        return oldSize > newSizeCopy ? oldSize : newSizeCopy;
      };

      if (hasRollbackLogFiles) {
        if (!partitionToAppendedFiles.containsKey(partition)) {
          partitionToAppendedFiles.put(partition, new HashMap<>());
        }

        // Extract appended file name from the absolute paths saved in getAppendFiles()
        pm.getRollbackLogFiles().forEach((path, size) -> {
          partitionToAppendedFiles.get(partition).merge(new Path(path).getName(), size, fileMergeFn);
        });
      }
    });
  }

  /**
   * Convert rollback action metadata to files partition records.
   */
  private static List<HoodieRecord> convertFilesToFilesPartitionRecords(Map<String, List<String>> partitionToDeletedFiles,
                                                                        Map<String, Map<String, Long>> partitionToAppendedFiles,
                                                                        String instantTime, String operation) {
    List<HoodieRecord> records = new LinkedList<>();
    int[] fileChangeCount = {0, 0}; // deletes, appends

    partitionToDeletedFiles.forEach((partitionName, deletedFiles) -> {
      fileChangeCount[0] += deletedFiles.size();
      final String partition = partitionName.equals(EMPTY_PARTITION_NAME) ? NON_PARTITIONED_NAME : partitionName;

      Option<Map<String, Long>> filesAdded = Option.empty();
      if (partitionToAppendedFiles.containsKey(partitionName)) {
        filesAdded = Option.of(partitionToAppendedFiles.remove(partitionName));
      }

      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partition, filesAdded,
          Option.of(new ArrayList<>(deletedFiles)));
      records.add(record);
    });

    partitionToAppendedFiles.forEach((partitionName, appendedFileMap) -> {
      final String partition = partitionName.equals(EMPTY_PARTITION_NAME) ? NON_PARTITIONED_NAME : partitionName;
      fileChangeCount[1] += appendedFileMap.size();

      // Validate that no appended file has been deleted
      ValidationUtils.checkState(
          !appendedFileMap.keySet().removeAll(partitionToDeletedFiles.getOrDefault(partition, Collections.emptyList())),
          "Rollback file cannot both be appended and deleted");

      // New files added to a partition
      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partition, Option.of(appendedFileMap),
          Option.empty());
      records.add(record);
    });

    LOG.info("Found at " + instantTime + " from " + operation + ". #partitions_updated=" + records.size()
        + ", #files_deleted=" + fileChangeCount[0] + ", #files_appended=" + fileChangeCount[1]);

    return records;
  }

  /**
   * Convert rollback action metadata to bloom filter index records.
   */
  private static List<HoodieRecord> convertFilesToBloomFilterRecords(HoodieEngineContext engineContext,
                                                                     HoodieTableMetaClient dataMetaClient,
                                                                     Map<String, List<String>> partitionToDeletedFiles,
                                                                     Map<String, Map<String, Long>> partitionToAppendedFiles,
                                                                     String instantTime) {
    List<HoodieRecord> records = new LinkedList<>();
    partitionToDeletedFiles.forEach((partitionName, deletedFileList) -> deletedFileList.forEach(deletedFile -> {
      if (!FSUtils.isBaseFile(new Path(deletedFile))) {
        return;
      }

      final String partition = partitionName.equals(EMPTY_PARTITION_NAME) ? NON_PARTITIONED_NAME : partitionName;
      records.add(HoodieMetadataPayload.createBloomFilterMetadataRecord(
          partition, deletedFile, instantTime, ByteBuffer.allocate(0), true));
    }));

    partitionToAppendedFiles.forEach((partitionName, appendedFileMap) -> {
      final String partition = partitionName.equals(EMPTY_PARTITION_NAME) ? NON_PARTITIONED_NAME : partitionName;
      appendedFileMap.forEach((appendedFile, length) -> {
        if (!FSUtils.isBaseFile(new Path(appendedFile))) {
          return;
        }
        final String pathWithPartition = partitionName + "/" + appendedFile;
        final Path appendedFilePath = new Path(dataMetaClient.getBasePath(), pathWithPartition);
        try {
          HoodieFileReader<IndexedRecord> fileReader =
              HoodieFileReaderFactory.getFileReader(dataMetaClient.getHadoopConf(), appendedFilePath);
          final BloomFilter fileBloomFilter = fileReader.readBloomFilter();
          if (fileBloomFilter == null) {
            LOG.error("Failed to read bloom filter for " + appendedFilePath);
            return;
          }
          ByteBuffer bloomByteBuffer = ByteBuffer.wrap(fileBloomFilter.serializeToString().getBytes());
          HoodieRecord record = HoodieMetadataPayload.createBloomFilterMetadataRecord(
              partition, appendedFile, instantTime, bloomByteBuffer, false);
          records.add(record);
          fileReader.close();
        } catch (IOException e) {
          LOG.error("Failed to get bloom filter for file: " + appendedFilePath);
        }
      });
    });
    return records;
  }

  /**
   * Convert rollback action metadata to column stats index records.
   */
  private static List<HoodieRecord> convertFilesToColumnStatsRecords(HoodieEngineContext engineContext,
                                                                     HoodieTableMetaClient datasetMetaClient,
                                                                     Map<String, List<String>> partitionToDeletedFiles,
                                                                     Map<String, Map<String, Long>> partitionToAppendedFiles,
                                                                     String instantTime) {
    List<HoodieRecord> records = new LinkedList<>();
    List<String> latestColumns = getLatestColumns(datasetMetaClient);
    partitionToDeletedFiles.forEach((partitionName, deletedFileList) -> deletedFileList.forEach(deletedFile -> {
      final String partition = partitionName.equals(EMPTY_PARTITION_NAME) ? NON_PARTITIONED_NAME : partitionName;
      if (deletedFile.endsWith(HoodieFileFormat.PARQUET.getFileExtension())) {
        final String filePathWithPartition = partitionName + "/" + deletedFile;
        records.addAll(getColumnStats(partition, filePathWithPartition, datasetMetaClient,
            latestColumns, true).collect(Collectors.toList()));
      }
    }));

    partitionToAppendedFiles.forEach((partitionName, appendedFileMap) -> appendedFileMap.forEach(
        (appendedFile, size) -> {
          final String partition = partitionName.equals(EMPTY_PARTITION_NAME) ? NON_PARTITIONED_NAME : partitionName;
          if (appendedFile.endsWith(HoodieFileFormat.PARQUET.getFileExtension())) {
            final String filePathWithPartition = partitionName + "/" + appendedFile;
            records.addAll(getColumnStats(partition, filePathWithPartition, datasetMetaClient,
                latestColumns, false).collect(Collectors.toList()));
          }
        }));
    return records;
  }

  /**
   * Map a record key to a file group in partition of interest.
   * <p>
   * Note: For hashing, the algorithm is same as String.hashCode() but is being defined here as hashCode()
   * implementation is not guaranteed by the JVM to be consistent across JVM versions and implementations.
   *
   * @param recordKey record key for which the file group index is looked up for.
   * @return An integer hash of the given string
   */
  public static int mapRecordKeyToFileGroupIndex(String recordKey, int numFileGroups) {
    int h = 0;
    for (int i = 0; i < recordKey.length(); ++i) {
      h = 31 * h + recordKey.charAt(i);
    }

    return Math.abs(Math.abs(h) % numFileGroups);
  }

  /**
   * Get the latest file slices for a Metadata Table partition. If the file slice is
   * because of pending compaction instant, then merge the file slice with the one
   * just before the compaction instant time. The list of file slices returned is
   * sorted in the correct order of file group name.
   *
   * @param metaClient - Instance of {@link HoodieTableMetaClient}.
   * @param partition  - The name of the partition whose file groups are to be loaded.
   * @return List of latest file slices for all file groups in a given partition.
   */
  public static List<FileSlice> getPartitionLatestMergedFileSlices(HoodieTableMetaClient metaClient, String partition) {
    LOG.info("Loading latest merged file slices for metadata table partition " + partition);
    return getPartitionFileSlices(metaClient, Option.empty(), partition, true);
  }

  /**
   * Get the latest file slices for a Metadata Table partition. The list of file slices
   * returned is sorted in the correct order of file group name.
   *
   * @param metaClient - Instance of {@link HoodieTableMetaClient}.
   * @param fsView     - Metadata table filesystem view
   * @param partition  - The name of the partition whose file groups are to be loaded.
   * @return List of latest file slices for all file groups in a given partition.
   */
  public static List<FileSlice> getPartitionLatestFileSlices(HoodieTableMetaClient metaClient,
                                                             Option<HoodieTableFileSystemView> fsView, String partition) {
    LOG.info("Loading latest file slices for metadata table partition " + partition);
    return getPartitionFileSlices(metaClient, fsView, partition, false);
  }

  /**
   * Get metadata table file system view.
   *
   * @param metaClient - Metadata table meta client
   * @return Filesystem view for the metadata table
   */
  public static HoodieTableFileSystemView getFileSystemView(HoodieTableMetaClient metaClient) {
    // If there are no commits on the metadata table then the table's
    // default FileSystemView will not return any file slices even
    // though we may have initialized them.
    HoodieTimeline timeline = metaClient.getActiveTimeline();
    if (timeline.empty()) {
      final HoodieInstant instant = new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION,
          HoodieActiveTimeline.createNewInstantTime());
      timeline = new HoodieDefaultTimeline(Arrays.asList(instant).stream(), metaClient.getActiveTimeline()::getInstantDetails);
    }
    return new HoodieTableFileSystemView(metaClient, timeline);
  }

  /**
   * Get the latest file slices for a given partition.
   *
   * @param metaClient      - Instance of {@link HoodieTableMetaClient}.
   * @param partition       - The name of the partition whose file groups are to be loaded.
   * @param mergeFileSlices - When enabled, will merge the latest file slices with the last known
   *                        completed instant. This is useful for readers when there are pending
   *                        compactions. MergeFileSlices when disabled, will return the latest file
   *                        slices without any merging, and this is needed for the writers.
   * @return List of latest file slices for all file groups in a given partition.
   */
  private static List<FileSlice> getPartitionFileSlices(HoodieTableMetaClient metaClient,
                                                        Option<HoodieTableFileSystemView> fileSystemView,
                                                        String partition,
                                                        boolean mergeFileSlices) {
    HoodieTableFileSystemView fsView = fileSystemView.orElse(getFileSystemView(metaClient));
    Stream<FileSlice> fileSliceStream;
    if (mergeFileSlices) {
      fileSliceStream = fsView.getLatestMergedFileSlicesBeforeOrOn(
          partition, metaClient.getActiveTimeline().filterCompletedInstants().lastInstant().get().getTimestamp());
    } else {
      fileSliceStream = fsView.getLatestFileSlices(partition);
    }
    return fileSliceStream.sorted((s1, s2) -> s1.getFileId().compareTo(s2.getFileId())).collect(Collectors.toList());
  }

  public static List<HoodieRecord> convertMetadataToColumnStatsRecords(HoodieCommitMetadata commitMetadata,
                                                                       HoodieEngineContext engineContext,
                                                                       HoodieTableMetaClient dataMetaClient,
                                                                       boolean isMetaIndexColumnStatsForAllColumns,
                                                                       String instantTime) {

    try {
      List<HoodieWriteStat> allWriteStats = commitMetadata.getPartitionToWriteStats().values().stream()
          .flatMap(entry -> entry.stream()).collect(Collectors.toList());
      return HoodieTableMetadataUtil.createColumnStatsFromWriteStats(engineContext, dataMetaClient, allWriteStats,
          isMetaIndexColumnStatsForAllColumns);
    } catch (Exception e) {
      throw new HoodieException("Failed to generate column stats records for metadata table ", e);
    }
  }

  /**
   * Create column stats from write status.
   *
   * @param engineContext                       - Enging context
   * @param datasetMetaClient                   - Dataset meta client
   * @param allWriteStats                       - Write status to convert
   * @param isMetaIndexColumnStatsForAllColumns - Are all columns enabled for indexing
   */
  public static List<HoodieRecord> createColumnStatsFromWriteStats(HoodieEngineContext engineContext,
                                                                   HoodieTableMetaClient datasetMetaClient,
                                                                   List<HoodieWriteStat> allWriteStats,
                                                                   boolean isMetaIndexColumnStatsForAllColumns) throws Exception {
    if (allWriteStats.isEmpty()) {
      return Collections.emptyList();
    }

    List<HoodieWriteStat> prunedWriteStats = allWriteStats.stream().filter(writeStat -> {
      return !(writeStat instanceof HoodieDeltaWriteStat);
    }).collect(Collectors.toList());
    if (prunedWriteStats.isEmpty()) {
      return Collections.emptyList();
    }

    return engineContext.flatMap(prunedWriteStats,
        writeStat -> translateWriteStatToColumnStats(writeStat, datasetMetaClient,
            getLatestColumns(datasetMetaClient, isMetaIndexColumnStatsForAllColumns)),
        prunedWriteStats.size());
  }

  /**
   * Get the latest columns for the table for column stats indexing.
   *
   * @param datasetMetaClient                   - Data table meta client
   * @param isMetaIndexColumnStatsForAllColumns - Is column stats indexing enabled for all columns
   */
  private static List<String> getLatestColumns(HoodieTableMetaClient datasetMetaClient, boolean isMetaIndexColumnStatsForAllColumns) {
    if (!isMetaIndexColumnStatsForAllColumns
        || datasetMetaClient.getCommitsTimeline().filterCompletedInstants().countInstants() < 1) {
      return Collections.singletonList(datasetMetaClient.getTableConfig().getRecordKeyFieldProp());
    }

    TableSchemaResolver schemaResolver = new TableSchemaResolver(datasetMetaClient);
    // consider nested fields as well. if column stats is enabled only for a subset of columns,
    // directly use them instead of all columns from the latest table schema
    try {
      return schemaResolver.getTableAvroSchema().getFields().stream()
          .map(entry -> entry.name()).collect(Collectors.toList());
    } catch (Exception e) {
      throw new HoodieException("Failed to get latest columns for " + datasetMetaClient.getBasePath());
    }
  }

  private static List<String> getLatestColumns(HoodieTableMetaClient datasetMetaClient) {
    return getLatestColumns(datasetMetaClient, false);
  }

  public static Stream<HoodieRecord> translateWriteStatToColumnStats(HoodieWriteStat writeStat,
                                                                     HoodieTableMetaClient datasetMetaClient,
                                                                     List<String> latestColumns) {
    return getColumnStats(writeStat.getPartitionPath(), writeStat.getPath(), datasetMetaClient, latestColumns, false);

  }

  private static Stream<HoodieRecord> getColumnStats(final String partitionPath, final String filePathWithPartition,
                                                     HoodieTableMetaClient datasetMetaClient,
                                                     List<String> columns, boolean isDeleted) {
    final String partition = partitionPath.equals(EMPTY_PARTITION_NAME) ? NON_PARTITIONED_NAME : partitionPath;
    final int offset = partition.equals(NON_PARTITIONED_NAME) ? (filePathWithPartition.startsWith("/") ? 1 : 0)
        : partition.length() + 1;
    final String fileName = filePathWithPartition.substring(offset);
    if (!FSUtils.isBaseFile(new Path(fileName))) {
      return Stream.empty();
    }

    if (filePathWithPartition.endsWith(HoodieFileFormat.PARQUET.getFileExtension())) {
      List<HoodieColumnRangeMetadata<Comparable>> columnRangeMetadataList = new ArrayList<>();
      final Path fullFilePath = new Path(datasetMetaClient.getBasePath(), filePathWithPartition);
      if (!isDeleted) {
        try {
          columnRangeMetadataList = new ParquetUtils().readRangeFromParquetMetadata(
              datasetMetaClient.getHadoopConf(), fullFilePath, columns);
        } catch (Exception e) {
          LOG.error("Failed to read column stats for " + fullFilePath, e);
        }
      } else {
        columnRangeMetadataList =
            columns.stream().map(entry -> new HoodieColumnRangeMetadata<Comparable>(fileName,
                    entry, null, null, 0, 0, 0, 0))
                .collect(Collectors.toList());
      }
      return HoodieMetadataPayload.createColumnStatsRecords(partitionPath, columnRangeMetadataList, isDeleted);
    } else {
      throw new HoodieException("Column range index not supported for filePathWithPartition " + fileName);
    }
  }

  /**
   * Get file group count for a metadata table partition.
   *
   * @param partitionType        - Metadata table partition type
   * @param metaClient           - Metadata table meta client
   * @param fsView               - Filesystem view
   * @param metadataConfig       - Metadata config
   * @param isBootstrapCompleted - Is bootstrap completed for the metadata table
   * @return File group count for the requested metadata partition type
   */
  public static int getPartitionFileGroupCount(final MetadataPartitionType partitionType,
                                               final Option<HoodieTableMetaClient> metaClient,
                                               final Option<HoodieTableFileSystemView> fsView,
                                               final HoodieMetadataConfig metadataConfig, boolean isBootstrapCompleted) {
    if (isBootstrapCompleted) {
      final List<FileSlice> latestFileSlices = HoodieTableMetadataUtil
          .getPartitionLatestFileSlices(metaClient.get(), fsView, partitionType.getPartitionPath());
      return Math.max(latestFileSlices.size(), 1);
    }

    switch (partitionType) {
      case BLOOM_FILTERS:
        return metadataConfig.getBloomFilterIndexFileGroupCount();
      case COLUMN_STATS:
        return metadataConfig.getColumnStatsIndexFileGroupCount();
      default:
        return 1;
    }
  }

}
