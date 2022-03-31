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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
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

  // Suffix to use for bootstrapping additional indexes. Should be less than other suffixes.
  private static final String INDEX_BOOTSTRAP_TIMESTAMP_SUFFIX = "001";
  // Suffix to use for compaction
  private static final String COMPACTION_TIMESTAMP_SUFFIX = "002";
  // Suffix to use for clean
  private static final String CLEAN_TIMESTAMP_SUFFIX = "003";

  /**
   * Delete the metadata table for the dataset. This will be invoked during upgrade/downgrade operation during which no other
   * process should be running.
   *
   * @param metaClient {@code HoodieTableMetaClient} of the dataset for which metadata table is to be deleted
   * @param context instance of {@link HoodieEngineContext}.
   * @param backup Whether metadata table should be backed up before deletion. If true, the table is backed up to the
   *               directory with name metadata_<current_timestamp>.
   */
  public static void deleteMetadataTable(HoodieTableMetaClient dataMetaClient, HoodieEngineContext context, boolean backup) {
    final Path metadataTablePath = new Path(HoodieTableMetadata.getMetadataTableBasePath(dataMetaClient.getBasePath()));
    FileSystem fs = FSUtils.getFs(metadataTablePath.toString(), context.getHadoopConf().get());
    setMetadataPartitionState(dataMetaClient, MetadataPartitionType.FILES, false);
    try {
      if (!fs.exists(metadataTablePath)) {
        return;
      }
    } catch (FileNotFoundException e) {
      // Ignoring exception as metadata table already does not exist
      LOG.debug("Metadata table not found at path " + metadataTablePath);
      return;
    } catch (Exception e) {
      throw new HoodieMetadataException("Failed to check metadata table existence", e);
    }

    if (backup) {
      final Path metadataBackupPath = new Path(metadataTablePath.getParent(), "metadata_" + HoodieActiveTimeline.createNewInstantTime());
      LOG.info("Backing up metadata directory to " + metadataBackupPath + " before deletion");
      try {
        if (fs.rename(metadataTablePath, metadataBackupPath)) {
          return;
        }
      } catch (Exception e) {
        // If rename fails, we will try to delete the table instead
        LOG.error("Failed to backup metadata table using rename", e);
      }
    }

    LOG.info("Deleting metadata table from " + metadataTablePath);
    try {
      fs.delete(metadataTablePath, true);
    } catch (Exception e) {
      throw new HoodieMetadataException("Failed to delete metadata table from path " + metadataTablePath, e);
    }
  }

  /**
   * Finds all new files/partitions created as part of commit and creates metadata table records for them.
   *
   * @param commitMetadata
   * @param instantTime
   * @return a list of metadata table records
   */
  public static List<HoodieRecord> convertMetadataToRecords(HoodieCommitMetadata commitMetadata, String instantTime) {
    List<HoodieRecord> records = new LinkedList<>();
    List<String> allPartitions = new LinkedList<>();
    int[] newFileCount = {0};
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
      newFileCount[0] += newFiles.size();
    });

    // New partitions created
    HoodieRecord record = HoodieMetadataPayload.createPartitionListRecord(new ArrayList<>(allPartitions));
    records.add(record);

    LOG.info(String.format("Updating at %s from Commit/%s. #partitions_updated=%d, #files_added=%d", instantTime, commitMetadata.getOperationType(),
        records.size(), newFileCount[0]));
    return records;
  }

  /**
   * Finds all files that were deleted as part of a clean and creates metadata table records for them.
   *
   * @param cleanMetadata
   * @param instantTime
   * @return a list of metadata table records
   */
  public static List<HoodieRecord> convertMetadataToRecords(HoodieCleanMetadata cleanMetadata, String instantTime) {
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
   * Aggregates all files deleted and appended to from all rollbacks associated with a restore operation then
   * creates metadata table records for them.
   *
   * @param restoreMetadata
   * @param instantTime
   * @return a list of metadata table records
   */
  public static List<HoodieRecord> convertMetadataToRecords(HoodieActiveTimeline metadataTableTimeline,
                                                            HoodieRestoreMetadata restoreMetadata, String instantTime, Option<String> lastSyncTs) {
    Map<String, Map<String, Long>> partitionToAppendedFiles = new HashMap<>();
    Map<String, List<String>> partitionToDeletedFiles = new HashMap<>();
    restoreMetadata.getHoodieRestoreMetadata().values().forEach(rms -> {
      rms.forEach(rm -> processRollbackMetadata(metadataTableTimeline, rm, partitionToDeletedFiles, partitionToAppendedFiles, lastSyncTs));
    });

    return convertFilesToRecords(partitionToDeletedFiles, partitionToAppendedFiles, instantTime, "Restore");
  }

  public static List<HoodieRecord> convertMetadataToRecords(HoodieActiveTimeline metadataTableTimeline,
                                                            HoodieRollbackMetadata rollbackMetadata, String instantTime,
                                                            Option<String> lastSyncTs, boolean wasSynced) {

    Map<String, Map<String, Long>> partitionToAppendedFiles = new HashMap<>();
    Map<String, List<String>> partitionToDeletedFiles = new HashMap<>();
    processRollbackMetadata(metadataTableTimeline, rollbackMetadata, partitionToDeletedFiles, partitionToAppendedFiles, lastSyncTs);
    if (!wasSynced) {
      // Since the instant-being-rolled-back was never committed to the metadata table, the files added there
      // need not be deleted. For MOR Table, the rollback appends logBlocks so we need to keep the appended files.
      partitionToDeletedFiles.clear();
    }
    return convertFilesToRecords(partitionToDeletedFiles, partitionToAppendedFiles, instantTime, "Rollback");
  }

  /**
   * Extracts information about the deleted and append files from the {@code HoodieRollbackMetadata}.
   *
   * During a rollback files may be deleted (COW, MOR) or rollback blocks be appended (MOR only) to files. This
   * function will extract this change file for each partition.
   * @param metadataTableTimeline Current timeline of the Metdata Table
   * @param rollbackMetadata {@code HoodieRollbackMetadata}
   * @param partitionToDeletedFiles The {@code Map} to fill with files deleted per partition.
   * @param partitionToAppendedFiles The {@code Map} to fill with files appended per partition and their sizes.
   */
  private static void processRollbackMetadata(HoodieActiveTimeline metadataTableTimeline, HoodieRollbackMetadata rollbackMetadata,
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

      if (pm.getWrittenLogFiles() != null && !pm.getWrittenLogFiles().isEmpty()) {
        if (!partitionToAppendedFiles.containsKey(partition)) {
          partitionToAppendedFiles.put(partition, new HashMap<>());
        }

        // Extract appended file name from the absolute paths saved in getWrittenLogFiles()
        pm.getWrittenLogFiles().forEach((path, size) -> {
          partitionToAppendedFiles.get(partition).merge(new Path(path).getName(), size, fileMergeFn);
        });
      }
    });
  }

  private static List<HoodieRecord> convertFilesToRecords(Map<String, List<String>> partitionToDeletedFiles,
                                                          Map<String, Map<String, Long>> partitionToAppendedFiles, String instantTime,
                                                          String operation) {
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
   * Map a record key to a file group in partition of interest.
   *
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
    return getPartitionFileSlices(metaClient, partition, true);
  }

  /**
   * Get the latest file slices for a Metadata Table partition. The list of file slices
   * returned is sorted in the correct order of file group name.
   *
   * @param metaClient - Instance of {@link HoodieTableMetaClient}.
   * @param partition  - The name of the partition whose file groups are to be loaded.
   * @return List of latest file slices for all file groups in a given partition.
   */
  public static List<FileSlice> getPartitionLatestFileSlices(HoodieTableMetaClient metaClient, String partition) {
    LOG.info("Loading latest file slices for metadata table partition " + partition);
    return getPartitionFileSlices(metaClient, partition, false);
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
  private static List<FileSlice> getPartitionFileSlices(HoodieTableMetaClient metaClient, String partition,
                                                        boolean mergeFileSlices) {
    // If there are no commits on the metadata table then the table's
    // default FileSystemView will not return any file slices even
    // though we may have initialized them.
    HoodieTimeline timeline = metaClient.getActiveTimeline();
    if (timeline.empty()) {
      final HoodieInstant instant = new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION,
          HoodieActiveTimeline.createNewInstantTime());
      timeline = new HoodieDefaultTimeline(Arrays.asList(instant).stream(), metaClient.getActiveTimeline()::getInstantDetails);
    }

    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient, timeline);
    Stream<FileSlice> fileSliceStream;
    if (mergeFileSlices) {
      fileSliceStream = fsView.getLatestMergedFileSlicesBeforeOrOn(
          partition, timeline.filterCompletedInstants().lastInstant().get().getTimestamp());
    } else {
      fileSliceStream = fsView.getLatestFileSlices(partition);
    }
    return fileSliceStream.sorted((s1, s2) -> s1.getFileId().compareTo(s2.getFileId())).collect(Collectors.toList());
  }

  /**
   * Get the file slices for a given partition which have been initialized but dont appear in the timeline yet.
   *
   * @param metaClient      - Instance of {@link HoodieTableMetaClient}.
   * @param partition       - The name of the partition whose file groups are to be loaded.
   * @return List of latest file slices for all file groups in a given partition.
   */
  public static List<FileSlice> getBootstrappedFileSlices(HoodieTableMetaClient metaClient, String partition) {
    final HoodieInstant instant = new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION,
        HoodieActiveTimeline.createNewInstantTime());
    HoodieDefaultTimeline timeline = new HoodieDefaultTimeline(Arrays.asList(instant).stream(), metaClient.getActiveTimeline()::getInstantDetails);

    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient, timeline);
    Stream<FileSlice> fileSliceStream;
    fileSliceStream = fsView.getLatestFileSlices(partition);
    return fileSliceStream.sorted((s1, s2) -> s1.getFileId().compareTo(s2.getFileId())).collect(Collectors.toList());
  }

  /**
   * Returns a {@code HoodieTableMetaClient} for the metadata table.
   *
   * @param datasetMetaClient {@code HoodieTableMetaClient} for the dataset.
   * @return {@code HoodieTableMetaClient} for the metadata table.
   */
  public static HoodieTableMetaClient getMetadataTableMetaClient(HoodieTableMetaClient datasetMetaClient) {
    final String metadataBasePath = HoodieTableMetadata.getMetadataTableBasePath(datasetMetaClient.getBasePath());
    return HoodieTableMetaClient.builder().setBasePath(metadataBasePath).setConf(datasetMetaClient.getHadoopConf())
        .build();
  }

  /**
   * Create the timestamp for a clean operation on the metadata table.
   */
  public static String createCleanTimestamp(String timestamp) {
    return timestamp + CLEAN_TIMESTAMP_SUFFIX;
  }

  /**
   * Create the timestamp for a compaction operation on the metadata table.
   */
  public static String createCompactionTimestamp(String timestamp) {
    return timestamp + COMPACTION_TIMESTAMP_SUFFIX;
  }

  /**
   * Create the timestamp for an index initialization operation on the metadata table.
   */
  public static String createIndexInitTimestamp(String timestamp) {
    return timestamp + INDEX_BOOTSTRAP_TIMESTAMP_SUFFIX;
  }

  /**
   * Returns true if the given instant represents an index bootstrap operation on the metadata table.
   *
   * When an index is bootstrapped on its own, it will use a instant time which has the suffix
   * INDEX_BOOTSTRAP_TIMESTAMP_SUFFIX. Suppose the last deltacommit on the metadata table had the timestamp t1 then
   * the index bootstrap operation will have the timstamp t1 + INDEX_BOOTSTRAP_TIMESTAMP_SUFFIX = t1001.
   *
   * We have to satisfy the following constraints:
   * 1. This timestamp t1001 wont be present on the dataset timeline.
   * 2. The deltacommit t1 may itself have been archived.
   * 3. Dataset instants have millisecond resolution timestamp so they themselves may have timestamp which is ending in
   *    INDEX_BOOTSTRAP_TIMESTAMP_SUFFIX. We need to ignore such instants in this method.
   *
   * If the HUDI generated timestamps have millisecond resolution and length 17, then due to the suffix of
   * INDEX_BOOTSTRAP_TIMESTAMP_SUFFIX, the length of index timestamp will be 20. This additional check ensures that
   * we do not assume any dataset operation's timestamp ending in INDEX_BOOTSTRAP_TIMESTAMP_SUFFIX as a bootstrap index
   * operation.
   *
   */
  public static boolean isIndexInitInstant(HoodieInstant instant) {
    final String newInstantTime = HoodieActiveTimeline.createNewInstantTime();
    if (!instant.getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION)
        || !instant.getTimestamp().endsWith(INDEX_BOOTSTRAP_TIMESTAMP_SUFFIX)
        || instant.getTimestamp().length() != (newInstantTime.length() + INDEX_BOOTSTRAP_TIMESTAMP_SUFFIX.length())) {
      return false;
    }

    return true;
  }

  /**
   * Set the state of the metadata table.
   * @param dataMetaClient MetaClient for the dataset
   * @param enabled If true, metadata table is being used for this dataset, false otherwise
   */
  public static HoodieTableMetaClient setMetadataPartitionState(HoodieTableMetaClient dataMetaClient, MetadataPartitionType partition,
      boolean enabled) {
    dataMetaClient.getTableConfig().setMetadataPartitionState(partition, enabled);
    HoodieTableConfig.update(dataMetaClient.getFs(), new Path(dataMetaClient.getMetaPath()), dataMetaClient.getTableConfig().getProps());
    dataMetaClient = HoodieTableMetaClient.reload(dataMetaClient);
    ValidationUtils.checkState(dataMetaClient.getTableConfig().isMetadataPartitionEnabled(partition) == enabled,
        "Metadata table state change should be persisted");

    LOG.info(String.format("Metadata table %s partition %s has been %s", dataMetaClient.getBasePath(), partition,
        enabled ? "enabled" : "disabled"));
    return dataMetaClient;
  }

  /**
   * Returns true if any enabled metadata partition in the given hoodie table requires WriteStatus to track the
   * written records.
   * @param config
   *
   * @param hoodieTable HoodieTable
   * @return true if WriteStatus should track the written records else false.
   */
  public static boolean needsWriteStatusTracking(HoodieMetadataConfig config, HoodieTableMetaClient metaClient) {
    // Does any enabled partition need to track the written records
    if (MetadataPartitionType.needWriteStatusTracking().stream().anyMatch(p -> metaClient.getTableConfig().isMetadataPartitionEnabled(p))) {
      return true;
    }

    // Does any enabled partition being enabled need to track the written records
    if (config.createRecordIndex()) {
      return true;
    }

    return false;
  }
}
