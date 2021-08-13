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
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.apache.hudi.metadata.HoodieTableMetadata.NON_PARTITIONED_NAME;

/**
 * A utility to convert timeline information to metadata table records.
 */
public class HoodieTableMetadataUtil {

  private static final Logger LOG = LogManager.getLogger(HoodieTableMetadataUtil.class);

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
      final String partition = partitionStatName.equals("") ? NON_PARTITIONED_NAME : partitionStatName;
      allPartitions.add(partition);

      Map<String, Long> newFiles = new HashMap<>(writeStats.size());
      writeStats.forEach(hoodieWriteStat -> {
        String pathWithPartition = hoodieWriteStat.getPath();
        if (pathWithPartition == null) {
          // Empty partition
          LOG.warn("Unable to find path in write stat to update metadata table " + hoodieWriteStat);
          return;
        }

        int offset = partition.equals(NON_PARTITIONED_NAME) ? 0 : partition.length() + 1;
        String filename = pathWithPartition.substring(offset);
        ValidationUtils.checkState(!newFiles.containsKey(filename), "Duplicate files in HoodieCommitMetadata");
        newFiles.put(filename, hoodieWriteStat.getTotalWriteBytes());
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
   * Finds all files that will be deleted as part of a planned clean and creates metadata table records for them.
   *
   * @param cleanerPlan from timeline to convert
   * @param instantTime
   * @return a list of metadata table records
   */
  public static List<HoodieRecord> convertMetadataToRecords(HoodieCleanerPlan cleanerPlan, String instantTime) {
    List<HoodieRecord> records = new LinkedList<>();

    int[] fileDeleteCount = {0};
    cleanerPlan.getFilePathsToBeDeletedPerPartition().forEach((partition, deletedPathInfo) -> {
      fileDeleteCount[0] += deletedPathInfo.size();

      // Files deleted from a partition
      List<String> deletedFilenames = deletedPathInfo.stream().map(p -> new Path(p.getFilePath()).getName())
          .collect(Collectors.toList());
      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partition, Option.empty(),
          Option.of(deletedFilenames));
      records.add(record);
    });

    LOG.info("Found at " + instantTime + " from CleanerPlan. #partitions_updated=" + records.size()
        + ", #files_deleted=" + fileDeleteCount[0]);
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

    cleanMetadata.getPartitionMetadata().forEach((partition, partitionMetadata) -> {
      // Files deleted from a partition
      List<String> deletedFiles = partitionMetadata.getSuccessDeleteFiles();
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
  public static List<HoodieRecord> convertMetadataToRecords(HoodieRestoreMetadata restoreMetadata, String instantTime, Option<String> lastSyncTs) {
    Map<String, Map<String, Long>> partitionToAppendedFiles = new HashMap<>();
    Map<String, List<String>> partitionToDeletedFiles = new HashMap<>();
    restoreMetadata.getHoodieRestoreMetadata().values().forEach(rms -> {
      rms.forEach(rm -> processRollbackMetadata(rm, partitionToDeletedFiles, partitionToAppendedFiles, lastSyncTs));
    });

    return convertFilesToRecords(partitionToDeletedFiles, partitionToAppendedFiles, instantTime, "Restore");
  }

  public static List<HoodieRecord> convertMetadataToRecords(HoodieRollbackMetadata rollbackMetadata, String instantTime,
      Option<String> lastSyncTs, boolean wasSynced) {

    Map<String, Map<String, Long>> partitionToAppendedFiles = new HashMap<>();
    Map<String, List<String>> partitionToDeletedFiles = new HashMap<>();
    processRollbackMetadata(rollbackMetadata, partitionToDeletedFiles, partitionToAppendedFiles, lastSyncTs);
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
   *
   * @param rollbackMetadata {@code HoodieRollbackMetadata}
   * @param partitionToDeletedFiles The {@code Map} to fill with files deleted per partition.
   * @param partitionToAppendedFiles The {@code Map} to fill with files appended per partition and their sizes.
   */
  private static void processRollbackMetadata(HoodieRollbackMetadata rollbackMetadata,
                                              Map<String, List<String>> partitionToDeletedFiles,
                                              Map<String, Map<String, Long>> partitionToAppendedFiles,
                                              Option<String> lastSyncTs) {

    rollbackMetadata.getPartitionMetadata().values().forEach(pm -> {
      // Has this rollback produced new files?
      boolean hasRollbackLogFiles = pm.getRollbackLogFiles() != null && !pm.getRollbackLogFiles().isEmpty();
      boolean hasNonZeroRollbackLogFiles = hasRollbackLogFiles && pm.getRollbackLogFiles().values().stream().mapToLong(Long::longValue).sum() > 0;
      // If commit being rolled back has not been synced to metadata table yet then there is no need to update metadata
      boolean shouldSkip = lastSyncTs.isPresent()
          && HoodieTimeline.compareTimestamps(rollbackMetadata.getCommitsRollback().get(0), HoodieTimeline.GREATER_THAN, lastSyncTs.get());

      if (!hasNonZeroRollbackLogFiles && shouldSkip) {
        LOG.info(String.format("Skipping syncing of rollbackMetadata at %s, given metadata table is already synced upto to %s",
            rollbackMetadata.getCommitsRollback().get(0), lastSyncTs.get()));
        return;
      }

      final String partition = pm.getPartitionPath();
      if (!pm.getSuccessDeleteFiles().isEmpty() && !shouldSkip) {
        if (!partitionToDeletedFiles.containsKey(partition)) {
          partitionToDeletedFiles.put(partition, new ArrayList<>());
        }

        // Extract deleted file name from the absolute paths saved in getSuccessDeleteFiles()
        List<String> deletedFiles = pm.getSuccessDeleteFiles().stream().map(p -> new Path(p).getName())
            .collect(Collectors.toList());
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

    partitionToDeletedFiles.forEach((partition, deletedFiles) -> {
      fileChangeCount[0] += deletedFiles.size();

      Option<Map<String, Long>> filesAdded = Option.empty();
      if (partitionToAppendedFiles.containsKey(partition)) {
        filesAdded = Option.of(partitionToAppendedFiles.remove(partition));
      }

      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partition, filesAdded,
          Option.of(new ArrayList<>(deletedFiles)));
      records.add(record);
    });

    partitionToAppendedFiles.forEach((partition, appendedFileMap) -> {
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
   * Returns a list of commits which were rolled back as part of a Rollback or Restore operation.
   *
   * @param instant The Rollback operation to read
   * @param timeline
   */
  public static List<String> getCommitsRolledback(HoodieInstant instant, HoodieActiveTimeline timeline) {
    try {
      if (instant.getAction().equals(HoodieTimeline.ROLLBACK_ACTION)) {
        HoodieRollbackMetadata rollbackMetadata = TimelineMetadataUtils.deserializeHoodieRollbackMetadata(
            timeline.getInstantDetails(instant).get());
        return rollbackMetadata.getCommitsRollback();
      }

      List<String> commitsRolledback = new LinkedList<>();

      if (instant.getAction().equals(HoodieTimeline.RESTORE_ACTION)) {
        // Restore is made up of several rollbacks
        HoodieRestoreMetadata restoreMetadata = TimelineMetadataUtils.deserializeHoodieRestoreMetadata(
            timeline.getInstantDetails(instant).get());
        restoreMetadata.getHoodieRestoreMetadata().values().forEach(rms -> {
          rms.forEach(rm -> commitsRolledback.addAll(rm.getCommitsRollback()));
        });
      }

      return commitsRolledback;
    } catch (IOException e) {
      throw new HoodieMetadataException("Error retrieving rollback commits for instant " + instant, e);
    }
  }

  /**
   * Map a key to a shard.
   *
   * Note: For hashing, the algorithm is same as String.hashCode() but is being defined here as hashCode()
   * implementation is not guaranteed by the JVM to be consistent across JVM versions and implementations.
   *
   * @param str
   * @return An integer hash of the given string
   */
  public static int keyToShard(String str, int numShards) {
    int h = 0;
    for (int i = 0; i < str.length(); ++i) {
      h = 31 * h + str.charAt(i);
    }

    return Math.abs(Math.abs(h) % numShards);
  }

  /**
   * Loads the list of shards for a partition of the Metadata Table.
   *
   * The list of shards is returned sorted in the correct order of shard index.
   * @param metaClient
   * @param partition The name of the partition whose shards are to be loaded.
   * @return List of shards
   */
  public static List<FileSlice> loadPartitionShards(HoodieTableMetaClient metaClient, String partition) {
    LOG.info("Loading shards for metadata table partition " + partition);

    // If there are no commits on the metadata table then the table's default FileSystemView will not return any file
    // slices even though we may have initialized them.
    HoodieTimeline timeline = metaClient.getActiveTimeline();
    if (timeline.empty()) {
      final HoodieInstant instant = new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION, HoodieActiveTimeline.createNewInstantTime());
      timeline = new HoodieDefaultTimeline(Arrays.asList(instant).stream(), metaClient.getActiveTimeline()::getInstantDetails);
    }

    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient, timeline);
    return fsView.getLatestFileSlices(partition).sorted((s1, s2) -> s1.getFileId().compareTo(s2.getFileId()))
        .collect(Collectors.toList());
  }
}
