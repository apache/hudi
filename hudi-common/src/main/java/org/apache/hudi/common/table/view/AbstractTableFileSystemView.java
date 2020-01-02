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

package org.apache.hudi.common.table.view;

import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieDataFile;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.SyncableFileSystemView;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Common thread-safe implementation for multiple TableFileSystemView Implementations. Provides uniform handling of (a)
 * Loading file-system views from underlying file-system (b) Pending compaction operations and changing file-system
 * views based on that (c) Thread-safety in loading and managing file system views for this dataset. (d) resetting
 * file-system views The actual mechanism of fetching file slices from different view storages is delegated to
 * sub-classes.
 */
public abstract class AbstractTableFileSystemView implements SyncableFileSystemView, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractTableFileSystemView.class);

  protected HoodieTableMetaClient metaClient;

  // This is the commits timeline that will be visible for all views extending this view
  private HoodieTimeline visibleCommitsAndCompactionTimeline;

  // Used to concurrently load and populate partition views
  private ConcurrentHashMap<String, Boolean> addedPartitions = new ConcurrentHashMap<>(4096);

  // Locks to control concurrency. Sync operations use write-lock blocking all fetch operations.
  // For the common-case, we allow concurrent read of single or multiple partitions
  private final ReentrantReadWriteLock globalLock = new ReentrantReadWriteLock();
  private final ReadLock readLock = globalLock.readLock();
  private final WriteLock writeLock = globalLock.writeLock();

  private String getPartitionPathFromFilePath(String fullPath) {
    return FSUtils.getRelativePartitionPath(new Path(metaClient.getBasePath()), new Path(fullPath).getParent());
  }

  /**
   * Initialize the view.
   */
  protected void init(HoodieTableMetaClient metaClient, HoodieTimeline visibleActiveTimeline) {
    this.metaClient = metaClient;
    refreshTimeline(visibleActiveTimeline);

    // Load Pending Compaction Operations
    resetPendingCompactionOperations(CompactionUtils.getAllPendingCompactionOperations(metaClient).values().stream()
        .map(e -> Pair.of(e.getKey(), CompactionOperation.convertFromAvroRecordInstance(e.getValue()))));
  }

  /**
   * Refresh commits timeline.
   * 
   * @param visibleActiveTimeline Visible Active Timeline
   */
  protected void refreshTimeline(HoodieTimeline visibleActiveTimeline) {
    this.visibleCommitsAndCompactionTimeline = visibleActiveTimeline.getCommitsAndCompactionTimeline();
  }

  /**
   * Adds the provided statuses into the file system view, and also caches it inside this object.
   */
  protected List<HoodieFileGroup> addFilesToView(FileStatus[] statuses) {
    HoodieTimer timer = new HoodieTimer().startTimer();
    List<HoodieFileGroup> fileGroups = buildFileGroups(statuses, visibleCommitsAndCompactionTimeline, true);
    long fgBuildTimeTakenMs = timer.endTimer();
    timer.startTimer();
    // Group by partition for efficient updates for both InMemory and DiskBased stuctures.
    fileGroups.stream().collect(Collectors.groupingBy(HoodieFileGroup::getPartitionPath)).entrySet().forEach(entry -> {
      String partition = entry.getKey();
      if (!isPartitionAvailableInStore(partition)) {
        storePartitionView(partition, entry.getValue());
      }
    });
    long storePartitionsTs = timer.endTimer();
    LOG.info("addFilesToView: NumFiles={}, FileGroupsCreationTime={}, StoreTimeTaken={}", statuses.length, fgBuildTimeTakenMs, storePartitionsTs);
    return fileGroups;
  }

  /**
   * Build FileGroups from passed in file-status.
   */
  protected List<HoodieFileGroup> buildFileGroups(FileStatus[] statuses, HoodieTimeline timeline,
      boolean addPendingCompactionFileSlice) {
    return buildFileGroups(convertFileStatusesToDataFiles(statuses), convertFileStatusesToLogFiles(statuses), timeline,
        addPendingCompactionFileSlice);
  }

  protected List<HoodieFileGroup> buildFileGroups(Stream<HoodieDataFile> dataFileStream,
      Stream<HoodieLogFile> logFileStream, HoodieTimeline timeline, boolean addPendingCompactionFileSlice) {
    Map<Pair<String, String>, List<HoodieDataFile>> dataFiles =
        dataFileStream.collect(Collectors.groupingBy((dataFile) -> {
          String partitionPathStr = getPartitionPathFromFilePath(dataFile.getPath());
          return Pair.of(partitionPathStr, dataFile.getFileId());
        }));

    Map<Pair<String, String>, List<HoodieLogFile>> logFiles = logFileStream.collect(Collectors.groupingBy((logFile) -> {
      String partitionPathStr =
          FSUtils.getRelativePartitionPath(new Path(metaClient.getBasePath()), logFile.getPath().getParent());
      return Pair.of(partitionPathStr, logFile.getFileId());
    }));

    Set<Pair<String, String>> fileIdSet = new HashSet<>(dataFiles.keySet());
    fileIdSet.addAll(logFiles.keySet());

    List<HoodieFileGroup> fileGroups = new ArrayList<>();
    fileIdSet.forEach(pair -> {
      String fileId = pair.getValue();
      HoodieFileGroup group = new HoodieFileGroup(pair.getKey(), fileId, timeline);
      if (dataFiles.containsKey(pair)) {
        dataFiles.get(pair).forEach(group::addDataFile);
      }
      if (logFiles.containsKey(pair)) {
        logFiles.get(pair).forEach(group::addLogFile);
      }
      if (addPendingCompactionFileSlice) {
        Option<Pair<String, CompactionOperation>> pendingCompaction =
            getPendingCompactionOperationWithInstant(group.getFileGroupId());
        if (pendingCompaction.isPresent()) {
          // If there is no delta-commit after compaction request, this step would ensure a new file-slice appears
          // so that any new ingestion uses the correct base-instant
          group.addNewFileSliceAtInstant(pendingCompaction.get().getKey());
        }
      }
      fileGroups.add(group);
    });

    return fileGroups;
  }

  /**
   * Clears the partition Map and reset view states.
   */
  @Override
  public final void reset() {
    try {
      writeLock.lock();

      addedPartitions.clear();
      resetViewState();

      // Initialize with new Hoodie timeline.
      init(metaClient, getTimeline());
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Allows all view metadata in file system view storage to be reset by subclasses.
   */
  protected abstract void resetViewState();

  /**
   * Allows lazily loading the partitions if needed.
   *
   * @param partition partition to be loaded if not present
   */
  private void ensurePartitionLoadedCorrectly(String partition) {

    Preconditions.checkArgument(!isClosed(), "View is already closed");

    // ensure we list files only once even in the face of concurrency
    addedPartitions.computeIfAbsent(partition, (partitionPathStr) -> {
      long beginTs = System.currentTimeMillis();
      if (!isPartitionAvailableInStore(partitionPathStr)) {
        // Not loaded yet
        try {
          LOG.info("Building file system view for partition ({})", partitionPathStr);

          // Create the path if it does not exist already
          Path partitionPath = FSUtils.getPartitionPath(metaClient.getBasePath(), partitionPathStr);
          FSUtils.createPathIfNotExists(metaClient.getFs(), partitionPath);
          long beginLsTs = System.currentTimeMillis();
          FileStatus[] statuses = metaClient.getFs().listStatus(partitionPath);
          long endLsTs = System.currentTimeMillis();
          LOG.info("#files found in partition (" + partitionPathStr + ") =" + statuses.length + ", Time taken ="
              + (endLsTs - beginLsTs));
          List<HoodieFileGroup> groups = addFilesToView(statuses);

          if (groups.isEmpty()) {
            storePartitionView(partitionPathStr, new ArrayList<>());
          }
        } catch (IOException e) {
          throw new HoodieIOException("Failed to list data files in partition " + partitionPathStr, e);
        }
      } else {
        LOG.debug("View already built for Partition :{}, FOUND is ", partitionPathStr);
      }
      long endTs = System.currentTimeMillis();
      LOG.info("Time to load partition ({}) = {}", partitionPathStr, (endTs - beginTs));
      return true;
    });
  }

  /**
   * Helper to convert file-status to data-files.
   *
   * @param statuses List of File-Status
   */
  private Stream<HoodieDataFile> convertFileStatusesToDataFiles(FileStatus[] statuses) {
    Predicate<FileStatus> roFilePredicate = fileStatus -> fileStatus.getPath().getName()
        .contains(metaClient.getTableConfig().getROFileFormat().getFileExtension());
    return Arrays.stream(statuses).filter(roFilePredicate).map(HoodieDataFile::new);
  }

  /**
   * Helper to convert file-status to log-files.
   *
   * @param statuses List of FIle-Status
   */
  private Stream<HoodieLogFile> convertFileStatusesToLogFiles(FileStatus[] statuses) {
    Predicate<FileStatus> rtFilePredicate = fileStatus -> fileStatus.getPath().getName()
        .contains(metaClient.getTableConfig().getRTFileFormat().getFileExtension());
    return Arrays.stream(statuses).filter(rtFilePredicate).map(HoodieLogFile::new);
  }

  /**
   * With async compaction, it is possible to see partial/complete data-files due to inflight-compactions, Ignore those
   * data-files.
   *
   * @param dataFile Data File
   */
  protected boolean isDataFileDueToPendingCompaction(HoodieDataFile dataFile) {
    final String partitionPath = getPartitionPathFromFilePath(dataFile.getPath());

    Option<Pair<String, CompactionOperation>> compactionWithInstantTime =
        getPendingCompactionOperationWithInstant(new HoodieFileGroupId(partitionPath, dataFile.getFileId()));
    return (compactionWithInstantTime.isPresent()) && (null != compactionWithInstantTime.get().getKey())
        && dataFile.getCommitTime().equals(compactionWithInstantTime.get().getKey());
  }

  /**
   * Returns true if the file-group is under pending-compaction and the file-slice' baseInstant matches compaction
   * Instant.
   *
   * @param fileSlice File Slice
   */
  protected boolean isFileSliceAfterPendingCompaction(FileSlice fileSlice) {
    Option<Pair<String, CompactionOperation>> compactionWithInstantTime =
        getPendingCompactionOperationWithInstant(fileSlice.getFileGroupId());
    LOG.info("Pending Compaction instant for ({}) is :{}", fileSlice, compactionWithInstantTime);
    return (compactionWithInstantTime.isPresent())
        && fileSlice.getBaseInstantTime().equals(compactionWithInstantTime.get().getKey());
  }

  /**
   * With async compaction, it is possible to see partial/complete data-files due to inflight-compactions, Ignore those
   * data-files.
   *
   * @param fileSlice File Slice
   */
  protected FileSlice filterDataFileAfterPendingCompaction(FileSlice fileSlice) {
    if (isFileSliceAfterPendingCompaction(fileSlice)) {
      LOG.info("File Slice ({}) is in pending compaction", fileSlice);
      // Data file is filtered out of the file-slice as the corresponding compaction
      // instant not completed yet.
      FileSlice transformed =
          new FileSlice(fileSlice.getPartitionPath(), fileSlice.getBaseInstantTime(), fileSlice.getFileId());
      fileSlice.getLogFiles().forEach(transformed::addLogFile);
      return transformed;
    }
    return fileSlice;
  }

  @Override
  public final Stream<Pair<String, CompactionOperation>> getPendingCompactionOperations() {
    try {
      readLock.lock();
      return fetchPendingCompactionOperations();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Stream<HoodieDataFile> getLatestDataFiles(String partitionStr) {
    try {
      readLock.lock();
      String partitionPath = formatPartitionKey(partitionStr);
      ensurePartitionLoadedCorrectly(partitionPath);
      return fetchLatestDataFiles(partitionPath);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Stream<HoodieDataFile> getLatestDataFiles() {
    try {
      readLock.lock();
      return fetchLatestDataFiles();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Stream<HoodieDataFile> getLatestDataFilesBeforeOrOn(String partitionStr, String maxCommitTime) {
    try {
      readLock.lock();
      String partitionPath = formatPartitionKey(partitionStr);
      ensurePartitionLoadedCorrectly(partitionPath);
      return fetchAllStoredFileGroups(partitionPath)
          .map(fileGroup -> Option.fromJavaOptional(fileGroup.getAllDataFiles()
              .filter(dataFile -> HoodieTimeline.compareTimestamps(dataFile.getCommitTime(), maxCommitTime,
                  HoodieTimeline.LESSER_OR_EQUAL))
              .filter(df -> !isDataFileDueToPendingCompaction(df)).findFirst()))
          .filter(Option::isPresent).map(Option::get);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Option<HoodieDataFile> getDataFileOn(String partitionStr, String instantTime, String fileId) {
    try {
      readLock.lock();
      String partitionPath = formatPartitionKey(partitionStr);
      ensurePartitionLoadedCorrectly(partitionPath);
      return fetchHoodieFileGroup(partitionPath, fileId).map(fileGroup -> fileGroup.getAllDataFiles()
          .filter(
              dataFile -> HoodieTimeline.compareTimestamps(dataFile.getCommitTime(), instantTime, HoodieTimeline.EQUAL))
          .filter(df -> !isDataFileDueToPendingCompaction(df)).findFirst().orElse(null));
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Get Latest data file for a partition and file-Id.
   */
  @Override
  public final Option<HoodieDataFile> getLatestDataFile(String partitionStr, String fileId) {
    try {
      readLock.lock();
      String partitionPath = formatPartitionKey(partitionStr);
      ensurePartitionLoadedCorrectly(partitionPath);
      return fetchLatestDataFile(partitionPath, fileId);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Stream<HoodieDataFile> getLatestDataFilesInRange(List<String> commitsToReturn) {
    try {
      readLock.lock();
      return fetchAllStoredFileGroups().map(fileGroup -> {
        return Option.fromJavaOptional(
            fileGroup.getAllDataFiles().filter(dataFile -> commitsToReturn.contains(dataFile.getCommitTime())
                && !isDataFileDueToPendingCompaction(dataFile)).findFirst());
      }).filter(Option::isPresent).map(Option::get);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Stream<HoodieDataFile> getAllDataFiles(String partitionStr) {
    try {
      readLock.lock();
      String partitionPath = formatPartitionKey(partitionStr);
      ensurePartitionLoadedCorrectly(partitionPath);
      return fetchAllDataFiles(partitionPath)
          .filter(df -> visibleCommitsAndCompactionTimeline.containsOrBeforeTimelineStarts(df.getCommitTime()))
          .filter(df -> !isDataFileDueToPendingCompaction(df));
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Stream<FileSlice> getLatestFileSlices(String partitionStr) {
    try {
      readLock.lock();
      String partitionPath = formatPartitionKey(partitionStr);
      ensurePartitionLoadedCorrectly(partitionPath);
      return fetchLatestFileSlices(partitionPath).map(fs -> filterDataFileAfterPendingCompaction(fs));
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Get Latest File Slice for a given fileId in a given partition.
   */
  @Override
  public final Option<FileSlice> getLatestFileSlice(String partitionStr, String fileId) {
    try {
      readLock.lock();
      String partitionPath = formatPartitionKey(partitionStr);
      ensurePartitionLoadedCorrectly(partitionPath);
      Option<FileSlice> fs = fetchLatestFileSlice(partitionPath, fileId);
      return fs.map(f -> filterDataFileAfterPendingCompaction(f));
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Stream<FileSlice> getLatestUnCompactedFileSlices(String partitionStr) {
    try {
      readLock.lock();
      String partitionPath = formatPartitionKey(partitionStr);
      ensurePartitionLoadedCorrectly(partitionPath);
      return fetchAllStoredFileGroups(partitionPath).map(fileGroup -> {
        FileSlice fileSlice = fileGroup.getLatestFileSlice().get();
        // if the file-group is under compaction, pick the latest before compaction instant time.
        Option<Pair<String, CompactionOperation>> compactionWithInstantPair =
            getPendingCompactionOperationWithInstant(fileSlice.getFileGroupId());
        if (compactionWithInstantPair.isPresent()) {
          String compactionInstantTime = compactionWithInstantPair.get().getLeft();
          return fileGroup.getLatestFileSliceBefore(compactionInstantTime);
        }
        return Option.of(fileSlice);
      }).map(Option::get);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Stream<FileSlice> getLatestFileSlicesBeforeOrOn(String partitionStr, String maxCommitTime,
      boolean includeFileSlicesInPendingCompaction) {
    try {
      readLock.lock();
      String partitionPath = formatPartitionKey(partitionStr);
      ensurePartitionLoadedCorrectly(partitionPath);
      Stream<FileSlice> fileSliceStream = fetchLatestFileSlicesBeforeOrOn(partitionPath, maxCommitTime);
      if (includeFileSlicesInPendingCompaction) {
        return fileSliceStream.map(fs -> filterDataFileAfterPendingCompaction(fs));
      } else {
        return fileSliceStream.filter(fs -> !isPendingCompactionScheduledForFileId(fs.getFileGroupId()));
      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Stream<FileSlice> getLatestMergedFileSlicesBeforeOrOn(String partitionStr, String maxInstantTime) {
    try {
      readLock.lock();
      String partition = formatPartitionKey(partitionStr);
      ensurePartitionLoadedCorrectly(partition);
      return fetchAllStoredFileGroups(partition).map(fileGroup -> {
        Option<FileSlice> fileSlice = fileGroup.getLatestFileSliceBeforeOrOn(maxInstantTime);
        // if the file-group is under construction, pick the latest before compaction instant time.
        if (fileSlice.isPresent()) {
          fileSlice = Option.of(fetchMergedFileSlice(fileGroup, fileSlice.get()));
        }
        return fileSlice;
      }).filter(Option::isPresent).map(Option::get);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Stream<FileSlice> getLatestFileSliceInRange(List<String> commitsToReturn) {
    try {
      readLock.lock();
      return fetchLatestFileSliceInRange(commitsToReturn);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Stream<FileSlice> getAllFileSlices(String partitionStr) {
    try {
      readLock.lock();
      String partition = formatPartitionKey(partitionStr);
      ensurePartitionLoadedCorrectly(partition);
      return fetchAllFileSlices(partition);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Ensure there is consistency in handling trailing slash in partition-path. Always trim it which is what is done in
   * other places.
   */
  private String formatPartitionKey(String partitionStr) {
    return partitionStr.endsWith("/") ? partitionStr.substring(0, partitionStr.length() - 1) : partitionStr;
  }

  @Override
  public final Stream<HoodieFileGroup> getAllFileGroups(String partitionStr) {
    try {
      readLock.lock();
      // Ensure there is consistency in handling trailing slash in partition-path. Always trim it which is what is done
      // in other places.
      String partition = formatPartitionKey(partitionStr);
      ensurePartitionLoadedCorrectly(partition);
      return fetchAllStoredFileGroups(partition);
    } finally {
      readLock.unlock();
    }
  }

  // Fetch APIs to be implemented by concrete sub-classes

  /**
   * Check if there is an outstanding compaction scheduled for this file.
   *
   * @param fgId File-Group Id
   * @return true if there is a pending compaction, false otherwise
   */
  protected abstract boolean isPendingCompactionScheduledForFileId(HoodieFileGroupId fgId);

  /**
   * resets the pending compaction operation and overwrite with the new list.
   *
   * @param operations Pending Compaction Operations
   */
  abstract void resetPendingCompactionOperations(Stream<Pair<String, CompactionOperation>> operations);

  /**
   * Add pending compaction operations to store.
   *
   * @param operations Pending compaction operations to be added
   */
  abstract void addPendingCompactionOperations(Stream<Pair<String, CompactionOperation>> operations);

  /**
   * Remove pending compaction operations from store.
   *
   * @param operations Pending compaction operations to be removed
   */
  abstract void removePendingCompactionOperations(Stream<Pair<String, CompactionOperation>> operations);

  /**
   * Return pending compaction operation for a file-group.
   *
   * @param fileGroupId File-Group Id
   */
  protected abstract Option<Pair<String, CompactionOperation>> getPendingCompactionOperationWithInstant(
      HoodieFileGroupId fileGroupId);

  /**
   * Fetch all pending compaction operations.
   */
  abstract Stream<Pair<String, CompactionOperation>> fetchPendingCompactionOperations();

  /**
   * Checks if partition is pre-loaded and available in store.
   *
   * @param partitionPath Partition Path
   */
  abstract boolean isPartitionAvailableInStore(String partitionPath);

  /**
   * Add a complete partition view to store.
   *
   * @param partitionPath Partition Path
   * @param fileGroups File Groups for the partition path
   */
  abstract void storePartitionView(String partitionPath, List<HoodieFileGroup> fileGroups);

  /**
   * Fetch all file-groups stored for a partition-path.
   *
   * @param partitionPath Partition path for which the file-groups needs to be retrieved.
   * @return file-group stream
   */
  abstract Stream<HoodieFileGroup> fetchAllStoredFileGroups(String partitionPath);

  /**
   * Fetch all Stored file-groups across all partitions loaded.
   *
   * @return file-group stream
   */
  abstract Stream<HoodieFileGroup> fetchAllStoredFileGroups();

  /**
   * Check if the view is already closed.
   */
  abstract boolean isClosed();

  /**
   * Default implementation for fetching latest file-slice in commit range.
   *
   * @param commitsToReturn Commits
   */
  Stream<FileSlice> fetchLatestFileSliceInRange(List<String> commitsToReturn) {
    return fetchAllStoredFileGroups().map(fileGroup -> fileGroup.getLatestFileSliceInRange(commitsToReturn))
        .map(Option::get);
  }

  /**
   * Default implementation for fetching all file-slices for a partition-path.
   *
   * @param partitionPath Partition path
   * @return file-slice stream
   */
  Stream<FileSlice> fetchAllFileSlices(String partitionPath) {
    return fetchAllStoredFileGroups(partitionPath).map(HoodieFileGroup::getAllFileSlices)
        .flatMap(sliceList -> sliceList);
  }

  /**
   * Default implementation for fetching latest data-files for the partition-path.
   */
  Stream<HoodieDataFile> fetchLatestDataFiles(final String partitionPath) {
    return fetchAllStoredFileGroups(partitionPath).map(this::getLatestDataFile).filter(Option::isPresent)
        .map(Option::get);
  }

  protected Option<HoodieDataFile> getLatestDataFile(HoodieFileGroup fileGroup) {
    return Option
        .fromJavaOptional(fileGroup.getAllDataFiles().filter(df -> !isDataFileDueToPendingCompaction(df)).findFirst());
  }

  /**
   * Default implementation for fetching latest data-files across all partitions.
   */
  Stream<HoodieDataFile> fetchLatestDataFiles() {
    return fetchAllStoredFileGroups().map(this::getLatestDataFile).filter(Option::isPresent).map(Option::get);
  }

  /**
   * Default implementation for fetching all data-files for a partition.
   *
   * @param partitionPath partition-path
   */
  Stream<HoodieDataFile> fetchAllDataFiles(String partitionPath) {
    return fetchAllStoredFileGroups(partitionPath).map(HoodieFileGroup::getAllDataFiles)
        .flatMap(dataFileList -> dataFileList);
  }

  /**
   * Default implementation for fetching file-group.
   */
  Option<HoodieFileGroup> fetchHoodieFileGroup(String partitionPath, String fileId) {
    return Option.fromJavaOptional(fetchAllStoredFileGroups(partitionPath)
        .filter(fileGroup -> fileGroup.getFileGroupId().getFileId().equals(fileId)).findFirst());
  }

  /**
   * Default implementation for fetching latest file-slices for a partition path.
   */
  Stream<FileSlice> fetchLatestFileSlices(String partitionPath) {
    return fetchAllStoredFileGroups(partitionPath).map(HoodieFileGroup::getLatestFileSlice).filter(Option::isPresent)
        .map(Option::get);
  }

  /**
   * Default implementation for fetching latest file-slices for a partition path as of instant.
   *
   * @param partitionPath Partition Path
   * @param maxCommitTime Instant Time
   */
  Stream<FileSlice> fetchLatestFileSlicesBeforeOrOn(String partitionPath, String maxCommitTime) {
    return fetchAllStoredFileGroups(partitionPath)
        .map(fileGroup -> fileGroup.getLatestFileSliceBeforeOrOn(maxCommitTime)).filter(Option::isPresent)
        .map(Option::get);
  }

  /**
   * Helper to merge last 2 file-slices. These 2 file-slices do not have compaction done yet.
   *
   * @param lastSlice Latest File slice for a file-group
   * @param penultimateSlice Penultimate file slice for a file-group in commit timeline order
   */
  private static FileSlice mergeCompactionPendingFileSlices(FileSlice lastSlice, FileSlice penultimateSlice) {
    FileSlice merged = new FileSlice(penultimateSlice.getPartitionPath(), penultimateSlice.getBaseInstantTime(),
        penultimateSlice.getFileId());
    if (penultimateSlice.getDataFile().isPresent()) {
      merged.setDataFile(penultimateSlice.getDataFile().get());
    }
    // Add Log files from penultimate and last slices
    penultimateSlice.getLogFiles().forEach(merged::addLogFile);
    lastSlice.getLogFiles().forEach(merged::addLogFile);
    return merged;
  }

  /**
   * If the file-slice is because of pending compaction instant, this method merges the file-slice with the one before
   * the compaction instant time.
   *
   * @param fileGroup File Group for which the file slice belongs to
   * @param fileSlice File Slice which needs to be merged
   */
  private FileSlice fetchMergedFileSlice(HoodieFileGroup fileGroup, FileSlice fileSlice) {
    // if the file-group is under construction, pick the latest before compaction instant time.
    Option<Pair<String, CompactionOperation>> compactionOpWithInstant =
        getPendingCompactionOperationWithInstant(fileGroup.getFileGroupId());
    if (compactionOpWithInstant.isPresent()) {
      String compactionInstantTime = compactionOpWithInstant.get().getKey();
      if (fileSlice.getBaseInstantTime().equals(compactionInstantTime)) {
        Option<FileSlice> prevFileSlice = fileGroup.getLatestFileSliceBefore(compactionInstantTime);
        if (prevFileSlice.isPresent()) {
          return mergeCompactionPendingFileSlices(fileSlice, prevFileSlice.get());
        }
      }
    }
    return fileSlice;
  }

  /**
   * Default implementation for fetching latest data-file.
   * 
   * @param partitionPath Partition path
   * @param fileId File Id
   * @return Data File if present
   */
  protected Option<HoodieDataFile> fetchLatestDataFile(String partitionPath, String fileId) {
    return Option
        .fromJavaOptional(fetchLatestDataFiles(partitionPath).filter(fs -> fs.getFileId().equals(fileId)).findFirst());
  }

  /**
   * Default implementation for fetching file-slice.
   * 
   * @param partitionPath Partition path
   * @param fileId File Id
   * @return File Slice if present
   */
  protected Option<FileSlice> fetchLatestFileSlice(String partitionPath, String fileId) {
    return Option
        .fromJavaOptional(fetchLatestFileSlices(partitionPath).filter(fs -> fs.getFileId().equals(fileId)).findFirst());
  }

  @Override
  public Option<HoodieInstant> getLastInstant() {
    return getTimeline().lastInstant();
  }

  @Override
  public HoodieTimeline getTimeline() {
    return visibleCommitsAndCompactionTimeline;
  }

  @Override
  public void sync() {
    HoodieTimeline oldTimeline = getTimeline();
    HoodieTimeline newTimeline = metaClient.reloadActiveTimeline().filterCompletedAndCompactionInstants();
    try {
      writeLock.lock();
      runSync(oldTimeline, newTimeline);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Performs complete reset of file-system view. Subsequent partition view calls will load file slices against latest
   * timeline
   *
   * @param oldTimeline Old Hoodie Timeline
   * @param newTimeline New Hoodie Timeline
   */
  protected void runSync(HoodieTimeline oldTimeline, HoodieTimeline newTimeline) {
    refreshTimeline(newTimeline);
    addedPartitions.clear();
    resetViewState();
    // Initialize with new Hoodie timeline.
    init(metaClient, newTimeline);
  }

  /**
   * Return Only Commits and Compaction timeline for building file-groups.
   * 
   * @return
   */
  public HoodieTimeline getVisibleCommitsAndCompactionTimeline() {
    return visibleCommitsAndCompactionTimeline;
  }
}
