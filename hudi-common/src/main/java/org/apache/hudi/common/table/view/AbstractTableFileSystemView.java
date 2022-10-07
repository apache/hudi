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

import org.apache.hudi.common.bootstrap.index.BootstrapIndex;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BootstrapBaseFileMapping;
import org.apache.hudi.common.model.BootstrapFileMapping;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
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

import static org.apache.hudi.common.table.timeline.HoodieTimeline.GREATER_THAN;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.GREATER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS;

/**
 * Common thread-safe implementation for multiple TableFileSystemView Implementations. Provides uniform handling of (a)
 * Loading file-system views from underlying file-system (b) Pending compaction operations and changing file-system
 * views based on that (c) Thread-safety in loading and managing file system views for this table. (d) resetting
 * file-system views The actual mechanism of fetching file slices from different view storages is delegated to
 * sub-classes.
 */
public abstract class AbstractTableFileSystemView implements SyncableFileSystemView, Serializable {

  private static final Logger LOG = LogManager.getLogger(AbstractTableFileSystemView.class);

  protected HoodieTableMetaClient metaClient;

  // This is the commits timeline that will be visible for all views extending this view
  // This is nothing but the write timeline, which contains both ingestion and compaction(major and minor) writers.
  private HoodieTimeline visibleCommitsAndCompactionTimeline;

  // Used to concurrently load and populate partition views
  private final ConcurrentHashMap<String, Boolean> addedPartitions = new ConcurrentHashMap<>(4096);

  // Locks to control concurrency. Sync operations use write-lock blocking all fetch operations.
  // For the common-case, we allow concurrent read of single or multiple partitions
  private final ReentrantReadWriteLock globalLock = new ReentrantReadWriteLock();
  private final ReadLock readLock = globalLock.readLock();
  private final WriteLock writeLock = globalLock.writeLock();

  private BootstrapIndex bootstrapIndex;

  private String getPartitionPathFor(HoodieBaseFile baseFile) {
    return FSUtils.getRelativePartitionPath(metaClient.getBasePathV2(), baseFile.getHadoopPath().getParent());
  }

  /**
   * Initialize the view.
   */
  protected void init(HoodieTableMetaClient metaClient, HoodieTimeline visibleActiveTimeline) {
    this.metaClient = metaClient;
    refreshTimeline(visibleActiveTimeline);
    resetFileGroupsReplaced(visibleCommitsAndCompactionTimeline);
    this.bootstrapIndex =  BootstrapIndex.getBootstrapIndex(metaClient);
    // Load Pending Compaction Operations
    resetPendingCompactionOperations(CompactionUtils.getAllPendingCompactionOperations(metaClient).values().stream()
        .map(e -> Pair.of(e.getKey(), CompactionOperation.convertFromAvroRecordInstance(e.getValue()))));
    // Load Pending LogCompaction Operations.
    resetPendingLogCompactionOperations(CompactionUtils.getAllPendingLogCompactionOperations(metaClient).values().stream()
        .map(e -> Pair.of(e.getKey(), CompactionOperation.convertFromAvroRecordInstance(e.getValue()))));

    resetBootstrapBaseFileMapping(Stream.empty());
    resetFileGroupsInPendingClustering(ClusteringUtils.getAllFileGroupsInPendingClusteringPlans(metaClient));
  }

  /**
   * Refresh commits timeline.
   * 
   * @param visibleActiveTimeline Visible Active Timeline
   */
  protected void refreshTimeline(HoodieTimeline visibleActiveTimeline) {
    this.visibleCommitsAndCompactionTimeline = visibleActiveTimeline.getWriteTimeline();
  }

  /**
   * Adds the provided statuses into the file system view, and also caches it inside this object.
   */
  public List<HoodieFileGroup> addFilesToView(FileStatus[] statuses) {
    HoodieTimer timer = new HoodieTimer().startTimer();
    List<HoodieFileGroup> fileGroups = buildFileGroups(statuses, visibleCommitsAndCompactionTimeline, true);
    long fgBuildTimeTakenMs = timer.endTimer();
    timer.startTimer();
    // Group by partition for efficient updates for both InMemory and DiskBased stuctures.
    fileGroups.stream().collect(Collectors.groupingBy(HoodieFileGroup::getPartitionPath)).forEach((partition, value) -> {
      if (!isPartitionAvailableInStore(partition)) {
        if (bootstrapIndex.useIndex()) {
          try (BootstrapIndex.IndexReader reader = bootstrapIndex.createReader()) {
            LOG.info("Bootstrap Index available for partition " + partition);
            List<BootstrapFileMapping> sourceFileMappings =
                reader.getSourceFileMappingForPartition(partition);
            addBootstrapBaseFileMapping(sourceFileMappings.stream()
                .map(s -> new BootstrapBaseFileMapping(new HoodieFileGroupId(s.getPartitionPath(),
                    s.getFileId()), s.getBootstrapFileStatus())));
          }
        }
        storePartitionView(partition, value);
      }
    });
    long storePartitionsTs = timer.endTimer();
    LOG.info("addFilesToView: NumFiles=" + statuses.length + ", NumFileGroups=" + fileGroups.size()
        + ", FileGroupsCreationTime=" + fgBuildTimeTakenMs
        + ", StoreTimeTaken=" + storePartitionsTs);
    return fileGroups;
  }

  /**
   * Build FileGroups from passed in file-status.
   */
  protected List<HoodieFileGroup> buildFileGroups(FileStatus[] statuses, HoodieTimeline timeline,
      boolean addPendingCompactionFileSlice) {
    return buildFileGroups(convertFileStatusesToBaseFiles(statuses), convertFileStatusesToLogFiles(statuses), timeline,
        addPendingCompactionFileSlice);
  }

  protected List<HoodieFileGroup> buildFileGroups(Stream<HoodieBaseFile> baseFileStream,
      Stream<HoodieLogFile> logFileStream, HoodieTimeline timeline, boolean addPendingCompactionFileSlice) {
    Map<Pair<String, String>, List<HoodieBaseFile>> baseFiles =
        baseFileStream.collect(Collectors.groupingBy(baseFile -> {
          String partitionPathStr = getPartitionPathFor(baseFile);
          return Pair.of(partitionPathStr, baseFile.getFileId());
        }));

    Map<Pair<String, String>, List<HoodieLogFile>> logFiles = logFileStream.collect(Collectors.groupingBy((logFile) -> {
      String partitionPathStr =
          FSUtils.getRelativePartitionPath(metaClient.getBasePathV2(), logFile.getPath().getParent());
      return Pair.of(partitionPathStr, logFile.getFileId());
    }));

    Set<Pair<String, String>> fileIdSet = new HashSet<>(baseFiles.keySet());
    fileIdSet.addAll(logFiles.keySet());

    List<HoodieFileGroup> fileGroups = new ArrayList<>();
    fileIdSet.forEach(pair -> {
      String fileId = pair.getValue();
      String partitionPath = pair.getKey();
      HoodieFileGroup group = new HoodieFileGroup(partitionPath, fileId, timeline);
      if (baseFiles.containsKey(pair)) {
        baseFiles.get(pair).forEach(group::addBaseFile);
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
   * Get replaced instant for each file group by looking at all commit instants.
   */
  private void resetFileGroupsReplaced(HoodieTimeline timeline) {
    HoodieTimer hoodieTimer = new HoodieTimer();
    hoodieTimer.startTimer();
    // for each REPLACE instant, get map of (partitionPath -> deleteFileGroup)
    HoodieTimeline replacedTimeline = timeline.getCompletedReplaceTimeline();
    Stream<Map.Entry<HoodieFileGroupId, HoodieInstant>> resultStream = replacedTimeline.getInstants().flatMap(instant -> {
      try {
        HoodieReplaceCommitMetadata replaceMetadata = HoodieReplaceCommitMetadata.fromBytes(metaClient.getActiveTimeline().getInstantDetails(instant).get(),
            HoodieReplaceCommitMetadata.class);

        // get replace instant mapping for each partition, fileId
        return replaceMetadata.getPartitionToReplaceFileIds().entrySet().stream().flatMap(entry -> entry.getValue().stream().map(e ->
                new AbstractMap.SimpleEntry<>(new HoodieFileGroupId(entry.getKey(), e), instant)));
      } catch (HoodieIOException ex) {

        if (ex.getIOException() instanceof FileNotFoundException) {
          // Replace instant could be deleted by archive and FileNotFoundException could be threw during getInstantDetails function
          // So that we need to catch the FileNotFoundException here and continue
          LOG.warn(ex.getMessage());
          return Stream.empty();
        } else {
          throw ex;
        }

      } catch (IOException e) {
        throw new HoodieIOException("error reading commit metadata for " + instant);
      }
    });

    Map<HoodieFileGroupId, HoodieInstant> replacedFileGroups = resultStream.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    resetReplacedFileGroups(replacedFileGroups);
    LOG.info("Took " + hoodieTimer.endTimer() + " ms to read  " + replacedTimeline.countInstants() + " instants, "
        + replacedFileGroups.size() + " replaced file groups");
  }

  @Override
  public void close() {
    try {
      writeLock.lock();
      clear();
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Clears the partition Map and reset view states.
   */
  @Override
  public void reset() {
    try {
      writeLock.lock();
      clear();
      // Initialize with new Hoodie timeline.
      init(metaClient, getTimeline());
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Clear the resource.
   */
  private void clear() {
    addedPartitions.clear();
    resetViewState();
    bootstrapIndex = null;
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

    ValidationUtils.checkArgument(!isClosed(), "View is already closed");

    // ensure we list files only once even in the face of concurrency
    addedPartitions.computeIfAbsent(partition, (partitionPathStr) -> {
      long beginTs = System.currentTimeMillis();
      if (!isPartitionAvailableInStore(partitionPathStr)) {
        // Not loaded yet
        try {
          LOG.info("Building file system view for partition (" + partitionPathStr + ")");

          Path partitionPath = FSUtils.getPartitionPath(metaClient.getBasePathV2(), partitionPathStr);
          long beginLsTs = System.currentTimeMillis();
          FileStatus[] statuses = listPartition(partitionPath);
          long endLsTs = System.currentTimeMillis();
          LOG.debug("#files found in partition (" + partitionPathStr + ") =" + statuses.length + ", Time taken ="
              + (endLsTs - beginLsTs));
          List<HoodieFileGroup> groups = addFilesToView(statuses);

          if (groups.isEmpty()) {
            storePartitionView(partitionPathStr, new ArrayList<>());
          }
        } catch (IOException e) {
          throw new HoodieIOException("Failed to list base files in partition " + partitionPathStr, e);
        }
      } else {
        LOG.debug("View already built for Partition :" + partitionPathStr + ", FOUND is ");
      }
      long endTs = System.currentTimeMillis();
      LOG.debug("Time to load partition (" + partitionPathStr + ") =" + (endTs - beginTs));
      return true;
    });
  }

  /**
   * Return all the files from the partition.
   *
   * @param partitionPath The absolute path of the partition
   * @throws IOException
   */
  protected FileStatus[] listPartition(Path partitionPath) throws IOException {
    try {
      return metaClient.getFs().listStatus(partitionPath);
    } catch (IOException e) {
      // Create the path if it does not exist already
      if (!metaClient.getFs().exists(partitionPath)) {
        metaClient.getFs().mkdirs(partitionPath);
        return new FileStatus[0];
      } else {
        // in case the partition path was created by another caller
        return metaClient.getFs().listStatus(partitionPath);
      }
    }
  }

  /**
   * Helper to convert file-status to base-files.
   *
   * @param statuses List of File-Status
   */
  private Stream<HoodieBaseFile> convertFileStatusesToBaseFiles(FileStatus[] statuses) {
    Predicate<FileStatus> roFilePredicate = fileStatus -> fileStatus.getPath().getName()
        .contains(metaClient.getTableConfig().getBaseFileFormat().getFileExtension());
    return Arrays.stream(statuses).filter(roFilePredicate).map(HoodieBaseFile::new);
  }

  /**
   * Helper to convert file-status to log-files.
   *
   * @param statuses List of FIle-Status
   */
  private Stream<HoodieLogFile> convertFileStatusesToLogFiles(FileStatus[] statuses) {
    Predicate<FileStatus> rtFilePredicate = fileStatus -> fileStatus.getPath().getName()
        .contains(metaClient.getTableConfig().getLogFileFormat().getFileExtension());
    return Arrays.stream(statuses).filter(rtFilePredicate).map(HoodieLogFile::new);
  }

  /**
   * With async compaction, it is possible to see partial/complete base-files due to inflight-compactions, Ignore those
   * base-files.
   *
   * @param baseFile base File
   */
  protected boolean isBaseFileDueToPendingCompaction(HoodieBaseFile baseFile) {
    final String partitionPath = getPartitionPathFor(baseFile);

    Option<Pair<String, CompactionOperation>> compactionWithInstantTime =
        getPendingCompactionOperationWithInstant(new HoodieFileGroupId(partitionPath, baseFile.getFileId()));
    return (compactionWithInstantTime.isPresent()) && (null != compactionWithInstantTime.get().getKey())
        && baseFile.getCommitTime().equals(compactionWithInstantTime.get().getKey());
  }

  /**
   * With async clustering, it is possible to see partial/complete base-files due to inflight-clustering, Ignore those
   * base-files.
   *
   * @param baseFile base File
   */
  protected boolean isBaseFileDueToPendingClustering(HoodieBaseFile baseFile) {
    List<String> pendingReplaceInstants =
        metaClient.getActiveTimeline().filterPendingReplaceTimeline().getInstants().map(HoodieInstant::getTimestamp).collect(Collectors.toList());

    return !pendingReplaceInstants.isEmpty() && pendingReplaceInstants.contains(baseFile.getCommitTime());
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
    return (compactionWithInstantTime.isPresent())
        && fileSlice.getBaseInstantTime().equals(compactionWithInstantTime.get().getKey());
  }

  /**
   * With async compaction, it is possible to see partial/complete base-files due to inflight-compactions, Ignore those
   * base-files.
   *
   * @param fileSlice File Slice
   * @param includeEmptyFileSlice include empty file-slice
   */
  protected Stream<FileSlice> filterBaseFileAfterPendingCompaction(FileSlice fileSlice, boolean includeEmptyFileSlice) {
    if (isFileSliceAfterPendingCompaction(fileSlice)) {
      LOG.debug("File Slice (" + fileSlice + ") is in pending compaction");
      // Base file is filtered out of the file-slice as the corresponding compaction
      // instant not completed yet.
      FileSlice transformed = new FileSlice(fileSlice.getPartitionPath(), fileSlice.getBaseInstantTime(), fileSlice.getFileId());
      fileSlice.getLogFiles().forEach(transformed::addLogFile);
      if (transformed.isEmpty() && !includeEmptyFileSlice) {
        return Stream.of();
      }
      return Stream.of(transformed);
    }
    return Stream.of(fileSlice);
  }

  protected HoodieFileGroup addBootstrapBaseFileIfPresent(HoodieFileGroup fileGroup) {
    boolean hasBootstrapBaseFile = fileGroup.getAllFileSlices()
        .anyMatch(fs -> fs.getBaseInstantTime().equals(METADATA_BOOTSTRAP_INSTANT_TS));
    if (hasBootstrapBaseFile) {
      HoodieFileGroup newFileGroup = new HoodieFileGroup(fileGroup);
      newFileGroup.getAllFileSlices().filter(fs -> fs.getBaseInstantTime().equals(METADATA_BOOTSTRAP_INSTANT_TS))
          .forEach(fs -> fs.setBaseFile(
              addBootstrapBaseFileIfPresent(fs.getFileGroupId(), fs.getBaseFile().get())));
      return newFileGroup;
    }
    return fileGroup;
  }

  protected FileSlice addBootstrapBaseFileIfPresent(FileSlice fileSlice) {
    if (fileSlice.getBaseInstantTime().equals(METADATA_BOOTSTRAP_INSTANT_TS)) {
      FileSlice copy = new FileSlice(fileSlice);
      copy.getBaseFile().ifPresent(dataFile -> {
        Option<BootstrapBaseFileMapping> edf = getBootstrapBaseFile(copy.getFileGroupId());
        edf.ifPresent(e -> dataFile.setBootstrapBaseFile(e.getBootstrapBaseFile()));
      });
      return copy;
    }
    return fileSlice;
  }

  protected HoodieBaseFile addBootstrapBaseFileIfPresent(HoodieFileGroupId fileGroupId, HoodieBaseFile baseFile) {
    if (baseFile.getCommitTime().equals(METADATA_BOOTSTRAP_INSTANT_TS)) {
      HoodieBaseFile copy = new HoodieBaseFile(baseFile);
      Option<BootstrapBaseFileMapping> edf = getBootstrapBaseFile(fileGroupId);
      edf.ifPresent(e -> copy.setBootstrapBaseFile(e.getBootstrapBaseFile()));
      return copy;
    }
    return baseFile;
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

  public final List<Path> getPartitionPaths() {
    try {
      readLock.lock();
      return fetchAllStoredFileGroups()
          .filter(fg -> !isFileGroupReplaced(fg))
          .map(HoodieFileGroup::getPartitionPath)
          .distinct()
          .map(name -> name.isEmpty() ? metaClient.getBasePathV2() : new Path(metaClient.getBasePathV2(), name))
          .collect(Collectors.toList());
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Stream<Pair<String, CompactionOperation>> getPendingLogCompactionOperations() {
    try {
      readLock.lock();
      return fetchPendingLogCompactionOperations();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Stream<HoodieBaseFile> getLatestBaseFiles(String partitionStr) {
    try {
      readLock.lock();
      String partitionPath = formatPartitionKey(partitionStr);
      ensurePartitionLoadedCorrectly(partitionPath);
      return fetchLatestBaseFiles(partitionPath)
          .filter(df -> !isFileGroupReplaced(partitionPath, df.getFileId()))
          .map(df -> addBootstrapBaseFileIfPresent(new HoodieFileGroupId(partitionPath, df.getFileId()), df));
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Stream<HoodieBaseFile> getLatestBaseFiles() {
    try {
      readLock.lock();
      return fetchLatestBaseFiles();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Stream<HoodieBaseFile> getLatestBaseFilesBeforeOrOn(String partitionStr, String maxCommitTime) {
    try {
      readLock.lock();
      String partitionPath = formatPartitionKey(partitionStr);
      ensurePartitionLoadedCorrectly(partitionPath);
      return fetchAllStoredFileGroups(partitionPath)
          .filter(fileGroup -> !isFileGroupReplacedBeforeOrOn(fileGroup.getFileGroupId(), maxCommitTime))
          .map(fileGroup -> Option.fromJavaOptional(fileGroup.getAllBaseFiles()
              .filter(baseFile -> HoodieTimeline.compareTimestamps(baseFile.getCommitTime(), HoodieTimeline.LESSER_THAN_OR_EQUALS, maxCommitTime
              ))
              .filter(df -> !isBaseFileDueToPendingCompaction(df) && !isBaseFileDueToPendingClustering(df)).findFirst()))
          .filter(Option::isPresent).map(Option::get)
          .map(df -> addBootstrapBaseFileIfPresent(new HoodieFileGroupId(partitionPath, df.getFileId()), df));
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Option<HoodieBaseFile> getBaseFileOn(String partitionStr, String instantTime, String fileId) {
    try {
      readLock.lock();
      String partitionPath = formatPartitionKey(partitionStr);
      ensurePartitionLoadedCorrectly(partitionPath);
      if (isFileGroupReplacedBeforeOrOn(new HoodieFileGroupId(partitionPath, fileId), instantTime)) {
        return Option.empty();
      } else {
        return fetchHoodieFileGroup(partitionPath, fileId).map(fileGroup -> fileGroup.getAllBaseFiles()
            .filter(baseFile -> HoodieTimeline.compareTimestamps(baseFile.getCommitTime(), HoodieTimeline.EQUALS,
                instantTime)).filter(df -> !isBaseFileDueToPendingCompaction(df) && !isBaseFileDueToPendingClustering(df)).findFirst().orElse(null))
            .map(df -> addBootstrapBaseFileIfPresent(new HoodieFileGroupId(partitionPath, fileId), df));
      }
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Get Latest base file for a partition and file-Id.
   */
  @Override
  public final Option<HoodieBaseFile> getLatestBaseFile(String partitionStr, String fileId) {
    try {
      readLock.lock();
      String partitionPath = formatPartitionKey(partitionStr);
      ensurePartitionLoadedCorrectly(partitionPath);
      if (isFileGroupReplaced(partitionPath, fileId)) {
        return Option.empty();
      } else {
        return fetchLatestBaseFile(partitionPath, fileId)
            .map(df -> addBootstrapBaseFileIfPresent(new HoodieFileGroupId(partitionPath, fileId), df));
      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Stream<HoodieBaseFile> getLatestBaseFilesInRange(List<String> commitsToReturn) {
    try {
      readLock.lock();
      return fetchAllStoredFileGroups()
          .filter(fileGroup -> !isFileGroupReplacedBeforeAny(fileGroup.getFileGroupId(), commitsToReturn))
          .map(fileGroup -> Pair.of(fileGroup.getFileGroupId(), Option.fromJavaOptional(
          fileGroup.getAllBaseFiles().filter(baseFile -> commitsToReturn.contains(baseFile.getCommitTime())
              && !isBaseFileDueToPendingCompaction(baseFile) && !isBaseFileDueToPendingClustering(baseFile)).findFirst()))).filter(p -> p.getValue().isPresent())
          .map(p -> addBootstrapBaseFileIfPresent(p.getKey(), p.getValue().get()));
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Stream<HoodieBaseFile> getAllBaseFiles(String partitionStr) {
    try {
      readLock.lock();
      String partitionPath = formatPartitionKey(partitionStr);
      ensurePartitionLoadedCorrectly(partitionPath);
      return fetchAllBaseFiles(partitionPath)
          .filter(df -> !isFileGroupReplaced(partitionPath, df.getFileId()))
          .filter(df -> visibleCommitsAndCompactionTimeline.containsOrBeforeTimelineStarts(df.getCommitTime()))
          .filter(df -> !isBaseFileDueToPendingCompaction(df) && !isBaseFileDueToPendingClustering(df))
          .map(df -> addBootstrapBaseFileIfPresent(new HoodieFileGroupId(partitionPath, df.getFileId()), df));
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
      return fetchLatestFileSlices(partitionPath)
              .filter(slice -> !isFileGroupReplaced(slice.getFileGroupId()))
              .flatMap(slice -> this.filterBaseFileAfterPendingCompaction(slice, true))
              .map(this::addBootstrapBaseFileIfPresent);
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
      if (isFileGroupReplaced(partitionPath, fileId)) {
        return Option.empty();
      } else {
        Option<FileSlice> fs = fetchLatestFileSlice(partitionPath, fileId);
        if (!fs.isPresent()) {
          return Option.empty();
        }
        return Option.ofNullable(filterBaseFileAfterPendingCompaction(fs.get(), true).map(this::addBootstrapBaseFileIfPresent).findFirst().orElse(null));
      }
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
      return fetchAllStoredFileGroups(partitionPath)
          .filter(fg -> !isFileGroupReplaced(fg.getFileGroupId()))
          .map(fileGroup -> {
            FileSlice fileSlice = fileGroup.getLatestFileSlice().get();
            // if the file-group is under compaction, pick the latest before compaction instant time.
            Option<Pair<String, CompactionOperation>> compactionWithInstantPair =
                getPendingCompactionOperationWithInstant(fileSlice.getFileGroupId());
            if (compactionWithInstantPair.isPresent()) {
              String compactionInstantTime = compactionWithInstantPair.get().getLeft();
              return fileGroup.getLatestFileSliceBefore(compactionInstantTime);
            }
            return Option.of(fileSlice);
          }).map(Option::get).map(this::addBootstrapBaseFileIfPresent);
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
      Stream<Stream<FileSlice>> allFileSliceStream = fetchAllStoredFileGroups(partitionPath)
              .filter(slice -> !isFileGroupReplacedBeforeOrOn(slice.getFileGroupId(), maxCommitTime))
              .map(fg -> fg.getAllFileSlicesBeforeOn(maxCommitTime));
      if (includeFileSlicesInPendingCompaction) {
        return allFileSliceStream.map(sliceStream -> sliceStream.flatMap(slice -> this.filterBaseFileAfterPendingCompaction(slice, false)))
                .map(sliceStream -> Option.fromJavaOptional(sliceStream.findFirst())).filter(Option::isPresent).map(Option::get)
                .map(this::addBootstrapBaseFileIfPresent);
      } else {
        return allFileSliceStream
                .map(sliceStream ->
                        Option.fromJavaOptional(sliceStream
                                .filter(slice -> !isPendingCompactionScheduledForFileId(slice.getFileGroupId()))
                                .filter(slice -> !slice.isEmpty())
                                .findFirst()))
                .filter(Option::isPresent).map(Option::get).map(this::addBootstrapBaseFileIfPresent);
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
      return fetchAllStoredFileGroups(partition)
          .filter(fg -> !isFileGroupReplacedBeforeOrOn(fg.getFileGroupId(), maxInstantTime))
          .map(fileGroup -> {
            Option<FileSlice> fileSlice = fileGroup.getLatestFileSliceBeforeOrOn(maxInstantTime);
            // if the file-group is under construction, pick the latest before compaction instant time.
            if (fileSlice.isPresent()) {
              fileSlice = Option.of(fetchMergedFileSlice(fileGroup, fileSlice.get()));
            }
            return fileSlice;
          }).filter(Option::isPresent).map(Option::get).map(this::addBootstrapBaseFileIfPresent);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Stream<FileSlice> getLatestFileSliceInRange(List<String> commitsToReturn) {
    try {
      readLock.lock();
      return fetchLatestFileSliceInRange(commitsToReturn)
          .filter(slice -> !isFileGroupReplacedBeforeAny(slice.getFileGroupId(), commitsToReturn))
          .map(this::addBootstrapBaseFileIfPresent);
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
      return fetchAllFileSlices(partition).filter(slice -> !isFileGroupReplaced(slice.getFileGroupId())).map(this::addBootstrapBaseFileIfPresent);
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
    return getAllFileGroupsIncludingReplaced(partitionStr).filter(fg -> !isFileGroupReplaced(fg));
  }

  private Stream<HoodieFileGroup> getAllFileGroupsIncludingReplaced(final String partitionStr) {
    try {
      readLock.lock();
      // Ensure there is consistency in handling trailing slash in partition-path. Always trim it which is what is done
      // in other places.
      String partition = formatPartitionKey(partitionStr);
      ensurePartitionLoadedCorrectly(partition);
      return fetchAllStoredFileGroups(partition).map(this::addBootstrapBaseFileIfPresent);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Stream<HoodieFileGroup> getReplacedFileGroupsBeforeOrOn(String maxCommitTime, String partitionPath) {
    return getAllFileGroupsIncludingReplaced(partitionPath).filter(fg -> isFileGroupReplacedBeforeOrOn(fg.getFileGroupId(), maxCommitTime));
  }

  @Override
  public Stream<HoodieFileGroup> getReplacedFileGroupsBefore(String maxCommitTime, String partitionPath) {
    return getAllFileGroupsIncludingReplaced(partitionPath).filter(fg -> isFileGroupReplacedBefore(fg.getFileGroupId(), maxCommitTime));
  }

  @Override
  public Stream<HoodieFileGroup> getAllReplacedFileGroups(String partitionPath) {
    return getAllFileGroupsIncludingReplaced(partitionPath).filter(fg -> isFileGroupReplaced(fg.getFileGroupId()));
  }

  @Override
  public final Stream<Pair<HoodieFileGroupId, HoodieInstant>> getFileGroupsInPendingClustering() {
    try {
      readLock.lock();
      return fetchFileGroupsInPendingClustering();
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
   * Check if there is an outstanding log compaction scheduled for this file.
   *
   * @param fgId File-Group Id
   * @return true if there is a pending log compaction, false otherwise
   */
  protected abstract boolean isPendingLogCompactionScheduledForFileId(HoodieFileGroupId fgId);

  /**
   * resets the pending Log compaction operation and overwrite with the new list.
   *
   * @param operations Pending Log Compaction Operations
   */
  abstract void resetPendingLogCompactionOperations(Stream<Pair<String, CompactionOperation>> operations);

  /**
   * Add pending Log compaction operations to store.
   *
   * @param operations Pending Log compaction operations to be added
   */
  abstract void addPendingLogCompactionOperations(Stream<Pair<String, CompactionOperation>> operations);

  /**
   * Remove pending Log compaction operations from store.
   *
   * @param operations Pending Log compaction operations to be removed
   */
  abstract void removePendingLogCompactionOperations(Stream<Pair<String, CompactionOperation>> operations);

  /**
   * Check if there is an outstanding clustering operation (requested/inflight) scheduled for this file.
   *
   * @param fgId File-Group Id
   * @return true if there is a pending clustering, false otherwise
   */
  protected abstract boolean isPendingClusteringScheduledForFileId(HoodieFileGroupId fgId);

  /**
   *  Get pending clustering instant time for specified file group. Return None if file group is not in pending
   *  clustering operation.
   */
  protected abstract Option<HoodieInstant> getPendingClusteringInstant(final HoodieFileGroupId fileGroupId);

  /**
   * Fetch all file groups in pending clustering.
   */
  protected abstract Stream<Pair<HoodieFileGroupId, HoodieInstant>> fetchFileGroupsInPendingClustering();

  /**
   * resets the pending clustering operation and overwrite with the new list.
   */
  abstract void resetFileGroupsInPendingClustering(Map<HoodieFileGroupId, HoodieInstant> fgIdToInstantMap);

  /**
   * Add metadata for file groups in pending clustering operations to the view.
   */
  abstract void addFileGroupsInPendingClustering(Stream<Pair<HoodieFileGroupId, HoodieInstant>> fileGroups);

  /**
   * Remove metadata for file groups in pending clustering operations from the view.
   */
  abstract void removeFileGroupsInPendingClustering(Stream<Pair<HoodieFileGroupId, HoodieInstant>> fileGroups);

  /**
   * Return pending compaction operation for a file-group.
   *
   * @param fileGroupId File-Group Id
   */
  protected abstract Option<Pair<String, CompactionOperation>> getPendingCompactionOperationWithInstant(
      HoodieFileGroupId fileGroupId);

  /**
   * Return pending Log compaction operation for a file-group.
   *
   * @param fileGroupId File-Group Id
   */
  protected abstract Option<Pair<String, CompactionOperation>> getPendingLogCompactionOperationWithInstant(
      HoodieFileGroupId fileGroupId);

  /**
   * Fetch all pending compaction operations.
   */
  abstract Stream<Pair<String, CompactionOperation>> fetchPendingCompactionOperations();

  /**
   * Fetch all pending log compaction operations.
   */
  abstract Stream<Pair<String, CompactionOperation>> fetchPendingLogCompactionOperations();

  /**
   * Check if there is an bootstrap base file present for this file.
   *
   * @param fgId File-Group Id
   * @return true if there is associated bootstrap base-file, false otherwise
   */
  protected abstract boolean isBootstrapBaseFilePresentForFileId(HoodieFileGroupId fgId);

  /**
   * Resets the bootstrap base file stream and overwrite with the new list.
   *
   * @param bootstrapBaseFileStream bootstrap Base File Stream
   */
  abstract void resetBootstrapBaseFileMapping(Stream<BootstrapBaseFileMapping> bootstrapBaseFileStream);

  /**
   * Add bootstrap base file stream to store.
   *
   * @param bootstrapBaseFileStream bootstrap Base File Stream to be added
   */
  abstract void addBootstrapBaseFileMapping(Stream<BootstrapBaseFileMapping> bootstrapBaseFileStream);

  /**
   * Remove bootstrap base file stream from store.
   *
   * @param bootstrapBaseFileStream bootstrap Base File Stream to be removed
   */
  abstract void removeBootstrapBaseFileMapping(Stream<BootstrapBaseFileMapping> bootstrapBaseFileStream);

  /**
   * Return pending compaction operation for a file-group.
   *
   * @param fileGroupId File-Group Id
   */
  protected abstract Option<BootstrapBaseFileMapping> getBootstrapBaseFile(HoodieFileGroupId fileGroupId);

  /**
   * Fetch all bootstrap data files.
   */
  abstract Stream<BootstrapBaseFileMapping> fetchBootstrapBaseFiles();

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
   * Track instant time for file groups replaced.
   */
  protected abstract void resetReplacedFileGroups(final Map<HoodieFileGroupId, HoodieInstant> replacedFileGroups);

  /**
   * Track instant time for new file groups replaced.
   */
  protected abstract void addReplacedFileGroups(final Map<HoodieFileGroupId, HoodieInstant> replacedFileGroups);

  /**
   * Remove file groups that are replaced in any of the specified instants.
   */
  protected abstract void removeReplacedFileIdsAtInstants(Set<String> instants);

  /**
   * Track instant time for file groups replaced.
   */
  protected abstract Option<HoodieInstant> getReplaceInstant(final HoodieFileGroupId fileGroupId);

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
        .map(Option::get).map(this::addBootstrapBaseFileIfPresent);
  }

  /**
   * Default implementation for fetching all file-slices for a partition-path.
   *
   * @param partitionPath Partition path
   * @return file-slice stream
   */
  Stream<FileSlice> fetchAllFileSlices(String partitionPath) {
    return fetchAllStoredFileGroups(partitionPath).map(this::addBootstrapBaseFileIfPresent)
        .flatMap(HoodieFileGroup::getAllFileSlices);
  }

  /**
   * Default implementation for fetching latest base-files for the partition-path.
   */
  public Stream<HoodieBaseFile> fetchLatestBaseFiles(final String partitionPath) {
    return fetchAllStoredFileGroups(partitionPath)
        .filter(fg -> !isFileGroupReplaced(fg))
        .map(fg -> Pair.of(fg.getFileGroupId(), getLatestBaseFile(fg)))
        .filter(p -> p.getValue().isPresent())
        .map(p -> addBootstrapBaseFileIfPresent(p.getKey(), p.getValue().get()));
  }

  protected Option<HoodieBaseFile> getLatestBaseFile(HoodieFileGroup fileGroup) {
    return Option
        .fromJavaOptional(fileGroup.getAllBaseFiles().filter(df -> !isBaseFileDueToPendingCompaction(df) && !isBaseFileDueToPendingClustering(df)).findFirst());
  }

  /**
   * Fetch latest base-files across all partitions.
   */
  private Stream<HoodieBaseFile> fetchLatestBaseFiles() {
    return fetchAllStoredFileGroups()
        .filter(fg -> !isFileGroupReplaced(fg))
        .map(fg -> Pair.of(fg.getFileGroupId(), getLatestBaseFile(fg)))
        .filter(p -> p.getValue().isPresent())
        .map(p -> addBootstrapBaseFileIfPresent(p.getKey(), p.getValue().get()));
  }

  /**
   * Default implementation for fetching all base-files for a partition.
   *
   * @param partitionPath partition-path
   */
  Stream<HoodieBaseFile> fetchAllBaseFiles(String partitionPath) {
    return fetchAllStoredFileGroups(partitionPath).flatMap(HoodieFileGroup::getAllBaseFiles);
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
   * Helper to merge last 2 file-slices. These 2 file-slices do not have compaction done yet.
   *
   * @param lastSlice Latest File slice for a file-group
   * @param penultimateSlice Penultimate file slice for a file-group in commit timeline order
   */
  private static FileSlice mergeCompactionPendingFileSlices(FileSlice lastSlice, FileSlice penultimateSlice) {
    FileSlice merged = new FileSlice(penultimateSlice.getPartitionPath(), penultimateSlice.getBaseInstantTime(),
        penultimateSlice.getFileId());
    if (penultimateSlice.getBaseFile().isPresent()) {
      merged.setBaseFile(penultimateSlice.getBaseFile().get());
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
   * Default implementation for fetching latest base-file.
   *
   * @param partitionPath Partition path
   * @param fileId File Id
   * @return base File if present
   */
  protected Option<HoodieBaseFile> fetchLatestBaseFile(String partitionPath, String fileId) {
    return Option.fromJavaOptional(fetchLatestBaseFiles(partitionPath)
        .filter(fs -> fs.getFileId().equals(fileId)).findFirst());
  }

  /**
   * Default implementation for fetching file-slice.
   *
   * @param partitionPath Partition path
   * @param fileId File Id
   * @return File Slice if present
   */
  public Option<FileSlice> fetchLatestFileSlice(String partitionPath, String fileId) {
    return Option
        .fromJavaOptional(fetchLatestFileSlices(partitionPath).filter(fs -> fs.getFileId().equals(fileId)).findFirst());
  }

  private boolean isFileGroupReplaced(String partitionPath, String fileId) {
    return isFileGroupReplaced(new HoodieFileGroupId(partitionPath, fileId));
  }

  private boolean isFileGroupReplaced(HoodieFileGroup fileGroup) {
    return isFileGroupReplaced(fileGroup.getFileGroupId());
  }

  private boolean isFileGroupReplaced(HoodieFileGroupId fileGroup) {
    return getReplaceInstant(fileGroup).isPresent();
  }

  private boolean isFileGroupReplacedBeforeAny(HoodieFileGroupId fileGroupId, List<String> instants) {
    return isFileGroupReplacedBeforeOrOn(fileGroupId, instants.stream().max(Comparator.naturalOrder()).get());
  }

  private boolean isFileGroupReplacedBefore(HoodieFileGroupId fileGroupId, String instant) {
    Option<HoodieInstant> hoodieInstantOption = getReplaceInstant(fileGroupId);
    if (!hoodieInstantOption.isPresent()) {
      return false;
    }

    return HoodieTimeline.compareTimestamps(instant, GREATER_THAN, hoodieInstantOption.get().getTimestamp());
  }

  private boolean isFileGroupReplacedBeforeOrOn(HoodieFileGroupId fileGroupId, String instant) {
    Option<HoodieInstant> hoodieInstantOption = getReplaceInstant(fileGroupId);
    if (!hoodieInstantOption.isPresent()) {
      return false;
    }

    return HoodieTimeline.compareTimestamps(instant, GREATER_THAN_OR_EQUALS, hoodieInstantOption.get().getTimestamp());
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
    HoodieTimeline newTimeline = metaClient.reloadActiveTimeline().filterCompletedOrMajorOrMinorCompactionInstants();
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
    clear();
    // Initialize with new Hoodie timeline.
    init(metaClient, newTimeline);
  }

  /**
   * Return Only Commits and Compaction timeline for building file-groups.
   *
   * @return {@code HoodieTimeline}
   */
  public HoodieTimeline getVisibleCommitsAndCompactionTimeline() {
    return visibleCommitsAndCompactionTimeline;
  }
}
