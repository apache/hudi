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
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.CompletionTimeQueryView;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS;
import static org.apache.hudi.common.table.timeline.InstantComparison.EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;

/**
 * Common thread-safe implementation for multiple TableFileSystemView Implementations.
 * Provides uniform handling of:
 * <ul>
 *   <li>Loading file-system views from underlying file-system;</li>
 *   <li>Pending compaction operations and changing file-system views based on that;</li>
 *   <li>Thread-safety in loading and managing file system views for this table;</li>
 *   <li>resetting file-system views.</li>
 * </ul>
 * The actual mechanism of fetching file slices from different view storages is delegated to sub-classes.
 */
public abstract class AbstractTableFileSystemView implements SyncableFileSystemView, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractTableFileSystemView.class);
  protected final HoodieTableMetadata tableMetadata;

  protected HoodieTableMetaClient metaClient;

  protected CompletionTimeQueryView completionTimeQueryView;

  // This is the commits timeline that will be visible for all views extending this view
  // This is nothing but the write timeline, which contains both ingestion and compaction(major and minor) writers.
  private HoodieTimeline visibleCommitsAndCompactionTimeline;

  // Used to concurrently load and populate partition views
  private final ConcurrentHashMap<String, Boolean> addedPartitions = new ConcurrentHashMap<>(4096);

  // Locks to control concurrency. Sync operations use write-lock blocking all fetch operations.
  // For the common-case, we allow concurrent read of single or multiple partitions
  private final ReentrantReadWriteLock globalLock = new ReentrantReadWriteLock();
  protected final ReadLock readLock = globalLock.readLock();
  protected final WriteLock writeLock = globalLock.writeLock();

  private BootstrapIndex bootstrapIndex;
  private HoodieTableVersion tableVersion;

  protected AbstractTableFileSystemView(HoodieTableMetadata tableMetadata) {
    this.tableMetadata = tableMetadata;
  }

  /**
   * Initialize the view.
   */
  protected void init(HoodieTableMetaClient metaClient, HoodieTimeline visibleActiveTimeline) {
    this.metaClient = metaClient;
    this.completionTimeQueryView = metaClient.getTableFormat().getTimelineFactory().createCompletionTimeQueryView(metaClient);
    this.tableVersion = metaClient.getTableConfig().getTableVersion();
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
   * Refresh the completion time query view.
   */
  protected void refreshCompletionTimeQueryView() {
    this.completionTimeQueryView = metaClient.getTableFormat().getTimelineFactory().createCompletionTimeQueryView(metaClient);
  }

  /**
   * Returns the completion time for instant.
   */
  public Option<String> getCompletionTime(String instantTime) {
    return this.completionTimeQueryView.getCompletionTime(instantTime, instantTime);
  }

  /**
   * Adds the provided statuses into the file system view, and also caches it inside this object.
   * If the file statuses are limited to a single partition, use {@link #addFilesToView(String, List)} instead.
   */
  public List<HoodieFileGroup> addFilesToView(List<StoragePathInfo> statuses) {
    Map<String, List<StoragePathInfo>> statusesByPartitionPath = statuses.stream()
        .collect(Collectors.groupingBy(fileStatus -> FSUtils.getRelativePartitionPath(metaClient.getBasePath(), fileStatus.getPath().getParent())));
    return statusesByPartitionPath.entrySet().stream().map(entry -> addFilesToView(entry.getKey(), entry.getValue()))
        .flatMap(List::stream).collect(Collectors.toList());
  }

  /**
   * Adds the provided statuses into the file system view for a single partition, and also caches it inside this object.
   */
  public List<HoodieFileGroup> addFilesToView(String partitionPath, List<StoragePathInfo> statuses) {
    HoodieTimer timer = HoodieTimer.start();
    List<HoodieFileGroup> fileGroups = buildFileGroups(partitionPath, statuses, visibleCommitsAndCompactionTimeline, true);
    long fgBuildTimeTakenMs = timer.endTimer();
    timer.startTimer();
    // Group by partition for efficient updates for both InMemory and DiskBased structures.
    fileGroups.stream().collect(Collectors.groupingBy(HoodieFileGroup::getPartitionPath))
        .forEach((partition, value) -> {
          if (!isPartitionAvailableInStore(partition)) {
            if (bootstrapIndex.useIndex()) {
              try (BootstrapIndex.IndexReader reader = bootstrapIndex.createReader()) {
                LOG.info("Bootstrap Index available for partition {}", partition);
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
    LOG.debug("addFilesToView: NumFiles={}, NumFileGroups={}, FileGroupsCreationTime={}, StoreTimeTaken={}",
        statuses.size(), fileGroups.size(), fgBuildTimeTakenMs, storePartitionsTs);
    return fileGroups;
  }

  /**
   * Build FileGroups from passed in file-status.
   */
  protected List<HoodieFileGroup> buildFileGroups(String partition, List<StoragePathInfo> statuses, HoodieTimeline timeline,
                                                  boolean addPendingCompactionFileSlice) {
    return buildFileGroups(partition, convertFileStatusesToBaseFiles(statuses), convertFileStatusesToLogFiles(statuses),
        timeline,
        addPendingCompactionFileSlice);
  }

  protected List<HoodieFileGroup> buildFileGroups(String partition, Stream<HoodieBaseFile> baseFileStream,
                                                  Stream<HoodieLogFile> logFileStream, HoodieTimeline timeline, boolean addPendingCompactionFileSlice) {
    Map<String, List<HoodieBaseFile>> baseFiles =
        baseFileStream.collect(Collectors.groupingBy(HoodieBaseFile::getFileId));

    Map<String, List<HoodieLogFile>> logFiles = logFileStream.collect(Collectors.groupingBy(HoodieLogFile::getFileId));

    Set<String> fileIdSet = new HashSet<>(baseFiles.keySet());
    fileIdSet.addAll(logFiles.keySet());

    List<HoodieFileGroup> fileGroups = new ArrayList<>(fileIdSet.size());
    fileIdSet.forEach(fileId -> {
      HoodieFileGroup group = new HoodieFileGroup(partition, fileId, timeline);
      if (baseFiles.containsKey(fileId)) {
        baseFiles.get(fileId).forEach(group::addBaseFile);
      }
      if (addPendingCompactionFileSlice) {
        // pending compaction file slice must be added before log files so that
        // the log files completed later than the compaction instant time could be included
        // in the file slice with that compaction instant time as base instant time.
        Option<Pair<String, CompactionOperation>> pendingCompaction =
            getPendingCompactionOperationWithInstant(group.getFileGroupId());
        if (pendingCompaction.isPresent()) {
          // If there is no delta-commit after compaction request, this step would ensure a new file-slice appears
          // so that any new ingestion uses the correct base-instant
          group.addNewFileSliceAtInstant(pendingCompaction.get().getKey());
        }
      }
      if (logFiles.containsKey(fileId)) {
        // this should work for both table versions >= 8 and lower.
        logFiles.get(fileId).stream().sorted(HoodieLogFile.getLogFileComparator())
            .forEach(logFile -> group.addLogFile(completionTimeQueryView, logFile));
      }
      fileGroups.add(group);
    });

    return fileGroups;
  }

  private boolean tableVersion8AndAbove() {
    return tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT);
  }

  /**
   * Get replaced instant for each file group by looking at all commit instants.
   */
  private void resetFileGroupsReplaced(HoodieTimeline timeline) {
    HoodieTimer hoodieTimer = HoodieTimer.start();
    // for each REPLACE instant, get map of (partitionPath -> deleteFileGroup)
    HoodieTimeline replacedTimeline = timeline.getCompletedReplaceTimeline();
    Stream<Map.Entry<HoodieFileGroupId, HoodieInstant>> resultStream = replacedTimeline.getInstantsAsStream().flatMap(instant -> {
      try {
        HoodieReplaceCommitMetadata replaceMetadata =
            metaClient.getActiveTimeline().readReplaceCommitMetadata(instant);

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
        throw new HoodieIOException("error reading commit metadata for " + instant, e);
      }
    });

    // Duplicate key error when insert_overwrite same partition in multi writer, keep the instant with greater timestamp when the file group id conflicts
    Map<HoodieFileGroupId, HoodieInstant> replacedFileGroups = resultStream.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
        (instance1, instance2) -> compareTimestamps(instance1.requestedTime(), LESSER_THAN, instance2.requestedTime()) ? instance2 : instance1));
    resetReplacedFileGroups(replacedFileGroups);
    LOG.info("Took {} ms to read {} instants, {} replaced file groups", hoodieTimer.endTimer(), replacedTimeline.countInstants(), replacedFileGroups.size());
  }

  @Override
  public void close() {
    try {
      writeLock.lock();
      closeResources();
      clear();
    } catch (Exception ex) {
      throw new HoodieException("Unable to close file system view", ex);
    } finally {
      writeLock.unlock();
    }
  }

  protected void closeResources() throws Exception {
    if (this.completionTimeQueryView != null) {
      this.completionTimeQueryView.close();
      this.completionTimeQueryView = null;
    }
    this.metaClient = null;
    this.visibleCommitsAndCompactionTimeline = null;
    tableMetadata.close();
  }

  /**
   * Clears the partition Map and reset view states.
   * <p>
   * NOTE: The logic MUST BE guarded by the write lock.
   */
  @Override
  public void reset() {
    try {
      writeLock.lock();
      clear();
      // Initialize with new Hoodie timeline.
      init(metaClient, getTimeline());
      tableMetadata.reset();
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Clear the resource.
   */
  protected void clear() {
    addedPartitions.clear();
    resetViewState();
    bootstrapIndex = null;
  }

  /**
   * Allows all view metadata in file system view storage to be reset by subclasses.
   */
  protected abstract void resetViewState();

  /**
   * Batch loading all the partitions if needed.
   *
   * @return A list of relative partition paths of all partitions.
   */
  private List<String> ensureAllPartitionsLoadedCorrectly() {
    ValidationUtils.checkArgument(!isClosed(), "View is already closed");
    try {
      List<String> formattedPartitionList = tableMetadata.getAllPartitionPaths().stream()
          .map(this::formatPartitionKey).collect(Collectors.toList());
      ensurePartitionsLoadedCorrectly(formattedPartitionList);
      return formattedPartitionList;
    } catch (IOException e) {
      throw new HoodieIOException("Failed to get all partition paths", e);
    }
  }

  /**
   * Allows lazily loading the partitions if needed.
   *
   * @param partitionList list of partitions to be loaded if not present.
   */
  private void ensurePartitionsLoadedCorrectly(List<String> partitionList) {

    ValidationUtils.checkArgument(!isClosed(), "View is already closed");

    Set<String> partitionSet = new HashSet<>();
    synchronized (addedPartitions) {
      partitionList.forEach(partition -> {
        if (!addedPartitions.containsKey(partition) && !isPartitionAvailableInStore(partition)) {
          partitionSet.add(partition);
        }
      });

      if (!partitionSet.isEmpty()) {
        long beginTs = System.currentTimeMillis();
        // Not loaded yet
        try {
          LOG.debug("Building file system view for partitions: {}", partitionSet);

          // Pairs of relative partition path and absolute partition path
          List<Pair<String, StoragePath>> absolutePartitionPathList = partitionSet.stream()
              .map(partition -> Pair.of(
                  partition, FSUtils.constructAbsolutePath(metaClient.getBasePath(), partition)))
              .collect(Collectors.toList());
          long beginLsTs = System.currentTimeMillis();
          Map<Pair<String, StoragePath>, List<StoragePathInfo>> pathInfoMap =
              tableMetadata.listPartitions(absolutePartitionPathList);
          long endLsTs = System.currentTimeMillis();
          LOG.debug("Time taken to list partitions {} ={}", partitionSet, (endLsTs - beginLsTs));
          pathInfoMap.forEach((partitionPair, statuses) -> {
            String relativePartitionStr = partitionPair.getLeft();
            List<HoodieFileGroup> groups = addFilesToView(relativePartitionStr, statuses);
            if (groups.isEmpty()) {
              storePartitionView(relativePartitionStr, Collections.emptyList());
            }
            LOG.debug("#files found in partition ({}) ={}", relativePartitionStr, statuses.size());
          });
        } catch (IOException e) {
          throw new HoodieIOException("Failed to list base files in partitions " + partitionSet, e);
        }
        long endTs = System.currentTimeMillis();
        LOG.debug("Time to load partition {} ={}", partitionSet, (endTs - beginTs));
      }

      partitionSet.forEach(partition ->
          addedPartitions.computeIfAbsent(partition, partitionPathStr -> true)
      );
    }
  }

  /**
   * Returns all files situated at the given partition.
   */
  private List<StoragePathInfo> getAllFilesInPartition(String relativePartitionPath)
      throws IOException {
    StoragePath partitionPath = FSUtils.constructAbsolutePath(metaClient.getBasePath(),
        relativePartitionPath);
    long beginLsTs = System.currentTimeMillis();
    List<StoragePathInfo> pathInfoList = tableMetadata.getAllFilesInPartition(partitionPath);
    long endLsTs = System.currentTimeMillis();
    LOG.debug(
        "#files found in partition ({}}) = {}, Time taken ={}", relativePartitionPath, pathInfoList.size(), (endLsTs - beginLsTs));
    return pathInfoList;
  }

  /**
   * Allows lazily loading the partitions if needed.
   *
   * @param partition partition to be loaded if not present
   */
  protected void ensurePartitionLoadedCorrectly(String partition) {

    ValidationUtils.checkArgument(!isClosed(), "View is already closed");

    // ensure we list files only once even in the face of concurrency
    addedPartitions.computeIfAbsent(partition, (partitionPathStr) -> {
      long beginTs = System.currentTimeMillis();
      if (!isPartitionAvailableInStore(partitionPathStr)) {
        // Not loaded yet
        try {
          LOG.info("Building file system view for partition ({})", partitionPathStr);
          List<HoodieFileGroup> groups = addFilesToView(partitionPathStr, getAllFilesInPartition(partitionPathStr));
          if (groups.isEmpty()) {
            storePartitionView(partitionPathStr, new ArrayList<>());
          }
        } catch (IOException e) {
          throw new HoodieIOException("Failed to list base files in partition " + partitionPathStr, e);
        }
      } else {
        LOG.debug("View already built for Partition :{}", partitionPathStr);
      }
      long endTs = System.currentTimeMillis();
      LOG.debug("Time to load partition ({}) ={}", partitionPathStr, (endTs - beginTs));
      return true;
    });
  }

  /**
   * Helper to convert file-status to base-files.
   *
   * @param pathInfoList List of StoragePathInfo
   */
  private Stream<HoodieBaseFile> convertFileStatusesToBaseFiles(List<StoragePathInfo> pathInfoList) {
    String baseFileExtension = metaClient.getTableConfig().getBaseFileFormat().getFileExtension();
    boolean isMultipleBaseFileFormatsEnabled = metaClient.getTableConfig().isMultipleBaseFileFormatsEnabled();
    Predicate<StoragePathInfo> roFilePredicate = pathInfo -> {
      String pathName = pathInfo.getPath().getName();
      // Filter base files if:
      // 1. file extension equals to table configured file extension
      // 2. file is not .hoodie_partition_metadata
      if (pathName.startsWith(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX)) {
        return false;
      } else if (isMultipleBaseFileFormatsEnabled) {
        return pathName.contains(HoodieFileFormat.PARQUET.getFileExtension())
            || pathName.contains(HoodieFileFormat.ORC.getFileExtension())
            || pathName.contains(HoodieFileFormat.HFILE.getFileExtension());
      } else {
        return pathName.contains(baseFileExtension);
      }
    };
    return pathInfoList.stream().filter(roFilePredicate).map(HoodieBaseFile::new);
  }

  /**
   * Helper to convert file-status to log-files.
   *
   * @param pathInfoList List of StoragePathInfo
   */
  private Stream<HoodieLogFile> convertFileStatusesToLogFiles(List<StoragePathInfo> pathInfoList) {
    String logFileExtension = metaClient.getTableConfig().getLogFileFormat().getFileExtension();
    Predicate<StoragePathInfo> rtFilePredicate = pathInfo -> {
      String fileName = pathInfo.getPath().getName();
      Matcher matcher = FSUtils.LOG_FILE_PATTERN.matcher(fileName);
      return matcher.matches() && fileName.contains(logFileExtension);
    };
    return pathInfoList.stream().filter(rtFilePredicate).map(HoodieLogFile::new);
  }

  /**
   * With async compaction, it is possible to see partial/complete base-files due to inflight-compactions, Ignore those
   * base-files.
   *
   * @param partitionPath partition path for the base file
   * @param baseFile base File
   */
  protected boolean isBaseFileDueToPendingCompaction(String partitionPath, HoodieBaseFile baseFile) {
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
    return metaClient.getActiveTimeline().isPendingClusteringInstant(baseFile.getCommitTime());
  }

  /**
   * Returns true if the file-group is under pending-compaction and the file-slice' baseInstant matches compaction
   * Instant.
   *
   * @param fileSlice File Slice
   */
  private boolean isFileSliceAfterPendingCompaction(FileSlice fileSlice) {
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
      LOG.debug("File Slice ({}) is in pending compaction", fileSlice);
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

  /**
   * Ignores the uncommitted base and log files.
   *
   * @param fileSlice File Slice
   * @param includeEmptyFileSlice include empty file-slice
   */
  private Stream<FileSlice> filterUncommittedFiles(FileSlice fileSlice, boolean includeEmptyFileSlice) {
    Option<HoodieBaseFile> committedBaseFile = fileSlice.getBaseFile().isPresent() && completionTimeQueryView.isCompleted(fileSlice.getBaseInstantTime()) ? fileSlice.getBaseFile() : Option.empty();
    List<HoodieLogFile> committedLogFiles = fileSlice.getLogFiles().filter(logFile -> completionTimeQueryView.isCompleted(logFile.getDeltaCommitTime())).collect(Collectors.toList());
    if ((fileSlice.getBaseFile().isPresent() && !committedBaseFile.isPresent())
        || committedLogFiles.size() != fileSlice.getLogFiles().count()) {
      LOG.debug("File Slice ({}) has uncommitted files.", fileSlice);
      // A file is filtered out of the file-slice if the corresponding
      // instant has not completed yet.
      FileSlice transformed = new FileSlice(fileSlice.getPartitionPath(), fileSlice.getBaseInstantTime(), fileSlice.getFileId());
      committedBaseFile.ifPresent(transformed::setBaseFile);
      committedLogFiles.forEach(transformed::addLogFile);
      if (transformed.isEmpty() && !includeEmptyFileSlice) {
        return Stream.of();
      }
      return Stream.of(transformed);
    }
    return Stream.of(fileSlice);
  }

  /**
   * Ignores the uncommitted log files.
   *
   * @param fileSlice File Slice
   */
  private FileSlice filterUncommittedLogs(FileSlice fileSlice) {
    List<HoodieLogFile> committedLogFiles = fileSlice.getLogFiles().filter(logFile -> completionTimeQueryView.isCompleted(logFile.getDeltaCommitTime())).collect(Collectors.toList());
    if (committedLogFiles.size() != fileSlice.getLogFiles().count()) {
      LOG.debug("File Slice ({}) has uncommitted log files.", fileSlice);
      // A file is filtered out of the file-slice if the corresponding
      // instant has not completed yet.
      FileSlice transformed = new FileSlice(fileSlice.getPartitionPath(), fileSlice.getBaseInstantTime(), fileSlice.getFileId());
      fileSlice.getBaseFile().ifPresent(transformed::setBaseFile);
      committedLogFiles.forEach(transformed::addLogFile);
      return transformed;
    }
    return fileSlice;
  }

  protected HoodieFileGroup addBootstrapBaseFileIfPresent(HoodieFileGroup fileGroup) {
    return addBootstrapBaseFileIfPresent(fileGroup, this::getBootstrapBaseFile);
  }

  protected HoodieFileGroup addBootstrapBaseFileIfPresent(HoodieFileGroup fileGroup, Function<HoodieFileGroupId, Option<BootstrapBaseFileMapping>> bootstrapBaseFileMappingFunc) {
    boolean hasBootstrapBaseFile = fileGroup.getAllFileSlices()
        .anyMatch(fs -> fs.getBaseInstantTime().equals(METADATA_BOOTSTRAP_INSTANT_TS));
    if (hasBootstrapBaseFile) {
      HoodieFileGroup newFileGroup = new HoodieFileGroup(fileGroup);
      newFileGroup.getAllFileSlices().filter(fs -> fs.getBaseInstantTime().equals(METADATA_BOOTSTRAP_INSTANT_TS))
          .forEach(fs -> fs.setBaseFile(
              addBootstrapBaseFileIfPresent(fs.getFileGroupId(), fs.getBaseFile().get(), bootstrapBaseFileMappingFunc)));
      return newFileGroup;
    }
    return fileGroup;
  }

  protected FileSlice addBootstrapBaseFileIfPresent(FileSlice fileSlice) {
    return addBootstrapBaseFileIfPresent(fileSlice, this::getBootstrapBaseFile);
  }

  protected FileSlice addBootstrapBaseFileIfPresent(FileSlice fileSlice, Function<HoodieFileGroupId, Option<BootstrapBaseFileMapping>> bootstrapBaseFileMappingFunc) {
    if (fileSlice.getBaseInstantTime().equals(METADATA_BOOTSTRAP_INSTANT_TS)) {
      FileSlice copy = new FileSlice(fileSlice);
      copy.getBaseFile().ifPresent(dataFile -> {
        Option<BootstrapBaseFileMapping> edf = getBootstrapBaseFile(copy.getFileGroupId());
        bootstrapBaseFileMappingFunc.apply(copy.getFileGroupId()).ifPresent(e -> dataFile.setBootstrapBaseFile(e.getBootstrapBaseFile()));
      });
      return copy;
    }
    return fileSlice;
  }

  protected HoodieBaseFile addBootstrapBaseFileIfPresent(HoodieFileGroupId fileGroupId, HoodieBaseFile baseFile) {
    return addBootstrapBaseFileIfPresent(fileGroupId, baseFile, this::getBootstrapBaseFile);
  }

  protected HoodieBaseFile addBootstrapBaseFileIfPresent(
      HoodieFileGroupId fileGroupId,
      HoodieBaseFile baseFile,
      Function<HoodieFileGroupId, Option<BootstrapBaseFileMapping>> bootstrapBaseFileMappingFunc) {
    if (baseFile.getCommitTime().equals(METADATA_BOOTSTRAP_INSTANT_TS)) {
      HoodieBaseFile copy = new HoodieBaseFile(baseFile);
      bootstrapBaseFileMappingFunc.apply(fileGroupId).ifPresent(e -> copy.setBootstrapBaseFile(e.getBootstrapBaseFile()));
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

  public final List<StoragePath> getPartitionPaths() {
    try {
      readLock.lock();
      return fetchAllStoredFileGroups()
          .filter(fg -> !isFileGroupReplaced(fg))
          .map(HoodieFileGroup::getPartitionPath)
          .distinct()
          .map(name -> name.isEmpty() ? metaClient.getBasePath() : new StoragePath(metaClient.getBasePath(), name))
          .collect(Collectors.toList());
    } finally {
      readLock.unlock();
    }
  }

  public final List<String> getPartitionNames() {
    try {
      readLock.lock();
      return fetchAllStoredFileGroups()
          .filter(fg -> !isFileGroupReplaced(fg))
          .map(HoodieFileGroup::getPartitionPath)
          .distinct()
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
      return getLatestBaseFilesBeforeOrOnFromCache(partitionPath, maxCommitTime);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Map<String, Stream<HoodieBaseFile>> getAllLatestBaseFilesBeforeOrOn(String maxCommitTime) {
    try {
      readLock.lock();
      List<String> formattedPartitionList = ensureAllPartitionsLoadedCorrectly();
      return formattedPartitionList.stream().collect(Collectors.toMap(
          Function.identity(),
          partitionPath -> getLatestBaseFilesBeforeOrOnFromCache(partitionPath, maxCommitTime)
      ));
    } finally {
      readLock.unlock();
    }
  }

  private Stream<HoodieBaseFile> getLatestBaseFilesBeforeOrOnFromCache(String partitionPath, String maxCommitTime) {
    return fetchAllStoredFileGroups(partitionPath)
        .filter(fileGroup -> !isFileGroupReplacedBeforeOrOn(fileGroup.getFileGroupId(), maxCommitTime))
        .map(fileGroup -> Option.fromJavaOptional(fileGroup.getAllBaseFiles()
            .filter(baseFile -> compareTimestamps(baseFile.getCommitTime(), LESSER_THAN_OR_EQUALS, maxCommitTime
            ))
            .filter(df -> !isBaseFileDueToPendingCompaction(partitionPath, df) && !isBaseFileDueToPendingClustering(df)).findFirst()))
        .filter(Option::isPresent).map(Option::get)
        .map(df -> addBootstrapBaseFileIfPresent(new HoodieFileGroupId(partitionPath, df.getFileId()), df));
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
                .filter(baseFile -> compareTimestamps(baseFile.getCommitTime(), EQUALS,
                    instantTime)).filter(df -> !isBaseFileDueToPendingCompaction(partitionPath, df) && !isBaseFileDueToPendingClustering(df)).findFirst().orElse(null))
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
                  && !isBaseFileDueToPendingCompaction(fileGroup.getPartitionPath(), baseFile) && !isBaseFileDueToPendingClustering(baseFile)).findFirst()))).filter(p -> p.getValue().isPresent())
          .map(p -> addBootstrapBaseFileIfPresent(p.getKey(), p.getValue().get()));
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void loadAllPartitions() {
    try {
      readLock.lock();
      ensureAllPartitionsLoadedCorrectly();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void loadPartitions(List<String> partitionPaths) {
    try {
      readLock.lock();
      ensurePartitionsLoadedCorrectly(partitionPaths);
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
          .filter(df -> !isBaseFileDueToPendingCompaction(partitionPath, df) && !isBaseFileDueToPendingClustering(df))
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
          .flatMap(slice -> tableVersion8AndAbove()
              ? this.filterUncommittedFiles(slice, true)
              : this.filterBaseFileAfterPendingCompaction(slice, true))
          .map(this::addBootstrapBaseFileIfPresent);
    } finally {
      readLock.unlock();
    }
  }

  public Stream<FileSlice> getLatestFileSlicesIncludingInflight(String partitionPath) {
    try {
      readLock.lock();
      ensurePartitionLoadedCorrectly(partitionPath);
      return fetchAllStoredFileGroups(partitionPath)
          .map(HoodieFileGroup::getLatestFileSlicesIncludingInflight)
          .filter(Option::isPresent)
          .map(Option::get);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Stream<FileSlice> getLatestFileSlicesStateless(String partitionStr) {
    String partition = formatPartitionKey(partitionStr);
    if (isPartitionAvailableInStore(partition)) {
      return getLatestFileSlices(partition);
    } else {
      try {
        Stream<FileSlice> fileSliceStream = buildFileGroups(partition, getAllFilesInPartition(partition), visibleCommitsAndCompactionTimeline, true).stream()
            .filter(fg -> !isFileGroupReplaced(fg))
            .map(HoodieFileGroup::getLatestFileSlice)
            .filter(Option::isPresent).map(Option::get)
            .flatMap(slice -> tableVersion8AndAbove()
                ? this.filterUncommittedFiles(slice, true)
                : this.filterBaseFileAfterPendingCompaction(slice, true));

        if (bootstrapIndex.useIndex()) {
          final Map<HoodieFileGroupId, BootstrapBaseFileMapping> bootstrapBaseFileMappings = getBootstrapBaseFileMappings(partition);
          if (!bootstrapBaseFileMappings.isEmpty()) {
            return fileSliceStream.map(fileSlice -> addBootstrapBaseFileIfPresent(fileSlice, fileGroupId -> Option.ofNullable(bootstrapBaseFileMappings.get(fileGroupId))));
          }
        }
        return fileSliceStream;
      } catch (IOException e) {
        throw new HoodieIOException("Failed to fetch all files in partition " + partition, e);
      }
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
        Stream<FileSlice> fileSlices = tableVersion8AndAbove()
            ? this.filterUncommittedFiles(fs.get(), true)
            : this.filterBaseFileAfterPendingCompaction(fs.get(), true);

        return Option.ofNullable(fileSlices
            .map(this::addBootstrapBaseFileIfPresent)
            .findFirst()
            .orElse(null)
        );
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
        Stream<Option<FileSlice>> fileSliceOpts;
        if (tableVersion8AndAbove()) {
          fileSliceOpts = allFileSliceStream.map(this::getLatestFileSliceFilteringUncommittedFiles);
        } else {
          fileSliceOpts = allFileSliceStream
              .map(sliceStream -> sliceStream.flatMap(slice -> this.filterBaseFileAfterPendingCompaction(slice, false)))
              .map(sliceStream -> Option.fromJavaOptional(sliceStream.findFirst()));
        }
        return fileSliceOpts.filter(Option::isPresent).map(Option::get)
            .map(this::addBootstrapBaseFileIfPresent);
      } else {
        Predicate<FileSlice> sliceFilter = (slice) -> !isPendingCompactionScheduledForFileId(slice.getFileGroupId())
            && !slice.isEmpty();
        return allFileSliceStream
            .map(sliceStream -> tableVersion8AndAbove()
                ? getLatestFileSliceFilteringUncommittedFiles(sliceStream.filter(sliceFilter))
                : Option.fromJavaOptional(sliceStream.filter(sliceFilter).findFirst()))
            .filter(Option::isPresent).map(Option::get).map(this::addBootstrapBaseFileIfPresent);
      }
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Looks for the latest file slice that is not empty after filtering out the uncommitted files.
   *
   * <p>Note: Checks from the latest file slice first to improve the efficiency. There is no need to check
   * every file slice, the uncommitted files only exist in the latest file slice basically.
   */
  private Option<FileSlice> getLatestFileSliceFilteringUncommittedFiles(Stream<FileSlice> fileSlices) {
    return Option.fromJavaOptional(fileSlices.flatMap(fileSlice -> filterUncommittedFiles(fileSlice, false)).findFirst());
  }

  @Override
  public final Map<String, Stream<FileSlice>> getAllLatestFileSlicesBeforeOrOn(String maxCommitTime) {
    try {
      readLock.lock();
      List<String> formattedPartitionList = ensureAllPartitionsLoadedCorrectly();
      return formattedPartitionList.stream().collect(Collectors.toMap(
          Function.identity(),
          partitionPath -> fetchAllStoredFileGroups(partitionPath)
              .filter(slice -> !isFileGroupReplacedBeforeOrOn(slice.getFileGroupId(), maxCommitTime))
              .map(fg -> fg.getAllFileSlicesBeforeOn(maxCommitTime))
              .map(sliceStream -> tableVersion8AndAbove()
                  ? getLatestFileSliceFilteringUncommittedFiles(sliceStream)
                  : Option.fromJavaOptional(sliceStream.flatMap(slice ->
                  this.filterBaseFileAfterPendingCompaction(slice, false)).findFirst()))
              .filter(Option::isPresent).map(Option::get)
              .map(this::addBootstrapBaseFileIfPresent)
      ));
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
              fileSlice = Option.of(fetchMergedFileSlice(fileGroup, tableVersion8AndAbove()
                      ? filterUncommittedLogs(fileSlice.get()) : fileSlice.get())
              );
            }
            return fileSlice;
          }).filter(Option::isPresent).map(Option::get).map(this::addBootstrapBaseFileIfPresent);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Option<FileSlice> getLatestMergedFileSliceBeforeOrOn(String partitionStr, String maxInstantTime, String fileId) {
    try {
      readLock.lock();
      return Option.fromJavaOptional(getLatestMergedFileSlicesBeforeOrOn(partitionStr, maxInstantTime)
          .filter(fileSlice -> fileSlice.getFileId().equals(fileId))
          .findFirst());
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Stream all "merged" file-slices before on an instant time
   * for a MERGE_ON_READ table with index that can index log files(which means it writes pure logs first).
   *
   * <p>In streaming read scenario, in order for better reading efficiency, the user can choose to skip the
   * base files that are produced by compaction. That is to say, we allow the users to consumer only from
   * these partitioned log files, these log files keep the record sequence just like the normal message queue.
   *
   * <p>NOTE: only local view is supported.
   *
   * @param partitionStr   Partition Path
   * @param maxInstantTime Max Instant Time
   */
  public final Stream<FileSlice> getAllLogsMergedFileSliceBeforeOrOn(String partitionStr, String maxInstantTime) {
    try {
      readLock.lock();
      String partition = formatPartitionKey(partitionStr);
      ensurePartitionLoadedCorrectly(partition);
      return fetchAllStoredFileGroups(partition)
          .filter(fg -> !isFileGroupReplacedBeforeOrOn(fg.getFileGroupId(), maxInstantTime))
          .map(fileGroup -> fetchAllLogsMergedFileSlice(fileGroup, maxInstantTime))
          .filter(Option::isPresent).map(Option::get)
          .map(slice -> tableVersion8AndAbove() ? filterUncommittedLogs(slice) : slice)
          .map(this::addBootstrapBaseFileIfPresent);
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

  @Override
  public final Stream<HoodieFileGroup> getAllFileGroupsStateless(String partitionStr) {
    String partition = formatPartitionKey(partitionStr);
    if (isPartitionAvailableInStore(partition)) {
      return getAllFileGroups(partition);
    } else {
      try {
        Stream<HoodieFileGroup> fileGroupStream = buildFileGroups(partition, getAllFilesInPartition(partition), visibleCommitsAndCompactionTimeline, true).stream()
            .filter(fg -> !isFileGroupReplaced(fg));
        if (bootstrapIndex.useIndex()) {
          final Map<HoodieFileGroupId, BootstrapBaseFileMapping> bootstrapBaseFileMappings = getBootstrapBaseFileMappings(partition);
          if (!bootstrapBaseFileMappings.isEmpty()) {
            return fileGroupStream.map(fileGroup -> addBootstrapBaseFileIfPresent(fileGroup, fileGroupId -> Option.ofNullable(bootstrapBaseFileMappings.get(fileGroupId))));
          }
        }
        return fileGroupStream;
      } catch (IOException e) {
        throw new HoodieIOException("Failed to fetch all files in partition " + partition, e);
      }
    }
  }

  private Map<HoodieFileGroupId, BootstrapBaseFileMapping> getBootstrapBaseFileMappings(String partition) {
    try (BootstrapIndex.IndexReader reader = bootstrapIndex.createReader()) {
      LOG.info("Bootstrap Index available for partition {}", partition);
      List<BootstrapFileMapping> sourceFileMappings =
          reader.getSourceFileMappingForPartition(partition);
      return sourceFileMappings.stream()
          .map(s -> new BootstrapBaseFileMapping(new HoodieFileGroupId(s.getPartitionPath(),
              s.getFileId()), s.getBootstrapFileStatus())).collect(Collectors.toMap(BootstrapBaseFileMapping::getFileGroupId, s -> s));
    }
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
    String partition = formatPartitionKey(partitionPath);
    if (hasReplacedFilesInPartition(partition)) {
      return getAllFileGroupsIncludingReplaced(partition).filter(fg -> isFileGroupReplacedBeforeOrOn(fg.getFileGroupId(), maxCommitTime));
    }
    return Stream.empty();
  }

  @Override
  public Stream<HoodieFileGroup> getReplacedFileGroupsBefore(String maxCommitTime, String partitionPath) {
    String partition = formatPartitionKey(partitionPath);
    if (hasReplacedFilesInPartition(partition)) {
      return getAllFileGroupsIncludingReplaced(partition).filter(fg -> isFileGroupReplacedBefore(fg.getFileGroupId(), maxCommitTime));
    }
    return Stream.empty();
  }

  @Override
  public Stream<HoodieFileGroup> getReplacedFileGroupsAfterOrOn(String minCommitTime, String partitionPath) {
    String partition = formatPartitionKey(partitionPath);
    if (hasReplacedFilesInPartition(partition)) {
      return getAllFileGroupsIncludingReplaced(partition).filter(fg -> isFileGroupReplacedAfterOrOn(fg.getFileGroupId(), minCommitTime));
    }
    return Stream.empty();
  }

  @Override
  public Stream<HoodieFileGroup> getAllReplacedFileGroups(String partitionPath) {
    String partition = formatPartitionKey(partitionPath);
    if (hasReplacedFilesInPartition(partition)) {
      return getAllFileGroupsIncludingReplaced(partition).filter(fg -> isFileGroupReplaced(fg.getFileGroupId()));
    }
    return Stream.empty();
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
   * Check if there is a bootstrap base file present for this file.
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
   * Returns whether there are replaced files within the given partition.
   */
  protected abstract boolean hasReplacedFilesInPartition(String partitionPath);

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
        .fromJavaOptional(fileGroup.getAllBaseFiles().filter(df -> !isBaseFileDueToPendingCompaction(fileGroup.getPartitionPath(), df) && !isBaseFileDueToPendingClustering(df)).findFirst());
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
   * Returns the file slice with all the file slice log files merged.
   * <p> CAUTION: the method requires that all the file slices must only contain log files.
   *
   * @param fileGroup File Group for which the file slice belongs to
   * @param maxInstantTime The max instant time
   */
  private Option<FileSlice> fetchAllLogsMergedFileSlice(HoodieFileGroup fileGroup, String maxInstantTime) {
    List<FileSlice> fileSlices = fileGroup.getAllFileSlicesBeforeOn(maxInstantTime).collect(Collectors.toList());
    if (fileSlices.size() == 0) {
      return Option.empty();
    }
    if (fileSlices.size() == 1) {
      return Option.of(fileSlices.get(0));
    }
    final FileSlice latestSlice = fileSlices.get(0);
    FileSlice merged = new FileSlice(latestSlice.getPartitionPath(), latestSlice.getBaseInstantTime(),
        latestSlice.getFileId());

    // add log files from the latest slice to the earliest
    fileSlices.forEach(slice -> slice.getLogFiles().forEach(merged::addLogFile));
    return Option.of(merged);
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

    return compareTimestamps(instant, GREATER_THAN, hoodieInstantOption.get().requestedTime());
  }

  private boolean isFileGroupReplacedBeforeOrOn(HoodieFileGroupId fileGroupId, String instant) {
    Option<HoodieInstant> hoodieInstantOption = getReplaceInstant(fileGroupId);
    if (!hoodieInstantOption.isPresent()) {
      return false;
    }

    return compareTimestamps(instant, GREATER_THAN_OR_EQUALS, hoodieInstantOption.get().requestedTime());
  }

  private boolean isFileGroupReplacedAfterOrOn(HoodieFileGroupId fileGroupId, String instant) {
    Option<HoodieInstant> hoodieInstantOption = getReplaceInstant(fileGroupId);
    if (!hoodieInstantOption.isPresent()) {
      return false;
    }

    return compareTimestamps(instant, LESSER_THAN_OR_EQUALS, hoodieInstantOption.get().requestedTime());
  }

  @Override
  public Option<HoodieInstant> getLastInstant() {
    return getTimeline().lastInstant();
  }

  @Override
  public HoodieTimeline getTimeline() {
    return visibleCommitsAndCompactionTimeline;
  }

  /**
   * Syncs the file system view from storage to memory.  Performs complete reset of file-system
   * view. Subsequent partition view calls will load file slices against the latest timeline.
   * <p>
   * NOTE: The logic MUST BE guarded by the write lock.
   */
  @Override
  public void sync() {
    try {
      writeLock.lock();
      HoodieTimeline newTimeline = metaClient.reloadActiveTimeline().filterCompletedOrMajorOrMinorCompactionInstants();
      clear();
      // Initialize with new Hoodie timeline.
      init(metaClient, newTimeline);
    } finally {
      writeLock.unlock();
    }
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
