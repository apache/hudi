/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.table.view;

import com.google.common.collect.ImmutableMap;
import com.uber.hoodie.common.model.FileSlice;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieFileGroup;
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.TableFileSystemView;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.CompactionUtils;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.exception.HoodieIOException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Common abstract implementation for multiple TableFileSystemView Implementations. 2 possible
 * implementations are ReadOptimizedView and RealtimeView <p> Concrete implementations extending
 * this abstract class, should only implement getDataFilesInPartition which includes files to be
 * included in the view
 *
 * @see TableFileSystemView
 * @since 0.3.0
 */
public class HoodieTableFileSystemView implements TableFileSystemView,
    TableFileSystemView.ReadOptimizedView,
    TableFileSystemView.RealtimeView, Serializable {

  private static Logger log = LogManager.getLogger(HoodieTableFileSystemView.class);

  protected HoodieTableMetaClient metaClient;
  // This is the commits that will be visible for all views extending this view
  protected HoodieTimeline visibleActiveTimeline;

  // mapping from partition paths to file groups contained within them
  protected HashMap<String, List<HoodieFileGroup>> partitionToFileGroupsMap;
  // mapping from file id to the file group.
  protected HashMap<String, HoodieFileGroup> fileGroupMap;

  /**
   * File Id to pending compaction instant time
   */
  private final Map<String, String> fileIdToPendingCompactionInstantTime;

  /**
   * Create a file system view, as of the given timeline
   */
  public HoodieTableFileSystemView(HoodieTableMetaClient metaClient,
      HoodieTimeline visibleActiveTimeline) {
    this.metaClient = metaClient;
    this.visibleActiveTimeline = visibleActiveTimeline;
    this.fileGroupMap = new HashMap<>();
    this.partitionToFileGroupsMap = new HashMap<>();

    // Build fileId to Pending Compaction Instants
    List<HoodieInstant> pendingCompactionInstants =
        metaClient.getActiveTimeline().filterPendingCompactionTimeline().getInstants().collect(Collectors.toList());
    this.fileIdToPendingCompactionInstantTime = ImmutableMap.copyOf(
        CompactionUtils.getAllPendingCompactionOperations(metaClient).entrySet().stream().map(entry -> {
          return Pair.of(entry.getKey(), entry.getValue().getKey());
        }).collect(Collectors.toMap(Pair::getKey, Pair::getValue)));
  }

  /**
   * Create a file system view, as of the given timeline, with the provided file statuses.
   */
  public HoodieTableFileSystemView(HoodieTableMetaClient metaClient,
      HoodieTimeline visibleActiveTimeline,
      FileStatus[] fileStatuses) {
    this(metaClient, visibleActiveTimeline);
    addFilesToView(fileStatuses);
  }


  /**
   * This method is only used when this object is deserialized in a spark executor.
   *
   * @deprecated
   */
  private void readObject(java.io.ObjectInputStream in)
      throws IOException, ClassNotFoundException {
    in.defaultReadObject();
  }

  private void writeObject(java.io.ObjectOutputStream out)
      throws IOException {
    out.defaultWriteObject();
  }

  /**
   * Adds the provided statuses into the file system view, and also caches it inside this object.
   */
  private List<HoodieFileGroup> addFilesToView(FileStatus[] statuses) {
    Map<Pair<String, String>, List<HoodieDataFile>> dataFiles = convertFileStatusesToDataFiles(
        statuses)
        .collect(Collectors.groupingBy((dataFile) -> {
          String partitionPathStr = FSUtils.getRelativePartitionPath(
              new Path(metaClient.getBasePath()),
              dataFile.getFileStatus().getPath().getParent());
          return Pair.of(partitionPathStr, dataFile.getFileId());
        }));
    Map<Pair<String, String>, List<HoodieLogFile>> logFiles = convertFileStatusesToLogFiles(
        statuses)
        .collect(Collectors.groupingBy((logFile) -> {
          String partitionPathStr = FSUtils.getRelativePartitionPath(
              new Path(metaClient.getBasePath()),
              logFile.getPath().getParent());
          return Pair.of(partitionPathStr, logFile.getFileId());
        }));

    Set<Pair<String, String>> fileIdSet = new HashSet<>(dataFiles.keySet());
    fileIdSet.addAll(logFiles.keySet());

    List<HoodieFileGroup> fileGroups = new ArrayList<>();
    fileIdSet.forEach(pair -> {
      String fileId = pair.getValue();
      HoodieFileGroup group = new HoodieFileGroup(pair.getKey(), fileId, visibleActiveTimeline);
      if (dataFiles.containsKey(pair)) {
        dataFiles.get(pair).forEach(dataFile -> group.addDataFile(dataFile));
      }
      if (logFiles.containsKey(pair)) {
        logFiles.get(pair).forEach(logFile -> group.addLogFile(logFile));
      }
      if (fileIdToPendingCompactionInstantTime.containsKey(fileId)) {
        // If there is no delta-commit after compaction request, this step would ensure a new file-slice appears
        // so that any new ingestion uses the correct base-instant
        group.addNewFileSliceAtInstant(fileIdToPendingCompactionInstantTime.get(fileId));
      }
      fileGroups.add(group);
    });

    // add to the cache.
    fileGroups.forEach(group -> {
      fileGroupMap.put(group.getId(), group);
      if (!partitionToFileGroupsMap.containsKey(group.getPartitionPath())) {
        partitionToFileGroupsMap.put(group.getPartitionPath(), new ArrayList<>());
      }
      partitionToFileGroupsMap.get(group.getPartitionPath()).add(group);
    });

    return fileGroups;
  }

  private Stream<HoodieDataFile> convertFileStatusesToDataFiles(FileStatus[] statuses) {
    Predicate<FileStatus> roFilePredicate = fileStatus ->
        fileStatus.getPath().getName()
            .contains(metaClient.getTableConfig().getROFileFormat().getFileExtension());
    return Arrays.stream(statuses).filter(roFilePredicate).map(HoodieDataFile::new);
  }

  private Stream<HoodieLogFile> convertFileStatusesToLogFiles(FileStatus[] statuses) {
    Predicate<FileStatus> rtFilePredicate = fileStatus ->
        fileStatus.getPath().getName()
            .contains(metaClient.getTableConfig().getRTFileFormat().getFileExtension());
    return Arrays.stream(statuses).filter(rtFilePredicate).map(HoodieLogFile::new);
  }

  /**
   * With async compaction, it is possible to see partial/complete data-files due to inflight-compactions, Ignore
   * those data-files
   *
   * @param dataFile Data File
   */
  private boolean isDataFileDueToPendingCompaction(HoodieDataFile dataFile) {
    String compactionInstantTime = fileIdToPendingCompactionInstantTime.get(dataFile.getFileId());
    if ((null != compactionInstantTime) && dataFile.getCommitTime().equals(compactionInstantTime)) {
      return true;
    }
    return false;
  }

  @Override
  public Stream<HoodieDataFile> getLatestDataFiles(final String partitionPath) {
    return getAllFileGroups(partitionPath)
        .map(fileGroup -> {
          return fileGroup.getAllDataFiles().filter(df -> !isDataFileDueToPendingCompaction(df)).findFirst();
        })
        .filter(Optional::isPresent)
        .map(Optional::get);
  }

  @Override
  public Stream<HoodieDataFile> getLatestDataFiles() {
    return fileGroupMap.values().stream()
        .map(fileGroup -> {
          return fileGroup.getAllDataFiles().filter(df -> !isDataFileDueToPendingCompaction(df)).findFirst();
        })
        .filter(Optional::isPresent)
        .map(Optional::get);
  }

  @Override
  public Stream<HoodieDataFile> getLatestDataFilesBeforeOrOn(String partitionPath,
      String maxCommitTime) {
    return getAllFileGroups(partitionPath)
        .map(fileGroup -> {
          return fileGroup.getAllDataFiles()
              .filter(dataFile ->
                  HoodieTimeline.compareTimestamps(dataFile.getCommitTime(),
                      maxCommitTime,
                      HoodieTimeline.LESSER_OR_EQUAL))
              .filter(df -> !isDataFileDueToPendingCompaction(df))
              .findFirst();
        })
        .filter(Optional::isPresent)
        .map(Optional::get);
  }

  @Override
  public Stream<HoodieDataFile> getLatestDataFilesInRange(List<String> commitsToReturn) {
    return fileGroupMap.values().stream()
        .map(fileGroup -> {
          return fileGroup.getAllDataFiles()
              .filter(dataFile -> commitsToReturn.contains(dataFile.getCommitTime())
                  && !isDataFileDueToPendingCompaction(dataFile))
              .findFirst();
        })
        .filter(Optional::isPresent)
        .map(Optional::get);
  }

  @Override
  public Stream<HoodieDataFile> getAllDataFiles(String partitionPath) {
    return getAllFileGroups(partitionPath)
        .map(fileGroup -> fileGroup.getAllDataFiles())
        .flatMap(dataFileList -> dataFileList)
        .filter(df -> !isDataFileDueToPendingCompaction(df));
  }

  @Override
  public Stream<FileSlice> getLatestFileSlices(String partitionPath) {
    return getAllFileGroups(partitionPath)
        .map(fileGroup -> fileGroup.getLatestFileSlice())
        .filter(Optional::isPresent)
        .map(Optional::get)
        .map(this::filterDataFileAfterPendingCompaction);
  }

  @Override
  public Stream<FileSlice> getLatestUnCompactedFileSlices(String partitionPath) {
    return getAllFileGroups(partitionPath)
        .map(fileGroup -> {
          FileSlice fileSlice = fileGroup.getLatestFileSlice().get();
          // if the file-group is under compaction, pick the latest before compaction instant time.
          if (isFileSliceAfterPendingCompaction(fileSlice)) {
            String compactionInstantTime = fileIdToPendingCompactionInstantTime.get(fileSlice.getFileId());
            return fileGroup.getLatestFileSliceBefore(compactionInstantTime);
          }
          return Optional.of(fileSlice);
        })
        .map(Optional::get);
  }

  /**
   * Returns true if the file-group is under pending-compaction and the file-slice' baseInstant matches
   * compaction Instant
   * @param fileSlice File Slice
   * @return
   */
  private boolean isFileSliceAfterPendingCompaction(FileSlice fileSlice) {
    String compactionInstantTime = fileIdToPendingCompactionInstantTime.get(fileSlice.getFileId());
    if ((null != compactionInstantTime) && fileSlice.getBaseInstantTime().equals(compactionInstantTime)) {
      return true;
    }
    return false;
  }

  /**
   * With async compaction, it is possible to see partial/complete data-files due to inflight-compactions,
   * Ignore those data-files
   * @param fileSlice File Slice
   * @return
   */
  private FileSlice filterDataFileAfterPendingCompaction(FileSlice fileSlice) {
    if (isFileSliceAfterPendingCompaction(fileSlice)) {
      // Data file is filtered out of the file-slice as the corresponding compaction
      // instant not completed yet.
      FileSlice transformed = new FileSlice(fileSlice.getBaseInstantTime(), fileSlice.getFileId());
      fileSlice.getLogFiles().forEach(lf -> transformed.addLogFile(lf));
      return transformed;
    }
    return fileSlice;
  }

  @Override
  public Stream<FileSlice> getLatestFileSlicesBeforeOrOn(String partitionPath,
      String maxCommitTime) {
    return getAllFileGroups(partitionPath)
        .map(fileGroup -> fileGroup.getLatestFileSliceBeforeOrOn(maxCommitTime))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .map(this::filterDataFileAfterPendingCompaction);
  }

  /**
   * Helper to merge last 2 file-slices. These 2 file-slices do not have compaction done yet.
   *
   * @param lastSlice        Latest File slice for a file-group
   * @param penultimateSlice Penultimate file slice for a file-group in commit timeline order
   */
  private static FileSlice mergeCompactionPendingFileSlices(FileSlice lastSlice, FileSlice penultimateSlice) {
    FileSlice merged = new FileSlice(penultimateSlice.getBaseInstantTime(), penultimateSlice.getFileId());
    if (penultimateSlice.getDataFile().isPresent()) {
      merged.setDataFile(penultimateSlice.getDataFile().get());
    }
    // Add Log files from penultimate and last slices
    penultimateSlice.getLogFiles().forEach(lf -> merged.addLogFile(lf));
    lastSlice.getLogFiles().forEach(lf -> merged.addLogFile(lf));
    return merged;
  }

  /**
   * If the file-slice is because of pending compaction instant, this method merges the file-slice with the one before
   * the compaction instant time
   * @param fileGroup File Group for which the file slice belongs to
   * @param fileSlice File Slice which needs to be merged
   * @return
   */
  private FileSlice getMergedFileSlice(HoodieFileGroup fileGroup, FileSlice fileSlice) {
    // if the file-group is under construction, pick the latest before compaction instant time.
    if (fileIdToPendingCompactionInstantTime.containsKey(fileSlice.getFileId())) {
      String compactionInstantTime = fileIdToPendingCompactionInstantTime.get(fileSlice.getFileId());
      if (fileSlice.getBaseInstantTime().equals(compactionInstantTime)) {
        Optional<FileSlice> prevFileSlice = fileGroup.getLatestFileSliceBefore(compactionInstantTime);
        if (prevFileSlice.isPresent()) {
          return mergeCompactionPendingFileSlices(fileSlice, prevFileSlice.get());
        }
      }
    }
    return fileSlice;
  }

  @Override
  public Stream<FileSlice> getLatestMergedFileSlicesBeforeOrOn(String partitionPath, String maxInstantTime) {
    return getAllFileGroups(partitionPath)
        .map(fileGroup -> {
          Optional<FileSlice> fileSlice = fileGroup.getLatestFileSliceBeforeOrOn(maxInstantTime);
          // if the file-group is under construction, pick the latest before compaction instant time.
          if (fileSlice.isPresent()) {
            fileSlice = Optional.of(getMergedFileSlice(fileGroup, fileSlice.get()));
          }
          return fileSlice;
        })
        .filter(Optional::isPresent)
        .map(Optional::get);
  }

  @Override
  public Stream<FileSlice> getLatestFileSliceInRange(List<String> commitsToReturn) {
    return fileGroupMap.values().stream()
        .map(fileGroup -> fileGroup.getLatestFileSliceInRange(commitsToReturn))
        .map(Optional::get);
  }

  @Override
  public Stream<FileSlice> getAllFileSlices(String partitionPath) {
    return getAllFileGroups(partitionPath)
        .map(group -> group.getAllFileSlices())
        .flatMap(sliceList -> sliceList);
  }

  /**
   * Given a partition path, obtain all filegroups within that. All methods, that work at the
   * partition level go through this.
   */
  @Override
  public Stream<HoodieFileGroup> getAllFileGroups(String partitionPathStr) {
    // return any previously fetched groups.
    if (partitionToFileGroupsMap.containsKey(partitionPathStr)) {
      return partitionToFileGroupsMap.get(partitionPathStr).stream();
    }

    try {
      // Create the path if it does not exist already
      Path partitionPath = new Path(metaClient.getBasePath(), partitionPathStr);
      FSUtils.createPathIfNotExists(metaClient.getFs(), partitionPath);
      FileStatus[] statuses = metaClient.getFs().listStatus(partitionPath);
      List<HoodieFileGroup> fileGroups = addFilesToView(statuses);
      return fileGroups.stream();
    } catch (IOException e) {
      throw new HoodieIOException(
          "Failed to list data files in partition " + partitionPathStr, e);
    }
  }
}
