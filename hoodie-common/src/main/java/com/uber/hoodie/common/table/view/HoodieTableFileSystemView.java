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

import com.uber.hoodie.common.model.FileSlice;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieFileGroup;
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.TableFileSystemView;
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

  protected HoodieTableMetaClient metaClient;
  // This is the commits that will be visible for all views extending this view
  protected HoodieTimeline visibleActiveTimeline;

  // mapping from partition paths to file groups contained within them
  protected HashMap<String, List<HoodieFileGroup>> partitionToFileGroupsMap;
  // mapping from file id to the file group.
  protected HashMap<String, HoodieFileGroup> fileGroupMap;

  /**
   * Create a file system view, as of the given timeline
   */
  public HoodieTableFileSystemView(HoodieTableMetaClient metaClient,
      HoodieTimeline visibleActiveTimeline) {
    this.metaClient = metaClient;
    this.visibleActiveTimeline = visibleActiveTimeline;
    this.fileGroupMap = new HashMap<>();
    this.partitionToFileGroupsMap = new HashMap<>();
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
    //TODO: vb: Need to check/fix dataFile corresponding to inprogress/failed async compaction do not show up here
    fileIdSet.forEach(pair -> {
      HoodieFileGroup.Builder groupBuilder = new HoodieFileGroup.Builder().withPartitionPath(pair.getKey())
          .withId(pair.getValue()).withTimeline(visibleActiveTimeline);
      if (dataFiles.containsKey(pair)) {
        dataFiles.get(pair).forEach(dataFile -> groupBuilder.withDataFile(dataFile));
      }
      if (logFiles.containsKey(pair)) {
        logFiles.get(pair).forEach(logFile -> groupBuilder.withLogFile(logFile));
      }
      fileGroups.add(groupBuilder.build());
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

  @Override
  public Stream<HoodieDataFile> getLatestDataFiles(final String partitionPath) {
    return getAllFileGroups(partitionPath)
        .map(fileGroup -> fileGroup.getLatestDataFile())
        .filter(dataFileOpt -> dataFileOpt.isPresent())
        .map(Optional::get);
  }

  @Override
  public Stream<HoodieDataFile> getLatestDataFiles() {
    return fileGroupMap.values().stream()
        .map(fileGroup -> fileGroup.getLatestDataFile())
        .filter(dataFileOpt -> dataFileOpt.isPresent())
        .map(Optional::get);
  }

  @Override
  public Stream<HoodieDataFile> getLatestDataFilesBeforeOrOn(String partitionPath,
      String maxCommitTime) {
    return getAllFileGroups(partitionPath)
        .map(fileGroup -> fileGroup.getLatestDataFileBeforeOrOn(maxCommitTime))
        .filter(dataFileOpt -> dataFileOpt.isPresent())
        .map(Optional::get);
  }

  @Override
  public Stream<HoodieDataFile> getLatestDataFilesInRange(List<String> commitsToReturn) {
    return fileGroupMap.values().stream()
        .map(fileGroup -> fileGroup.getLatestDataFileInRange(commitsToReturn))
        .filter(dataFileOpt -> dataFileOpt.isPresent())
        .map(Optional::get);
  }

  @Override
  public Stream<HoodieDataFile> getAllDataFiles(String partitionPath) {
    return getAllFileGroups(partitionPath)
        .map(fileGroup -> fileGroup.getAllDataFiles())
        .flatMap(dataFileList -> dataFileList);
  }

  @Override
  public Stream<FileSlice> getLatestFileSlices(String partitionPath) {
    return getAllFileGroups(partitionPath)
        .map(fileGroup -> fileGroup.getLatestFileSlice())
        .filter(dataFileOpt -> dataFileOpt.isPresent())
        .map(Optional::get);
  }

  @Override
  public Stream<FileSlice> getLatestFileSlicesBeforeOrOn(String partitionPath,
      String maxCommitTime) {
    return getAllFileGroups(partitionPath)
        .map(fileGroup -> fileGroup.getLatestFileSliceBeforeOrOn(maxCommitTime))
        .filter(dataFileOpt -> dataFileOpt.isPresent())
        .map(Optional::get);
  }

  @Override
  public Stream<FileSlice> getLatestFileSliceInRange(List<String> commitsToReturn) {
    return fileGroupMap.values().stream()
        .map(fileGroup -> fileGroup.getLatestFileSliceInRange(commitsToReturn))
        .filter(dataFileOpt -> dataFileOpt.isPresent())
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