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

package org.apache.hudi.timeline.service.handlers;

import org.apache.hudi.common.table.timeline.dto.ClusteringOpDTO;
import org.apache.hudi.common.table.timeline.dto.CompactionOpDTO;
import org.apache.hudi.common.table.timeline.dto.FileGroupDTO;
import org.apache.hudi.common.table.timeline.dto.FileSliceDTO;
import org.apache.hudi.common.table.timeline.dto.SecondaryIndexBaseFilesDTO;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.timeline.service.TimelineService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * REST Handler servicing file-slice requests.
 */
public class FileSliceHandler extends Handler {

  public FileSliceHandler(Configuration conf, TimelineService.Config timelineServiceConfig,
                          FileSystem fileSystem, FileSystemViewManager viewManager) throws IOException {
    super(conf, timelineServiceConfig, fileSystem, viewManager);
  }

  public List<FileSliceDTO> getAllFileSlices(String basePath, String partitionPath) {
    return viewManager.getFileSystemView(basePath).getAllFileSlices(partitionPath).map(FileSliceDTO::fromFileSlice)
        .collect(Collectors.toList());
  }

  public List<FileSliceDTO> getLatestFileSliceInRange(String basePath, List<String> instantsToReturn) {
    return viewManager.getFileSystemView(basePath).getLatestFileSliceInRange(instantsToReturn)
        .map(FileSliceDTO::fromFileSlice).collect(Collectors.toList());
  }

  public List<FileSliceDTO> getLatestMergedFileSlicesBeforeOrOn(String basePath, String partitionPath,
      String maxInstantTime) {
    return viewManager.getFileSystemView(basePath).getLatestMergedFileSlicesBeforeOrOn(partitionPath, maxInstantTime)
        .map(FileSliceDTO::fromFileSlice).collect(Collectors.toList());
  }

  public List<FileSliceDTO> getLatestFileSlicesBeforeOrOn(String basePath, String partitionPath, String maxInstantTime,
      boolean includeFileSlicesInPendingCompaction) {
    return viewManager.getFileSystemView(basePath)
        .getLatestFileSlicesBeforeOrOn(partitionPath, maxInstantTime, includeFileSlicesInPendingCompaction)
        .map(FileSliceDTO::fromFileSlice).collect(Collectors.toList());
  }

  public List<FileSliceDTO> getLatestUnCompactedFileSlices(String basePath, String partitionPath) {
    return viewManager.getFileSystemView(basePath).getLatestUnCompactedFileSlices(partitionPath)
        .map(FileSliceDTO::fromFileSlice).collect(Collectors.toList());
  }

  public List<FileSliceDTO> getLatestFileSlices(String basePath, String partitionPath) {
    return viewManager.getFileSystemView(basePath).getLatestFileSlices(partitionPath).map(FileSliceDTO::fromFileSlice)
        .collect(Collectors.toList());
  }

  public List<FileSliceDTO> getLatestFileSlice(String basePath, String partitionPath, String fileId) {
    return viewManager.getFileSystemView(basePath).getLatestFileSlice(partitionPath, fileId)
        .map(FileSliceDTO::fromFileSlice).map(Arrays::asList).orElse(new ArrayList<>());
  }

  public List<CompactionOpDTO> getPendingCompactionOperations(String basePath) {
    return viewManager.getFileSystemView(basePath).getPendingCompactionOperations()
        .map(instantOp -> CompactionOpDTO.fromCompactionOperation(instantOp.getKey(), instantOp.getValue()))
        .collect(Collectors.toList());
  }

  public List<FileGroupDTO> getAllFileGroups(String basePath, String partitionPath) {
    return viewManager.getFileSystemView(basePath).getAllFileGroups(partitionPath).map(FileGroupDTO::fromFileGroup)
        .collect(Collectors.toList());
  }

  public List<FileGroupDTO> getReplacedFileGroupsBeforeOrOn(String basePath, String maxCommitTime, String partitionPath) {
    return viewManager.getFileSystemView(basePath).getReplacedFileGroupsBeforeOrOn(maxCommitTime, partitionPath).map(FileGroupDTO::fromFileGroup)
        .collect(Collectors.toList());
  }

  public List<FileGroupDTO> getReplacedFileGroupsBefore(String basePath, String maxCommitTime, String partitionPath) {
    return viewManager.getFileSystemView(basePath).getReplacedFileGroupsBefore(maxCommitTime, partitionPath).map(FileGroupDTO::fromFileGroup)
        .collect(Collectors.toList());
  }

  public List<FileGroupDTO> getAllReplacedFileGroups(String basePath, String partitionPath) {
    return viewManager.getFileSystemView(basePath).getAllReplacedFileGroups(partitionPath).map(FileGroupDTO::fromFileGroup)
        .collect(Collectors.toList());
  }

  public List<ClusteringOpDTO> getFileGroupsInPendingClustering(String basePath) {
    return viewManager.getFileSystemView(basePath).getFileGroupsInPendingClustering()
        .map(fgInstant -> ClusteringOpDTO.fromClusteringOp(fgInstant.getLeft(), fgInstant.getRight()))
        .collect(Collectors.toList());
  }

  public List<SecondaryIndexBaseFilesDTO> getPendingSecondaryIndexFiles(String basePath) {
    return viewManager.getFileSystemView(basePath).getPendingSecondaryIndexBaseFiles()
        .map(p -> SecondaryIndexBaseFilesDTO.fromSecondaryIndexBaseFiles(p.getLeft(), p.getRight()))
        .collect(Collectors.toList());
  }

  public List<SecondaryIndexBaseFilesDTO> getSecondaryIndexFiles(String basePath) {
    return viewManager.getFileSystemView(basePath).getSecondaryIndexBaseFiles()
        .map(p -> SecondaryIndexBaseFilesDTO.fromSecondaryIndexBaseFiles(p.getLeft(), p.getRight()))
        .collect(Collectors.toList());
  }

  public boolean refreshTable(String basePath) {
    viewManager.clearFileSystemView(basePath);
    return true;
  }
}
