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

import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.table.timeline.dto.ClusteringOpDTO;
import org.apache.hudi.common.table.timeline.dto.CompactionOpDTO;
import org.apache.hudi.common.table.timeline.dto.DTOUtils;
import org.apache.hudi.common.table.timeline.dto.FileGroupDTO;
import org.apache.hudi.common.table.timeline.dto.FileSliceDTO;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.timeline.service.TimelineService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * REST Handler servicing file-slice requests.
 */
public class FileSliceHandler extends Handler {

  public FileSliceHandler(StorageConfiguration<?> conf, TimelineService.Config timelineServiceConfig,
                          FileSystemViewManager viewManager) {
    super(conf, timelineServiceConfig, viewManager);
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

  public List<FileSliceDTO> getLatestMergedFileSlicesBeforeOrOnIncludingInflight(String basePath, String partitionPath,
                                                                                 String maxInstantTime) {
    return viewManager.getFileSystemView(basePath)
        .getLatestMergedFileSlicesBeforeOrOnIncludingInflight(partitionPath, maxInstantTime)
        .map(FileSliceDTO::fromFileSlice).collect(Collectors.toList());
  }

  public List<FileSliceDTO> getLatestMergedFileSliceBeforeOrOn(String basePath, String partitionPath,
                                                               String maxInstantTime, String fileId) {
    return viewManager.getFileSystemView(basePath).getLatestMergedFileSliceBeforeOrOn(partitionPath, maxInstantTime, fileId)
        .map(FileSliceDTO::fromFileSlice).map(Collections::singletonList).orElse(Collections.emptyList());
  }

  public List<FileSliceDTO> getLatestFileSlicesBeforeOrOn(String basePath, String partitionPath, String maxInstantTime,
      boolean includeFileSlicesInPendingCompaction) {
    return viewManager.getFileSystemView(basePath)
        .getLatestFileSlicesBeforeOrOn(partitionPath, maxInstantTime, includeFileSlicesInPendingCompaction)
        .map(FileSliceDTO::fromFileSlice).collect(Collectors.toList());
  }

  public Map<String, List<FileSliceDTO>> getAllLatestFileSlicesBeforeOrOn(String basePath, String maxInstantTime) {
    return viewManager.getFileSystemView(basePath)
        .getAllLatestFileSlicesBeforeOrOn(maxInstantTime)
        .entrySet().stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> entry.getValue().map(FileSliceDTO::fromFileSlice).collect(Collectors.toList())
        ));
  }

  public List<FileSliceDTO> getLatestUnCompactedFileSlices(String basePath, String partitionPath) {
    return viewManager.getFileSystemView(basePath).getLatestUnCompactedFileSlices(partitionPath)
        .map(FileSliceDTO::fromFileSlice).collect(Collectors.toList());
  }

  public List<FileSliceDTO> getLatestFileSlices(String basePath, String partitionPath) {
    return viewManager.getFileSystemView(basePath).getLatestFileSlices(partitionPath).map(FileSliceDTO::fromFileSlice)
        .collect(Collectors.toList());
  }

  public List<FileSliceDTO> getLatestFileSlicesIncludingInflight(String basePath, String partitionPath) {
    return viewManager.getFileSystemView(basePath).getLatestFileSlicesIncludingInflight(partitionPath).map(FileSliceDTO::fromFileSlice)
        .collect(Collectors.toList());
  }

  public List<FileSliceDTO> getLatestFileSlicesStateless(String basePath, String partitionPath) {
    return viewManager.getFileSystemView(basePath).getLatestFileSlicesStateless(partitionPath).map(FileSliceDTO::fromFileSlice)
        .collect(Collectors.toList());
  }

  public List<FileSliceDTO> getLatestFileSlice(String basePath, String partitionPath, String fileId) {
    return viewManager.getFileSystemView(basePath).getLatestFileSlice(partitionPath, fileId)
        .map(FileSliceDTO::fromFileSlice).map(Arrays::asList).orElse(Collections.emptyList());
  }

  public List<CompactionOpDTO> getPendingCompactionOperations(String basePath) {
    return viewManager.getFileSystemView(basePath).getPendingCompactionOperations()
        .map(instantOp -> CompactionOpDTO.fromCompactionOperation(instantOp.getKey(), instantOp.getValue()))
        .collect(Collectors.toList());
  }

  public List<CompactionOpDTO> getPendingLogCompactionOperations(String basePath) {
    return viewManager.getFileSystemView(basePath).getPendingLogCompactionOperations()
        .map(instantOp -> CompactionOpDTO.fromCompactionOperation(instantOp.getKey(), instantOp.getValue()))
        .collect(Collectors.toList());
  }

  public List<FileGroupDTO> getAllFileGroups(String basePath, String partitionPath) {
    List<HoodieFileGroup> fileGroups =  viewManager.getFileSystemView(basePath).getAllFileGroups(partitionPath)
        .collect(Collectors.toList());
    return DTOUtils.fileGroupDTOsfromFileGroups(fileGroups);
  }

  public List<FileGroupDTO> getAllFileGroupsStateless(String basePath, String partitionPath) {
    List<HoodieFileGroup> fileGroups =  viewManager.getFileSystemView(basePath).getAllFileGroupsStateless(partitionPath)
        .collect(Collectors.toList());
    return DTOUtils.fileGroupDTOsfromFileGroups(fileGroups);
  }

  public List<FileGroupDTO> getReplacedFileGroupsBeforeOrOn(String basePath, String maxCommitTime, String partitionPath) {
    List<HoodieFileGroup> fileGroups =  viewManager.getFileSystemView(basePath).getReplacedFileGroupsBeforeOrOn(maxCommitTime, partitionPath)
        .collect(Collectors.toList());
    return DTOUtils.fileGroupDTOsfromFileGroups(fileGroups);
  }

  public List<FileGroupDTO> getReplacedFileGroupsBefore(String basePath, String maxCommitTime, String partitionPath) {
    List<HoodieFileGroup> fileGroups = viewManager.getFileSystemView(basePath).getReplacedFileGroupsBefore(maxCommitTime, partitionPath)
        .collect(Collectors.toList());
    return DTOUtils.fileGroupDTOsfromFileGroups(fileGroups);
  }

  public List<FileGroupDTO> getReplacedFileGroupsAfterOrOn(String basePath, String minCommitTime, String partitionPath) {
    List<HoodieFileGroup> fileGroups =  viewManager.getFileSystemView(basePath).getReplacedFileGroupsAfterOrOn(minCommitTime, partitionPath)
        .collect(Collectors.toList());
    return DTOUtils.fileGroupDTOsfromFileGroups(fileGroups);
  }
  
  public List<FileGroupDTO> getAllReplacedFileGroups(String basePath, String partitionPath) {
    List<HoodieFileGroup> fileGroups =  viewManager.getFileSystemView(basePath).getAllReplacedFileGroups(partitionPath)
        .collect(Collectors.toList());
    return DTOUtils.fileGroupDTOsfromFileGroups(fileGroups);
  }

  public List<ClusteringOpDTO> getFileGroupsInPendingClustering(String basePath) {
    return viewManager.getFileSystemView(basePath).getFileGroupsInPendingClustering()
        .map(fgInstant -> ClusteringOpDTO.fromClusteringOp(fgInstant.getLeft(), fgInstant.getRight()))
        .collect(Collectors.toList());
  }

  public boolean refreshTable(String basePath) {
    viewManager.clearFileSystemView(basePath);
    return true;
  }

  public boolean loadAllPartitions(String basePath) {
    viewManager.getFileSystemView(basePath).loadAllPartitions();
    return true;
  }

  public boolean loadPartitions(String basePath, List<String> partitionPaths) {
    viewManager.getFileSystemView(basePath).loadPartitions(partitionPaths);
    return true;
  }
}
