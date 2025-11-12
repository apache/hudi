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

import org.apache.hudi.common.table.timeline.dto.BaseFileDTO;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.timeline.service.TimelineService;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * REST Handler servicing base-file requests.
 */
public class BaseFileHandler extends Handler {

  public BaseFileHandler(StorageConfiguration<?> conf, TimelineService.Config timelineServiceConfig,
                         FileSystemViewManager viewManager) {
    super(conf, timelineServiceConfig, viewManager);
  }

  public List<BaseFileDTO> getLatestDataFiles(String basePath, String partitionPath) {
    return viewManager.getFileSystemView(basePath).getLatestBaseFiles(partitionPath)
        .map(BaseFileDTO::fromHoodieBaseFile).collect(Collectors.toList());
  }

  public List<BaseFileDTO> getLatestDataFile(String basePath, String partitionPath, String fileId) {
    return viewManager.getFileSystemView(basePath).getLatestBaseFile(partitionPath, fileId)
        .map(BaseFileDTO::fromHoodieBaseFile).map(Collections::singletonList).orElse(Collections.emptyList());
  }

  public List<BaseFileDTO> getLatestDataFiles(String basePath) {
    return viewManager.getFileSystemView(basePath).getLatestBaseFiles().map(BaseFileDTO::fromHoodieBaseFile)
        .collect(Collectors.toList());
  }

  public List<BaseFileDTO> getLatestDataFilesBeforeOrOn(String basePath, String partitionPath, String maxInstantTime) {
    return viewManager.getFileSystemView(basePath).getLatestBaseFilesBeforeOrOn(partitionPath, maxInstantTime)
        .map(BaseFileDTO::fromHoodieBaseFile).collect(Collectors.toList());
  }

  public Map<String, List<BaseFileDTO>> getAllLatestDataFilesBeforeOrOn(String basePath, String maxInstantTime) {
    return viewManager.getFileSystemView(basePath)
        .getAllLatestBaseFilesBeforeOrOn(maxInstantTime)
        .entrySet().stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> entry.getValue().map(BaseFileDTO::fromHoodieBaseFile).collect(Collectors.toList())
        ));
  }

  public List<BaseFileDTO> getLatestDataFileOn(String basePath, String partitionPath, String instantTime,
                                               String fileId) {
    return viewManager.getFileSystemView(basePath).getBaseFileOn(partitionPath, instantTime, fileId)
        .map(BaseFileDTO::fromHoodieBaseFile).map(Collections::singletonList).orElse(Collections.emptyList());
  }

  public List<BaseFileDTO> getLatestDataFilesInRange(String basePath, List<String> instants) {
    return viewManager.getFileSystemView(basePath).getLatestBaseFilesInRange(instants)
        .map(BaseFileDTO::fromHoodieBaseFile).collect(Collectors.toList());
  }

  public List<BaseFileDTO> getAllDataFiles(String basePath, String partitionPath) {
    return viewManager.getFileSystemView(basePath).getAllBaseFiles(partitionPath).map(BaseFileDTO::fromHoodieBaseFile)
        .collect(Collectors.toList());
  }

}
