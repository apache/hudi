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
import org.apache.hudi.timeline.service.TimelineService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * REST Handler servicing base-file requests.
 */
public class BaseFileHandler extends Handler {

  public BaseFileHandler(Configuration conf, TimelineService.Config timelineServiceConfig,
                         FileSystem fileSystem, FileSystemViewManager viewManager) throws IOException {
    super(conf, timelineServiceConfig, fileSystem, viewManager);
  }

  public List<BaseFileDTO> getLatestDataFiles(String basePath, String partitionPath) {
    return viewManager.getFileSystemView(basePath).getLatestBaseFiles(partitionPath)
        .map(BaseFileDTO::fromHoodieBaseFile).collect(Collectors.toList());
  }

  public List<BaseFileDTO> getLatestDataFile(String basePath, String partitionPath, String fileId) {
    return viewManager.getFileSystemView(basePath).getLatestBaseFile(partitionPath, fileId)
        .map(BaseFileDTO::fromHoodieBaseFile).map(Arrays::asList).orElse(new ArrayList<>());
  }

  public List<BaseFileDTO> getLatestDataFiles(String basePath) {
    return viewManager.getFileSystemView(basePath).getLatestBaseFiles().map(BaseFileDTO::fromHoodieBaseFile)
        .collect(Collectors.toList());
  }

  public List<BaseFileDTO> getLatestDataFilesBeforeOrOn(String basePath, String partitionPath, String maxInstantTime) {
    return viewManager.getFileSystemView(basePath).getLatestBaseFilesBeforeOrOn(partitionPath, maxInstantTime)
        .map(BaseFileDTO::fromHoodieBaseFile).collect(Collectors.toList());
  }

  public List<BaseFileDTO> getLatestDataFileOn(String basePath, String partitionPath, String instantTime,
                                               String fileId) {
    List<BaseFileDTO> result = new ArrayList<>();
    viewManager.getFileSystemView(basePath).getBaseFileOn(partitionPath, instantTime, fileId)
        .map(BaseFileDTO::fromHoodieBaseFile).ifPresent(result::add);
    return result;
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
