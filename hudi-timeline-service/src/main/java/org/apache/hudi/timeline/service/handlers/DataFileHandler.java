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

import org.apache.hudi.common.table.timeline.dto.DataFileDTO;
import org.apache.hudi.common.table.view.FileSystemViewManager;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * REST Handler servicing data-file requests.
 */
public class DataFileHandler extends Handler {

  public DataFileHandler(Configuration conf, FileSystemViewManager viewManager) throws IOException {
    super(conf, viewManager);
  }

  public List<DataFileDTO> getLatestDataFiles(String basePath, String partitionPath) {
    return viewManager.getFileSystemView(basePath).getLatestDataFiles(partitionPath)
        .map(DataFileDTO::fromHoodieDataFile).collect(Collectors.toList());
  }

  public List<DataFileDTO> getLatestDataFile(String basePath, String partitionPath, String fileId) {
    return viewManager.getFileSystemView(basePath).getLatestDataFile(partitionPath, fileId)
        .map(DataFileDTO::fromHoodieDataFile).map(dto -> Arrays.asList(dto)).orElse(new ArrayList<>());
  }

  public List<DataFileDTO> getLatestDataFiles(String basePath) {
    return viewManager.getFileSystemView(basePath).getLatestDataFiles().map(DataFileDTO::fromHoodieDataFile)
        .collect(Collectors.toList());
  }

  public List<DataFileDTO> getLatestDataFilesBeforeOrOn(String basePath, String partitionPath, String maxInstantTime) {
    return viewManager.getFileSystemView(basePath).getLatestDataFilesBeforeOrOn(partitionPath, maxInstantTime)
        .map(DataFileDTO::fromHoodieDataFile).collect(Collectors.toList());
  }

  public List<DataFileDTO> getLatestDataFileOn(String basePath, String partitionPath, String instantTime,
      String fileId) {
    List<DataFileDTO> result = new ArrayList<>();
    viewManager.getFileSystemView(basePath).getDataFileOn(partitionPath, instantTime, fileId)
        .map(DataFileDTO::fromHoodieDataFile).ifPresent(result::add);
    return result;
  }

  public List<DataFileDTO> getLatestDataFilesInRange(String basePath, List<String> instants) {
    return viewManager.getFileSystemView(basePath).getLatestDataFilesInRange(instants)
        .map(DataFileDTO::fromHoodieDataFile).collect(Collectors.toList());
  }

  public List<DataFileDTO> getAllDataFiles(String basePath, String partitionPath) {
    return viewManager.getFileSystemView(basePath).getAllDataFiles(partitionPath).map(DataFileDTO::fromHoodieDataFile)
        .collect(Collectors.toList());
  }

}
