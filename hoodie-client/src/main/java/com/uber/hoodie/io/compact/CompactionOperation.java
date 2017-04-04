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

package com.uber.hoodie.io.compact;

import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.table.log.HoodieLogFile;

import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.io.compact.strategy.CompactionStrategy;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Encapsulates all the needed information about a compaction
 * and make a decision whether this compaction is effective or not
 *
 * @see CompactionStrategy
 */
public class CompactionOperation implements Serializable {

  private String dataFileCommitTime;
  private long dataFileSize;
  private List<String> deltaFilePaths;
  private String dataFilePath;
  private String fileId;
  private String partitionPath;
  private Map<String, Object> metrics;

  //Only for serialization/de-serialization
  @Deprecated
  public CompactionOperation() {
  }

  public CompactionOperation(HoodieDataFile dataFile, String partitionPath,
      List<HoodieLogFile> logFiles, HoodieWriteConfig writeConfig) {
    this.dataFilePath = dataFile.getPath();
    this.fileId = dataFile.getFileId();
    this.partitionPath = partitionPath;
    this.dataFileCommitTime = dataFile.getCommitTime();
    this.dataFileSize = dataFile.getFileSize();
    this.deltaFilePaths = logFiles.stream().map(s -> s.getPath().toString()).collect(
        Collectors.toList());
    this.metrics = writeConfig.getCompactionStrategy()
        .captureMetrics(dataFile, partitionPath, logFiles);
  }

  public String getDataFileCommitTime() {
    return dataFileCommitTime;
  }

  public long getDataFileSize() {
    return dataFileSize;
  }

  public List<String> getDeltaFilePaths() {
    return deltaFilePaths;
  }

  public String getDataFilePath() {
    return dataFilePath;
  }

  public String getFileId() {
    return fileId;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public Map<String, Object> getMetrics() {
    return metrics;
  }
}
