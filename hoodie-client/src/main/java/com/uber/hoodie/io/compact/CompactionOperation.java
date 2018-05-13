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
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.io.compact.strategy.CompactionStrategy;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Encapsulates all the needed information about a compaction and make a decision whether this
 * compaction is effective or not
 *
 * @see CompactionStrategy
 */
public class CompactionOperation implements Serializable {

  private Optional<String> dataFileCommitTime;
  private Optional<Long> dataFileSize;
  private List<String> deltaFilePaths;
  private Optional<String> dataFilePath;
  private String fileId;
  private String partitionPath;
  private Map<String, Object> metrics;

  //Only for serialization/de-serialization
  @Deprecated
  public CompactionOperation() {
  }

  public CompactionOperation(Optional<HoodieDataFile> dataFile, String partitionPath,
      List<HoodieLogFile> logFiles, HoodieWriteConfig writeConfig) {
    if (dataFile.isPresent()) {
      this.dataFilePath = Optional.of(dataFile.get().getPath());
      this.fileId = dataFile.get().getFileId();
      this.dataFileCommitTime = Optional.of(dataFile.get().getCommitTime());
      this.dataFileSize = Optional.of(dataFile.get().getFileSize());
    } else {
      assert logFiles.size() > 0;
      this.dataFilePath = Optional.empty();
      this.fileId = FSUtils.getFileIdFromLogPath(logFiles.get(0).getPath());
      this.dataFileCommitTime = Optional.empty();
      this.dataFileSize = Optional.empty();
    }
    this.partitionPath = partitionPath;
    this.deltaFilePaths = logFiles.stream().map(s -> s.getPath().toString())
        .collect(Collectors.toList());
    this.metrics = writeConfig.getCompactionStrategy()
        .captureMetrics(writeConfig, dataFile, partitionPath, logFiles);
  }

  public Optional<String> getDataFileCommitTime() {
    return dataFileCommitTime;
  }

  public Optional<Long> getDataFileSize() {
    return dataFileSize;
  }

  public List<String> getDeltaFilePaths() {
    return deltaFilePaths;
  }

  public Optional<String> getDataFilePath() {
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
