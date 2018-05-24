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

package com.uber.hoodie.common.model;

import com.uber.hoodie.avro.model.HoodieCompactionOperation;
import com.uber.hoodie.common.util.FSUtils;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Encapsulates all the needed information about a compaction and make a decision whether this
 * compaction is effective or not
 *
 */
public class CompactionOperation implements Serializable {

  private String baseInstantTime;
  private Optional<String> dataFileCommitTime;
  private List<String> deltaFilePaths;
  private Optional<String> dataFilePath;
  private String fileId;
  private String partitionPath;
  private Map<String, Double> metrics;

  //Only for serialization/de-serialization
  @Deprecated
  public CompactionOperation() {
  }

  public CompactionOperation(Optional<HoodieDataFile> dataFile, String partitionPath,
      List<HoodieLogFile> logFiles, Map<String, Double> metrics) {
    if (dataFile.isPresent()) {
      this.baseInstantTime = dataFile.get().getCommitTime();
      this.dataFilePath = Optional.of(dataFile.get().getPath());
      this.fileId = dataFile.get().getFileId();
      this.dataFileCommitTime = Optional.of(dataFile.get().getCommitTime());
    } else {
      assert logFiles.size() > 0;
      this.dataFilePath = Optional.empty();
      this.baseInstantTime = FSUtils.getBaseCommitTimeFromLogPath(logFiles.get(0).getPath());
      this.fileId = FSUtils.getFileIdFromLogPath(logFiles.get(0).getPath());
      this.dataFileCommitTime = Optional.empty();
    }

    this.partitionPath = partitionPath;
    this.deltaFilePaths = logFiles.stream().map(s -> s.getPath().toString())
        .collect(Collectors.toList());
    this.metrics = metrics;
  }

  public String getBaseInstantTime() {
    return baseInstantTime;
  }

  public Optional<String> getDataFileCommitTime() {
    return dataFileCommitTime;
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

  public Map<String, Double> getMetrics() {
    return metrics;
  }

  /**
   * Convert Avro generated Compaction operation to POJO for Spark RDD operation
   * @param operation Hoodie Compaction Operation
   * @return
   */
  public static CompactionOperation convertFromAvroRecordInstance(HoodieCompactionOperation operation) {
    CompactionOperation op = new CompactionOperation();
    op.baseInstantTime = operation.getBaseInstantTime();
    op.dataFilePath = Optional.ofNullable(operation.getDataFilePath());
    op.deltaFilePaths = new ArrayList<>(operation.getDeltaFilePaths());
    op.fileId = operation.getFileId();
    op.metrics = new HashMap<>(operation.getMetrics());
    op.partitionPath = operation.getPartitionPath();
    return op;
  }
}
