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

package org.apache.hudi.common.model;

import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;

import org.apache.hadoop.fs.Path;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Encapsulates all the needed information about a compaction and make a decision whether this compaction is effective
 * or not.
 */
public class CompactionOperation implements Serializable {

  private String baseInstantTime;
  private Option<String> baseFileCommitTime;
  private List<String> deltaFileNames;
  private Option<String> baseFileName;
  private HoodieFileGroupId id;
  private Map<String, Double> metrics;

  // Only for serialization/de-serialization
  @Deprecated
  public CompactionOperation() {}

  public CompactionOperation(String fileId, String partitionPath, String baseInstantTime,
      Option<String> baseFileCommitTime, List<String> deltaFileNames, Option<String> baseFileName,
      Map<String, Double> metrics) {
    this.baseInstantTime = baseInstantTime;
    this.baseFileCommitTime = baseFileCommitTime;
    this.deltaFileNames = deltaFileNames;
    this.baseFileName = baseFileName;
    this.id = new HoodieFileGroupId(partitionPath, fileId);
    this.metrics = metrics;
  }

  public CompactionOperation(Option<HoodieBaseFile> baseFile, String partitionPath, List<HoodieLogFile> logFiles,
      Map<String, Double> metrics) {
    if (baseFile.isPresent()) {
      this.baseInstantTime = baseFile.get().getCommitTime();
      this.baseFileName = Option.of(baseFile.get().getFileName());
      this.id = new HoodieFileGroupId(partitionPath, baseFile.get().getFileId());
      this.baseFileCommitTime = Option.of(baseFile.get().getCommitTime());
    } else {
      assert logFiles.size() > 0;
      this.baseFileName = Option.empty();
      this.baseInstantTime = FSUtils.getBaseCommitTimeFromLogPath(logFiles.get(0).getPath());
      this.id = new HoodieFileGroupId(partitionPath, FSUtils.getFileIdFromLogPath(logFiles.get(0).getPath()));
      this.baseFileCommitTime = Option.empty();
    }

    this.deltaFileNames = logFiles.stream().map(s -> s.getPath().getName()).collect(Collectors.toList());
    this.metrics = metrics;
  }

  public String getBaseInstantTime() {
    return baseInstantTime;
  }

  public Option<String> getBaseFileCommitTime() {
    return baseFileCommitTime;
  }

  public List<String> getDeltaFileNames() {
    return deltaFileNames;
  }

  public Option<String> getBaseFileName() {
    return baseFileName;
  }

  public String getFileId() {
    return id.getFileId();
  }

  public String getPartitionPath() {
    return id.getPartitionPath();
  }

  public Map<String, Double> getMetrics() {
    return metrics;
  }

  public HoodieFileGroupId getFileGroupId() {
    return id;
  }

  public Option<HoodieBaseFile> getBaseFile(String basePath, String partitionPath) {
    Path dirPath = FSUtils.getPartitionPath(basePath, partitionPath);
    return baseFileName.map(bf -> new HoodieBaseFile(new Path(dirPath, bf).toString()));
  }

  /**
   * Convert Avro generated Compaction operation to POJO for Spark RDD operation.
   * 
   * @param operation Hoodie Compaction Operation
   * @return
   */
  public static CompactionOperation convertFromAvroRecordInstance(HoodieCompactionOperation operation) {
    CompactionOperation op = new CompactionOperation();
    op.baseInstantTime = operation.getBaseInstantTime();
    op.baseFileName = Option.ofNullable(operation.getBaseFilePath());
    op.baseFileCommitTime = op.baseFileName.map(p -> FSUtils.getCommitTime(new Path(p).getName()));
    op.deltaFileNames = new ArrayList<>(operation.getDeltaFilePaths());
    op.id = new HoodieFileGroupId(operation.getPartitionPath(), operation.getFileId());
    op.metrics = operation.getMetrics() == null ? new HashMap<>() : new HashMap<>(operation.getMetrics());
    return op;
  }

  @Override
  public String toString() {
    return "CompactionOperation{baseInstantTime='" + baseInstantTime + '\'' + ", baseFileCommitTime="
        + baseFileCommitTime + ", deltaFileNames=" + deltaFileNames + ", baseFileName=" + baseFileName + ", id='" + id
        + '\'' + ", metrics=" + metrics + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CompactionOperation operation = (CompactionOperation) o;
    return Objects.equals(baseInstantTime, operation.baseInstantTime)
        && Objects.equals(baseFileCommitTime, operation.baseFileCommitTime)
        && Objects.equals(deltaFileNames, operation.deltaFileNames)
        && Objects.equals(baseFileName, operation.baseFileName) && Objects.equals(id, operation.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(baseInstantTime, id);
  }
}
