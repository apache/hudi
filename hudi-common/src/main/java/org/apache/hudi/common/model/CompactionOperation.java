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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.Option;

/**
 * Encapsulates all the needed information about a compaction and make a decision whether this compaction is effective
 * or not
 *
 */
public class CompactionOperation implements Serializable {

  private String baseInstantTime;
  private Option<String> dataFileCommitTime;
  private List<String> deltaFilePaths;
  private Option<String> dataFilePath;
  private HoodieFileGroupId id;
  private Map<String, Double> metrics;

  // Only for serialization/de-serialization
  @Deprecated
  public CompactionOperation() {}

  public CompactionOperation(String fileId, String partitionPath, String baseInstantTime,
      Option<String> dataFileCommitTime, List<String> deltaFilePaths, Option<String> dataFilePath,
      Map<String, Double> metrics) {
    this.baseInstantTime = baseInstantTime;
    this.dataFileCommitTime = dataFileCommitTime;
    this.deltaFilePaths = deltaFilePaths;
    this.dataFilePath = dataFilePath;
    this.id = new HoodieFileGroupId(partitionPath, fileId);
    this.metrics = metrics;
  }

  public CompactionOperation(Option<HoodieDataFile> dataFile, String partitionPath, List<HoodieLogFile> logFiles,
      Map<String, Double> metrics) {
    if (dataFile.isPresent()) {
      this.baseInstantTime = dataFile.get().getCommitTime();
      this.dataFilePath = Option.of(dataFile.get().getPath());
      this.id = new HoodieFileGroupId(partitionPath, dataFile.get().getFileId());
      this.dataFileCommitTime = Option.of(dataFile.get().getCommitTime());
    } else {
      assert logFiles.size() > 0;
      this.dataFilePath = Option.empty();
      this.baseInstantTime = FSUtils.getBaseCommitTimeFromLogPath(logFiles.get(0).getPath());
      this.id = new HoodieFileGroupId(partitionPath, FSUtils.getFileIdFromLogPath(logFiles.get(0).getPath()));
      this.dataFileCommitTime = Option.empty();
    }

    this.deltaFilePaths = logFiles.stream().map(s -> s.getPath().toString()).collect(Collectors.toList());
    this.metrics = metrics;
  }

  public String getBaseInstantTime() {
    return baseInstantTime;
  }

  public Option<String> getDataFileCommitTime() {
    return dataFileCommitTime;
  }

  public List<String> getDeltaFilePaths() {
    return deltaFilePaths;
  }

  public Option<String> getDataFilePath() {
    return dataFilePath;
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

  public Option<HoodieDataFile> getBaseFile() {
    // TODO: HUDI-130 - Paths return in compaction plan needs to be relative to base-path
    return dataFilePath.map(df -> new HoodieDataFile(df));
  }

  /**
   * Convert Avro generated Compaction operation to POJO for Spark RDD operation
   * 
   * @param operation Hoodie Compaction Operation
   * @return
   */
  public static CompactionOperation convertFromAvroRecordInstance(HoodieCompactionOperation operation) {
    CompactionOperation op = new CompactionOperation();
    op.baseInstantTime = operation.getBaseInstantTime();
    op.dataFilePath = Option.ofNullable(operation.getDataFilePath());
    op.dataFileCommitTime = op.dataFilePath.map(p -> FSUtils.getCommitTime(new Path(p).getName()));
    op.deltaFilePaths = new ArrayList<>(operation.getDeltaFilePaths());
    op.id = new HoodieFileGroupId(operation.getPartitionPath(), operation.getFileId());
    op.metrics = operation.getMetrics() == null ? new HashMap<>() : new HashMap<>(operation.getMetrics());
    return op;
  }

  @Override
  public String toString() {
    return "CompactionOperation{" + "baseInstantTime='" + baseInstantTime + '\'' + ", dataFileCommitTime="
        + dataFileCommitTime + ", deltaFilePaths=" + deltaFilePaths + ", dataFilePath=" + dataFilePath + ", id='" + id
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
        && Objects.equals(dataFileCommitTime, operation.dataFileCommitTime)
        && Objects.equals(deltaFilePaths, operation.deltaFilePaths)
        && Objects.equals(dataFilePath, operation.dataFilePath) && Objects.equals(id, operation.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(baseInstantTime, id);
  }
}
