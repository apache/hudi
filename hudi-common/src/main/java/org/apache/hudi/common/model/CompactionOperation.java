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
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.storage.StoragePath;

import lombok.Getter;

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

  @Getter
  private String baseInstantTime;
  @Getter
  private Option<String> dataFileCommitTime;
  @Getter
  private List<String> deltaFileNames;
  @Getter
  private Option<String> dataFileName;
  private HoodieFileGroupId id;
  @Getter
  private Map<String, Double> metrics;
  @Getter
  private Option<String> bootstrapFilePath;

  // Only for serialization/de-serialization
  @Deprecated
  public CompactionOperation() {
  }

  public CompactionOperation(String fileId, String partitionPath, String baseInstantTime,
                             Option<String> dataFileCommitTime, List<String> deltaFileNames, Option<String> dataFileName,
                             Option<String> bootstrapFilePath, Map<String, Double> metrics) {
    this.baseInstantTime = baseInstantTime;
    this.dataFileCommitTime = dataFileCommitTime;
    this.deltaFileNames = deltaFileNames;
    this.dataFileName = dataFileName;
    this.bootstrapFilePath = bootstrapFilePath;
    this.id = new HoodieFileGroupId(partitionPath, fileId);
    this.metrics = metrics;
  }

  public CompactionOperation(Option<HoodieBaseFile> dataFile, String partitionPath, List<HoodieLogFile> logFiles,
      Map<String, Double> metrics) {
    ValidationUtils.checkArgument(!logFiles.isEmpty(), "log files should not be empty.");
    if (dataFile.isPresent()) {
      this.baseInstantTime = dataFile.get().getCommitTime();
      this.dataFileName = Option.of(dataFile.get().getFileName());
      this.id = new HoodieFileGroupId(partitionPath, dataFile.get().getFileId());
      this.dataFileCommitTime = Option.of(dataFile.get().getCommitTime());
      this.bootstrapFilePath = dataFile.get().getBootstrapBaseFile().map(BaseFile::getPath);
    } else {
      this.dataFileName = Option.empty();
      this.baseInstantTime = logFiles.get(0).getDeltaCommitTime();
      this.id = new HoodieFileGroupId(partitionPath, logFiles.get(0).getFileId());
      this.dataFileCommitTime = Option.empty();
      this.bootstrapFilePath = Option.empty();
    }
    this.deltaFileNames = logFiles.stream().map(s -> s.getPath().getName()).collect(Collectors.toList());
    this.metrics = metrics;
  }

  public String getFileId() {
    return id.getFileId();
  }

  public String getPartitionPath() {
    return id.getPartitionPath();
  }

  public HoodieFileGroupId getFileGroupId() {
    return id;
  }

  public Option<HoodieBaseFile> getBaseFile(String basePath, String partitionPath) {
    Option<BaseFile> externalBaseFile = bootstrapFilePath.map(BaseFile::new);
    StoragePath dirPath = FSUtils.constructAbsolutePath(basePath, partitionPath);
    return dataFileName.map(df -> {
      return externalBaseFile.map(ext -> new HoodieBaseFile(new StoragePath(dirPath, df).toString(), ext))
          .orElseGet(() -> new HoodieBaseFile(new StoragePath(dirPath, df).toString()));
    });
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
    op.dataFileName = Option.ofNullable(operation.getDataFilePath());
    op.dataFileCommitTime = op.dataFileName.map(p -> FSUtils.getCommitTime(new StoragePath(p).getName()));
    op.deltaFileNames = new ArrayList<>(operation.getDeltaFilePaths());
    op.id = new HoodieFileGroupId(operation.getPartitionPath(), operation.getFileId());
    op.metrics = operation.getMetrics() == null ? new HashMap<>() : new HashMap<>(operation.getMetrics());
    op.bootstrapFilePath = Option.ofNullable(operation.getBootstrapFilePath());
    return op;
  }

  @Override
  public String toString() {
    return "CompactionOperation{baseInstantTime='" + baseInstantTime + '\'' + ", dataFileCommitTime="
        + dataFileCommitTime + ", deltaFileNames=" + deltaFileNames + ", dataFileName=" + dataFileName + ", id='" + id
        + '\'' + ", metrics=" + metrics + ", bootstrapFilePath=" + bootstrapFilePath + '}';
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
        && Objects.equals(deltaFileNames, operation.deltaFileNames)
        && Objects.equals(dataFileName, operation.dataFileName) && Objects.equals(id, operation.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(baseInstantTime, id);
  }
}
