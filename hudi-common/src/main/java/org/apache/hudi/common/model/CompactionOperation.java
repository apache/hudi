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

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Encapsulates all the needed information about a compaction and make a decision whether this compaction is effective
 * or not.
 */
@NoArgsConstructor
@Getter
@EqualsAndHashCode
@ToString
public class CompactionOperation implements Serializable {

  private String baseInstantTime;
  private Option<String> dataFileCommitTime;
  private List<String> deltaFileNames;
  private Option<String> dataFileName;
  private HoodieFileGroupId fileGroupId;
  private Map<String, Double> metrics;
  private Option<String> bootstrapFilePath;

  public CompactionOperation(String fileId, String partitionPath, String baseInstantTime,
                             Option<String> dataFileCommitTime, List<String> deltaFileNames, Option<String> dataFileName,
                             Option<String> bootstrapFilePath, Map<String, Double> metrics) {
    this.baseInstantTime = baseInstantTime;
    this.dataFileCommitTime = dataFileCommitTime;
    this.deltaFileNames = deltaFileNames;
    this.dataFileName = dataFileName;
    this.bootstrapFilePath = bootstrapFilePath;
    this.fileGroupId = new HoodieFileGroupId(partitionPath, fileId);
    this.metrics = metrics;
  }

  public CompactionOperation(Option<HoodieBaseFile> dataFile, String partitionPath, List<HoodieLogFile> logFiles,
      Map<String, Double> metrics) {
    ValidationUtils.checkArgument(!logFiles.isEmpty(), "log files should not be empty.");
    if (dataFile.isPresent()) {
      this.baseInstantTime = dataFile.get().getCommitTime();
      this.dataFileName = Option.of(dataFile.get().getFileName());
      this.fileGroupId = new HoodieFileGroupId(partitionPath, dataFile.get().getFileId());
      this.dataFileCommitTime = Option.of(dataFile.get().getCommitTime());
      this.bootstrapFilePath = dataFile.get().getBootstrapBaseFile().map(BaseFile::getFullPath);
    } else {
      this.dataFileName = Option.empty();
      this.baseInstantTime = logFiles.get(0).getDeltaCommitTime();
      this.fileGroupId = new HoodieFileGroupId(partitionPath, logFiles.get(0).getFileId());
      this.dataFileCommitTime = Option.empty();
      this.bootstrapFilePath = Option.empty();
    }
    this.deltaFileNames = logFiles.stream().map(s -> s.getPath().getName()).collect(Collectors.toList());
    this.metrics = metrics;
  }

  public String getFileId() {
    return fileGroupId.getFileId();
  }

  public String getPartitionPath() {
    return fileGroupId.getPartitionPath();
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
    op.fileGroupId = new HoodieFileGroupId(operation.getPartitionPath(), operation.getFileId());
    op.metrics = operation.getMetrics() == null ? new HashMap<>() : new HashMap<>(operation.getMetrics());
    op.bootstrapFilePath = Option.ofNullable(operation.getBootstrapFilePath());
    return op;
  }
}
