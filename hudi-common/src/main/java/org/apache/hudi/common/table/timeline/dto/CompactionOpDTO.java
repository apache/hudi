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

package org.apache.hudi.common.table.timeline.dto;

import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The data transfer object of compaction.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CompactionOpDTO {

  @JsonProperty("baseInstant")
  String baseInstantTime;

  @JsonProperty("compactionInstant")
  String compactionInstantTime;

  @JsonProperty("dataFileInstant")
  private String dataFileCommitTime;

  @JsonProperty("deltaFiles")
  private List<String> deltaFilePaths;

  @JsonProperty("baseFile")
  private String dataFilePath;

  @JsonProperty("id")
  private String fileId;

  @JsonProperty("partition")
  private String partitionPath;

  @JsonProperty("metrics")
  private Map<String, Double> metrics;

  @JsonProperty("bootstrapBaseFile")
  private String bootstrapBaseFile;

  public static CompactionOpDTO fromCompactionOperation(String compactionInstantTime, CompactionOperation op) {
    CompactionOpDTO dto = new CompactionOpDTO();
    dto.fileId = op.getFileId();
    dto.compactionInstantTime = compactionInstantTime;
    dto.baseInstantTime = op.getBaseInstantTime();
    dto.dataFileCommitTime = op.getDataFileCommitTime().orElse(null);
    dto.dataFilePath = op.getDataFileName().orElse(null);
    dto.deltaFilePaths = new ArrayList<>(op.getDeltaFileNames());
    dto.partitionPath = op.getPartitionPath();
    dto.metrics = op.getMetrics() == null ? new HashMap<>() : new HashMap<>(op.getMetrics());
    dto.bootstrapBaseFile = op.getBootstrapFilePath().orElse(null);
    return dto;
  }

  public static Pair<String, CompactionOperation> toCompactionOperation(CompactionOpDTO dto) {
    return Pair.of(dto.compactionInstantTime,
        new CompactionOperation(dto.fileId, dto.partitionPath, dto.baseInstantTime,
            Option.ofNullable(dto.dataFileCommitTime), dto.deltaFilePaths,
            Option.ofNullable(dto.dataFilePath), Option.ofNullable(dto.bootstrapBaseFile), dto.metrics));
  }
}
