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

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieLogFile;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * The data transfer object of file slice.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FileSliceDTO {

  @JsonProperty("baseFile")
  BaseFileDTO baseFile;
  @JsonProperty("logFiles")
  List<LogFileDTO> logFiles;
  @JsonProperty("partition")
  private String partitionPath;
  @JsonProperty("fileId")
  private String fileId;
  @JsonProperty("baseInstant")
  private String baseInstantTime;

  public static FileSliceDTO fromFileSlice(FileSlice slice) {
    FileSliceDTO dto = new FileSliceDTO();
    dto.partitionPath = slice.getPartitionPath();
    dto.baseInstantTime = slice.getBaseInstantTime();
    dto.fileId = slice.getFileId();
    dto.baseFile = slice.getBaseFile().map(BaseFileDTO::fromHoodieBaseFile).orElse(null);
    dto.logFiles = slice.getLogFiles().map(LogFileDTO::fromHoodieLogFile).collect(Collectors.toList());
    return dto;
  }

  public static FileSlice toFileSlice(FileSliceDTO dto) {
    FileSlice slice = new FileSlice(dto.partitionPath, dto.baseInstantTime, dto.fileId);
    slice.setBaseFile(BaseFileDTO.toHoodieBaseFile(dto.baseFile));
    dto.logFiles.forEach(lf -> slice.addLogFile(LogFileDTO.toHoodieLogFile(lf)));
    return slice;
  }

  public static FileSlice toLSMFileSlice(FileSliceDTO dto) {
    FileSlice slice = new FileSlice(dto.partitionPath, dto.baseInstantTime, dto.fileId);
    TreeSet<HoodieLogFile> logfiles = new TreeSet<>(HoodieLogFile.getReverseLogFileComparator());
    dto.logFiles.forEach(lf -> {
      HoodieLogFile lsmLog = LogFileDTO.toLSMHoodieLogFile(lf);
      logfiles.add(lsmLog);
    });
    slice.resetLogFiles(logfiles);
    return slice;
  }
}
