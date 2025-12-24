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

import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.lsm.HoodieLSMLogFile;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.fs.FileStatus;

/**
 * The data transfer object of log file.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class LogFileDTO {

  @JsonProperty("fileStatus")
  private FileStatusDTO fileStatus;
  @JsonProperty("path")
  private String pathStr;
  @JsonProperty("len")
  private long fileLen;

  public static HoodieLogFile toHoodieLogFile(LogFileDTO dto) {
    FileStatus status = FileStatusDTO.toFileStatus(dto.fileStatus);
    HoodieLogFile logFile = (status == null) ? new HoodieLogFile(dto.pathStr) : new HoodieLogFile(status);
    logFile.setFileLen(dto.fileLen);
    return logFile;
  }

  public static LogFileDTO fromHoodieLogFile(HoodieLogFile dataFile) {
    LogFileDTO logFile = new LogFileDTO();
    logFile.fileLen = dataFile.getFileSize();
    logFile.pathStr = dataFile.getPath().toString();
    logFile.fileStatus = FileStatusDTO.fromFileStatus(dataFile.getFileStatus());
    return logFile;
  }

  public static HoodieLogFile toLSMHoodieLogFile(LogFileDTO dto) {
    FileStatus status = FileStatusDTO.toFileStatus(dto.fileStatus);
    HoodieLogFile logFile = (status == null) ? new HoodieLSMLogFile(dto.pathStr) : new HoodieLSMLogFile(status);
    logFile.setFileLen(dto.fileLen);
    return logFile;
  }
}
