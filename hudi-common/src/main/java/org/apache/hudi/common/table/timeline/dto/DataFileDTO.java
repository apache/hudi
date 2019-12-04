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

import org.apache.hudi.common.model.HoodieDataFile;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The data transfer object of data file.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataFileDTO {

  @JsonProperty("fileStatus")
  private FileStatusDTO fileStatus;
  @JsonProperty("fullPath")
  private String fullPath;
  @JsonProperty("fileLen")
  private long fileLen;

  public static HoodieDataFile toHoodieDataFile(DataFileDTO dto) {
    if (null == dto) {
      return null;
    }

    HoodieDataFile dataFile = null;
    if (null != dto.fileStatus) {
      dataFile = new HoodieDataFile(FileStatusDTO.toFileStatus(dto.fileStatus));
    } else {
      dataFile = new HoodieDataFile(dto.fullPath);
      dataFile.setFileLen(dto.fileLen);
    }
    return dataFile;
  }

  public static DataFileDTO fromHoodieDataFile(HoodieDataFile dataFile) {
    if (null == dataFile) {
      return null;
    }

    DataFileDTO dto = new DataFileDTO();
    dto.fileStatus = FileStatusDTO.fromFileStatus(dataFile.getFileStatus());
    dto.fullPath = dataFile.getPath();
    dto.fileLen = dataFile.getFileLen();
    return dto;
  }

}
