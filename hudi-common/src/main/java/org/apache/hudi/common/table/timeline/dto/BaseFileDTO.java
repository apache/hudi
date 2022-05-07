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

import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.HoodieBaseFile;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The data transfer object of data file.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class BaseFileDTO {

  @JsonProperty("fileStatus")
  private FileStatusDTO fileStatus;
  @JsonProperty("fullPath")
  private String fullPath;
  @JsonProperty("fileLen")
  private long fileLen;
  @JsonProperty("bootstrapBaseFile")
  private BaseFileDTO bootstrapBaseFile;

  public static HoodieBaseFile toHoodieBaseFile(BaseFileDTO dto) {
    if (null == dto) {
      return null;
    }

    HoodieBaseFile baseFile;
    if (null != dto.fileStatus) {
      baseFile = new HoodieBaseFile(FileStatusDTO.toFileStatus(dto.fileStatus));
    } else {
      baseFile = new HoodieBaseFile(dto.fullPath);
      baseFile.setFileLen(dto.fileLen);
    }

    baseFile.setBootstrapBaseFile(toBaseFile(dto.bootstrapBaseFile));
    return baseFile;
  }

  private static BaseFile toBaseFile(BaseFileDTO dto) {
    if (null == dto) {
      return null;
    }

    BaseFile baseFile;
    if (null != dto.fileStatus) {
      baseFile = new BaseFile(FileStatusDTO.toFileStatus(dto.fileStatus));
    } else {
      baseFile = new BaseFile(dto.fullPath);
      baseFile.setFileLen(dto.fileLen);
    }
    return baseFile;
  }

  public static BaseFileDTO fromHoodieBaseFile(BaseFile baseFile) {
    if (null == baseFile) {
      return null;
    }

    BaseFileDTO dto = new BaseFileDTO();
    dto.fileStatus = FileStatusDTO.fromFileStatus(baseFile.getFileStatus());
    dto.fullPath = baseFile.getPath();
    dto.fileLen = baseFile.getFileLen();
    if (baseFile instanceof HoodieBaseFile) {
      dto.bootstrapBaseFile = ((HoodieBaseFile)baseFile).getBootstrapBaseFile()
          .map(BaseFileDTO::fromHoodieBaseFile).orElse(null);
    }
    return dto;
  }

}
