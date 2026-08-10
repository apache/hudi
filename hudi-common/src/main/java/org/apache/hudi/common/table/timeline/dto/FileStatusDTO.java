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

import org.apache.hudi.storage.StoragePathInfo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The data transfer object of file status.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FileStatusDTO {

  @JsonProperty("path")
  FilePathDTO path;
  @JsonProperty("length")
  long length;
  @JsonProperty("isdir")
  boolean isdir;
  @JsonProperty("blockReplication")
  short blockReplication;
  @JsonProperty("blocksize")
  long blocksize;
  @JsonProperty("modificationTime")
  long modificationTime;

  public static FileStatusDTO fromStoragePathInfo(StoragePathInfo pathInfo) {
    if (null == pathInfo) {
      return null;
    }

    FileStatusDTO dto = new FileStatusDTO();
    dto.path = FilePathDTO.fromStoragePath(pathInfo.getPath());
    dto.length = pathInfo.getLength();
    dto.blocksize = pathInfo.getBlockSize();
    dto.isdir = pathInfo.isDirectory();
    dto.modificationTime = pathInfo.getModificationTime();

    return dto;
  }

  public static StoragePathInfo toStoragePathInfo(FileStatusDTO dto) {
    if (null == dto) {
      return null;
    }

    return new StoragePathInfo(
        FilePathDTO.toStoragePath(dto.path), dto.length, dto.isdir, dto.blockReplication, dto.blocksize, dto.modificationTime);
  }
}
