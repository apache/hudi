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

import org.apache.hudi.exception.HoodieException;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;

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
  @JsonProperty("accessTime")
  long accessTime;
  @JsonProperty("permission")
  FSPermissionDTO permission;
  @JsonProperty("owner")
  String owner;
  @JsonProperty("group")
  String group;
  @JsonProperty("symlink")
  FilePathDTO symlink;

  public static FileStatusDTO fromFileStatus(FileStatus fileStatus) {
    if (null == fileStatus) {
      return null;
    }

    FileStatusDTO dto = new FileStatusDTO();
    try {
      dto.path = FilePathDTO.fromPath(fileStatus.getPath());
      dto.length = fileStatus.getLen();
      dto.isdir = fileStatus.isDirectory();
      dto.blockReplication = fileStatus.getReplication();
      dto.blocksize = fileStatus.getBlockSize();
      dto.modificationTime = fileStatus.getModificationTime();
      dto.accessTime = fileStatus.getAccessTime();
      dto.symlink = fileStatus.isSymlink() ? FilePathDTO.fromPath(fileStatus.getSymlink()) : null;
      safeReadAndSetMetadata(dto, fileStatus);
    } catch (IOException ioe) {
      throw new HoodieException(ioe);
    }
    return dto;
  }

  /**
   * Used to safely handle FileStatus calls which might fail on some FileSystem implementation.
   * (DeprecatedLocalFileSystem)
   */
  private static void safeReadAndSetMetadata(FileStatusDTO dto, FileStatus fileStatus) {
    try {
      dto.owner = fileStatus.getOwner();
      dto.group = fileStatus.getGroup();
      dto.permission = FSPermissionDTO.fromFsPermission(fileStatus.getPermission());
    } catch (IllegalArgumentException ie) {
      // Deprecated File System (testing) does not work well with this call
      // skipping
    }
  }

  public static FileStatus toFileStatus(FileStatusDTO dto) {
    if (null == dto) {
      return null;
    }

    return new FileStatus(dto.length, dto.isdir, dto.blockReplication, dto.blocksize, dto.modificationTime,
        dto.accessTime, FSPermissionDTO.fromFsPermissionDTO(dto.permission), dto.owner, dto.group,
        FilePathDTO.toPath(dto.symlink), FilePathDTO.toPath(dto.path));
  }
}
