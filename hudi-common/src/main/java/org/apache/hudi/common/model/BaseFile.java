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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * Represents common metadata about base-file.
 * A base file can be Hudi base file or even an external (non-hudi) base file too.
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@ToString
public class BaseFile implements Serializable {

  private static final long serialVersionUID = 1L;

  @ToString.Exclude
  private transient StoragePathInfo pathInfo;
  @EqualsAndHashCode.Include
  // fullPath of baseFile
  private final String path;
  @ToString.Exclude
  protected final String fileName;
  @Setter
  private long fileSize;

  public BaseFile(BaseFile dataFile) {
    this(dataFile.pathInfo,
        dataFile.path,
        dataFile.getFileName(),
        dataFile.getFileSize());
  }

  public BaseFile(StoragePathInfo pathInfo) {
    this(pathInfo,
        pathInfo.getPath().toString(),
        pathInfo.getPath().getName(),
        pathInfo.getLength());
  }

  public BaseFile(String filePath) {
    this(null, filePath, getFileName(filePath), -1);
  }

  public StoragePath getStoragePath() {
    if (pathInfo != null) {
      return pathInfo.getPath();
    }
    return new StoragePath(path);
  }

  /**
   * Returns the path info of the file, built from the path and the file size when it is not at hand (e.g. after
   * deserialization), or empty when the file size is unknown.
   */
  public Option<StoragePathInfo> getPathInfoWithKnownSize() {
    if (pathInfo != null) {
      return Option.of(pathInfo);
    }
    return fileSize > 0 ? Option.of(new StoragePathInfo(getStoragePath(), fileSize, false, (short) 0, 0, 0)) : Option.empty();
  }

  private static String getFileName(String fullPath) {
    return new StoragePath(fullPath).getName();
  }
}
