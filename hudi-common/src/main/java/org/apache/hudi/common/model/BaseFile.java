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

import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents common metadata about base-file.
 * A base file can be Hudi base file or even an external (non-hudi) base file too.
 */
public class BaseFile implements Serializable {

  private static final long serialVersionUID = 1L;

  private transient StoragePathInfo pathInfo;
  private final String fullPath;
  protected final String fileName;
  private long fileLen;

  public BaseFile(BaseFile dataFile) {
    this(dataFile.pathInfo,
        dataFile.fullPath,
        dataFile.getFileName(),
        dataFile.getFileLen());
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

  private BaseFile(StoragePathInfo pathInfo, String fullPath, String fileName, long fileLen) {
    this.pathInfo = pathInfo;
    this.fullPath = fullPath;
    this.fileLen = fileLen;
    this.fileName = fileName;
  }

  public String getPath() {
    return fullPath;
  }

  public StoragePath getStoragePath() {
    if (pathInfo != null) {
      return pathInfo.getPath();
    }
    return new StoragePath(fullPath);
  }

  public String getFileName() {
    return fileName;
  }

  public StoragePathInfo getPathInfo() {
    return pathInfo;
  }

  public long getFileSize() {
    return fileLen;
  }

  public void setFileLen(long fileLen) {
    this.fileLen = fileLen;
  }

  public long getFileLen() {
    return fileLen;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BaseFile dataFile = (BaseFile) o;
    return Objects.equals(fullPath, dataFile.fullPath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fullPath);
  }

  @Override
  public String toString() {
    return "BaseFile{fullPath=" + fullPath + ", fileLen=" + fileLen + '}';
  }

  private static String getFileName(String fullPath) {
    return new StoragePath(fullPath).getName();
  }
}
