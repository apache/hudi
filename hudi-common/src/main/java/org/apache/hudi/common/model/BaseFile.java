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

import org.apache.hudi.storage.HoodieFileInfo;
import org.apache.hudi.storage.HoodieLocation;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents common metadata about base-file.
 * A base file can be Hudi base file or even an external (non-hudi) base file too.
 */
public class BaseFile implements Serializable {

  private static final long serialVersionUID = 1L;

  private transient HoodieFileInfo fileStatus;
  private final String fullPath;
  protected final String fileName;
  private long fileLen;

  public BaseFile(BaseFile dataFile) {
    this(dataFile.fileStatus,
        dataFile.fullPath,
        dataFile.getFileName(),
        dataFile.getFileLen());
  }

  public BaseFile(HoodieFileInfo fileStatus) {
    this(fileStatus,
        fileStatus.getLocation().toString(),
        fileStatus.getLocation().getName(),
        fileStatus.getLength());
  }

  public BaseFile(String filePath) {
    this(null, filePath, getFileName(filePath), -1);
  }

  private BaseFile(HoodieFileInfo fileStatus, String fullPath, String fileName, long fileLen) {
    this.fileStatus = fileStatus;
    this.fullPath = fullPath;
    this.fileLen = fileLen;
    this.fileName = fileName;
  }

  public String getPath() {
    return fullPath;
  }

  /*
  public Path getHadoopPath() {
    if (fileStatus != null) {
      return fileStatus.getPath();
    }

    return new CachingPath(fullPath);
  }*/

  public String getFileName() {
    return fileName;
  }

  public HoodieFileInfo getFileStatus() {
    return fileStatus;
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
    return new HoodieLocation(fullPath).getName();
  }
}
